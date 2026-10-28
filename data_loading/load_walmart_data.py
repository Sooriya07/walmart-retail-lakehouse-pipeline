import csv
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "walmart_dataset" / "data"
SCHEMA_PATH = PROJECT_DIR / "walmart_dataset" / "ddl" / "walmart_schema.sql"

# Parent tables come before tables that refer to them.
TABLES = [
    ("customers", "customers.csv", "customer_id", (
        "customer_id", "first_name", "last_name", "email", "phone",
        "city", "province", "country", "created_timestamp",
        "updated_timestamp", "is_active",
    )),
    ("stores", "stores.csv", "store_id", (
        "store_id", "store_name", "city", "province", "country",
        "created_timestamp", "updated_timestamp", "is_active",
    )),
    ("products", "products.csv", "product_id", (
        "product_id", "product_name", "category", "brand", "price",
        "created_timestamp", "updated_timestamp", "is_active",
    )),
    ("employees", "employees.csv", "employee_id", (
        "employee_id", "store_id", "first_name", "last_name", "email",
        "job_title", "salary", "created_timestamp", "updated_timestamp",
        "is_active",
    )),
    ("orders", "orders.csv", "order_id", (
        "order_id", "customer_id", "store_id", "order_timestamp",
        "payment_method", "order_status", "total_amount",
        "created_timestamp", "updated_timestamp", "is_active",
    )),
    ("order_items", "order_items.csv", "order_item_id", (
        "order_item_id", "order_id", "product_id", "quantity",
        "unit_price", "line_amount", "created_timestamp",
        "updated_timestamp", "is_active",
    )),
]

FOREIGN_KEYS = [
    ("employees", "store_id", "stores", "store_id"),
    ("orders", "customer_id", "customers", "customer_id"),
    ("orders", "store_id", "stores", "store_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
]


def read_csv_rows(table_name, file_name, expected_columns, primary_key):
    csv_path = DATA_DIR / file_name
    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames != list(expected_columns):
            raise ValueError(
                f"{file_name} columns do not match the {table_name} table."
            )

        rows = []
        for row_number, row in enumerate(reader, start=2):
            cleaned_row = {
                column: value if value not in (None, "") else None
                for column, value in row.items()
            }
            if cleaned_row[primary_key] is None:
                raise ValueError(
                    f"{file_name} has an empty {primary_key} at row {row_number}."
                )
            try:
                int(cleaned_row[primary_key])
            except ValueError as error:
                raise ValueError(
                    f"{file_name} has an invalid {primary_key} at row {row_number}."
                ) from error
            rows.append(cleaned_row)

    return rows


def unique_rows_by_key(rows, primary_key):
    unique_rows = {}
    for row in rows:
        key = int(row[primary_key])
        unique_rows.setdefault(key, row)
    return unique_rows


def check_foreign_keys(cursor, rows, table_name):
    for child_table, child_column, parent_table, parent_key in FOREIGN_KEYS:
        if child_table != table_name:
            continue

        query = sql.SQL("SELECT {} FROM {}").format(
            sql.Identifier(parent_key),
            sql.Identifier(parent_table),
        )
        cursor.execute(query)
        parent_ids = {int(result[0]) for result in cursor.fetchall()}

        missing_ids = {
            int(row[child_column])
            for row in rows
            if row[child_column] is not None
            and int(row[child_column]) not in parent_ids
        }
        if missing_ids:
            examples = ", ".join(str(value) for value in sorted(missing_ids)[:5])
            raise ValueError(
                f"Foreign-key check failed: {table_name}.{child_column} has "
                f"{len(missing_ids)} value(s) missing from "
                f"{parent_table}.{parent_key}. Example IDs: {examples}."
            )


def values_match(csv_value, database_value):
    if csv_value is None or database_value is None:
        return csv_value is None and database_value is None
    if isinstance(database_value, Decimal):
        return Decimal(csv_value) == database_value
    if isinstance(database_value, datetime):
        return datetime.fromisoformat(csv_value) == database_value
    if isinstance(database_value, int):
        return int(csv_value) == database_value
    return csv_value == database_value


def insert_and_verify(cursor, table_name, primary_key, columns, rows):
    expected_rows = unique_rows_by_key(rows, primary_key)
    column_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in columns
    )
    insert_query = sql.SQL(
        "INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING"
    ).format(
        sql.Identifier(table_name),
        column_sql,
        sql.Identifier(primary_key),
    ).as_string(cursor)
    values = [
        tuple(row[column] for column in columns)
        for row in expected_rows.values()
    ]
    if values:
        execute_values(cursor, insert_query, values, page_size=1000)

    key_values = list(expected_rows)
    select_query = sql.SQL("SELECT {} FROM {} WHERE {} = ANY(%s)").format(
        column_sql,
        sql.Identifier(table_name),
        sql.Identifier(primary_key),
    )
    cursor.execute(select_query, (key_values,))
    database_rows = {
        int(result[0]): dict(zip(columns, result))
        for result in cursor.fetchall()
    }

    mismatches = []
    for key, expected in expected_rows.items():
        actual = database_rows.get(key)
        if actual is None:
            mismatches.append((key, ["missing row"]))
            continue
        different_columns = [
            column
            for column in columns
            if not values_match(expected[column], actual[column])
        ]
        if different_columns:
            mismatches.append((key, different_columns))

    if mismatches:
        examples = "; ".join(
            f"{primary_key}={key} ({', '.join(columns)})"
            for key, columns in mismatches[:5]
        )
        raise ValueError(
            f"Verification failed for {table_name}: "
            f"{len(mismatches)} CSV record(s) do not match the database. "
            f"Examples: {examples}."
        )

    count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(
        sql.Identifier(table_name)
    )
    cursor.execute(count_query)
    table_count = cursor.fetchone()[0]

    return {
        "table": table_name,
        "csv_rows": len(rows),
        "unique_rows": len(expected_rows),
        "duplicate_rows": len(rows) - len(expected_rows),
        "matched_rows": len(expected_rows),
        "table_rows": table_count,
    }


def safe_error_message(error, database_url):
    message = str(error)
    if not database_url:
        return message

    secrets = {database_url}
    try:
        password = urlsplit(database_url).password
        if password:
            secrets.add(password)
            secrets.add(unquote(password))
    except ValueError:
        return f"{type(error).__name__}: [connection details redacted]"

    for secret in secrets:
        if secret:
            message = message.replace(secret, "[redacted]")
    return message


def main():
    load_dotenv(PROJECT_DIR / ".env", override=True)
    database_url = os.getenv("NEON_DATABASE_URL")
    if not database_url:
        print("Error: NEON_DATABASE_URL is missing from the project .env file.")
        return 1

    try:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        table_rows = {
            table_name: read_csv_rows(
                table_name, file_name, columns, primary_key
            )
            for table_name, file_name, primary_key, columns in TABLES
        }
    except (OSError, csv.Error, ValueError) as error:
        print(f"Input error: {error}")
        return 1

    connection = None
    summaries = []
    try:
        connection = psycopg2.connect(
            database_url,
            sslmode="require",
            connect_timeout=10,
        )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(schema)
                for table_name, _, primary_key, columns in TABLES:
                    rows = table_rows[table_name]
                    check_foreign_keys(cursor, rows, table_name)
                    summaries.append(
                        insert_and_verify(
                            cursor,
                            table_name,
                            primary_key,
                            columns,
                            rows,
                        )
                    )

        print("Load committed successfully.")
        for summary in summaries:
            print(
                f"{summary['table']}: CSV rows={summary['csv_rows']}, "
                f"unique IDs={summary['unique_rows']}, "
                f"verified={summary['matched_rows']}, "
                f"duplicates skipped={summary['duplicate_rows']}, "
                f"database table rows={summary['table_rows']}"
            )
        return 0
    except (psycopg2.Error, OSError, ValueError) as error:
        print(f"Load failed: {type(error).__name__}: "
              f"{safe_error_message(error, database_url)}")
        print("The transaction was rolled back; no partial load was committed.")
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except psycopg2.Error as error:
                print(
                    f"Connection close failed ({type(error).__name__}): "
                    f"{safe_error_message(error, database_url)}"
                )


if __name__ == "__main__":
    raise SystemExit(main())
