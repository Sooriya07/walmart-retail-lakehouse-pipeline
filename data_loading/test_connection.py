import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def main() -> int:
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=env_path)

    database_url = os.getenv("NEON_DATABASE_URL")
    if not database_url:
        print("Connection failed: NEON_DATABASE_URL is missing from .env.")
        return 1

    connection = None
    exit_code = 0
    try:
        connection = psycopg2.connect(
            database_url,
            sslmode="require",
            connect_timeout=10,
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            db_name = cursor.fetchone()[0]

            cursor.execute("SELECT version()")
            result = cursor.fetchone()

            print(f"Database name: {db_name}")
        
        print("Connection succeeded.")
        if result is not None:
            print(f"PostgreSQL version: {result[0]}")
    except psycopg2.Error as error:
        print(f"Connection failed ({type(error).__name__}).")
        exit_code = 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except psycopg2.Error as error:
                print(f"Connection close failed ({type(error).__name__}).")
                exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
