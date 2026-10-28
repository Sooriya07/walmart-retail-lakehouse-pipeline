from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk import WorkspaceClient
import time
import pendulum
import os

@dag(
        dag_id="orchestrator",
        schedule=0 10 * * *,
        catchup=False,
        start_date=pendulum.datetime(year=2026, month=10, day=28, tz="IST")
)
def orchestrate():
    
    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token = os.getenv("DATABRICKS_TOKEN")
        )

        job_trigger = ws.jobs.run_now(job_id=611980659886611)

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            print(f"Job run status: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)

        return "CDC Ingestion Completed" 

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/dbt/target && rm -rf /opt/airflow/dbt/logs"
    
    @task.bash
    def source_freshness():
        return "cd /opt/airflow/dbt && dbt source freshness"


    silver_technical = BashOperator(
        task_id = 'silver_technical',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id = 'silver_technical_tests',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt test --select silver_t'
    )

    silver_business = BashOperator(
        task_id='silver_business',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt run --select silver_b'
    )

    silver_business_tests = BashOperator(
        task_id = 'silver_business_tests',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt test --select silver_b'
    )

    gold_ephemeral = BashOperator(
        task_id = 'gold_ephemeral',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt run --select gold/ephemeral'
    )

    gold_dimensions = BashOperator(
        task_id = 'gold_dimensions',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id = 'gold_facts',
        cwd = '/opt/airflow/dbt',
        bash_command = 'dbt run --select gold/fact'
    )






    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts 

orchestrate_dag = orchestrate()
