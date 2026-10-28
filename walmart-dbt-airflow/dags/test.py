from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time

ws = WorkspaceClient(
    host="https://dbc-09b940ca-9485.cloud.databricks.com",
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