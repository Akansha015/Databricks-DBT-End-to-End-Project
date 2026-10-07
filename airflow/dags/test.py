import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState,RunResultState

def main():
    workspace_client = WorkspaceClient()
    job_trigger = workspace_client.jobs.run_now(job_id=1078916565043452)

    while True:
        job_run = workspace_client.jobs.get_run(job_trigger.run_id)
        if job_run.state.life_cycle_state in [
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        ]:
            if job_run.state.result_state == RunResultState.SUCCESS:
                print("job completed successfully")
                break
            raise Exception(f"Job failed with state: {job_run.state.result_state}")

        time.sleep(5)


if __name__ == "__main__":
    main()
