from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState,RunResultState


@dag
def orchestrate():
    @task
    def ingest_cdc():
        WS = WorkspaceClient()

        job_trigger = WS.jobs.run_now(job_id=1078916565043452)

        while True:

            job_run = WS.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED , RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("job completed successfully")
                    break
                else:
                    raise Exception(f"Job failed with state :{job_run.state.result_state}")
            time.sleep(5)

        return "CDC Ingestion Completed"


    @task.bash
    def clean_target():
        return "rm -rf opt/airflow/wm_project/target && rm -rf opt/airflow/wm_project/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/wm_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt test --select silver_t'
    )

    silver_business = BashOperator(
        task_id='silver_business',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt run --select silver_b'
    )

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt test --select silver_b'
    )

    gold_eph = BashOperator(
        task_id='gold_eph',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt run --select gold/ephemeral'
    )

    gold_dim = BashOperator(
        task_id='gold_dim',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt snapshot'
    )

    gold_fact = BashOperator(
        task_id='gold_fact',
        cwd = '/opt/airflow/wm_project',
        bash_command = 'dbt run --select gold/fact'
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_eph >> gold_dim >> gold_fact  #define the order of execution of the tasks

orchestrate_dag = orchestrate()  #instantiate the DAG