from airflow.decorators import dag, task
from datetime import datetime
import subprocess


DBT_PATH = "/usr/local/bin/dbt" 

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def full_etl_pipeline():

    @task
    def load_to_snowflake():
        # You can call your custom Python function or script here
        subprocess.run(["python", "/usr/local/airflow/connect.py"], check=True)

    @task
    def dbt_run():
        subprocess.run([DBT_PATH, "run", "--project-dir", "/usr/local/airflow/dbt"], check=True)

    @task
    def dbt_test():
        subprocess.run([DBT_PATH, "test", "--project-dir", "/usr/local/airflow/dbt"], check=True)

    @task
    def dbt_snapshot():
        subprocess.run([DBT_PATH, "snapshot", "--project-dir", "/usr/local/airflow/dbt"], check=True)

    @task
    def notify_success():
        print("Full ETL pipeline completed successfully!")

    # Define task execution order
    load = load_to_snowflake()
    run = dbt_run()
    test = dbt_test()
    snapshot = dbt_snapshot()
    notify = notify_success()

    load >> run >> snapshot >> test >> notify

dag = full_etl_pipeline()