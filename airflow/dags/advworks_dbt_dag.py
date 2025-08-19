import subprocess
from airflow.decorators import dag, task
from datetime import datetime, timedelta

DBT_PATH = "/usr/local/bin/dbt" 
DBT_PROJECT_PATH = "/usr/local/airflow/dbt" 

default_args = {
    'owner': 'manikanta',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="advworks_dbt_pipeline",
    start_date=datetime(2025, 8, 10),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "advworks"]
)
def advworks_dbt_dag():

    @task
    def load_to_snowflake():
        subprocess.run(["python", "/usr/local/airflow/connect.py"], check=True)

    @task()
    def dbt_run(stage):
        subprocess.run(
            [DBT_PATH, "run", "--project-dir", DBT_PROJECT_PATH, "--select", stage],
            check=True
        )

    @task()
    def dbt_test():
        subprocess.run(
            [DBT_PATH, "test", "--project-dir", DBT_PROJECT_PATH],
            check=True
        )

    load = load_to_snowflake()
    stg = dbt_run("staging")
    inter = dbt_run("intermediate")
    marts = dbt_run("marts")
    test = dbt_test()

    load >>stg >> inter >> marts >> test

dag = advworks_dbt_dag()

