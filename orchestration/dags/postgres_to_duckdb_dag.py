from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="postgres_to_duckdb_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule=None,
    tags=["dlt", "postgres", "duckdb"],
)

dag.concurrency = 1

with dag:
    BashOperator(
        task_id="run_postgres_to_duckdb_pipeline",
        bash_command="cd /opt/airflow/ingestion/postgres_to_duckdb_pipeline && python pipeline.py",
    )
