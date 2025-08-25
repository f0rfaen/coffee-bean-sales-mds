
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="postgres_to_duckdb_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["dlt", "postgres", "duckdb"],
) as dag:
    BashOperator(
        task_id="run_postgres_to_duckdb_pipeline",
        bash_command="cd /opt/airflow/ingestion/postgres_to_duckdb_pipeline && python pipeline.py",
    )
