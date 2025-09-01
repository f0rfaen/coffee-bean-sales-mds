from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="dbt_run_sequential_dag",
    default_args=default_args,
    description="Run dbt models sequentially",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt"],
) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="dbt run --project-dir /opt/airflow/transformation --profiles-dir /opt/airflow/transformation --profile dev_docker --select models/staging",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="dbt run --project-dir /opt/airflow/transformation --profiles-dir /opt/airflow/transformation --profile dev_docker --select models/intermediate",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="dbt run --project-dir /opt/airflow/transformation --profiles-dir /opt/airflow/transformation --profile dev_docker --select models/marts",
    )

    dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts