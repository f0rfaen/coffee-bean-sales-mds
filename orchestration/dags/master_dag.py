from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id="master_pipeline_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["master"],
) as dag:

    trigger_dlt = TriggerDagRunOperator(
        task_id="trigger_dlt_pipeline",
        trigger_dag_id="postgres_to_duckdb_pipeline",
        wait_for_completion=True,
    )

    trigger_dbt_run_sequential_dag = TriggerDagRunOperator(
        task_id="dbt_run_sequential_dag",
        trigger_dag_id="dbt_run_sequential_dag",
        wait_for_completion=True,
    )

    trigger_dbt_test = TriggerDagRunOperator(
        task_id="trigger_dbt_test",
        trigger_dag_id="dbt_test_dag",
        wait_for_completion=True,
    )

    # Set dependencies
    trigger_dlt >> trigger_dbt_run_sequential_dag >> trigger_dbt_test
