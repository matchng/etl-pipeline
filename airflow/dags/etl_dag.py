from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_pipeline import run_etl, export_sql_query

with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # run once per day
    catchup=False,
    tags=["etl"],
) as dag:

    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl
    )

    export_summary_task = PythonOperator(
        task_id="export_summary",
        python_callable=export_sql_query
    )

    run_etl_task >> export_summary_task