from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="taxi_dwh_etl",
    description="ETL-процесс загрузки данных NYC Taxi Trips в DWH",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["diploma", "etl", "dwh", "nyc_taxi"],
) as dag:

    load_staging = BashOperator(
        task_id="load_staging",
        bash_command="python /opt/airflow/scripts/load_staging.py",
    )

    load_dwh = BashOperator(
        task_id="load_dwh",
        bash_command="python /opt/airflow/scripts/load_dwh.py",
    )

    validate_results = BashOperator(
        task_id="validate_results",
        bash_command="python /opt/airflow/scripts/validate_results.py",
    )

    load_staging >> load_dwh >> validate_results