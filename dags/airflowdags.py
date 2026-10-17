import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from transforming import (getting_parquet,cleaning_data)


defualt_args = {
    'owner': 'airflow',
    'retries': 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='kafka_pipeline',
    default_args=defualt_args,
    start_date=datetime(2026, 10, 15),
    schedule="@once",
    catchup=False,
)as kafka_streaming_pipeline:

    getting_parquet=PythonOperator(
        task_id='getting_parquet',
        python_callable=getting_parquet,
    )

    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data,
    )

    getting_parquet >> cleaning_data

