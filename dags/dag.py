from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import logging

from datetime import datetime

with DAG(
    'dag',
    start_date=datetime(2023, 4, 2),
    schedule='@daily'
) as dag:
    ls = BashOperator(
        task_id='bash_task',
        bash_command='ls'
    )