from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import logging

from datetime import datetime

with DAG(
    'dag',
    start_date=datetime(2023, 4, 2),
    schedule='@daily'
) as dag:
    def to_log():
        logging.error('AAAAAAAAAAAAAA')

    task=EmptyOperator(task_id='task_id')

    log=PythonOperator(task_id='log_id', python_callable=to_log)