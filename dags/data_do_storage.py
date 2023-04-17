from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,

)

import uuid
import logging
import datetime
import os
import glob

import requests
from bs4 import BeautifulSoup

import boto3
from botocore.exceptions import ClientError

# Common settings for your environment
YC_FOLDER_ID = 'b1g1l7pihs57iohepe9q'  # YC catalog to create cluster
YC_SUBNET_ID = 'e9bhc6hgqnagfnesilob'  # YC subnet to create cluster
YC_SA_ID = 'YCAJEk_7yyNCA9n6i8X4sJye8' # YC service account
YC_AZ = 'ru-central1-a'                # YC availability zone

# Settings for S3 buckets
#YC_INPUT_DATA_BUCKET = 'staging'  # YC S3 bucket for input data
#YC_INPUT_FILES_PREFIX = 'sensors-data-part'  # Input CSV files prefix
YC_SOURCE_BUCKET = 'staging'     # YC S3 bucket for pyspark source files
# YC_DP_LOGS_BUCKET = 'airflow-demo-logs'      # YC S3 bucket for Data Proc cluster logs
S3_KEY_ID = Variable.get('S3_KEY_ID')
S3_SECRET_KEY = Variable.get('S3_SECRET_KEY')


def create_connection():
    session = settings.Session()

    ycS3_connection = Connection(
        conn_id='yc-s3',
        conn_type='s3',
        host='https://storage.yandexcloud.net/',
        extra={
            'aws_access_key_id': S3_KEY_ID,
            'aws_secret_access_key': S3_SECRET_KEY,
            'host': 'https://storage.yandexcloud.net/'
        }
    )

    if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
        session.add(ycS3_connection)
        session.commit()



def create_bucket(s3, bucket_name):
    try:    
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True # для чего я его поставил?

def read_data(bucket_name, destination, schema=None):
    response = requests.get('https://datasets.imdbws.com/')
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        file_links = [link.get('href') for link in soup.find_all('a')][1::]

def data_to_storage(
        bucket_name
):

    # Connecting to S3
    session = boto3.session.Session()
    s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=S3_KEY_ID,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name='ru-central1'
    )

    

    with DAG(
            'data_to_storage',
            start_date=days_ago(1),
            catchup=False
    ) as dag:
        
        create_bucket_task = PythonOperator(
            task_id='create_bucket_task',
            python_callable=create_bucket,
            op_kwargs={
                's3': s3,
                'bucket_name': bucket_name
            }
        )

data_to_storage(
    bucket_name='kurkuma'
)