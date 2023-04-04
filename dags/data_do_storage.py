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
import datetime

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

session = settings.Session()

ycS3_connection = Connection(
    conn_id='yc-s3',
    conn_type='s3',
    host='https://storage.yandexcloud.net/',
    extra={
        'aws_access_key_id': Variable.get('S3_KEY_ID'),
        'aws_secret_access_key': Variable.get('S3_SECRET_KEY'),
        'host': 'https://storage.yandexcloud.net/'
    }
)

if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

with DAG(
        'data_to_storage',
        start_date=days_ago(1),
        catchup=False
) as ingest_dag:
    


    
    def test_func():
        print()
        print()
        print('AAAAA')
        print(session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first())

    task1 = PythonOperator(task_id='test_task', python_callable=test_func)