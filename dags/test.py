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
import datetime
import uuid

S3_KEY_ID = Variable.get('S3_KEY_ID')
S3_SECRET_KEY = Variable.get('S3_SECRET_KEY')

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

# ycSA_connection = Connection(
#     conn_id='yc-airflow-sa',
#     conn_type='yandexcloud',
#     extra={
#         "extra__yandexcloud__public_ssh_key": Variable.get("DP_PUBLIC_SSH_KEY"),
#         "extra__yandexcloud__service_account_json": Variable.get("DP_SA_AUTH_JSON")
#     }
# )

# if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
#     session.add(ycSA_connection)
#     session.commit()

with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        tags=['airflow-demo'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id='b1g1l7pihs57iohepe9q',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Temporary cluster for Spark processing under Airflow orchestration',
        subnet_id='e9bhc6hgqnagfnesilob',
        s3_bucket='staging',
        service_account_id='ajeh79ubefobqrgqoeb9',
        zone='ru-central1-a',
        cluster_image_version='2.0.43',
        enable_ui_proxy=False,
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=10,
        computenode_resource_preset='m2.large',
        computenode_disk_type='network-ssd',
        computenode_disk_size=10,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'],  # Creating lightweight Spark cluster
        datanode_count=0,            # With no data nodes
        connection_id='yc-airflow-sa',
        dag=ingest_dag
)
        
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri='./data_processing.py',
        connection_id='yc-airflow-sa',
        dag=ingest_dag
    )

    create_spark_cluster >> poke_spark_processing