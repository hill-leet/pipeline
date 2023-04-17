import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

from airflow.models import Variable

import os, json
import pyarrow.csv as pv
import pyarrow.parquet as pq
import glob
import boto3
import requests
from bs4 import BeautifulSoup

spark_schemas = {
        'name.basics': types.StructType([
            types.StructField('nconst',types.StringType(),True),
            types.StructField('primaryName',types.StringType(),True),
            types.StructField('birthYear',types.IntegerType(),True),
            types.StructField('deathYear',types.IntegerType(),True),
            types.StructField('primaryProfession',types.StringType(),True),
            types.StructField('knownForTitles',types.StringType(),True)
        ]),
        'title.akas': types.StructType([
            types.StructField('titleId',types.StringType(),True),
            types.StructField('ordering',types.IntegerType(),True),
            types.StructField('title',types.StringType(),True),
            types.StructField('region',types.StringType(),True),
            types.StructField('language',types.StringType(),True),
            types.StructField('types',types.StringType(),True),
            types.StructField('attributes',types.StringType(),True),
            types.StructField('isOriginalTitle',types.IntegerType(),True)
        ]),
        'title.basics': types.StructType([
            types.StructField('tconst',types.StringType(),True),
            types.StructField('titleType',types.StringType(),True),
            types.StructField('primaryTitle',types.StringType(),True),
            types.StructField('originalTitle',types.StringType(),True),
            types.StructField('isAdult',types.IntegerType(),True),
            types.StructField('startYear',types.IntegerType(),True),
            types.StructField('endYear',types.IntegerType(),True),
            types.StructField('runtimeMinutes',types.IntegerType(),True),
            types.StructField('genres',types.StringType(),True)
        ]),
        'title.crew': types.StructType([
            types.StructField('tconst',types.StringType(),True),
            types.StructField('directors',types.StringType(),True),
            types.StructField('writers',types.StringType(),True)
        ]),
        'title.episode': types.StructType([
            types.StructField('tconst',types.StringType(),True),
            types.StructField('parentTconst',types.StringType(),True),
            types.StructField('seasonNumber',types.IntegerType(),True),
            types.StructField('episodeNumber',types.IntegerType(),True)
        ]),
        'title.principals': types.StructType([
            types.StructField('tconst',types.StringType(),True),
            types.StructField('ordering',types.IntegerType(),True),
            types.StructField('nconst',types.StringType(),True),
            types.StructField('category',types.StringType(),True),
            types.StructField('job',types.StringType(),True),
            types.StructField('characters',types.StringType(),True)
        ]),
        'title.rating': types.StructType([
            types.StructField('tconst',types.StringType(),True),
            types.StructField('averageRating',types.DoubleType(),True),
            types.StructField('numVotes',types.IntegerType(),True)
        ]),
    }

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),  
    aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'), 
    region_name='ru-central1'
)

def ingest_data(bucket_name):
    response = requests.get('https://datasets.imdbws.com/')
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        file_links = [link.get('href') for link in soup.find_all('a')][1::]

        for url in file_links:
            file_name = os.path.basename(url)
            file_name_formated = file_name[0:-7]
            spark.sparkContext.addFile(url)
            df = spark.read \
            .option('header', 'true') \
            .csv(pyspark.SparkFiles.get(file_name), sep='\t', schema=spark_schemas[file_name_formated])
            df.write.parquet(f'./data/{file_name_formated}', mode='overwrite')

            local_file = glob.glob(f'./data/{file_name_formated}/*.parquet')
            base_name = os.path.basename(local_file[0])
            s3.create_bucket(Bucket=bucket_name)
            s3.upload_file(f'./data/{file_name_formated}/{base_name}', bucket_name, file_name_formated + '.parquet')

ingest_data(bucket_name='staging')