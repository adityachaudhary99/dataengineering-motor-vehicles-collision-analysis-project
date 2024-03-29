import os
import pandas as pd
import glob

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from pyspark.sql import functions as F

from google.cloud import storage

from datetime import datetime, timedelta

url = 'https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD'

data = 'data/collisions_dataset.csv'
FILE_NAME = 'processed_collisions'

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'raw')


OUTPUT_PATH = f"{AIRFLOW_HOME}/processed/{FILE_NAME}"


def _pre_process_data(csv_file):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    spark_df = spark.read \
                .format('csv') \
                .option('header','true') \
                .option('inferSchema','true') \
                .load(csv_file)

    columns_to_drop = ['LATITUDE','LONGITUDE','LOCATION','CROSS STREET NAME','OFF STREET NAME','CONTRIBUTING FACTOR VEHICLE 2','CONTRIBUTING FACTOR VEHICLE 3','CONTRIBUTING FACTOR VEHICLE 4','CONTRIBUTING FACTOR VEHICLE 5','VEHICLE TYPE CODE 2','VEHICLE TYPE CODE 3','VEHICLE TYPE CODE 4','VEHICLE TYPE CODE 5']

    spark_df = spark_df.drop(*columns_to_drop)

    for column in spark_df.columns:
        spark_df = spark_df \
            .withColumnRenamed(column, column.lower().replace(' ', '_'))
    
    spark_df = spark_df \
                    .withColumnRenamed('on_street_name', 'street_name') \
                    .withColumnRenamed('number_of_persons_injured', 'persons_injured') \
                    .withColumnRenamed('number_of_persons_killed', 'persons_killed') \
                    .withColumnRenamed('number_of_pedestrians_injured', 'pedestrians_injured') \
                    .withColumnRenamed('number_of_pedestrians_killed', 'pedestrians_killed') \
                    .withColumnRenamed('number_of_cyclist_injured', 'cyclists_injured') \
                    .withColumnRenamed('number_of_cyclist_killed', 'cyclists_killed') \
                    .withColumnRenamed('number_of_motorist_injured', 'motorists_injured') \
                    .withColumnRenamed('number_of_motorist_killed', 'motorists_killed') \
                    .withColumnRenamed('contributing_factor_vehicle_1', 'contributing_factor') \
                    .withColumnRenamed('vehicle_type_code_1', 'vehicle_type')

    spark_df = spark_df.withColumn("borough", F.col("borough").cast("string")) \
                    .withColumn("street_name", F.col("street_name").cast("string")) \
                    .withColumn("contributing_factor", F.col("contributing_factor").cast("string")) \
                    .withColumn("vehicle_type", F.col("vehicle_type").cast("string")) \
                    .withColumn('crash_time', F.date_format(F.col('crash_time'), 'HH:mm:ss')) \
                    .withColumn("crash_date", F.to_date(F.col("crash_date"),"MM/dd/yyyy")) \
                    .withColumn('crash_timestamp', F.to_timestamp(F.concat(F.col('crash_date'), F.lit(' '), F.col('crash_time')), 'yyyy-MM-dd HH:mm:ss')) \
                    .withColumn("zip_code", F.col("zip_code").cast("int")) \
                    .withColumn("persons_injured", F.col("persons_injured").cast("int")) \
                    .withColumn("persons_killed", F.col("persons_killed").cast("int")) \
                    .withColumn("pedestrians_injured", F.col("pedestrians_injured").cast("int")) \
                    .withColumn("pedestrians_killed", F.col("pedestrians_killed").cast("int")) \
                    .withColumn("cyclists_injured", F.col("cyclists_injured").cast("int")) \
                    .withColumn("cyclists_killed", F.col("cyclists_killed").cast("int")) \
                    .withColumn("motorists_injured", F.col("motorists_injured").cast("int")) \
                    .withColumn("motorists_killed", F.col("motorists_killed").cast("int")) \
                    .withColumn("collision_id", F.col("collision_id").cast("int"))

    spark_df = spark_df.na.drop()

    spark_df.repartition(1).write.parquet(OUTPUT_PATH, mode='overwrite')

    spark.stop()


def _process_files(**kwargs):
    csv_directory = f"{AIRFLOW_HOME}/data"

    csv_files = [os.path.join(csv_directory, f) for f in os.listdir(csv_directory) if f.endswith('.csv')]

    for csv_file in csv_files:
        _pre_process_data(csv_file)


def _upload_to_gcs(bucket, object_name, local_path):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # https://github.com/googleapis/python-storage/issues/74
    # blob._chunk_size = 8388608  # 1024 * 1024 B * 16 = 8 MB
    # End of Workaround
    assert os.path.isdir(local_path), 'Provide a valid directory path'

    client = storage.Client()
    bucket = client.bucket(bucket)

    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
            _upload_to_gcs(bucket, object_name + "/" + os.path.basename(local_file), local_file)
        else:
            remote_path = os.path.join(object_name, local_file[1 + len(local_path):])

    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id = "pre_process_dag",
    schedule_interval="@once",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"wget {url} -O {AIRFLOW_HOME}/{data}"
    )

    process_files_task = PythonOperator(
        task_id="process_files_task",
        python_callable=_process_files,
        provide_context=True
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=_upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"processed/{FILE_NAME}",
            "local_path": f"{AIRFLOW_HOME}/processed/{FILE_NAME}"
        },
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=BUCKET,
        source_objects=[f"processed/{FILE_NAME}/*.parquet"],
        destination_project_dataset_table='de-capstone-project.raw.pre_processed',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    bq_create_partitioned_table = BigQueryExecuteQueryOperator(
        task_id=f"bq_create_partitioned_table",
        sql='''
        CREATE OR REPLACE TABLE `de-capstone-project.raw.pre_processed_partitioned`
        PARTITION BY DATE_TRUNC(timestamp, MONTH)
        CLUSTER BY borough
        AS SELECT * FROM `de-capstone-project.raw.pre_processed`
        ''',
        use_legacy_sql=False,
        bigquery_conn_id='google_cloud_default'
    )

    clean_up_task = BashOperator(
        task_id="clean_up_raw_task",
        bash_command=f"rm -r {AIRFLOW_HOME}/data/*"
    )


    download_dataset_task >> process_files_task >> local_to_gcs_task >> gcs_to_bq >> bq_create_partitioned_table >> clean_up_task
