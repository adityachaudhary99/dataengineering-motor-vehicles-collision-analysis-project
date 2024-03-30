import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

FILE_NAME = 'processed_collisions'

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'raw')

dag_start_date = datetime(2024, 3, 30) + timedelta(minutes=1)

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id = "gcs_to_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    start_date=dag_start_date,
    catchup=False,
    max_active_runs=1
) as dag:
    
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id='gcs_to_bq_task',
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

    gcs_to_bq_task >> bq_create_partitioned_table >> clean_up_task