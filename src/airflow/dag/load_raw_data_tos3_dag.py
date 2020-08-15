from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFilesToS3Operator, CheckS3FileCount


raw_data_bucket = 'covid19_raw_datalake'

default_args = {
    'owner': 'kehinde',
    'start_date': datetime(2020, 08, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('load_raw_data',
          default_args=default_args,
          description='Load data into raw S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Start', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id='Create_raw_datalake',
    bucket_name=raw_data_bucket,
    dag=dag
)

upload_covidus_data = UploadFilesToS3Operator(
    task_id='Upload_covidus_data',
    bucket_name=raw_data_bucket,
    path='/opt/bitnami/sampledata/covidus/',
    dag=dag
)

upload_uscounty_data = UploadFilesToS3Operator(
    task_id='Upload_uscounty_data',
    bucket_name=raw_data_bucket,
    path='/opt/bitnami/sampledata/uscounty/',
    dag=dag
)

check_data_quality = CheckS3FileCount(
    task_id='Check_data_quality',
    bucket_name=raw_data_bucket,
    expected_count=356,
    dag=dag
)

end_operator = DummyOperator(task_id='end', dag=dag)

start_operator >> create_raw_datalake
create_raw_datalake >> upload_covidus_data
create_raw_datalake >> upload_uscounty_data
upload_covidus_data >> check_data_quality
upload_uscounty_data >> check_data_quality
check_data_quality >> end_operator