from airflow.decorators import dag, task
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from includes.data.datasets import INFO_FOUND_CSV
from includes.data.utils import get_extra_triggering_run, update_outlet
from includes.batch_processing.utils import get_active_processing_files, add_object_to_active_processing_files
from typing import List
from pendulum import DateTime
from airflow.utils.helpers import chain
import os
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='s3_file_discovery',
    default_args=default_args,
    description='DAG to discover new S3 files and trigger processing',
    schedule_interval=timedelta(minutes=30),
    catchup=False
)
def discovery():
    
    @task
    def get_objects(bucket_name: str, prefix: str) -> List[str]:
        s3_hook = S3Hook(aws_conn_id='minio_connection')
        objects = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        for obj in objects:
            add_object_to_active_processing_files(
                variable_name='amazon_csv_active_files', 
                object_name=obj
            )
        
        return get_active_processing_files(variable_name='amazon_csv_active_files')

    s3_file_sensor = S3KeySensor(
        task_id='s3_file_sensor',
        bucket_key='landing/*.csv',
        wildcard_match=True,
        bucket_name=os.getenv("QUEUED_BUCKET"),
        aws_conn_id='minio_connection',
        timeout=18 * 60 * 60,  # 18 hours
        poke_interval=5 * 60,  # 5 minutes
    )

    @task(task_id='update_dataset', outlets=[INFO_FOUND_CSV])
    def update_dataset(obj: str, **context):
        current_csv_name = obj.split('/')[-1]
        timestamp = datetime.now()

        nessie_branch_prefix = os.getenv("BRANCH_AMAZON_ORDERS_PIPELINE_PREFEX")
        branch_recommended_name = f'{nessie_branch_prefix}_{current_csv_name.replace(".csv", "")}_{timestamp.strftime("%Y%m%d%H%M%S")}'
        queued_bucket = os.getenv("QUEUED_BUCKET")

        spark_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        full_file_path = f"s3a://{queued_bucket}/{obj}"

        spark_scripts_args = f"-o {full_file_path} -b {branch_recommended_name} -t {spark_timestamp}"

        extra = {
            "object_path": obj,
            "branch_name": branch_recommended_name,
            "updated_at": spark_timestamp,
            "spark_jobs_script_args": spark_scripts_args
        }
        
        context["outlet_events"][INFO_FOUND_CSV].extra = extra


    objects_list = get_objects(
        bucket_name=os.getenv("QUEUED_BUCKET"),
        prefix='landing'
    )

    chain(s3_file_sensor, objects_list, update_dataset.expand(obj=objects_list))

discovery()