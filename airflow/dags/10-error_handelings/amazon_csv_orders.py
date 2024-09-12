from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from datetime import datetime
from airflow.models import Variable

from includes.data.datasets import (
    FAIL_INGESTION_DATASET,
    FAIL_CLEANING_DATASET,
    FAIL_DBT_TRANSFORM_DATASET,
    FAIL_PUBLISH_DATASET,
    SUCCESS_ERROR_HANDLING_DATASET, 
    FAIL_ERROR_HANDLING_DATASET 
    )

from airflow.datasets.metadata import Metadata # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os, logging 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="etl_error_handling",
    catchup=False,
    tags=["Minio", "Amazon_orders_CSV", "error_handeling"],
    default_args=default_args,
    schedule=(
        FAIL_CLEANING_DATASET | 
        FAIL_INGESTION_DATASET |
        FAIL_DBT_TRANSFORM_DATASET |
        FAIL_PUBLISH_DATASET
    ),
    doc_md="""
    # **Dag_id**: etl_error_handling
    - This DAG moves rejected CSV files to a rejected_csv folder in an S3 bucket whenever a failure occurs during data ingestion, cleaning, transformation, or publishing. 
    - It updates relevant success or failure datasets based on the result.
    """
)

def error_handling():
    @task(task_id='move_rejected_csvs')
    def move_rejected(**kwargs):
        object_name = Variable.get("curent_csv")

        
        s3hook = S3Hook(aws_conn_id='minio_connection')
        
        bucket = os.getenv('QUEUED_BUCKET')
        object_path = f'{object_name}'
        dist_path = f'rejected_csv/{object_name}'
        
        copy = s3hook.copy_object(
            source_bucket_key=object_path,  
            dest_bucket_key=dist_path,      
            source_bucket_name=bucket,      
            dest_bucket_name=bucket          
        )
        
        if copy:
            logging.info(f"Successfully copied {object_path} to {dist_path}")
        else:
            logging.error(f"Failed to copy {object_path} to {dist_path}")
        

        delete = s3hook.delete_objects(
            bucket=bucket,
            keys=object_name
        )
        
        if delete:
            logging.info(f"Successfully deleted {object_name} from {bucket}")
        else:
            logging.error(f"Failed to delete {object_name} from {bucket}")
    
    
    @task(task_id='update_fail_dataset', outlets=[FAIL_ERROR_HANDLING_DATASET],  trigger_rule="all_failed")
    def update_fail_dataset():
        Metadata(FAIL_ERROR_HANDLING_DATASET, {"failed at": {datetime.now()}})
            
    # 1.3.2 update dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_ERROR_HANDLING_DATASET])
    def update_success_dataset():
        Metadata(SUCCESS_ERROR_HANDLING_DATASET, {"succeded at": {datetime.now()}})
    
    
    move_rejected() >> [update_fail_dataset(), update_success_dataset()]
    
error_handling()