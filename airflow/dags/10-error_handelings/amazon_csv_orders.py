from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from datetime import datetime
from airflow.models import Variable
from includes.data.utils import update_outlet


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
from includes.data.utils import get_extra_triggering_run, update_outlet

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

    @task(task_id = 'retrieve_extra')
    def retrieve_extra(**context)-> dict:
        return get_extra_triggering_run(context)[0]


    
    bucket = os.getenv('QUEUED_BUCKET')
    extra = retrieve_extra()



    @task(task_id = 'move')
    def move(ti=None):

        s3hook = S3Hook(aws_conn_id='minio_connection')
        
        object_path = ti.xcom_pull(task_ids='retrieve_extra')['object_path']
        dist_path = f'rejected_csv/{object_path}'
        
        copy = s3hook.copy_object(
            source_bucket_key=object_path,  
            dest_bucket_key=dist_path,      
            source_bucket_name=bucket,      
            dest_bucket_name=bucket          
        )

        logging.info(f"Successfully copied {object_path}")

        delete = s3hook.delete_objects(
            bucket=bucket,
            keys=object_path
        )
        

        logging.info(f"Successfully deleted {object_path}")
        
        from includes.batch_processing.utils import remove_object_from_active_processing_files
        remove_object_from_active_processing_files(
            'amazon_csv_active_files',
            object_path
        )
    


    @task(task_id = 'update_fail_dataset', outlets=[FAIL_ERROR_HANDLING_DATASET], trigger_rule="one_failed")
    def update_success(extra, **context):
        update_outlet(
            FAIL_ERROR_HANDLING_DATASET, 
            content=extra,
            context=context
        )
    
    @task(task_id = 'update_success_dataset', outlets=[SUCCESS_ERROR_HANDLING_DATASET])
    def update_failed(extra, **context):
        update_outlet(
            SUCCESS_ERROR_HANDLING_DATASET,
            content=extra,
            context=context
        )


    move = move()
    extra >> move
    move >> [update_success(extra), update_failed(extra)]
    
error_handling()