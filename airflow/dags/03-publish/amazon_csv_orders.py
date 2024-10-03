from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from includes.data.datasets import SUCCESS_DBT_TRANSFORM_DATASET, SUCCESS_PUBLISH_DATASET, FAIL_PUBLISH_DATASET
from datetime import datetime
from airflow.datasets.metadata import Metadata # type: ignore
from airflow.models import Variable
from includes.data.utils import get_extra_triggering_run, update_outlet
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os, logging, json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="publish_and_move",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie", "PUBLISHING_DATASET", "CSV", "Amazon"],
    default_args=default_args,
    schedule=[SUCCESS_DBT_TRANSFORM_DATASET],
    doc_md="""
    # **Dag_id**: publish_and_move
    This DAG is responsible for publishing transformed datasets by merging them into the main table using Spark, 
    then copying and moving processed CSV files from a "queued" to a "processed" S3 bucket. 
    Finally, the DAG updates success or failure datasets based on the outcome.
    """
)
def publish():
    
    @task(task_id = 'retrieve_extra')
    def retrieve_extra(**context)-> dict:
        return get_extra_triggering_run(context)[0]
    
    extra_python_args = "{{ ti.xcom_pull(task_ids='retrieve_extra')['spark_jobs_script_args'] }}"
    queued_bucket = os.getenv("QUEUED_BUCKET")

    merge_and_publish = SSHSparkOperator(
        task_id='merge_publish',
        ssh_conn_id='sparkSSH',
        application_path='/spark-container/spark/jobs/merge_into_main.py',
        python_args=extra_python_args
    )

    @task(task_id='copy_processed')
    def copy_processed(ti=None):    
        s3hook = S3Hook(aws_conn_id='minio_connection')        

        object_path = ti.xcom_pull(task_ids='retrieve_extra')['object_path']
        
        try:
            copy = s3hook.copy_object(
                source_bucket_key=object_path,  
                dest_bucket_key=f"processed/{object_path}",      
                source_bucket_name=queued_bucket,      
                dest_bucket_name=queued_bucket          
            )

            logging.info(f"Successfully copied {object_path}")
        except Exception as e:  
            logging.error(f"Failed to copy {object_path}: {e}")
            raise


    @task(task_id='delete')
    def delete(ti=None):
        s3hook = S3Hook(aws_conn_id='minio_connection')

        object_path = ti.xcom_pull(task_ids='retrieve_extra')['object_path']
        
        try:
            delete = s3hook.delete_objects(
                bucket=queued_bucket,
                keys=object_path
            )
        
            logging.info(f"Successfully deleted {object_path}")
            
            from includes.batch_processing.utils import remove_object_from_active_processing_files
            
            remove_object_from_active_processing_files(
                'amazon_csv_active_files',
                object_path
            )
            
        except Exception as e:  
            logging.error(f"Failed to delete {object_path}: {e}")
            raise 


    @task(task_id = 'update_fail_dataset', outlets=[FAIL_PUBLISH_DATASET], trigger_rule="one_failed")
    def update_success(extra, **context):
        update_outlet(
            FAIL_PUBLISH_DATASET, 
            content=extra,
            context=context
        )
    
    @task(task_id = 'update_success_dataset', outlets=[SUCCESS_PUBLISH_DATASET])
    def update_failed(extra, **context):
        update_outlet(
            SUCCESS_PUBLISH_DATASET,
            content=extra,
            context=context
        )
    
    
    extra = retrieve_extra()
    copy = copy_processed()
    delete = delete()

    extra >> merge_and_publish
    merge_and_publish >> copy
    copy >> delete
    delete >> [update_success(extra), update_failed(extra)]

publish()