from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from includes.data.datasets import SUCCESS_DBT_TRANSFORM_DATASET, SUCCESS_PUBLISH_DATASET, FAIL_PUBLISH_DATASET
from datetime import datetime
from airflow.datasets.metadata import Metadata # type: ignore

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
)

def publish():
    
    # 3.2 audit transformed data
    merge_and_publish = SSHSparkOperator(
        task_id                 ='merge_publish',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark-container/spark/jobs/merge_into_main.py'
    )
    
    
    @task(task_id='move_processed_csvs')
    def move_processed(**kwargs):
        import logging, os 
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        object_name = kwargs['ti'].xcom_pull(
            dag_id='ingest_amazon_csv_orders', 
            task_ids='pick_minio_object',
            key='current_csv'
        )
        
        s3hook = S3Hook(aws_conn_id='minio_connection')
        
        src_bucket = os.getenv('QUEUED_BUCKET')
        dest_bucket = os.getenv('PROCESSED_BUCKET')
        object_path = f'{src_bucket}/{object_name}'
        dist_path = f'{dest_bucket}/{datetime.now().year()}/{datetime.now().month()}/{datetime.now().day()}/{object_name}'
        
        copy = s3hook.copy_object(
            source_bucket_key=object_path,  
            dest_bucket_key=dist_path,      
            source_bucket_name=src_bucket,      
            dest_bucket_name=dest_bucket          
        )
        
        if copy:
            logging.info(f"Successfully copied {object_path} to {dist_path}")
        else:
            logging.error(f"Failed to copy {object_path} to {dist_path}")
        

        delete = s3hook.delete_objects(
            bucket=src_bucket,
            keys=object_name
        )
        
        if delete:
            logging.info(f"Successfully deleted {object_name} from {src_bucket}")
        else:
            logging.error(f"Failed to delete {object_name} from {src_bucket}")
    
    @task(task_id='update_fail_dataset', outlets=[FAIL_PUBLISH_DATASET],  trigger_rule="all_failed")
    def update_fail_dataset():
        Metadata(FAIL_PUBLISH_DATASET, {"failed at": {datetime.now()}})
            
    # 1.3.2 update dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_PUBLISH_DATASET])
    def update_success_dataset():
        Metadata(SUCCESS_PUBLISH_DATASET, {"succeded at": {datetime.now()}})
        
    merge_and_publish >> move_processed() >> [update_success_dataset() , update_fail_dataset()]
publish()