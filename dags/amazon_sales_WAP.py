"""
Write-Audit-Publish DAG

This DAG implements the Write-Audit-Publish (WAP) methodology for data processing.
The DAG performs the following stages:
1. Write: Ingest CSV data from MinIO into a Spark processing system.
2. Audit: Validate the ingested data and transformed data using Soda checks.
3. Publish: Handle the final data processing and move the processed files.

Tasks are grouped into three main stages:
- Write: Handles data ingestion from MinIO.
- Audit: Validates both ingested and transformed data.
- Publish: Final data processing and cleanup.

Dependencies:
- The DAG starts by picking the latest CSV file from MinIO.
- If no file is found, it stops the DAG.
- Ingested files are then validated. If validation fails, an email (wink wink) is sent; otherwise, the data is cleaned and transformed.
- The transformed data is validated. If validation fails, an email (wink wink) is sent; otherwise, the data is published.
- After publishing, processed files are moved to a different bucket, and the DAG is triggered to rerun.
"""

from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from hooks.minio_hook import MinIOHook
from airflow.models import Variable
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="Write-Audit-Publish",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie"],
    default_args=default_args
)
def wap():
    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 0. Locate file
    @task.branch(task_id="pick_minio_object")
    def pick_minio_object(**kwargs):
        minio_hook = MinIOHook(
            endpoint    =Variable.get("MINIO_END_POINT"), 
            access_key  =Variable.get("MINIO_ACCESS_KEY"),
            secret_key  =Variable.get("MINIO_SECRET_KEY"),
        )
        obj = minio_hook.pick_object(
            bucket_name=Variable.get("QUEUED_BUCKET"), 
            prefix=None, 
            strategy='fifo'
        )
        if obj is None:
            logging.info("could not found any objects")
            return 'stop'
        else:
            logging.info(f"object found: {obj.object_name}")
            kwargs['ti'].xcom_push(key='current_csv', value=obj.object_name)
            return 'csv_ingestion'
    
    # 0.1 STOP !!!
    stop = EmptyOperator(task_id='stop')
    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 1. Ingest csv
    ingest_batch = SSHSparkOperator(
        task_id                 ='csv_ingestion',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark/jobs/ingest.py',
        python_args             ="--object-path {{ var.value.QUEUED_BUCKET }}/{{ ti.xcom_pull(task_ids='pick_minio_object', key='current_csv').split('/')[0] }}"
    )

    # 1.2 Validate ingested data with soda
    validate_batch = SSHSparkOperator(
        task_id             ='Audit_bronze_batch',
        ssh_conn_id         ='sodaSSH',
        application_path    ='/soda/checks/bronz_amazon_sales.py',
    )
    
    # 1.3 Do something on error
    send_audit_failure_email_bronze = EmptyOperator(
        task_id                 ='send_bronze_audit_failure_email',
        trigger_rule            ="all_failed",
    )
   
    ##############################################################################
    ##############################################################################
    ##############################################################################
   
    # 2.1 Clean and enrich
    clean_batch = SSHSparkOperator(
        task_id                 ='clean_and_load_to_silver',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark/jobs/cleansing.py'
    )
    
    # 2.2 Validate data with soda
    validate_cleaned = SSHSparkOperator(
        task_id             ='Audit_cleaned_batch',
        ssh_conn_id         ='sodaSSH',
        application_path    ='/soda/checks/silver_amazon_sales.py',
    )
    
    # 2.3 Do something on error
    send_audit_failure_email_silver = EmptyOperator(
        task_id                 ='send_silver_audit_failure_email',
        trigger_rule            ="all_failed",
    ) 
    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 3.1 load data on success
    load_to_silver = SSHSparkOperator(
        task_id                 ='load',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark/jobs/load.py'
    )
    
    # 3.2 merge and publish data on success
    merge_and_publish = SSHSparkOperator(
        task_id                 ='merge_publish',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark/jobs/merge_into_main.py'
    )
    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 4. Move processed file
    @task.python(task_id='move_processed_file')
    def move_processed(**kwargs):
        minio_hook = MinIOHook(
            endpoint            =Variable.get("MINIO_END_POINT"), 
            access_key          =Variable.get("MINIO_ACCESS_KEY"),
            secret_key          =Variable.get("MINIO_SECRET_KEY"),
        )
        
        last_obj_name = kwargs['ti'].xcom_pull(task_ids='pick_minio_object')
        if last_obj_name and last_obj_name.count('/') > 0:
            last_obj_name = last_obj_name.split('/')[0]
        
        minio_hook.move_object(
            source_bucket       =Variable.get("QUEUED_BUCKET"),
            target_bucket       =Variable.get("PROCESSED_BUCKET"),
            prefix              =last_obj_name
        )
    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 5. Rerun untill process all files
    rerun = TriggerDagRunOperator(
        task_id = 'rerun',
        trigger_dag_id="Write-Audit-Publish"
    )

    pick_minio_object() >> [stop, ingest_batch]
    ingest_batch >> validate_batch >> [send_audit_failure_email_bronze, clean_batch]
    clean_batch >> validate_cleaned >> [send_audit_failure_email_silver, load_to_silver]
    load_to_silver >> merge_and_publish >> move_processed() >> rerun

wap()