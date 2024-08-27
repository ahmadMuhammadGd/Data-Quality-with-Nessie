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
from operators.sodaSSH import SSHSodaOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from hooks.minio_hook import MinIOHook
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
    # 0. Locate file
    @task.branch(task_id="pick_minio_object")
    def pick_minio_object(**kwargs):
        minio_hook = MinIOHook(
            endpoint='', 
            access_key='', 
            secret_key=''
        )
        obj = minio_hook.pick_object(bucket_name='', prefix='', strategy='fifo')
        if obj is None:
            return 'stop'
        else:
            kwargs['ti'].xcom_push(key='current_csv', value=obj.object_name)
            return 'csv_ingestion'
    
    # 0.1 STOP !!!
    stop = EmptyOperator(task_id='stop')
    
    # 1. Ingest csv
    ingest_batch = SSHSparkOperator(
        task_id             ='csv_ingestion',
        ssh_conn_id         ='sparkSSH',
        application_path    =''
    )
        
    @task_group(group_id='Audit_ingested_data')
    def source_batch_validation_group():
        # 2.1 Validate ingested data with soda
        validate_batch = SSHSodaOperator(
            task_id             ='ingested_batch_validation',
            ssh_conn_id         ='sodaSSH',
            check_path          ='',
            do_xcom_push        =True
        )
        
        # 2.2 Check validation result
        @task.branch(task_id="batch_validation_branching")
        def check_source_failures(**kwargs):
            import re
            soda_output = kwargs['ti'].xcom_pull(task_ids='ingested_batch_validation')
            if re.search(r'Oops!\s+\d+\sfailures', soda_output, re.MULTILINE):
                return 'send_audit_failure_email'
            else:
                return 'transform_write'
        validate_batch >> check_source_failures()
      
    # 3.1 Do something on error
    send_audit_failure_email = EmptyOperator(
        task_id             ='send_audit_failure_email'
    )
    
    # 3.2 Clean and transform
    clean_batch = SSHSparkOperator(
        task_id             ='transform_write',
        ssh_conn_id         ='sparkSSH',
        application_path    =''
    )
    
    @task_group(group_id='Audit_before_publishing')
    def transformed_validation_group():
        # 4.1 Validate transformed data with soda
        validate_cleaned = SSHSodaOperator(
            task_id             ='validate_cleaned_data',
            ssh_conn_id         ='sodaSSH',
            check_path          ='',
            do_xcom_push        =True
        )

        # 4.2 check validation result
        @task.branch(task_id="cleaned_batch_validation_branching")
        def check_cleaned_failures(**kwargs):
            import re
            soda_output = kwargs['ti'].xcom_pull(task_ids='validate_cleaned_data')
            if re.search(r'Oops!\s+\d+\sfailures', soda_output, re.MULTILINE):
                return 'send_audit_failure_email'
            else:
                return 'publish'
        validate_cleaned >> check_cleaned_failures()
    
    
    # 5. Publish data on success
    merge_silver = SSHSparkOperator(
        task_id             ='publish',
        ssh_conn_id         ='sparkSSH',
        application_path    =''
    )
    
    # 6. Move processed file
    @task.python(task_id='move_processed_file')
    def move_processed(**kwargs):
        minio_hook = MinIOHook(
            endpoint='your-minio-endpoint', 
            access_key='your-access-key', 
            secret_key='your-secret-key'
        )
        
        last_obj = kwargs['ti'].xcom_pull(task_ids='pick_minio_object')
        if last_obj and last_obj.count('/') > 0:
            last_obj = last_obj.split('/')[0]
        
        minio_hook.move_object(
            source_bucket='source-bucket',
            target_bucket='target-bucket',
            prefix='your-prefix/'
        )
    
    # 7. Rerun untill process all files
    rerun = TriggerDagRunOperator(
        task_id = 'rerun',
        trigger_dag_id="Write-Audit-Publish"
    )

    pick_minio_object() >> [stop, ingest_batch]
    ingest_batch >> source_batch_validation_group() >> [send_audit_failure_email, clean_batch]
    clean_batch >> transformed_validation_group() >> [send_audit_failure_email, merge_silver]
    merge_silver >> move_processed() >> rerun

wap()