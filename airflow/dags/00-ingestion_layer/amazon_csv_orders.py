from operators.sparkSSH import SSHSparkOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago, timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from includes.data.datasets import FAIL_INGESTION_DATASET, SUCCESS_INGESTION_DATASET, INFO_INGESTION_DATASET
from airflow.datasets.metadata import Metadata # type: ignore
import logging, os 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="ingest_amazon_csv_orders",
    catchup=False,  
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie", "ingestion", "CSV", "Amazon"],
    schedule=timedelta(days=1),
    default_args=default_args,
)

    
def ingest():
    @task.branch(task_id="pick_minio_object")
    def pick_s3_object(**kwargs):
        s3_hook = S3Hook(
            aws_conn_id='minio_connection', 
        )

        bucket_name = os.getenv("QUEUED_BUCKET")
        objects = s3_hook.list_keys(bucket_name=bucket_name)

        if not objects:
            logging.info("No objects found in the bucket")
            return 'update_info_dataset'
        else:
            # Pick the first object (FIFO)
            object_name = objects[0]
            
            # object_name = objects[-1] # UNCOMMENT to pick the last object (LIFO)
            
            logging.info(f"Object found: {object_name}")
            
            nessie_branch_prefex = os.getenv("BRANCH_AMAZON_ORDERS_PIPELINE_PREFEX")
            
            # Push values to XCom for further tasks
            kwargs['ti'].xcom_push(key='current_csv'    , value=object_name.split('/')[0])
            kwargs['ti'].xcom_push(key='timestamp'      , value=datetime.now())
            kwargs['ti'].xcom_push(key='nessie_branch'  , value=f'{nessie_branch_prefex}_{(object_name.split('/')[0]).replace('.csv', '')}')
            
            return 'defining_new_branch'
    
    # 0.1 STOP !!!    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 0.2 preparing dag branch
    @task(task_id = 'defining_new_branch')
    def define_branch(**kwargs)->None:
        '''
        This task overwrites BRANCH_AMAZON_ORDERS_PIPELINE environment variable in spark cluster
        return: None
        '''
        nessie_branch_new = kwargs['ti'].xcom_pull(key='nessie_branch')
        inegstion_timestamp = kwargs['ti'].xcom_pull(key='timestamp').strftime('%Y%m%d_%H%M%S')
        
        ssh_hook = SSHHook(ssh_conn_id='sparkSSH',)
        with ssh_hook.get_conn() as client:
            '''
            This command assumes that 'BRANCH_AMAZON_ORDERS_PIPELINE_PREFEX' exists in spark's environment variables
            '''
            client.exec_command(
            f'''
            sed -i "s|^BRANCH_AMAZON_ORDERS_PIPELINE\s*=.*|BRANCH_AMAZON_ORDERS_PIPELINE={nessie_branch_new}_{str(inegstion_timestamp)}|" /config/nessie.env
            '''
            )
    
    # 1. Ingest csv
    ingest_batch = SSHSparkOperator(
        task_id             ='csv_ingestion',
        ssh_conn_id         ='sparkSSH',
        application_path    ='/spark-container/spark/jobs/ingest.py',
        python_args         ="--object-path {{ var.value.QUEUED_BUCKET }}/{{ ti.xcom_pull(task_ids='pick_minio_object', key='current_csv') }} -t {{ ti.xcom_pull(task_ids='pick_minio_object', key='timestamp') }}"
    )

    # 1.2 Validate ingested data with soda
    validate_batch = SSHSparkOperator(
        task_id             ='Audit_bronze_batch',
        ssh_conn_id         ='sparkSSH',
        application_path    ="/spark-container/soda/checks/bronz_amazon_orders.py",
        python_args         ="-t {{ ti.xcom_pull(task_ids='pick_minio_object', key='timestamp') }}"
    )
    
    # 1.3.1 do something on validation failure
    @task(task_id='update_fail_dataset', outlets=[FAIL_INGESTION_DATASET],  trigger_rule="all_failed")
    def update_fail_dataset():
        Metadata(FAIL_INGESTION_DATASET, {"failed at": {datetime.now()}})
            
    # 1.3.2 update dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_INGESTION_DATASET])
    def update_success_dataset():
        Metadata(SUCCESS_INGESTION_DATASET, {"succeded at": {datetime.now()}})
    
    @task(task_id='update_info_dataset', outlets=[INFO_INGESTION_DATASET])
    def update_info_dataset():
        Metadata(INFO_INGESTION_DATASET, {"info: No files found": {datetime.now()}})
        
    
    define_branch = define_branch()
    pick_s3_object() >> [define_branch,  update_info_dataset()]
    define_branch >> ingest_batch >> validate_batch
    validate_batch >> [update_success_dataset(), update_fail_dataset()]

ingest()