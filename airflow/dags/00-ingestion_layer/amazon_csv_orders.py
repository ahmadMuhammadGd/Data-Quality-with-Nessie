from operators.sparkSSH import SSHSparkOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from includes.data.datasets import FAIL_INGESTION_DATASET, SUCCESS_INGESTION_DATASET, INFO_INGESTION_DATASET
from includes.data.utils import custom_extra_template
from airflow.datasets.metadata import Metadata # type: ignore
from airflow.models import Variable
import logging, os 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

doc_md_DAG="""
# **Dag_id**: start_amazon_csv_orders
- This DAG ingests Amazon order data from a CSV stored in an S3-compatible MinIO bucket into a Spark-based data processing pipeline. 
- The pipeline validates the data with Soda checks and updates ingestion metadata on success or failure. 
- It leverages version control via Nessie to create a new branch for each ingestion batch.
"""

@dag(
    dag_id="start_amazon_csv_orders",
    catchup=False,  
    tags=["Spark", "SSH", "Iceberg", "bronz", "Soda", "Nessie", "ingestion", "CSV", "Amazon"],
    schedule=timedelta(days=1),
    default_args=default_args,
    doc_md=doc_md_DAG
)
def ingest():
    @task.branch(task_id="start")
    def pick_s3_object(**kwargs):
        """
        Fetches the list of CSV files from the S3 bucket (landing prefix). 
        If no files are found, it logs an informational message and updates the info dataset. 
        If a file is found, it selects the first/last CSV and pushes the file path, timestamp, and recommended Nessie branch name to XCom.
        """
        s3_hook = S3Hook(
            aws_conn_id='minio_connection', 
        )

        bucket_name = os.getenv("QUEUED_BUCKET")
        objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='landing')
        objects = [obj for obj in objects if obj.endswith('csv')]

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
            current_csv_name = object_name.split('/')[-1]
            current_csv_path = object_name
            
            timestamp = datetime.now()
            curent_timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S') # for spark SLQ, better readability while debugging sparksubmit commands
            branch_recommended_name = f'{nessie_branch_prefex}_{(current_csv_name).replace('.csv', '')}_{str(timestamp.strftime('%Y%m%d%H%M%S'))}'
            
            kwargs['ti'].xcom_push(key='current_csv'    , value=current_csv_path)
            kwargs['ti'].xcom_push(key='timestamp'      , value=curent_timestamp)
            kwargs['ti'].xcom_push(key='nessie_branch'  , value=branch_recommended_name)
            
            return 'defining_new_branch'
    
    # 0.1 STOP !!!    
    ##############################################################################
    ##############################################################################
    ##############################################################################
    
    # 0.2 preparing dag branch
    @task(task_id = 'defining_new_branch')
    def define_branch(**kwargs)->None:
        '''
        Defines a new branch for Nessie in the Spark environment by updating the BRANCH_AMAZON_ORDERS_PIPELINE variable in the Nessie configuration (nessie.env). 
        This is done via an SSH connection to the Spark cluster.
        '''
        nessie_branch_new = kwargs['ti'].xcom_pull(key='nessie_branch')
        
        ssh_hook = SSHHook(ssh_conn_id='sparkSSH',)
        with ssh_hook.get_conn() as client:
            '''
            This command assumes that 'BRANCH_AMAZON_ORDERS_PIPELINE_PREFEX' exists in spark's environment variables
            '''
            client.exec_command(
            f'''
            sed -i "s|^BRANCH_AMAZON_ORDERS_PIPELINE\s*=.*|BRANCH_AMAZON_ORDERS_PIPELINE={nessie_branch_new}|" /config/nessie.env
            '''
            )
    
    # 1. Ingest csv

    ingest_batch = SSHSparkOperator(
        task_id             ='csv_ingestion',
        ssh_conn_id         ='sparkSSH',
        application_path    ='/spark-container/spark/jobs/ingest.py',
        python_args         = 
            f"--object-path {os.getenv('QUEUED_BUCKET')}/{{{{ ti.xcom_pull(task_ids='start', key='current_csv') }}}} -t {{{{ ti.xcom_pull(task_ids='start', key='timestamp') }}}}",
        doc_md              =
        """
        Executes the ingestion of the selected CSV file into the bronze layer using Spark. 
        It runs a Python script (ingest.py) on the Spark cluster via SSH, with arguments like the object path and timestamp retrieved from XCom.
        """
    ) 

    # 1.2 Validate ingested data with soda
    validate_batch = SSHSparkOperator(
        task_id             ='Audit_bronze_batch',
        ssh_conn_id         ='sparkSSH',
        application_path    ="/spark-container/soda/checks/bronz_amazon_orders.py",
        python_args         ="-t {{ ti.xcom_pull(task_ids='start', key='timestamp') }}",
        doc_md              =
        """
         Audits the ingested data in the bronze layer using Soda checks. 
         This validation task runs another Spark job via SSH (bronz_amazon_orders.py) to ensure data quality.
        """
    )
    
    # 1.3.1 do something on validation failure
    @task(task_id='update_fail_dataset', outlets=[FAIL_INGESTION_DATASET], trigger_rule="all_failed")
    def update_fail_dataset(**kwargs):
        file_name = kwargs['ti'].xcom_pull(key='current_csv')
        branch_name = kwargs['ti'].xcom_pull(key='nessie_branch')
        extra = custom_extra_template(file_name, branch_name)
        yield Metadata(FAIL_INGESTION_DATASET, extra)
    
    # 1.3.2 Update dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_INGESTION_DATASET])
    def update_success_dataset(**kwargs):
        file_name = kwargs['ti'].xcom_pull(key='current_csv')
        branch_name = kwargs['ti'].xcom_pull(key='nessie_branch')
        extra = custom_extra_template(file_name, branch_name)
        yield Metadata(SUCCESS_INGESTION_DATASET, extra)
    
    @task(task_id='update_info_dataset', outlets=[INFO_INGESTION_DATASET])
    def update_info_dataset():
        yield Metadata(INFO_INGESTION_DATASET, {"info: No files found": {datetime.now()}})
        
    
    define_branch = define_branch()
    pick_s3_object() >> [define_branch,  update_info_dataset()]
    define_branch >> ingest_batch >> validate_batch
    validate_batch >> [update_success_dataset(), update_fail_dataset()]

ingest()