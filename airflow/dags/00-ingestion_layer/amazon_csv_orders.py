from operators.sparkSSH import SSHSparkOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from includes.data.datasets import INFO_FOUND_CSV, FAIL_INGESTION_DATASET, SUCCESS_INGESTION_DATASET, INFO_INGESTION_DATASET
from includes.data.utils import get_extra_triggering_run, update_outlet
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
    dag_id="ingest_orders",
    catchup=False,  
    tags=["Spark", "SSH", "Iceberg", "bronz", "Soda", "Nessie", "ingestion", "CSV", "Amazon"],
    schedule=[INFO_FOUND_CSV],
    default_args=default_args,
    doc_md=doc_md_DAG
)
def ingest():

    @task(task_id = 'retrieve_extra')
    def retrieve_extra(**context)-> dict:
        return get_extra_triggering_run(context)[0]

    extra = retrieve_extra()
    extra_python_args = "{{ ti.xcom_pull(task_ids='retrieve_extra')['spark_jobs_script_args'] }}"

    ingest_batch = SSHSparkOperator(
        task_id             ='csv_ingestion',
        ssh_conn_id         ='sparkSSH',
        application_path    ='/spark-container/spark/jobs/ingest.py',
        python_args         = extra_python_args,
        doc_md=
        """
        Executes the ingestion of the selected CSV file into the bronze layer using Spark. 
        It runs a Python script (ingest.py) on the Spark cluster via SSH, with arguments like the object path and timestamp retrieved from XCom.
        """
    ) 


    validate_batch = SSHSparkOperator(
        task_id             ='Audit_bronze_batch',
        ssh_conn_id         ='sparkSSH',
        application_path    ="/spark-container/soda/checks/bronz_amazon_orders.py",
        python_args         = extra_python_args,
        doc_md              =
        """
        Audits the ingested data in the bronze layer using Soda checks. 
        This validation task runs another Spark job via SSH (bronz_amazon_orders.py) to ensure data quality.
        """
    )


    @task(task_id = 'update_fail_dataset', outlets=[FAIL_INGESTION_DATASET], trigger_rule="one_failed")
    def update_success(extra, **context):
        update_outlet(
            FAIL_INGESTION_DATASET, 
            content=extra,
            context=context
        )
    
    @task(task_id = 'update_success_dataset', outlets=[SUCCESS_INGESTION_DATASET])
    def update_failed(extra, **context):
        update_outlet(
            SUCCESS_INGESTION_DATASET,
            content=extra,
            context=context
        )

    
    extra >> ingest_batch >> validate_batch
    validate_batch >> [update_success(extra), update_failed(extra)]

ingest()