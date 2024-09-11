from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from datetime import datetime
from includes.data.datasets import SUCCESS_INGESTION_DATASET, SUCCESS_CLEANING_DATASET, FAIL_CLEANING_DATASET
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
    dag_id="clean_audit_load_to_silver",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie", "cleaning", "CSV", "Amazon"],
    default_args=default_args,
    schedule=[SUCCESS_INGESTION_DATASET],
)
def cleansing_and_loading():
    clean_batch = SSHSparkOperator(
        task_id                 ='clean_and_load_to_silver',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark-container/spark/jobs/cleansing.py'
    )
    
    # 2.2 Validate data with soda
    validate_cleaned = SSHSparkOperator(
        task_id             ='Audit_cleaned_batch',
        ssh_conn_id         ='sparkSSH',
        application_path    ='/spark-container/soda/checks/silver_amazon_orders.py',
    )
    
    # 2.3 Do something on error
    @task(task_id='update_fail_dataset', outlets=[FAIL_CLEANING_DATASET],  trigger_rule="all_failed")
    def update_fail_dataset():
        Metadata(FAIL_CLEANING_DATASET, {"failed at": {datetime.now()}})
            
    # 1.3.2 update dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_CLEANING_DATASET])
    def update_success_dataset():
        Metadata(SUCCESS_CLEANING_DATASET, {"succeded at": {datetime.now()}})
    
    clean_batch >> validate_cleaned
    validate_cleaned >> [update_fail_dataset(), update_success_dataset()]
    
cleansing_and_loading()