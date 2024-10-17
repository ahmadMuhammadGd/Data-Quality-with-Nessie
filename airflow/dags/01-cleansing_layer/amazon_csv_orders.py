from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from datetime import datetime
from includes.data.datasets import SUCCESS_INGESTION_DATASET, SUCCESS_CLEANING_DATASET, FAIL_CLEANING_DATASET
from airflow.datasets.metadata import Metadata # type: ignore
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
    dag_id="clean_audit_load_to_silver",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie", "cleaning", "CSV", "Amazon"],
    default_args=default_args,
    schedule=[SUCCESS_INGESTION_DATASET],
    doc_md = """
    # **Dag_id:** clean_audit_load_to_silver
    - This DAG cleans and audits ingested Amazon order data and loads it into the silver layer of a data lake using Spark. 
    - It triggers based on the successful ingestion of data and performs data validation using Soda checks, followed by updating relevant datasets based on success or failure.
    """
)
def cleansing_and_loading():

    @task(task_id = 'retrieve_extra')
    def retrieve_extra(**context):
        return get_extra_triggering_run(context)[0]

    extra = retrieve_extra()
    extra_python_args = "{{ ti.xcom_pull(task_ids='retrieve_extra')['spark_jobs_script_args'] }}"

    clean_batch = SSHSparkOperator(
        task_id                 ='clean_and_load_to_silver',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark-container/spark/jobs/cleansing.py',
        python_args             = extra_python_args
    )
    

    validate_cleaned = SSHSparkOperator(
        task_id                 ='Audit_cleaned_batch',
        ssh_conn_id             ='sparkSSH',
        application_path        ='/spark-container/soda/checks/silver_amazon_orders.py',
        python_args             = extra_python_args

    )

    @task(task_id = 'update_fail_dataset', outlets=[FAIL_CLEANING_DATASET], trigger_rule="one_failed")
    def update_success(extra, **context):
        update_outlet(
            FAIL_CLEANING_DATASET, 
            content=extra,
            context=context
        )
    
    @task(task_id = 'update_success_dataset', outlets=[SUCCESS_CLEANING_DATASET])
    def update_failed(extra, **context):
        update_outlet(
            SUCCESS_CLEANING_DATASET,
            content=extra,
            context=context
        )
    
    
    extra >> clean_batch >> validate_cleaned
    validate_cleaned >> [update_success(extra), update_failed(extra)]
    
cleansing_and_loading()