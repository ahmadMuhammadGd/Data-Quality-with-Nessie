from operators.sparkSSH import SSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from includes.data.datasets import SUCCESS_CLEANING_DATASET, SUCCESS_DBT_TRANSFORM_DATASET, FAIL_DBT_TRANSFORM_DATASET
from airflow.datasets.metadata import Metadata # type: ignore
from datetime import datetime
from airflow.models import Variable
import os 
from includes.data.utils import get_extra_triggering_run

    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="transform_to_gold",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg", "Soda", "Nessie", "transforming", "CSV", "Amazon"],
    default_args=default_args,
    schedule=[SUCCESS_CLEANING_DATASET],
    doc_md="""
    # **Dag_id**: transform_to_gold
    - This DAG is responsible for transforming Amazon order data from the silver layer to the gold layer using dbt (Data Build Tool). 
    - It processes source, dimension, and fact models in parallel, with tests after each transformation. 
    - The workflow updates success or failure datasets depending on the outcome of the dbt transformations.
    """
)

def transform_audit():
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    DBT_PROJECTS_DIR = os.path.join(AIRFLOW_HOME, 'includes', 'dbt_projects')
    DBT_AMAZON_ORDERS_DIR = os.path.join(DBT_PROJECTS_DIR, 'amazon_orders')
    DBT_MODELS_DIR = os.path.join(DBT_AMAZON_ORDERS_DIR, 'models')
    DBT_SOURCE_MODELS_DIR = os.path.join(DBT_MODELS_DIR, 'sources')
    DBT_DIM_MODELS_DIR = os.path.join(DBT_MODELS_DIR, 'dims')
    DBT_FACTS_MODELS_DIR = os.path.join(DBT_MODELS_DIR, 'facts')
    


    @task(task_id="get_branch_name_from_extra")
    def get_branch_name_from_extra(**context):
        extra = get_extra_triggering_run(context)  
        branch_name = extra["branch_name"]
        return branch_name

    # Task to get models from the dbt models directory
    def get_models(dbt_models_dir_path: str) -> list:
        from glob import glob
        models_paths = glob(f"{dbt_models_dir_path}/*.sql")
        models = [os.path.splitext(os.path.basename(path))[0] for path in models_paths]
        return models

    # Task group creator function
    def create_task_group(model_name: str, dbt_project_dir: str, branch_name: str):

        @task_group(group_id=f'{model_name}')
        def dbt_group():

            # BashOperator to run dbt command
            run = BashOperator(
                task_id=f"run_{model_name}",
                bash_command=f"""
                cd "{dbt_project_dir}" 
                dbt run --select {model_name} \
                --vars 'BRANCH_AMAZON_ORDERS_PIPELINE: {branch_name}'
                """
            )

            # BashOperator to test dbt command
            test = BashOperator(
                task_id=f"test_{model_name}",
                bash_command=f"""
                cd "{dbt_project_dir}" 
                dbt test --select {model_name} \
                --vars 'BRANCH_AMAZON_ORDERS_PIPELINE: {branch_name}'
                """
            )

            run >> test

        return dbt_group()

    # Task to update fail dataset (triggered on failure)
    @task(task_id='update_fail_dataset', outlets=[FAIL_DBT_TRANSFORM_DATASET], trigger_rule="all_failed")
    def update_fail_dataset(**context):
        extra = get_extra_triggering_run(context)
        yield Metadata(FAIL_DBT_TRANSFORM_DATASET, extra)

    # Task to update success dataset
    @task(task_id='update_success_dataset', outlets=[SUCCESS_DBT_TRANSFORM_DATASET])
    def update_success_dataset(**context):
        extra = get_extra_triggering_run(context)
        yield Metadata(SUCCESS_DBT_TRANSFORM_DATASET, extra)


    # Get the branch name from a separate task
    branch_name = get_branch_name_from_extra()

    # Source task groups
    parallel_source_task_groups = [
        create_task_group(t, DBT_AMAZON_ORDERS_DIR, branch_name) for t in get_models(DBT_SOURCE_MODELS_DIR)
    ]

    # Dimension task groups
    parallel_dim_task_groups = [
        create_task_group(t, DBT_AMAZON_ORDERS_DIR, branch_name) for t in get_models(DBT_DIM_MODELS_DIR)
    ]

    # Fact task groups
    parallel_fact_task_groups = [
        create_task_group(t, DBT_AMAZON_ORDERS_DIR, branch_name) for t in get_models(DBT_FACTS_MODELS_DIR)
    ]

    # Define task group dependencies
    parallel_source_task_groups >> EmptyOperator(task_id='bridge_1') >> parallel_dim_task_groups
    parallel_dim_task_groups >> EmptyOperator(task_id='bridge_2') >> parallel_fact_task_groups
    parallel_fact_task_groups >> EmptyOperator(task_id='bridge_3') >> [update_fail_dataset(), update_success_dataset()]

transform_audit()