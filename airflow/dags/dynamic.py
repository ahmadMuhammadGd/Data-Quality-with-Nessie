from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="dynamic_task_group_example",
    catchup=False,
    default_args=default_args,
)
def example():
    def tasks_list():
        return ['a', 'b', 'c']

    # Define task group structure outside the loop
    def create_task_group(group_name):
        @task_group(group_id=f'{group_name}_group')
        def group():
            a = EmptyOperator(task_id=group_name)
            b = EmptyOperator(task_id=f'{group_name}_b')
            a >> b
        return group()

    intro = EmptyOperator(task_id='intro')
    outro = EmptyOperator(task_id='outro')

    # Create parallel task groups
    parallel_task_groups = [create_task_group(t) for t in tasks_list()]

    # Set task dependencies
    intro >> parallel_task_groups >> outro

example()
