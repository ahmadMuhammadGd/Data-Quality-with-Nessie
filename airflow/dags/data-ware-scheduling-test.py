
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from airflow.utils.dates import days_ago, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
my_dataset = Dataset('my_dataset')

@dag(
    dag_id='data_aware_scheduler_upstream',
    catchup=False,  
    tags=["test"],
    schedule=timedelta(days=1),
    default_args = default_args
)



def run_triggers():
    from airflow.utils.helpers import chain
    items = ["apple", "panana", "orange"]
    
    def create_task(item):
        
        @task(task_id = f"{item}_event" ,outlets=[my_dataset])
        def use_outlet_events(**context):
            context["outlet_events"][my_dataset].extra = {"item": item}
        
        return use_outlet_events()
    
    my_chain = [create_task(item) for item in items]
    chain(*my_chain)
    
run_triggers()



@dag(
    dag_id='data_aware_scheduler_downstream',
    catchup=False,  
    tags=["test"],
    schedule=[my_dataset],
    default_args = default_args
    
)

def run_downstream():
    @task
    def retreive_item(**context):
        triggering_dataset_events = context["triggering_dataset_events"]
        for _, dataset_list in triggering_dataset_events.items():
            extra = dataset_list[0].extra
            print( extra )
    
    retreive_item()
run_downstream()