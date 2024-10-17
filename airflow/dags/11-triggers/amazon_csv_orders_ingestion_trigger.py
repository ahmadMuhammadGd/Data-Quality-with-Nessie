# from airflow.utils.dates import days_ago
# from airflow.decorators import dag
# from datetime import datetime
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# from includes.data.datasets import SUCCESS_ERROR_HANDLING_DATASET, SUCCESS_PUBLISH_DATASET




# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0,
# }



# @dag(
#     dag_id="retrigger_csv_ingestion",
#     catchup=False,
#     default_args=default_args,
#     schedule=(SUCCESS_PUBLISH_DATASET | SUCCESS_ERROR_HANDLING_DATASET),
#     doc_md="""
#     # **Dag_id**: retrigger_csv_ingestion
#     This DAG re-triggers the Amazon CSV orders ingestion DAG after either a successful publishing step or successful error handling.
#     """
# )
# def trigger():
#     re_ingest = TriggerDagRunOperator(
#         task_id = 're_ingest',
#         trigger_dag_id= 'start_amazon_csv_orders'
#     )
#     re_ingest
# trigger()