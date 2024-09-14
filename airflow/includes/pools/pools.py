from airflow.models import Pool

CSV_PIPLEINE_POOL= Pool.create_or_update_pool(
    "amazon_csv_pipeline_1_slot_pool",
    slots=1,
    include_deferred=False,
    description="""
    This pool is used to manage DAG runs for the Amazon CSV pipeline. 
    It ensures that the pipeline runs with environment variables to share Nessie branch names with dbt, 
    avoiding potential conflicts between DAG runs that could occur due to parallel execution.
    To prevent race conditions, the pool limits the number of concurrent runs to just one (1).
    """,
)