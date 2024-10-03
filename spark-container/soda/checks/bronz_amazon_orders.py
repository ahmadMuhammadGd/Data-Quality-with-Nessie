import sys, os 
sys.path.insert(1, '/soda')
from modules.soda.helper import check

from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from env_loader import *

from modules.CLI import cli
object_path, timestamp, nessie_branch = cli()
    

ingestion_timestamp = timestamp
spark = init_or_get_spark_session(app_name="raw data soda validation")

try:
    spark.sql(f"USE REFERENCE {nessie_branch} IN {NESSIE_CATALOG_NAME}")
    current_branch_df = spark.sql('SHOW REFERENCE IN nessie')
    current_branch_pd = current_branch_df.toPandas()["name"]
    logging.info(
        f"""current branches:
        {current_branch_pd}
        """
    )
    df = spark.sql(
        f"""
        SELECT 
            * 
        FROM 
            { NESSIE_CATALOG_NAME }.{ AMAZON_PIPELINE_NAMESPACE }.{ BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH }
        WHERE
            ingestion_date = CAST(\'{ingestion_timestamp }\' AS TIMESTAMP)
        """
    )
    
    df.show()
    df.createOrReplaceTempView(f"bronze_amazon_orders")
    print('bronz table')
except Exception as e:
    print(e)
    raise

check(
    spark_session=spark,
    scan_definition_name="DQ for bronz amazon ORDERS",
    data_source_name="bronz_layer",
    check_path_yaml=f"/spark-container/soda/tables/bronze_amazon_orders.yaml",
    stop_on_fail=True
)