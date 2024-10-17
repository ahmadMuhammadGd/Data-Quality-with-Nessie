import sys, os 
sys.path.insert(1, '/soda')
from modules.soda.helper import check

from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from env_loader import *

from modules.CLI import cli
object_path, timestamp, nessie_branch = cli()

spark = init_or_get_spark_session(app_name="data cleaning")

try:
    spark.sql(f"USE REFERENCE {nessie_branch} IN {NESSIE_CATALOG_NAME}")
    
    # selecting last batchs from bronz and silver
    for table_name in [SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH, BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH]:
        sql = f"""
        SELECT
            * 
        FROM 
            {NESSIE_CATALOG_NAME}.{AMAZON_PIPELINE_NAMESPACE}.{table_name}
        """
        print(f"Creating '{table_name}' view ..")
        df = spark.sql(sql)
        df.show()
        df.createOrReplaceTempView(table_name)
except Exception as e:
    print(e)
    raise

check(
    spark_session=spark,
    scan_definition_name="DQ for silver amazon ORDERS",
    data_source_name="Silver_Layer",
    check_path_yaml=f"/spark-container/soda/tables/silver_amazon_orders.yaml",
    stop_on_fail=True
)