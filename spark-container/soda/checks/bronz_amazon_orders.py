import sys, os 
sys.path.insert(1, '/soda')
from modules.soda.helper import check

from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from env_loader import *


import getopt, sys

options = "ht:"  
long_options = ['timestamp=', 'help']

argument_list = sys.argv[1:]
try:
    arguments, values = getopt.getopt(argument_list, options, long_options)
    timestamp = None

    for arg, val in arguments:
        if arg in ("-h", "--help"):
            print(f"Usage: {os.path.dirname(os.path.abspath(__file__))} --object-path <path> [options]")
            print("Options:")
            print("  -t, --timestamp <timestamp> Timestamp when the ingestion process started")
            sys.exit()
        elif arg in ("-t", "--timestamp"):
            timestamp = val

    if timestamp is None:
        print("Error: --timestamp (-t) is required")
        raise RuntimeError
        sys.exit(2)
        
except getopt.GetoptError as err:
    print(f"Error: {err}")
    print("Use -h or --help for usage information.")
    sys.exit(2)
    
    

ingestion_timestamp = timestamp
spark = init_or_get_spark_session(app_name="raw data soda validation")

try:
    spark.sql(f"USE REFERENCE {BRANCH_AMAZON_ORDERS_PIPELINE} IN {NESSIE_CATALOG_NAME}")

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