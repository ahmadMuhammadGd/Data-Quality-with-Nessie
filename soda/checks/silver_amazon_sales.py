import sys, os 
sys.path.insert(1, '/spark')
sys.path.insert(1, '/soda')
from modules.soda.helper import check

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from SparkCleaner import *
import dotenv
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME                 =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN                         =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE                     =os.getenv("BRANCH_VALIDATE")        
BRONZE_AMAZON_SALES_TABLE           =os.getenv("BRONZE_AMAZON_SALES_TABLE")        
SILVER_AMAZON_SALES_TABLE           =os.getenv("SILVER_AMAZON_SALES_TABLE")        
NAMESPACE                          =os.getenv("NAMESPACE")

spark = init_spark_session(app_name="data cleaning")

try:
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.{NAMESPACE}.{BRONZE_AMAZON_SALES_TABLE}").createOrReplaceTempView(f"{BRONZE_AMAZON_SALES_TABLE}")
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.{NAMESPACE}.{SILVER_AMAZON_SALES_TABLE}_temp").createOrReplaceTempView(f"{SILVER_AMAZON_SALES_TABLE}_temp")
except Exception as e:
    print(e)
    raise

check(
    spark_session=spark,
    scan_definition_name="DQ for silver amazon sales",
    data_source_name="bronz_sales",
    check_path_yaml=f"/soda/tables/{SILVER_AMAZON_SALES_TABLE}.yaml",
    stop_on_fail=True
)