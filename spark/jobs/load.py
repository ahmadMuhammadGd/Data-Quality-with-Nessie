import sys, os 
sys.path.insert(1, '/spark')
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


spark = init_spark_session(app_name="data loading")


try:    
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    
    spark.sql(f"""
    INSERT INTO 
        {NESSIE_CATALOG_NAME}.{NAMESPACE}.{SILVER_AMAZON_SALES_TABLE}
    SELECT
        *
    FROM
        {NESSIE_CATALOG_NAME}.{NAMESPACE}.{SILVER_AMAZON_SALES_TABLE}_temp
    """)
    
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()