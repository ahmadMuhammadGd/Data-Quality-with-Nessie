import sys, os 
sys.path.insert(1, '/spark-jobs')

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from SparkCleaner import *
from datetime import datetime 
from pyspark.sql.functions import to_date 
import dotenv
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME                 =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN                         =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE                     =os.getenv("BRANCH_VALIDATE")        
BRONZE_AMAZON_SALES_TABLE           =os.getenv("BRONZE_AMAZON_SALES_TABLE")        
SILVER_AMAZON_SALES_TABLE           =os.getenv("SILVER_AMAZON_SALES_TABLE")        
NAME_SPACE                          =os.getenv("NAME_SPACE")

spark = init_spark_session(app_name="data ingestion")


try:    
    df=spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv("/batchs/sampled_data_2.csv/sampled_data_2.csv") \
        .withColumn("Date", to_date("Date", format="dd-mm-yy"))
    
    df.createOrReplaceTempView("batch")
    
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    
    spark.sql(f"""
        INSERT INTO {NESSIE_CATALOG_NAME}.{NAME_SPACE}.{BRONZE_AMAZON_SALES_TABLE}
        SELECT
        {', '.join([f'''
            {col} AS {col}
        ''' 
        for col in df.columns
        ])}, TIMESTAMP \'{datetime.now()}\' AS ingestion_date
        FROM batch 
    """)
    
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()
