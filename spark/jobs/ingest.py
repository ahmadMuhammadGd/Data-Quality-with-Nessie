
import getopt
import sys

# Define command-line options
options = "ho:"  
long_options = ['object-path=', 'help']

argument_list = sys.argv[1:]
try:
    arguments, values = getopt.getopt(argument_list, options, long_options)
    object_path = None 

    for arg, val in arguments:
        if arg in ("-h", "--help"):
            print("Usage: python path/to/sparkjob --object-path <path> [options]")
            print("Options:")
            print("  -h, --help            Show this help message")
            print("  -o, --object-path <path>   Path to the object withou s3a//")
            sys.exit()
        elif arg in ("-o", "--object-path"):
            object_path = val

    if object_path is None:
        print("Error: --object-path is required")
        sys.exit(2)

    print(f"Object path: {object_path}")

except getopt.GetoptError as err:
    print(f"Error: {err}")
    print("Use -h or --help for usage information.")
    sys.exit(2)

import sys, os 
sys.path.insert(1, '/spark')
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
NAMESPACE                           =os.getenv("NAMESPACE")

spark = init_spark_session(app_name="data ingestion")


try:    
    df=spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv(f"s3a://{object_path}") \
        .withColumn("Date", to_date("Date", format="dd-mm-yy"))
    
    df.createOrReplaceTempView("batch")
    print(f"ingested row count: {df.count()}")
    print(f"switchin to {BRANCH_VALIDATE}")
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    
    spark.sql(f"""
        INSERT INTO {NESSIE_CATALOG_NAME}.{NAMESPACE}.{BRONZE_AMAZON_SALES_TABLE}
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
