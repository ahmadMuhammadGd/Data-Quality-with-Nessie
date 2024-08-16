
import sys, os 
sys.path.insert(1, '/spark-jobs')

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
import dotenv, glob
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME     =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN             =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE         =os.getenv("BRANCH_VALIDATE")        

spark = init_spark_session(app_name="data cleaning")

# picking up columns that has determined values
# such as: tags, size, status, category, etc
# this stage should discover if there any typos in these columns
target_col_names = [os.path.splitext(os.path.basename(file_name))[0] for file_name in glob.glob('/unique_keys/*.csv')]

# table columns
columns = ['Order_ID', 'Date', 'Status', 'Fulfilment', 'Sales_Channel', 'ship_service_level', 'Category', 'Size', 'Courier_Status', 'Qty', 'currency', 'Amount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'B2B', 'fulfilled_by', 'New', 'PendingS']
# prepared sql
sql = f"""
CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.db.orders_2
USING iceberg
AS SELECT
    {', '.join([
        f'ROUND(src.{col}, 2) AS Amount' if col == 'Amount' else f'src.{col}'
        for col in columns
    ])}
FROM 
    {NESSIE_CATALOG_NAME}.db.orders_1 AS src
WHERE
    {" AND ".join([
        f'''
        src.{col} IN 
            (
                SELECT 
                    {col}
                FROM
                    {NESSIE_CATALOG_NAME}.keys.{col}
            )
        '''.strip() for col in target_col_names
    ])};
"""
print('SQL:\n', sql)
try:
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    print("Data from the previous stage")
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.db.orders_1").show()
    spark.sql(sql)
    print("Data after validation")
    spark.sql(f"SELECT count(*) as cnt_2 FROM {NESSIE_CATALOG_NAME}.db.orders_2").show()
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.db.orders_2").show()
    spark.stop()
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()