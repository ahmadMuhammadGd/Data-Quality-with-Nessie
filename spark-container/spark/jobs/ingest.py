"""
This script is designed to automate the process of data ingestion and 
transformation in a Spark environment, integrating various technologies
such as Iceberg, Nessie, and MinIO. 

It begins by parsing command-line arguments to determine the path of the
object to be ingested, ensuring that the necessary parameters are provided. 
If the required `--object-path` argument is missing, the script terminates
early, providing the user with usage information.

The script initializes a Spark session with a custom configuration, specifically
tailored for a data ingestion pipeline. 

The main logic of the script begins by reading a CSV file from an S3 path, 
inferred from the `object_path` argument, into a Spark DataFrame. 

The script infers the schema and processes the 'Date' column, converting it to a date
format and renaming it to 'order_date' for consistency. 

After reading and transforming the data, the script logs the ingestion attempt in a 
metadata table, including details such as the file path, the timestamp of ingestion, 
and initializing various row counts to zero (as placeholders for subsequent steps in the pipeline).

The DataFrame is temporarily registered as a SQL view to facilitate SQL operations on the dataset. 
A query is executed to determine the number of rows in the batch that are not already present in 
the target Bronze table based on `order_id` and `order_date`. 
This ensures that only new, non-duplicate records are ingested, maintaining the integrity of the dataset.

If there are new rows to ingest, they are inserted into the Bronze layer of the data lakehouse, 
along with an ingestion timestamp. 
The script provides feedback on the number of rows ingested at each step, ensuring transparency 
and traceability of the data processing workflow.

The script is designed with error handling to capture and print any exceptions that occur during execution, ensuring that the root cause of any issues can be easily identified. 
Finally, the script ensures that the Spark session is properly stopped, releasing resources and avoiding potential issues with resource allocation in subsequent operations.

"""
from modules.CLI import cli
object_path, timestamp, nessie_branch = cli()

import sys, os 
from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from pyspark.sql.functions import to_date 
from env_loader import *

spark = init_or_get_spark_session(app_name="data ingestion", direct_s3_read_write=True)

try:
    ingested_batch_view = 'batch'
    ingestion_timestamp = timestamp.replace('T', ' ')
    
    df=spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv(object_path) \
        .withColumn("Date", to_date("Date", format="dd-mm-yy")) \
        .withColumnRenamed("Date", "order_date")
    
    
    logging.info(f"Source row count: {df.count()}")
        
    logging.info(f"Creating branch: `{nessie_branch}` ..")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {nessie_branch} IN {NESSIE_CATALOG_NAME} FROM {BRANCH_MAIN}")
    
    logging.info(f"Switching to the branch: `{nessie_branch}` ..")
    spark.sql(f"USE REFERENCE {nessie_branch} IN {NESSIE_CATALOG_NAME}")
    
    min_order_date_in_batch = df.selectExpr("min(order_date)").collect()[0][0]
    
    df.createOrReplaceTempView('ingested_batch_view')
    df_batch = spark.sql(f"""
    SELECT 
        *
    FROM ingested_batch_view AS batch 
        WHERE
            batch.order_id NOT IN (
                SELECT
                    order_id
                FROM
                    {NESSIE_CATALOG_NAME}.{BRONZE_NAMESPACE}.{AMAZON_ORDERS_TABLE} AS bronze
                WHERE 
                    bronze.order_date >= TIMESTAMP ('{min_order_date_in_batch}')
                )
    """)
    
    
    df_batch_count = df_batch.count()
    
    logging.info(f"{df_batch_count} rows should be ingested into Bronze layer")
    df_batch.show()

    if df_batch_count != 0:
        df_batch.createOrReplaceTempView('df_batch')
        load_sql = f"""
            INSERT INTO { NESSIE_CATALOG_NAME }.{ BRONZE_NAMESPACE }.{ AMAZON_ORDERS_TABLE }
            SELECT
                *,
                TO_TIMESTAMP('{ingestion_timestamp}', 'yyyy-MM-dd HH:mm:ss') AS ingestion_date
            FROM ingested_batch_view 
        """
        logging.info(f'rendered load SQL: {load_sql}')
        spark.sql(load_sql)
    
    logging.info(f"{df_batch_count} rows has been loaded successfully into: `{ NESSIE_CATALOG_NAME }.{ BRONZE_NAMESPACE }.{ AMAZON_ORDERS_TABLE }` ..!")
    logging.info(f"""
    bronz preview:
    {spark.sql(f'SELECT * FROM { NESSIE_CATALOG_NAME }.{ BRONZE_NAMESPACE }.{ AMAZON_ORDERS_TABLE }').show()}
    """)
except Exception as e:
    logging.error (e)
    raise
finally:
    spark.stop()
