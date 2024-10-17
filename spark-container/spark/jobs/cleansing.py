


# Summary:
# 1.  The code initializes a Spark session for data cleaning, 
#     switches to a specific branch in the Nessie catalog, 
#     and retrieves the most recent batch of data from a specified table.

# 2.  It then applies a series of data cleaning strategies to the retrieved data, 
#     such as dropping missing values, filtering negative values, validating date
#     formats, and removing duplicates.

# 3.  After cleaning, the code checks for spelling errors in certain columns 
#     against a set of unique keys stored in CSV files.Finally, it prepares an
#     SQL statement to insert the cleaned and validated data into another table
#     within the Nessie catalog.

# Cleaning steps:
# 1. remove duplicates 
# 2. remove nulls from 'Qty', 'Amount', 'Order_ID', 'order_Date', 'currency'
# 3. remove negative values from 'Qty', 'Amount'
# 4. remove corrupted dates from Order_ID
# 5. standarize and fix typos for all string type columns


# Define command-line options
from modules.CLI import cli
object_path, timestamp, nessie_branch = cli()

import sys, os 
from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from SparkCleaner import CleaningPipeline
from SparkCleaner.Strategies import *
import logging
from env_loader import *

spark = init_or_get_spark_session(app_name="data cleaning")

try:
    spark.sql(f"USE REFERENCE {nessie_branch} IN {NESSIE_CATALOG_NAME}")
    logging.info(f'`{ NESSIE_CATALOG_NAME }.{ BRONZE_NAMESPACE }.{ AMAZON_ORDERS_TABLE }` Table:')
    spark.sql(f"""
    SELECT
        *
    FROM
        { NESSIE_CATALOG_NAME }.{ BRONZE_NAMESPACE }.{ AMAZON_ORDERS_TABLE }
    
    """).show()
    
    df = spark.sql(
        f"""
        SELECT 
            * 
        FROM 
            { NESSIE_CATALOG_NAME }.{ AMAZON_PIPELINE_NAMESPACE }.{ BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH }
        """
    )
    logging.info('Most Recent Batch')
    df.show()
    
    # now, this is the cleaning part !     
    from pyspark.sql.functions import col, to_date, upper, initcap

    clean_df = df.withColumn('Order_Date', to_date(df.Order_Date, format='MM-dd-yy')) \
        .dropna(subset=['Size', 'Qty', 'Amount', 'Order_ID', 'Order_Date', 'Currency']) \
        .filter( (col('Qty') > 0) & (col('Amount') > 0) ) \
        .fillna({
            "Order_Status"          :   "INVALID_VALUE",
            "Fulfilment"            :   "INVALID_VALUE",
            "ORDERS_Channel"        :   "INVALID_VALUE",
            "ship_service_level"    :   "INVALID_VALUE",
            "Category"              :   "INVALID_VALUE",
            "Courier_Status"        :   "INVALID_VALUE",
            "Ship_City"             :   "INVALID_VALUE",
            "Ship_State"            :   "INVALID_VALUE",
            "Ship_Postal_Code"      :   "INVALID_VALUE",
            "Ship_Country"          :   "INVALID_VALUE",
            "Fulfilled_By"          :   "INVALID_VALUE",
            "New"                   :   "INVALID_VALUE",
            "PendingS"              :   "INVALID_VALUE",
        }) \
        .drop_duplicates(subset=['Order_ID', 'Category', 'Order_Date', 'Amount']) \
        .withColumn('Size', upper('Size')) \
        .filter(col("Size").isin(['XS', 'S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '5XL', '6XL', 'FREE', '7XL', '8XL', '9XL', 'XXXS', 'XXS', 'TALL', 'PETITE', 
         'SMALL', 'MEDIUM', 'LARGE', 'PLUS SIZE', 'OVERSIZED']))
    
    # before loading, we need to check spelling for some columns
    # picking up columns that has determined values
    # such as: size, status, category, etc
    # this stage should discover if there any typos in these columns
    
    target_col_names = [
        "Order_Status",
        "Fulfilment",
        "ORDERS_Channel",
        "ship_service_level",
        "Category",
        "Courier_Status",
        "Currency",
        "Ship_City",
        "Ship_State",
        "Ship_Country",
        "Fulfilled_By",
        "New",
    ]

    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType
    import pandas as pd # type: ignore
    from autocorrect import Speller # type: ignore

    spell = Speller()
    @pandas_udf(StringType())
    def correct_string_spelling_udf(string: pd.Series) -> pd.Series:
        return string.apply(lambda x: str(spell(x)) if x else x)


    # Apply the UDFs to the DataFrame columns
    for column in target_col_names:
        clean_df = clean_df \
                    .withColumn(column, initcap(col(column))) \
                    .withColumn(column, correct_string_spelling_udf(col(column)))
    
    logging.info('STAGE 2: Standarize string columns and fix typos')
    logging.info('Source df:', df.count())
    logging.info('Stage2: cleaned_df:', clean_df.count())
    clean_df.show()
    
    # LOAD DATA INTO SILVER 
    clean_df.createOrReplaceTempView("cleaned_batch")
    spark.sql(
        f"""
        INSERT INTO
            { NESSIE_CATALOG_NAME }.{ SILVER_NAMESPACE }.{ AMAZON_ORDERS_TABLE }
        SELECT
            *
        FROM
            cleaned_batch
        """
    )
    
    
except Exception as e:
    logging.info (e)
    raise
finally:
    spark.stop()