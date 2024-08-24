



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





import sys, os 
sys.path.insert(1, '/spark-jobs')

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from SparkCleaner import *
import dotenv
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME                 =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN                         =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE                     =os.getenv("BRANCH_VALIDATE")        
BRONZE_AMAZON_SALES_TABLE           =os.getenv("BRONZE_AMAZON_SALES_TABLE")        
SILVER_AMAZON_SALES_TABLE           =os.getenv("SILVER_AMAZON_SALES_TABLE")        
NAME_SPACE                          =os.getenv("NAME_SPACE")


spark = init_spark_session(app_name="data cleaning")


try:    
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    df = spark.sql(
        f"""
        WITH recent_batch_date AS (
            SELECT 
                max(ingestion_date) AS max_date
            FROM 
                {NESSIE_CATALOG_NAME}.{NAME_SPACE}.{BRONZE_AMAZON_SALES_TABLE}
        ),
        most_recent_batch AS (
            SELECT 
                * 
            FROM 
                {NESSIE_CATALOG_NAME}.{NAME_SPACE}.{BRONZE_AMAZON_SALES_TABLE}
            WHERE
                ingestion_date = (
                    SELECT
                        max_date
                    FROM
                        recent_batch_date
                )
        )
        SELECT * FROM most_recent_batch;
        """
    )

    print('Most recent batch')
    df.show()
    
    # now, this is the cleaning part !
    cleaning_strategies=[
        DropMissingValuesStrategy(columns=['Qty', 'Amount', 'Order_ID', 'order_Date', 'currency']),
        FilterNegativeValuesStrategy(columns=['Qty', 'Amount']),
        ValidateDatesStrategy(columns=['order_Date'], date_format="MM-dd-yy"),
        DropDuplicatesStrategy(columns=['Order_ID', 'order_Date']),
    ]
    
    cleaner = CleaningPipeline()
    cleaner.set_dataframe(df=df)
    cleaner.add_strategy(cleaning_strategies)
    clean_df = cleaner.run()
    
    print('STAGE 1: cleaned patch')
    print('source df:', df.count())
    print('stage1: cleaned_df:', clean_df.count())
    
    
    clean_df.createOrReplaceTempView("cleaned_batch")
    # before loading, we need to check spelling for some columns
    # picking up columns that has determined values
    # such as: tags, size, status, category, etc
    # this stage should discover if there any typos in these columns
    
    import glob
    target_col_names = [os.path.splitext(os.path.basename(file_name))[0] for file_name in glob.glob('/unique_keys/*.csv')]
    
    # prepared sql
    sql = f"""
    CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.{NAME_SPACE}.{SILVER_AMAZON_SALES_TABLE}_temp
    USING ICEBERG AS
    SELECT
        {', '.join([
            f'ROUND(src.{col}, 2) AS Amount' if col == 'Amount' else f'src.{col}'
            for col in df.columns
        ])}
    FROM 
        cleaned_batch AS src
    WHERE
        {" AND ".join([
            f'''
            src.{col} IN (
                SELECT 
                    {col}
                FROM
                    {NESSIE_CATALOG_NAME}.keys.{col}
            )
            '''.strip() for col in target_col_names if col != 'Date'
        ])}
    """
    print('''
\n\nSTAGE2 query:
Create or replace a temporary table in Iceberg format with the cleaned data.
- Select columns from `cleaned_batch`, rounding 'Amount' values to 2 decimal places.
- Filter rows where column values match entries in corresponding reference tables.
\n''', sql)
    
    spark.sql(sql)
    
    print('\n\nSTAGE 2: cleaned patch')
    print('Source df:', df.count())
    print('Stage1: cleaned_df:', clean_df.count())
    print('Stage2: cleaned_df:', spark.sql(f"SELECT count(*) AS cnt FROM {NESSIE_CATALOG_NAME}.{NAME_SPACE}.{SILVER_AMAZON_SALES_TABLE}_temp").collect()[0]['cnt'])
    
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()