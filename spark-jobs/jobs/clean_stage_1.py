import sys, os 
sys.path.insert(1, '/spark-jobs')

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from modules.Data_cleaner.Interface import CleaningPipeline
from modules.Data_cleaner.Strategies import (
    DropDuplicatesStrategy,
    DropMissingValuesStrategy,
    FilterNegativeValuesStrategy,
    ValidateDatesStrategy
)
import dotenv
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME     =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN             =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE         =os.getenv("BRANCH_VALIDATE")        

spark = init_spark_session(app_name="data cleaning")


try:    
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv("/batchs/sampled_data_2.csv/sampled_data_2.csv")
    
    cleaning_strategies=[
        DropMissingValuesStrategy(columns=['Qty', 'Amount', 'Order_ID', 'Date', 'currency']),
        FilterNegativeValuesStrategy(columns=['Qty', 'Amount']),
        ValidateDatesStrategy(columns=['Date'], date_format="MM-dd-yy"),
        DropDuplicatesStrategy(),
    ]
    
    print('source data')
    df.show()
    cleaner = CleaningPipeline()
    cleaner.set_dataframe(df=df)
    cleaner.add_strategy(cleaning_strategies)
    clean_df = cleaner.run()
    clean_df.show()
    clean_df.createOrReplaceTempView('stage_1')
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.db.orders_1
        USING iceberg
        AS SELECT
        {', '.join([f'''
            {col} AS {col}
        ''' 
        for col in clean_df.columns
        ])}
        FROM stage_1  
    """)
    spark.sql(f"SELECT COUNT(*) AS cnt_1 FROM {NESSIE_CATALOG_NAME}.db.orders_1").show()
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.db.orders_1").show()
    spark.stop()
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()