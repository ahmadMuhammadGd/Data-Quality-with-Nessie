import sys, os 
sys.path.insert(1, '/spark')
from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
import dotenv, glob
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME                 =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN                         =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE                     =os.getenv("BRANCH_VALIDATE")        
BRONZE_AMAZON_SALES_TABLE           =os.getenv("BRONZE_AMAZON_SALES_TABLE")        
SILVER_AMAZON_SALES_TABLE           =os.getenv("SILVER_AMAZON_SALES_TABLE")        
NAMESPACE                           =os.getenv("NAMESPACE")

MINIO_ACCESS_KEY                    = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY                    = os.getenv('MINIO_SECRET_KEY')
MINIO_ICEBERG_S3_BUCKET             = os.getenv('MINIO_ICEBERG_S3_BUCKET')
MINIO_LANDING_S3_BUCKET             = os.getenv('MINIO_LANDING_S3_BUCKET')
MINIO_END_POINT                     = os.getenv('MINIO_END_POINT')

spark = init_spark_session(app_name="tables and branches init")


try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG_NAME}.{NAMESPACE}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG_NAME}.keys")
    
    print("Namespaces .. done")
    
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.{NAMESPACE}.{BRONZE_AMAZON_SALES_TABLE}
        (
            Order_ID            string,
            order_Date          Date,
            Status              string,
            Fulfilment          string,
            Sales_Channel       string,
            ship_service_level  string,
            Category            string,
            Size                string,
            Courier_Status      string,
            Qty                 integer,
            currency            string,
            Amount              double,
            ship_city           string,
            ship_state          string,
            ship_postal_code    integer,
            ship_country        string,
            B2B                 boolean,
            fulfilled_by        string,
            New                 string,
            PendingS            string,
            ingestion_date      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (MONTH(order_Date));
        """
    )
    
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.{NAMESPACE}.{SILVER_AMAZON_SALES_TABLE}
        (
            Order_ID            string,
            order_Date          Date,
            Status              string,
            Fulfilment          string,
            Sales_Channel       string,
            ship_service_level  string,
            Category            string,
            Size                string,
            Courier_Status      string,
            Qty                 integer,
            currency            string,
            Amount              double,
            ship_city           string,
            ship_state          string,
            ship_postal_code    integer,
            ship_country        string,
            B2B                 boolean,
            fulfilled_by        string,
            New                 string,
            PendingS            string,
            ingestion_date      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (MONTH(order_Date));
        """
    )
    
    print("db tables .. done")

    keys_files_list = glob.glob(f"/unique_keys/*.csv/")
    print(f"keys_list: {keys_files_list}")
    
    for path in keys_files_list:
        file_name = path.split("/")[-2]
        file_name = file_name.split(".")[0]

        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path)
        
        df.createOrReplaceTempView("temp")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {NESSIE_CATALOG_NAME}.keys.{file_name}
            USING iceberg
            AS
            SELECT
                {file_name} AS {file_name}
            FROM
                temp
        """)
        spark.sql(f"""
            SELECT 
                * 
            FROM {NESSIE_CATALOG_NAME}.keys.{file_name}
            LIMIT 3
        """).show()
    
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    
    spark.sql('show tables in nessie').show()
    spark.sql('LIST REFERENCES in nessie').show()
    
    spark.stop()
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()