import dotenv, os, logging
from glob import glob
logging.basicConfig(level=logging.INFO)

ENV_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

files_path = glob(os.path.join(ENV_DIR_PATH, '*.env'))

for file in files_path:
    result = dotenv.load_dotenv(dotenv_path=file, override=True)
    if result:
        logging.info(f'Successfully loaded `{file}')
    else:
        logging.error(f'Failed to load the file {file}')
        raise RuntimeError
        

NESSIE_CATALOG_NAME                     = os.getenv('NESSIE_CATALOG_NAME')
BRANCH_MAIN                             = os.getenv('BRANCH_MAIN')
BRANCH_AMAZON_ORDERS_PIPELINE           = os.getenv('BRANCH_AMAZON_ORDERS_PIPELINE')
AMAZON_PIPELINE_NAMESPACE               = os.getenv('AMAZON_PIPELINE_NAMESPACE')
BRONZE_NAMESPACE                        = os.getenv('BRONZE_NAMESPACE')
SILVER_NAMESPACE                        = os.getenv('SILVER_NAMESPACE')
GOLD_NAMESPACE                          = os.getenv('GOLD_NAMESPACE')

AMAZON_ORDERS_TABLE                     = os.getenv('AMAZON_ORDERS_TABLE')
DATE_DIMENSION_NAME                     = os.getenv('DATE_DIMENSION_NAME')
LOCATION_DIMENSION_NAME                 = os.getenv('LOCATION_DIMENSION_NAME')
PRODUCT_DIMENSION_NAME                  = os.getenv('PRODUCT_DIMENSION_NAME')
CURRENCY_DIMENSION_NAME                 = os.getenv('CURRENCY_DIMENSION_NAME')
SHIPPING_DIMENSION_NAME                 = os.getenv('SHIPPING_DIMENSION_NAME')
FACT_ORDERS_NAME                        = os.getenv('FACT_ORDERS_NAME')

BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH   = os.getenv('BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH')
SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH   = os.getenv('SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH')

AMAZON_PIPELINE_NAMESPACE               = os.getenv('AMAZON_PIPELINE_NAMESPACE')
AMAZON_ETL_METADATA                     = os.getenv('AMAZON_ETL_METADATA')
DATE_DIMENSION_NAME                     = os.getenv('DATE_DIMENSION_NAME')

TABLE_NAMES_ARGS = {
    "NESSIE_CATALOG_NAME"                  :        NESSIE_CATALOG_NAME,
    "BRANCH_MAIN"                          :        BRANCH_MAIN,
    "AMAZON_PIPELINE_NAMESPACE"            :        AMAZON_PIPELINE_NAMESPACE,
    "BRONZE_NAMESPACE"                     :        BRONZE_NAMESPACE,
    "SILVER_NAMESPACE"                     :        SILVER_NAMESPACE,
    "GOLD_NAMESPACE"                       :        GOLD_NAMESPACE,
    "AMAZON_ORDERS_TABLE"                  :        AMAZON_ORDERS_TABLE,
    "DATE_DIMENSION_NAME"                  :        DATE_DIMENSION_NAME,
    "LOCATION_DIMENSION_NAME"              :        LOCATION_DIMENSION_NAME,
    "PRODUCT_DIMENSION_NAME"               :        PRODUCT_DIMENSION_NAME,
    "CURRENCY_DIMENSION_NAME"              :        CURRENCY_DIMENSION_NAME,
    "SHIPPING_DIMENSION_NAME"              :        SHIPPING_DIMENSION_NAME,
    "FACT_ORDERS_NAME"                     :        FACT_ORDERS_NAME,
    "BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH":        BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH,
    "SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH":        SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH,
    "AMAZON_PIPELINE_NAMESPACE"            :        AMAZON_PIPELINE_NAMESPACE,
    "AMAZON_ETL_METADATA"                  :        AMAZON_ETL_METADATA,
    "DATE_DIMENSION_NAME"                  :        DATE_DIMENSION_NAME,
}


DML_TEMPLATE_INIT_PATH                  = os.getenv("DML_TEMPLATE_INIT_PATH")
JSON_CONFIG_PATH                        = os.getenv("JSON_CONFIG_PATH")
DDL_TEMPLATE_PATH                       = os.getenv("DDL_TEMPLATE_PATH")


NESSIE_URI                              = os.getenv("NESSIE_URI")
MINIO_ACCESS_KEY                        = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY                        = os.getenv("MINIO_SECRET_KEY")
MINIO_ICEBERG_S3_BUCKET                 = os.getenv("MINIO_ICEBERG_S3_BUCKET")
MINIO_ENDPOINT                          = os.getenv("MINIO_ENDPOINT")
