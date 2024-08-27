import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Setup logging
logging.basicConfig(level=logging.INFO)

sys.path.insert(1, '/spark')
from modules.SparkIcebergNessieMinIO import CustomSparkConfig

import dotenv
dotenv.load_dotenv('/config.env', override=True )
NESSIE_URI                  = os.getenv('NESSIE_URI')
MINIO_ACCESS_KEY            = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY            = os.getenv('MINIO_SECRET_KEY')
MINIO_ICEBERG_S3_BUCKET     = os.getenv('MINIO_ICEBERG_S3_BUCKET')
MINIO_END_POINT             = os.getenv('MINIO_END_POINT')


def init_spark_session(app_name:str)->SparkSession:    
    conf = CustomSparkConfig.IceBergNessieMinio(
                nessie_url                      = NESSIE_URI,
                minio_access_key                = MINIO_ACCESS_KEY,
                minio_secret_key                = MINIO_SECRET_KEY,
                MINIO_ICEBERG_S3_BUCKET_path    = MINIO_ICEBERG_S3_BUCKET,
                minio_endpoint                  = MINIO_END_POINT
            )\
            .init()
    return SparkSession.builder\
        .appName(app_name)\
        .config(conf=conf)\
        .getOrCreate()