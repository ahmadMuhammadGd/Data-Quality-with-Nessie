import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Setup logging
logging.basicConfig(level=logging.INFO)


from modules.SparkIcebergNessieMinIO import CustomSparkConfig
from env_loader import *

def init_or_get_spark_session(app_name:str, master:str='local[*]', direct_s3_read_write:bool=False)-> SparkSession:
    """
    Initialize or retrieve a Spark session.

    Args:
        app_name (str): The application name for the Spark session.
        master (str): The master URL for the Spark session.
        direct_s3_read_write (bool): Whether to enable direct S3 read/write.

    Returns:
        SparkSession: The initialized or retrieved Spark session.
    """
    try:
        # Check if a Spark session already exists
        existing_spark_sessions = None
        
        try:
            existing_spark_sessions = [s for s in SparkSession._instantiatedSession if s and s._jvm.SparkSession.activeSession().appName() == app_name]
        except TypeError:
            pass
        
        if existing_spark_sessions is not None:
            return existing_spark_sessions[0]   

        conf = CustomSparkConfig.IceBergNessieMinio(
            nessie_url                      = NESSIE_URI,
            minio_access_key                = MINIO_ACCESS_KEY,
            minio_secret_key                = MINIO_SECRET_KEY,
            minio_s3_bucket_path            = MINIO_ICEBERG_S3_BUCKET,
            minio_endpoint                  = MINIO_ENDPOINT,
            main_nessie_ref                 = BRANCH_MAIN,
            direct_read_write_minio         = direct_s3_read_write
        )\
        .init()
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config(conf=conf)\
            .getOrCreate()
        
        logging.debug("Spark Configuration:")
        for item in spark.sparkContext.getConf().getAll():
            logging.debug(f"{item[0]} = {item[1]}")
        
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        raise