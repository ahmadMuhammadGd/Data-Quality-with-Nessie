import sys
from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
from env_loader import *


from modules.CLI import cli
object_path, timestamp, nessie_branch = cli()

spark = init_or_get_spark_session(app_name=f"merge into {BRANCH_MAIN} branch")

try:
    spark.sql(f"USE REFERENCE {nessie_branch} IN {NESSIE_CATALOG_NAME}")
    
    logging.info("merging validation branch into main ..")
    spark.sql(f"MERGE BRANCH {nessie_branch} INTO {BRANCH_MAIN} IN {NESSIE_CATALOG_NAME}")
    logging.info("done")
    
    logging.info("switching to main branch ..")
    spark.sql(f"USE REFERENCE {BRANCH_MAIN} IN {NESSIE_CATALOG_NAME}")
    logging.info("done")
    
    spark.sql('LIST REFERENCES in nessie').show()
    spark.stop()
except Exception as e:
    logging.info (e)
    raise
finally:
    spark.stop()