import sys, os 
sys.path.insert(1, '/spark-jobs')

from modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
import dotenv
dotenv.load_dotenv('/config.env')

NESSIE_CATALOG_NAME     =os.getenv("NESSIE_CATALOG_NAME")       
BRANCH_MAIN             =os.getenv("BRANCH_MAIN")        
BRANCH_VALIDATE         =os.getenv("BRANCH_VALIDATE")        

spark = init_spark_session(app_name="insert into main")

sql=f"""
INSERT INTO {NESSIE_CATALOG_NAME}.db.orders
SELECT 
    * 
FROM 
    {NESSIE_CATALOG_NAME}.db.orders_2
"""
try:
    spark.sql(f"USE REFERENCE {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    spark.sql(f"SELECT COUNT(*) AS main_orders_cnt_b4_insert FROM {NESSIE_CATALOG_NAME}.db.orders").show()
    spark.sql(sql)
    print("merging validation branch into main ..")
    spark.sql(f"MERGE BRANCH {BRANCH_VALIDATE} INTO {BRANCH_MAIN} IN {NESSIE_CATALOG_NAME}")
    print("done")
    print("dropping valdiation branch ..")
    spark.sql(f"DROP BRANCH IF EXISTS {BRANCH_VALIDATE} IN {NESSIE_CATALOG_NAME}")
    print("done")
    print("switching to main branch ..")
    spark.sql(f"USE REFERENCE {BRANCH_MAIN} IN {NESSIE_CATALOG_NAME}")
    print("done")
    spark.sql(f"SELECT COUNT(*) AS main_orders_cnt_after_insert FROM {NESSIE_CATALOG_NAME}.db.orders").show()
    spark.sql(f"SELECT * FROM {NESSIE_CATALOG_NAME}.db.orders").show()
    spark.sql('LIST REFERENCES in nessie').show()
    spark.stop()
except Exception as e:
    print (e)
    raise
finally:
    spark.stop()