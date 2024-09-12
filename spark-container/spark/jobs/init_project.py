"""
This script is designed to initialize a Spark-based data lakehouse environment using 
Nessie as the catalog and Iceberg as the table format. 
The process involves creating necessary namespaces, branches, and tables to set up a 
data pipeline for processing Amazon ORDERS data. The following steps outline the core 
functionality of the script:

1. Environment Setup: 
    - The script begins by setting up the environment, including loading necessary 
      Python modules and environment variables from a `.env` file.
    - Key environment variables include catalog names, namespace identifiers, and 
      table names that define the structure of the data lakehouse.

2. Spark Session Initialization: 
    - A Spark session is initiated, configured to interact with the Nessie catalog. 
      This session is crucial for executing SQL commands that manage the data lakehouse's structure.

3. Template Rendering: 
    - The script uses the Jinja2 templating engine to dynamically generate SQL queries 
      based on the loaded environment variables. 
    - The SQL queries are designed to create necessary tables and structures in the data
      lakehouse, tailored to specific business requirements.

4. Namespace and Branch Creation:
    - The script programmatically creates the necessary namespaces (e.g., `pipeline`, `bronze`, `silver`,
      `gold`) within the Nessie catalog. These namespaces represent different layers of data processing and
      storage.
    - It also creates branches in Nessie, ensuring version control and enabling isolated 
      environments for data processing tasks.

5. Execution of SQL Queries:
    - Once the namespaces and branches are set up, the script executes the SQL queries generated
      earlier. These queries create tables for storing raw, cleaned, and transformed data, 
      which are vital for subsequent data processing and analytics.

6. Summary and Validation:
    - After the tables and branches are created, the script provides a summary by listing all the
      tables and references within the Nessie catalog, ensuring everything is set up correctly.
    - The Spark session is then terminated, ensuring that all resources are properly released.
"""

from env_loader import *
import logging

required_namespaces=[
    AMAZON_PIPELINE_NAMESPACE,
    BRONZE_NAMESPACE,
    SILVER_NAMESPACE,
    GOLD_NAMESPACE,
]

required_branches=[
    BRANCH_MAIN, #main is created by default 
]


from jinja2 import Environment, FileSystemLoader # type: ignore
env = Environment(loader = FileSystemLoader(DML_TEMPLATE_INIT_PATH))
init_sql_template = env.get_template('lakehouse-init.sql')
init_sql_queries=init_sql_template.render(TABLE_NAMES_ARGS)


from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
spark = init_or_get_spark_session(app_name="tables and branches init")


try:
    for namespace in required_namespaces:
        logging.info(f"Creating \"{NESSIE_CATALOG_NAME}.{namespace}\" namespace")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG_NAME}.{namespace}")    
        logging.info(f"Namespace\"{NESSIE_CATALOG_NAME}.{namespace}\": Done ..!")
    
    logging.info("*"*35)
    
    for branch in required_branches:
        if branch != BRANCH_MAIN:
            logging.info(f"Dropping \"{branch}\" if exists ..")
            spark.sql(f"DROP BRANCH IF EXISTS {branch} IN {NESSIE_CATALOG_NAME}")
        logging.info(f"Creating \"{branch}\" nessie branch ..")
        spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN {NESSIE_CATALOG_NAME}")
        logging.info(f"Branch \"{branch}\": Done ..!")
    
    logging.info("*"*35)
    
    
    # create or replace data lakehouse tables 
    for query in init_sql_queries.split(';'):
        if query.strip():
            logging.info(query)
            spark.sql(query)           
    
    
    logging.info("Summary:")
    for branch in required_branches:
        if branch != BRANCH_MAIN:
        # this line will not excuted
            logging.info(f"Merging {BRANCH_MAIN} branch into {branch}")
            spark.sql(f"MERGE BRANCH {BRANCH_MAIN} INTO {branch} IN {NESSIE_CATALOG_NAME}")

        logging.info(f"Tables in branch: {branch}")
        spark.sql(f"USE REFERENCE {branch} IN {NESSIE_CATALOG_NAME}")
        spark.sql(f'SHOW TABLES IN {NESSIE_CATALOG_NAME}').show()    
    spark.stop()
except Exception as e:
    logging.info (e)
    raise
finally:
    spark.stop()