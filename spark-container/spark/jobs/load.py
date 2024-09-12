# import sys, os 
# from modules.SparkIcebergNessieMinIO.spark_setup import init_or_get_spark_session
# from env_loader import *
# from jinja2 import FileSystemLoader, Environment
# import json, logging


# sql_env                                 = Environment(loader = FileSystemLoader(DML_TEMPLATE_PATH))
# load_date_dim_template                  = sql_env.get_template("load_date_dim.sql")
# load_dimesions_template                 = sql_env.get_template("load_dimesions.sql")
# load_orders_fact_template               = sql_env.get_template("load_orders_fact.sql")

# config_env                              = Environment(loader = FileSystemLoader(JSON_CONFIG_PATH))
# order_dimensions_conf                   = config_env.get_template("orders_dimesions.json")
# orders_fact_conf                        = config_env.get_template("orders_fact.json")


# # render config 
# configs = [order_dimensions_conf, orders_fact_conf]
# for i, conf in enumerate(configs):
#     rendered_conf = conf.render(TABLE_NAMES_ARGS)
#     configs[i] = json.loads(rendered_conf)
# order_dimensions_conf = configs[0]
# orders_fact_conf = configs[1]

# logging.info(
#     f"""
# {'*'*45}
#     configs:
#     {json.dumps(order_dimensions_conf, indent=4)}
# {'-'*45}
#     {json.dumps(orders_fact_conf, indent=4)}
#     """
# )

# # render date_dim_sql
# date_dim_DML = load_date_dim_template.render(TABLE_NAMES_ARGS)

# # render fact
# fact_DML = load_orders_fact_template.render({
#     **TABLE_NAMES_ARGS,
#     "dimensions": order_dimensions_conf
# })

# # render other dims
# dims_query_DML=[]
# for dim in order_dimensions_conf:
#     if ('date' in dim['dimension_table']) or ('fact' in dim['dimension_table']):
#         continue
    
#     sql_query = load_dimesions_template.render(
#         dimension_table=dim['dimension_table'],
#         source_table=dim['source_table'],
#         source_columns=dim['source_columns'],
#         source_dim_attr_to_match=dim['source_dim_attr_to_match'],
#         dimension_id=dim['dimension_id'],
#         dimension_columns=dim['dimension_columns']
#     )
    
#     dims_query_DML.append(sql_query)


# dml_queries=[*dims_query_DML, fact_DML, date_dim_DML]

# logging.info(
#     f"""
# {'*'*45}
#     DML queries:
#     {f'''
# {'-'*45}
#     '''.join([f'{query}' for query in dml_queries])}
#     """
# )

# spark = init_or_get_spark_session(app_name="data loading")

# try:
#     for sql in dml_queries:
#         if sql:
#             spark.sql(sql)
# except Exception as e:
#     print (e)
#     raise
# finally:
#     spark.stop()