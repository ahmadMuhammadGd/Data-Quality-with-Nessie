{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ env_var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}


{% set source_table = ref('amazon_orders_silver') %}
{% set dim_table = 'shipping_dim' %}
{% set column_mapping = {
    'Order_Status'      :   'shipping_status',
    'Fulfilment'        :   'Fulfilment',
    'ship_service_level':   'ship_service_level',
    'fulfilled_by'      :   'fulfilled_by'
} %}
{% set id_column = 'id' %}
{{ scd0_incremental_load_with_mapping(source_table, dim_table, column_mapping, id_column) }}
