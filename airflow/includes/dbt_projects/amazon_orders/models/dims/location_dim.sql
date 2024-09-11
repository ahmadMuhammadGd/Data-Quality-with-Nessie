{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    partition_by=['ship_country', 'ship_state'],
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ env_var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}


{% set source_table = ref('amazon_orders_silver') %}
{% set dim_table = 'location_dim' %}
{% set column_mapping = {
    'ship_country'     :  'ship_country',
    'ship_state'       :  'ship_state',
    'ship_city'        :  'ship_city',
    'ship_postal_code' :  'ship_postal_code'
} %}
{% set id_column = 'id' %}
{{ scd0_incremental_load_with_mapping(source_table, dim_table, column_mapping, id_column) }}
