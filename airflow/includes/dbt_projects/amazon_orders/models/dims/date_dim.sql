{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ env_var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"],
    partition_by='MONTH(full_date)'
) }}

{% set source_table = ref('amazon_orders_silver') %}

{% set dim_table = 'date_dim' %}

{% set date_column_mapping = {
    'order_date': 'full_date',
} %}
{% set id_column = 'id' %}

{{ date_scd0_incremental_load_with_mapping(source_table, dim_table, date_column_mapping, id_column) }}
