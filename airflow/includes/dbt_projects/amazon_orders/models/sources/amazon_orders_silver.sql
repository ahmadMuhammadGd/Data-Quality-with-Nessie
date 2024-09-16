{{ config(
    unique_key='order_id',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ env_var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"],
    materialized='view',
) }}
    -- partition_by='MONTH(Ingested_at)',


SELECT
    *
FROM
    {{ source('amazon_csv_silver_orders', env_var('AMAZON_ORDERS_TABLE')) }}