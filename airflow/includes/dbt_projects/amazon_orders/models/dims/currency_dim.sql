{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}

WITH src AS (
    SELECT
        src.currency,
        MIN(src.ingestion_date) AS ingestion_date
    FROM 
        {{ ref('amazon_orders_silver') }} AS src
    
    
    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} AS dim
    ON
        dim.currency = src.currency
    WHERE
        dim.id IS NULL
    {% endif %}
    
    
    GROUP BY
        src.currency
)

SELECT
    {{ generate_id(this, 'id') }} AS id,
    src.*
FROM
    src
