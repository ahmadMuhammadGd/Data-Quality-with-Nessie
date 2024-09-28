{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}


WITH src AS (
    SELECT
        src.order_status AS shipping_status,
        src.Fulfilment,
        src.ship_service_level,
        src.fulfilled_by,
        MIN(src.ingestion_date) AS ingestion_date
    FROM 
        {{ ref('amazon_orders_silver') }} AS src
    
    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} AS dim
    ON
        src.order_status       =   dim.shipping_status 
    AND
        src.Fulfilment         =   dim.Fulfilment 
    AND
        src.ship_service_level =   dim.ship_service_level 
    AND
        src.fulfilled_by       =   dim.fulfilled_by
    WHERE
        dim.id IS NULL
    {% endif %}
    
    
    GROUP BY
        src.order_status,
        src.Fulfilment,
        src.ship_service_level,
        src.fulfilled_by
)

SELECT
    {{ generate_id(this, 'id') }} AS id,
    src.*
FROM
    src