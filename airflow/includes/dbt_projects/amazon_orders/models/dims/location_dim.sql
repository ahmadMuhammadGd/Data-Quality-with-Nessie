{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    partition_by=['ship_country', 'ship_state'],
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}


WITH src AS (
    SELECT
        src.ship_country,
        src.ship_state,
        src.ship_city,
        src.ship_postal_code,
        MIN(src.ingestion_date) AS ingestion_date
    FROM 
        {{ ref('amazon_orders_silver') }} AS src
    
    
    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} AS dim
    ON
        dim.ship_country     =  src.ship_country
    AND
        dim.ship_state       =  src.ship_state
    AND
        dim.ship_city        =  src.ship_city
    AND
        dim.ship_postal_code =  src.ship_postal_code
    WHERE
        dim.id IS NULL
    {% endif %}
    
    
    GROUP BY
        src.ship_country,
        src.ship_state,
        src.ship_city,
        src.ship_postal_code
)

SELECT
    {{ generate_id(this, 'id') }} AS id,
    src.*
FROM
    src