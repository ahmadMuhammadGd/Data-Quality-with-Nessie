{{ config(
    unique_key='id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"],
    partition_by='MONTH(full_date)'
) }}

WITH date_series AS (
    SELECT
        src.order_date AS full_date,
        MIN(ingestion_date) AS ingestion_date
    FROM
        {{ ref('amazon_orders_silver') }} AS src
    
    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} AS dim
    ON
        dim.full_date = src.order_date
    WHERE
        dim.id IS NULL
    {% endif %}
    
    GROUP BY
        src.order_date
),
date_attributes AS (
    SELECT
        {{ generate_id(this, 'id') }} AS id,
        full_date AS full_date,
        EXTRACT (YEAR FROM full_date) AS year,
        EXTRACT (MONTH FROM full_date) AS month,
        EXTRACT (DAY FROM full_date) AS day,
        CASE 
            WHEN EXTRACT(MONTH FROM full_date) IN (1, 2, 12) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season,
        WEEKOFYEAR(full_date) AS week_of_year,
        ingestion_date
    FROM
        date_series
)

SELECT 
    *
FROM 
    date_attributes