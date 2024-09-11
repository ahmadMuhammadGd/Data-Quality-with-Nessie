{{ config(
    unique_key='order_id',
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    pre_hook=["SET spark.sql.catalog.nessie.ref= {{ env_var('BRANCH_AMAZON_ORDERS_PIPELINE') }}"]
) }}

SELECT
    src.order_id          AS id,
    date_dim.id           AS date_id,
    curr_dim.id           AS currency_id,
    loc_dim.id            AS location_id,
    prod_dim.id           AS product_id,
    ship_dim.id           AS shipping_id,
    src.qty               AS quantity,
    ROUND(src.amount ,2)  AS amount

FROM
    {{ ref('amazon_orders_silver') }} as src
LEFT JOIN
    {{ ref('date_dim') }} as date_dim
ON
    src.order_date = date_dim.full_date
LEFT JOIN
    {{ ref('currency_dim') }} as curr_dim
ON
    src.currency = curr_dim.currency
LEFT JOIN
    {{ ref('location_dim') }} as loc_dim
ON
    src.Ship_Postal_Code = loc_dim.Ship_Postal_Code
LEFT JOIN
    {{ ref('product_dim') }} as prod_dim
ON
    src.category = prod_dim.category AND
    src.size = prod_dim.size
LEFT JOIN
    {{ ref('shipping_dim') }} as ship_dim
ON
    src.Order_Status        = ship_dim.shipping_status AND
    src.Fulfilment          = ship_dim.Fulfilment AND
    src.ship_service_level  = ship_dim.ship_service_level AND
    src.fulfilled_by        = ship_dim.fulfilled_by 
{% if is_incremental() %}
    WHERE datediff('day', Ingestion_Date, current_timestamp) < 2
{% endif %}
