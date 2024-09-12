-----------------------------------------------------
--               BRONZE TABLES                     --
-----------------------------------------------------
CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ BRONZE_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }}
(
    Order_ID            string,
    Order_Date          Date,
    Order_Status        string,
    Fulfilment          string,
    ORDERS_Channel      string,
    ship_service_level  string,
    Category            string,
    Size                string,
    Courier_Status      string,
    Qty                 integer,
    Currency            string,
    Amount              double,
    Ship_City           string,
    Ship_State          string,
    Ship_Postal_Code    integer,
    Ship_Country        string,
    B2B                 boolean,
    Fulfilled_By        string,
    New                 string,
    PendingS            string,
    Ingestion_Date      TIMESTAMP
)
USING iceberg
PARTITIONED BY (MONTH(Order_Date));


-----------------------------------------------------
--                 SILVER TABLES                   --
-----------------------------------------------------

CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ SILVER_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }}
(
    Order_ID            string,
    Order_Date          Date,
    Order_Status        string,
    Fulfilment          string,
    ORDERS_Channel      string,
    ship_service_level  string,
    Category            string,
    Size                string,
    Courier_Status      string,
    Qty                 integer,
    Currency            string,
    Amount              double,
    Ship_City           string,
    Ship_State          string,
    Ship_Postal_Code    integer,
    Ship_Country        string,
    B2B                 boolean,
    Fulfilled_By        string,
    New                 string,
    PendingS            string,
    Ingestion_Date      TIMESTAMP
)
USING iceberg
PARTITIONED BY (MONTH(Order_Date));

-----------------------------------------------------
--                 GOLD DIMENSIONS                 --
-----------------------------------------------------

-- done -- DATE DIMENSION 
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ DATE_DIMENSION_NAME }}
-- (
--     date_id         BIGINT NOT NULL,
--     full_date       DATE,
--     date_quarter    INT ,
--     date_month      INT ,
--     date_week       INT ,
--     date_day        INT ,
--     date_year       INT 
-- )
-- USING iceberg
-- PARTITIONED BY (MONTH(full_date))

-- done ..--LOCATION_DIMENSION
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ LOCATION_DIMENSION_NAME }}
-- (
--     location_id         bigint NOT NULL,
--     ship_country        STRING,
--     ship_state          STRING,
--     ship_city           STRING,
--     ship_postal_code    INT
-- )
-- USING iceberg
-- PARTITIONED BY (ship_country, ship_state)

-- done --PRODUCT_DIMENSION
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ PRODUCT_DIMENSION_NAME }}
-- (
--     product_id         bigint NOT NULL,
--     category           STRING,
--     product_size       VARCHAR(5)
-- )
-- USING iceberg
-- PARTITIONED BY (category)

-- --CURRENCY DIM
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ CURRENCY_DIMENSION_NAME }}
-- (
--     currency_id        bigint NOT NULL,
--     currency           VARCHAR(5)
-- )
-- USING iceberg

-- done --SHIPPING DIM
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ SHIPPING_DIMENSION_NAME }}
-- (
--     shipping_id         bigint NOT NULL,
--     shipping_status     STRING,
--     Fulfilment          STRING,
--     ship_service_level  STRING,
--     fulfilled_by        STRING
-- )
-- USING iceberg
-- PARTITIONED BY (fulfilled_by, ship_service_level)

-- --FACT ORDERS TABLE
-- CREATE OR REPLACE TABLE {{ NESSIE_CATALOG_NAME }}.{{ GOLD_NAMESPACE }}.{{ FACT_ORDERS_NAME }}
-- (
--     order_id           STRING,
--     date_id            bigint,
--     location_id        bigint,
--     product_id         bigint,
--     currency_id        bigint,
--     shipping_id        bigint,
--     qty                int NOT NULL,
--     amount             decimal(18, 2) NOT NULL
-- )
-- USING iceberg
-- PARTITIONED BY (date_id, location_id)

-----------------------------------------------------
--                      ETL_VIEWS                  --
-----------------------------------------------------
CREATE OR REPLACE VIEW {{ NESSIE_CATALOG_NAME }}.{{ AMAZON_PIPELINE_NAMESPACE }}.{{ BRONZE_AMAZON_ORDERS_TABLE_LAST_BATCH }}
AS
(
    WITH max_Ingestion_Date AS (
        SELECT
            MAX(Ingestion_Date) AS mx_igdate
        FROM 
            {{ NESSIE_CATALOG_NAME }}.{{ BRONZE_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }}
    )
    SELECT
        src.*
    FROM 
        {{ NESSIE_CATALOG_NAME }}.{{ BRONZE_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }} AS src
    WHERE
        src.Ingestion_Date = (SELECT mx_igdate FROM max_Ingestion_Date)
);

CREATE OR REPLACE VIEW {{ NESSIE_CATALOG_NAME }}.{{ AMAZON_PIPELINE_NAMESPACE }}.{{ SILVER_AMAZON_ORDERS_TABLE_LAST_BATCH }}
AS
(
    WITH max_Ingestion_Date AS (
        SELECT
            MAX(Ingestion_Date) AS mx_igdate
        FROM 
            {{ NESSIE_CATALOG_NAME }}.{{ SILVER_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }}
    )
    SELECT
        src.*
    FROM 
        {{ NESSIE_CATALOG_NAME }}.{{ SILVER_NAMESPACE }}.{{ AMAZON_ORDERS_TABLE }} AS src
    WHERE
        src.Ingestion_Date = (SELECT mx_igdate FROM max_Ingestion_Date)
);