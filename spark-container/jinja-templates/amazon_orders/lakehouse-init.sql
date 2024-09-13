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