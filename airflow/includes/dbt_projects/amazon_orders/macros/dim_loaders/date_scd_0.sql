{% macro date_scd0_incremental_load_with_mapping(source_table, dim_table, date_column_mapping, id_column) %}
    -- Extract source and target columns from the column mapping dictionary
    {% set date_columns_list = date_column_mapping.items() | list %}
    {% set source_date_column = date_columns_list[0][0] %}
    {% set target_date_column = date_columns_list[0][1] %}

    {% if is_incremental() %}
    -- Identify the maximum ID in the target table
    WITH max_id AS (
        SELECT COALESCE(MAX({{ id_column }}), 0) AS max_id
        FROM {{ dim_table }}
    ),
    
    -- Identify new rows in the source table that don't exist in the target table
    new_rows AS (
        SELECT DISTINCT
            {{ source_date_column }}
        FROM {{ source_table }} src
        LEFT JOIN {{ dim_table }} tgt
        ON                
        src.{{ source_date_column }} = tgt.{{ target_date_column }}
        WHERE tgt.{{ id_column }} IS NULL
    )
    
    -- Insert the new rows into the target table with a new ID
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY {{ source_date_column }}) + (SELECT max_id FROM max_id) AS {{ id_column }},
        src.{{ source_date_column }}                           AS     full_date,
        EXTRACT(QUARTER FROM src.{{ source_date_column }})     AS     date_quarter,
        EXTRACT(MONTH   FROM src.{{ source_date_column }})     AS     date_month,
        EXTRACT(WEEK    FROM src.{{ source_date_column }})     AS     date_week,
        EXTRACT(DAY     FROM src.{{ source_date_column }})     AS     date_day,
        EXTRACT(YEAR    FROM src.{{ source_date_column }})     AS     date_year
    FROM new_rows AS src;
    
    {% else %}
    
    WITH new_rows AS 
    (
        SELECT DISTINCT
                src.{{ source_date_column }}                           AS     full_date,
                EXTRACT(QUARTER FROM src.{{ source_date_column }})     AS     date_quarter,
                EXTRACT(MONTH   FROM src.{{ source_date_column }})     AS     date_month,
                EXTRACT(WEEK    FROM src.{{ source_date_column }})     AS     date_week,
                EXTRACT(DAY     FROM src.{{ source_date_column }})     AS     date_day,
                EXTRACT(YEAR    FROM src.{{ source_date_column }})     AS     date_year
        FROM {{ source_table }} AS src
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY {{ target_date_column }}) AS {{ id_column }},
        src.*
    FROM 
        new_rows AS src;
    {% endif %}
{% endmacro %}
