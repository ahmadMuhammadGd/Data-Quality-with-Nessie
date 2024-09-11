{% macro scd0_incremental_load_with_mapping(source_table, dim_table, column_mapping, id_column) %}
    -- Extract source and target columns from the column mapping dictionary
    {% set source_columns = column_mapping.keys() %}
    {% set target_columns = column_mapping.values() %}

    {% if is_incremental() %}
        -- Identify the maximum ID in the target table
        WITH max_id AS (
            SELECT COALESCE(MAX({{ id_column }}), 0) AS max_id
            FROM {{ dim_table }}
        ),
        
        -- Identify new rows in the source table that don't exist in the target table
        new_rows AS (
            SELECT DISTINCT
                {% for src_col in source_columns %}
                    src.{{ src_col }}{% if not loop.last %},{% endif %}
                {% endfor %}
            FROM {{ source_table }} src
            LEFT JOIN {{ dim_table }} tgt
            ON
                {% for src_col, tgt_col in column_mapping.items() %}
                    src.{{ src_col }} = tgt.{{ tgt_col }}{% if not loop.last %} AND {% endif %}
                {% endfor %}
            WHERE tgt.{{ id_column }} IS NULL
        )
        
        -- Insert the new rows into the target table with a new ID
        SELECT 
            ROW_NUMBER() OVER (ORDER BY {% for src_col in source_columns %}src.{{ src_col }}{% if not loop.last %}, {% endif %}{% endfor %}) + (SELECT max_id FROM max_id) AS {{ id_column }},
            {% for src_col, tgt_col in column_mapping.items() %}
                src.{{ src_col }} AS {{ tgt_col }}{% if not loop.last %} , {% endif %}
            {% endfor %}
        FROM new_rows AS src;
    {% else %}
        
        -- Initial load: Create the table and load all rows
        WITH new_rows AS (
            SELECT DISTINCT
            {% for src_col, tgt_col in column_mapping.items() %}
                src.{{ src_col }} AS {{ tgt_col }}{% if not loop.last %} , {% endif %}
            {% endfor %}
        FROM {{ source_table }} AS src
        )

        SELECT
            ROW_NUMBER() OVER (ORDER BY {% for tgt_col in target_columns %}btch.{{ tgt_col }}{% if not loop.last %}, {% endif %}{% endfor %}) AS {{ id_column }},
            btch.*
        FROM new_rows AS btch
        
    {% endif %}
{% endmacro %}
