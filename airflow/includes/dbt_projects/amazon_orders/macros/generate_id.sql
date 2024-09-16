{% macro generate_id(table, id_column) %}
    {% if is_incremental() %}
        (SELECT COALESCE(MAX({{ id_column }}), 0) FROM {{ table }}) + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    {% else %}
        (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
    {% endif %}
{% endmacro %}
