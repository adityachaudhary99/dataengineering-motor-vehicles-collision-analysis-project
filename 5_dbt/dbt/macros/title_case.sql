{% macro title_case(column_name) %}
    UPPER(SUBSTRING({{ column_name }}, 1, 1)) || LOWER(SUBSTRING({{ column_name }}, 2))
{% endmacro %}