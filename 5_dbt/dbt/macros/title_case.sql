{% macro title_case(column_name) %}
    INITCAP({{ column_name }})
{% endmacro %}
