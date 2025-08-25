-- macro to overwrite dbt's generate schema name macro

{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name }}
{% endmacro %}