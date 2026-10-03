{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set target_schema = target.schema | trim | upper -%}
    {%- if custom_schema_name is none -%}
        {{ target_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ target_schema }}_{{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
