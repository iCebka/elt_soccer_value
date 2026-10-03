{% macro report_effective_schemas() %}
    {% set staging_schema = generate_schema_name(env_var('SNOWFLAKE_STAGING_SCHEMA', 'STAGING'), none) | trim %}
    {% set silver_schema = generate_schema_name(env_var('SNOWFLAKE_SILVER_SCHEMA', 'SILVER'), none) | trim %}
    {% do log('target=' ~ target.name ~ '; staging=' ~ staging_schema ~ '; silver=' ~ silver_schema, info=true) %}
    {{ return({'target': target.name, 'staging': staging_schema, 'silver': silver_schema}) }}
{% endmacro %}
