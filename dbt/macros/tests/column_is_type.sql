{% test column_is_type(model, column_name, expected_type) %}
select table_catalog, table_schema, table_name, column_name, data_type
from {{ model.database }}.information_schema.columns
where upper(table_schema) = upper('{{ model.schema }}')
  and upper(table_name) = upper('{{ model.identifier }}')
  and upper(column_name) = upper('{{ column_name }}')
  and upper(data_type) <> upper('{{ expected_type }}')
{% endtest %}
