{% macro export_transfermarkt_manifest() %}
    {% if not execute %}
        {{ return({}) }}
    {% endif %}
    {% set result = run_query(
        'select asset_name, bronze_ingestion_run_id, source_version_checksum '
        ~ 'from ' ~ ref('base_tm__source_manifest') ~ ' order by asset_name'
    ) %}
    {% set manifest = {} %}
    {% for row in result.rows %}
        {% do manifest.update({
            row[0]: {
                'ingestion_run_id': row[1],
                'source_file_sha256': row[2]
            }
        }) %}
    {% endfor %}
    {% do log(tojson(manifest), info=true) %}
    {{ return(manifest) }}
{% endmacro %}
