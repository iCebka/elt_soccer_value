{% macro tm_text(expression) -%}
    nullif(trim({{ expression }}::varchar), '')
{%- endmacro %}

{% macro tm_lower_text(expression) -%}
    lower({{ tm_text(expression) }})
{%- endmacro %}

{% macro tm_upper_text(expression) -%}
    upper({{ tm_text(expression) }})
{%- endmacro %}

{% macro tm_country_name_key(expression) -%}
    nullif(
        upper(regexp_replace(trim({{ expression }}), '[[:space:]]+', ' ')),
        ''
    )
{%- endmacro %}

{% macro tm_integer(expression) -%}
    try_to_number({{ tm_text(expression) }}, 38, 0)::number(38, 0)
{%- endmacro %}

{% macro tm_decimal(expression, scale=2) -%}
    try_to_decimal({{ tm_text(expression) }}, 38, {{ scale }})::number(38, {{ scale }})
{%- endmacro %}

{% macro tm_date(expression) -%}
    try_to_date({{ tm_text(expression) }})
{%- endmacro %}

{% macro tm_boolean_01(expression) -%}
    case lower({{ tm_text(expression) }})
        when '1' then true
        when '0' then false
        when 'true' then true
        when 'false' then false
        else null
    end
{%- endmacro %}

{% macro tm_euro_amount(expression) -%}
    {{ tm_decimal(expression, 2) }}
{%- endmacro %}

{% macro tm_euro_text_amount(expression) -%}
    case
        when {{ tm_text(expression) }} is null then null
        when {{ tm_text(expression) }} = '+-0' then 0::number(38, 2)
        when regexp_like(
            lower({{ tm_text(expression) }}),
            '^[+]?(€)?-?[0-9]+([.][0-9]+)?(k|m|bn)?$'
        ) then (
            try_to_decimal(
                regexp_replace(lower({{ tm_text(expression) }}), '[^0-9.-]', ''),
                38,
                2
            )
            * case
                when lower({{ tm_text(expression) }}) like '%bn' then 1000000000
                when lower({{ tm_text(expression) }}) like '%m' then 1000000
                when lower({{ tm_text(expression) }}) like '%k' then 1000
                else 1
              end
        )::number(38, 2)
        else null
    end
{%- endmacro %}

{% macro tm_selected_bronze_rows(asset_name) -%}
    select
        raw.raw_record,
        raw.source_url,
        raw.source_file,
        raw.source_file_sha256 as source_version_checksum,
        raw.source_row_number,
        raw.ingestion_run_id as bronze_ingestion_run_id,
        raw.loaded_at as bronze_loaded_at,
        manifest.source_version,
        manifest.source_captured_at,
        manifest.source_checked_at,
        manifest.bronze_finished_at,
        manifest.manifest_resolved_at
    from {{ source('transfermarkt_bronze', asset_name ~ '_raw') }} as raw
    inner join {{ ref('base_tm__source_manifest') }} as manifest
        on manifest.asset_name = '{{ asset_name }}'
       and raw.ingestion_run_id = manifest.bronze_ingestion_run_id
       and raw.source_file_sha256 = manifest.source_version_checksum
{%- endmacro %}

{% macro tm_sql_string(value) -%}
    '{{ value | string | replace("'", "''") }}'
{%- endmacro %}
