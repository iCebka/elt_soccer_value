{% macro tm_business_key(key_columns) -%}
    {%- if key_columns | length == 0 -%}
        record_fingerprint
    {%- else -%}
        case
            when
                {%- for column in key_columns %}
                {{ column }} is null{% if not loop.last %} or {% endif %}
                {%- endfor %}
            then null
            else concat_ws(
                '||',
                {%- for column in key_columns %}
                to_varchar({{ column }}){% if not loop.last %}, {% endif %}
                {%- endfor %}
            )
        end
    {%- endif -%}
{%- endmacro %}

{% macro tm_classified_model(staging_model, key_columns, validation_rules) -%}
with staged as (
    select *
    from {{ ref(staging_model) }}
),
fingerprinted as (
    select
        staged.*,
        sha2(to_json(raw_record), 256) as record_fingerprint
    from staged
),
annotated as (
    select
        fingerprinted.*,
        {{ tm_business_key(key_columns) }} as business_key,
        array_construct_compact(
            {%- for rule in validation_rules %}
            iff({{ rule['condition'] }}, {{ tm_sql_string(rule['reason']) }}, null)
            {%- if not loop.last %}, {% endif -%}
            {%- endfor %}
        ) as validation_reasons,
        row_number() over (
            partition by record_fingerprint
            order by source_row_number
        ) as identical_duplicate_rank
    from fingerprinted
),
representatives as (
    select *
    from annotated
    where identical_duplicate_rank = 1
),
conflicting_keys as (
    select business_key
    from representatives
    where business_key is not null
    group by business_key
    having count(distinct record_fingerprint) > 1
),
classified as (
    select
        annotated.*,
        annotated.identical_duplicate_rank > 1 as is_identical_duplicate,
        conflicting_keys.business_key is not null as is_key_conflict,
        array_cat(
            annotated.validation_reasons,
            array_construct_compact(
                iff(
                    conflicting_keys.business_key is not null,
                    'conflicting_duplicate_key',
                    null
                )
            )
        ) as rejection_reasons
    from annotated
    left join conflicting_keys
        on annotated.business_key = conflicting_keys.business_key
)
select
    classified.*,
    case
        when is_identical_duplicate then 'duplicate_identical'
        when array_size(rejection_reasons) > 0 then 'rejected'
        else 'accepted'
    end as record_disposition
from classified
{%- endmacro %}

{% macro tm_base_accepted(classified_model) -%}
select
    classified.* exclude (
        validation_reasons,
        identical_duplicate_rank,
        is_identical_duplicate,
        is_key_conflict,
        rejection_reasons,
        record_disposition
    ),
    current_timestamp() as silver_processed_at
from {{ ref(classified_model) }} as classified
where record_disposition = 'accepted'
{%- endmacro %}
