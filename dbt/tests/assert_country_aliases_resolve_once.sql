{{ config(tags=['silver_stage_3', 'country_resolution'], severity='error') }}

with country_keys as (
    select
        {{ tm_country_name_key('country_name') }} as country_name_key,
        count(*) as candidate_count
    from {{ ref('base_tm__countries') }}
    group by 1
),
alias_checks as (
    select
        {{ tm_country_name_key('aliases.alias_name') }} as alias_key,
        count(*) as alias_definition_count,
        count_if(targets.candidate_count = 1) as resolved_target_count,
        count_if(direct_names.country_name_key is not null) as shadows_direct_name
    from {{ ref('tm_country_name_aliases') }} as aliases
    left join country_keys as targets
        on {{ tm_country_name_key('aliases.canonical_country_name') }} = targets.country_name_key
    left join country_keys as direct_names
        on {{ tm_country_name_key('aliases.alias_name') }} = direct_names.country_name_key
    group by 1
)
select *
from alias_checks
where alias_definition_count <> 1
   or resolved_target_count <> 1
   or shadows_direct_name <> 0

