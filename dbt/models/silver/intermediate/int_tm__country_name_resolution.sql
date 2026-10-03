{{ config(tags=['silver_stage_3', 'country_resolution']) }}

with observed_country_keys as (
    select distinct {{ tm_country_name_key('country_of_birth') }} as country_name_key
    from {{ ref('base_tm__players') }}
    where country_of_birth is not null

    union

    select distinct {{ tm_country_name_key('country_of_citizenship') }} as country_name_key
    from {{ ref('base_tm__players') }}
    where country_of_citizenship is not null
),
country_candidates as (
    select
        {{ tm_country_name_key('country_name') }} as country_name_key,
        count(*) as candidate_count,
        min(country_id) as candidate_country_id,
        min(country_name) as candidate_country_name
    from {{ ref('base_tm__countries') }}
    group by 1
),
alias_candidates as (
    select
        {{ tm_country_name_key('aliases.alias_name') }} as country_name_key,
        count(*) as alias_definition_count,
        count(countries.country_id) as target_candidate_count,
        min(countries.country_id) as candidate_country_id,
        min(countries.country_name) as candidate_country_name
    from {{ ref('tm_country_name_aliases') }} as aliases
    left join {{ ref('base_tm__countries') }} as countries
        on {{ tm_country_name_key('aliases.canonical_country_name') }}
         = {{ tm_country_name_key('countries.country_name') }}
    group by 1
)
select
    observed.country_name_key,
    case
        when coalesce(direct.candidate_count, 0) > 1
          or coalesce(aliases.alias_definition_count, 0) > 1
          or coalesce(aliases.target_candidate_count, 0) > 1
            then null
        when direct.candidate_count = 1 then direct.candidate_country_id
        when aliases.alias_definition_count = 1
         and aliases.target_candidate_count = 1
            then aliases.candidate_country_id
        else null
    end as resolved_country_id,
    case
        when coalesce(direct.candidate_count, 0) > 1
          or coalesce(aliases.alias_definition_count, 0) > 1
          or coalesce(aliases.target_candidate_count, 0) > 1
            then null
        when direct.candidate_count = 1 then direct.candidate_country_name
        when aliases.alias_definition_count = 1
         and aliases.target_candidate_count = 1
            then aliases.candidate_country_name
        else null
    end as resolved_country_name,
    case
        when coalesce(direct.candidate_count, 0) > 1
          or coalesce(aliases.alias_definition_count, 0) > 1
          or coalesce(aliases.target_candidate_count, 0) > 1
            then 'ambiguous'
        when direct.candidate_count = 1 then 'matched_name'
        when aliases.alias_definition_count = 1
         and aliases.target_candidate_count = 1
            then 'matched_alias'
        else 'not_found'
    end as resolution_status,
    coalesce(direct.candidate_count, 0) as direct_candidate_count,
    coalesce(aliases.alias_definition_count, 0) as alias_definition_count,
    coalesce(aliases.target_candidate_count, 0) as alias_target_candidate_count
from observed_country_keys as observed
left join country_candidates as direct
    on observed.country_name_key = direct.country_name_key
left join alias_candidates as aliases
    on observed.country_name_key = aliases.country_name_key

