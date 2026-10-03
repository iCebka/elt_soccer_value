{{ config(tags=['silver_stage_3', 'country_resolution'], severity='error') }}

select
    {{ tm_country_name_key('country_name') }} as country_name_key,
    count(*) as candidate_count
from {{ ref('base_tm__countries') }}
group by 1
having count(*) > 1

