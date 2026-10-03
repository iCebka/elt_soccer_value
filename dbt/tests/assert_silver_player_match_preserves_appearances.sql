{{ config(tags=['silver_stage_5', 'player_match'], severity='error') }}

with counts as (
    select
        (select count(*) from {{ ref('base_tm__appearances') }}) as base_rows,
        (select count(*) from {{ ref('silver_player_match') }}) as silver_rows,
        (
            select count(distinct appearance_id)
            from {{ ref('silver_player_match') }}
        ) as silver_distinct_appearances,
        (
            select count(*)
            from (
                select appearance_id from {{ ref('base_tm__appearances') }}
                minus
                select appearance_id from {{ ref('silver_player_match') }}
            )
        ) as missing_appearances,
        (
            select count(*)
            from (
                select appearance_id from {{ ref('silver_player_match') }}
                minus
                select appearance_id from {{ ref('base_tm__appearances') }}
            )
        ) as unexpected_appearances
)
select *
from counts
where base_rows <> silver_rows
   or silver_rows <> silver_distinct_appearances
   or missing_appearances <> 0
   or unexpected_appearances <> 0
