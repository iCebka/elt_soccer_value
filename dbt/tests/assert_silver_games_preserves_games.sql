{{ config(tags=['silver_stage_4', 'games_enriched'], severity='error') }}

with counts as (
    select
        (select count(*) from {{ ref('base_tm__games') }}) as base_rows,
        (select count(*) from {{ ref('silver_games_enriched') }}) as silver_rows,
        (select count(distinct game_id) from {{ ref('silver_games_enriched') }})
            as silver_distinct_games,
        (
            select count(*)
            from (
                select game_id from {{ ref('base_tm__games') }}
                minus
                select game_id from {{ ref('silver_games_enriched') }}
            )
        ) as missing_games,
        (
            select count(*)
            from (
                select game_id from {{ ref('silver_games_enriched') }}
                minus
                select game_id from {{ ref('base_tm__games') }}
            )
        ) as unexpected_games
)
select *
from counts
where base_rows <> silver_rows
   or silver_rows <> silver_distinct_games
   or missing_games <> 0
   or unexpected_games <> 0
