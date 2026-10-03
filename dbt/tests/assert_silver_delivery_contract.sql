with delivery_checks as (
    select
        'silver_players_current' as model_name,
        (select count(*) from {{ ref('silver_players_current') }}) as output_rows,
        (select count(*) from {{ ref('base_tm__players') }}) as expected_rows,
        (
            select count(*)
            from (
                select player_id
                from {{ ref('silver_players_current') }}
                group by player_id
            )
        ) as distinct_keys
    union all
    select
        'silver_games_enriched',
        (select count(*) from {{ ref('silver_games_enriched') }}),
        (select count(*) from {{ ref('base_tm__games') }}),
        (
            select count(*)
            from (
                select game_id
                from {{ ref('silver_games_enriched') }}
                group by game_id
            )
        )
    union all
    select
        'silver_player_match',
        (select count(*) from {{ ref('silver_player_match') }}),
        (select count(*) from {{ ref('base_tm__appearances') }}),
        (
            select count(*)
            from (
                select appearance_id
                from {{ ref('silver_player_match') }}
                group by appearance_id
            )
        )
    union all
    select
        'silver_player_valuations_enriched',
        (select count(*) from {{ ref('silver_player_valuations_enriched') }}),
        (select count(*) from {{ ref('base_tm__player_valuations') }}),
        (
            select count(*)
            from (
                select player_id, valuation_date
                from {{ ref('silver_player_valuations_enriched') }}
                group by player_id, valuation_date
            )
        )
)
select *
from delivery_checks
where output_rows <> expected_rows
   or output_rows <> distinct_keys
