{{ config(tags=['silver_stage_3', 'players_current'], severity='error') }}

with missing_from_silver as (
    select player_id from {{ ref('base_tm__players') }}
    minus
    select player_id from {{ ref('silver_players_current') }}
),
unexpected_in_silver as (
    select player_id from {{ ref('silver_players_current') }}
    minus
    select player_id from {{ ref('base_tm__players') }}
)
select player_id, 'missing_from_silver' as failure_reason from missing_from_silver
union all
select player_id, 'unexpected_in_silver' as failure_reason from unexpected_in_silver

