{{ config(tags=['silver_stage_2'], severity='error') }}

with required_assets as (
    select column1 as asset_name
    from values
        ('appearances'), ('club_games'), ('clubs'), ('competitions'),
        ('countries'), ('game_events'), ('game_lineups'), ('games'),
        ('national_teams'), ('player_valuations'), ('players'), ('transfers')
),
observed as (
    select asset_name
    from {{ ref('base_tm__source_manifest') }}
)
select required_assets.asset_name
from required_assets
left join observed using (asset_name)
where observed.asset_name is null
