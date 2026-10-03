{{ config(tags=['silver_stage_5', 'player_match'], severity='error') }}

select
    'appearances' as input_name,
    appearance_id as source_key,
    count(*) as row_count
from {{ ref('base_tm__appearances') }}
group by appearance_id
having count(*) > 1

union all

select
    'games',
    to_varchar(game_id),
    count(*)
from {{ ref('silver_games_enriched') }}
group by game_id
having count(*) > 1

union all

select
    'club_games',
    concat_ws('||', to_varchar(game_id), to_varchar(club_id)),
    count(*)
from {{ ref('base_tm__club_games') }}
group by game_id, club_id
having count(*) > 1
