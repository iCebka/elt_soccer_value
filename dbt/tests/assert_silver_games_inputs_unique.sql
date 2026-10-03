{{ config(tags=['silver_stage_4', 'games_enriched'], severity='error') }}

select 'games' as input_name, to_varchar(game_id) as source_key, count(*) as row_count
from {{ ref('base_tm__games') }}
group by game_id
having count(*) > 1

union all

select 'competitions', to_varchar(competition_id), count(*)
from {{ ref('base_tm__competitions') }}
group by competition_id
having count(*) > 1

union all

select 'clubs', to_varchar(club_id), count(*)
from {{ ref('base_tm__clubs') }}
group by club_id
having count(*) > 1
