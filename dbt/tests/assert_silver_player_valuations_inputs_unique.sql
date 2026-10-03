{{ config(tags=['silver_stage_6', 'player_valuations_enriched'], severity='error') }}

select
    'player_valuations' as input_name,
    concat_ws('||', to_varchar(player_id), to_varchar(valuation_date))
        as source_key,
    count(*) as row_count
from {{ ref('base_tm__player_valuations') }}
group by player_id, valuation_date
having count(*) > 1

union all

select
    'players_current',
    to_varchar(player_id),
    count(*)
from {{ ref('silver_players_current') }}
group by player_id
having count(*) > 1
