{{ config(tags=['silver_stage_5', 'player_match'], severity='error') }}

select appearance_id
from {{ ref('silver_player_match') }}
where minutes_played < 0
   or goals < 0
   or assists < 0
   or yellow_cards < 0
   or red_cards < 0
