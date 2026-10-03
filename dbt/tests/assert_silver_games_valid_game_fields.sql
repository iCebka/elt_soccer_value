{{ config(tags=['silver_stage_4', 'games_enriched'], severity='error') }}

select game_id
from {{ ref('silver_games_enriched') }}
where game_date is null
   or home_club_id is null
   or away_club_id is null
   or home_club_id = away_club_id
   or home_club_goals is null
   or away_club_goals is null
   or home_club_goals < 0
   or away_club_goals < 0
   or attendance < 0
   or home_club_position_at_game < 1
   or away_club_position_at_game < 1
