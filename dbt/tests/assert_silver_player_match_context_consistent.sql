{{ config(tags=['silver_stage_5', 'player_match'], severity='error') }}

select appearance_id
from {{ ref('silver_player_match') }}
where
    ((game_join_status = 'matched_game_id') <> (game_date_source is not null))
 or ((club_game_join_status = 'matched_game_and_player_club')
        <> (matched_player_club_id is not null))
 or (matched_player_club_id is not null
        and matched_player_club_id <> player_club_id)
 or (team_context_status = 'aligned_home' and (
        player_team_hosting <> 'home'
        or player_club_id <> home_club_id
        or opponent_club_id <> away_club_id
        or player_team_goals <> home_club_goals
        or opponent_goals <> away_club_goals
    ))
 or (team_context_status = 'aligned_away' and (
        player_team_hosting <> 'away'
        or player_club_id <> away_club_id
        or opponent_club_id <> home_club_id
        or player_team_goals <> away_club_goals
        or opponent_goals <> home_club_goals
    ))
 or (club_games_is_win is not null
        and club_games_is_win <> (player_team_goals > opponent_goals))
 or (player_team_result = 'win' and player_team_goals <= opponent_goals)
 or (player_team_result = 'draw' and player_team_goals <> opponent_goals)
 or (player_team_result = 'loss' and player_team_goals >= opponent_goals)
 or (player_team_result = 'unknown'
        and player_team_goals is not null and opponent_goals is not null)
