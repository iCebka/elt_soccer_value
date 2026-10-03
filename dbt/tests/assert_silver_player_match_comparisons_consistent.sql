{{ config(tags=['silver_stage_5', 'player_match'], severity='error') }}

select appearance_id
from {{ ref('silver_player_match') }}
where
    (date_comparison_status = 'match'
        and appearance_date_source <> game_date_source)
 or (date_comparison_status = 'mismatch_games_preferred' and (
        appearance_date_source = game_date_source
        or match_date <> game_date_source
    ))
 or (date_comparison_status = 'games_not_found_appearances_used' and (
        game_join_status <> 'not_found'
        or match_date <> appearance_date_source
    ))
 or (competition_comparison_status = 'match'
        and appearance_competition_id <> game_competition_id)
 or (competition_comparison_status = 'mismatch_games_preferred' and (
        appearance_competition_id = game_competition_id
        or competition_id <> game_competition_id
    ))
 or (competition_comparison_status = 'games_not_found_appearances_used' and (
        game_join_status <> 'not_found'
        or competition_id <> appearance_competition_id
    ))
