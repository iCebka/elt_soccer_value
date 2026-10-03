{% set rules = [
    {'condition': 'game_lineup_id is null', 'reason': 'missing_game_lineup_id'},
    {'condition': 'game_id is null', 'reason': 'missing_or_invalid_game_id'},
    {'condition': 'player_id is null', 'reason': 'missing_or_invalid_player_id'},
    {'condition': 'club_id is null', 'reason': 'missing_or_invalid_club_id'},
    {'condition': 'lineup_date is null', 'reason': 'missing_or_invalid_lineup_date'},
    {'condition': 'lineup_type_raw is null', 'reason': 'missing_lineup_type'},
    {'condition': "nullif(trim(raw_record:team_captain::varchar), '') is not null and is_team_captain is null", 'reason': 'invalid_team_captain'}
] %}
{{ tm_classified_model('stg_tm__game_lineups', ['game_lineup_id'], rules) }}
