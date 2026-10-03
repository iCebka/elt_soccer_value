{% set rules = [
    {'condition': 'game_id is null', 'reason': 'missing_or_invalid_game_id'},
    {'condition': 'club_id is null', 'reason': 'missing_or_invalid_club_id'},
    {'condition': 'opponent_id is null', 'reason': 'missing_or_invalid_opponent_id'},
    {'condition': 'club_id = opponent_id', 'reason': 'same_club_and_opponent'},
    {'condition': "hosting is null or hosting not in ('home', 'away')", 'reason': 'missing_or_invalid_hosting'},
    {'condition': 'own_goals < 0 or opponent_goals < 0', 'reason': 'negative_score'},
    {'condition': "nullif(trim(raw_record:is_win::varchar), '') is not null and is_win is null", 'reason': 'invalid_is_win'}
] %}
{{ tm_classified_model('stg_tm__club_games', ['game_id', 'club_id'], rules) }}
