{% set rules = [
    {'condition': 'game_id is null', 'reason': 'missing_or_invalid_game_id'},
    {'condition': 'competition_id is null', 'reason': 'missing_competition_id'},
    {'condition': 'game_date is null', 'reason': 'missing_or_invalid_game_date'},
    {'condition': 'home_club_id is null', 'reason': 'missing_or_invalid_home_club_id'},
    {'condition': 'away_club_id is null', 'reason': 'missing_or_invalid_away_club_id'},
    {'condition': 'home_club_id = away_club_id', 'reason': 'same_home_and_away_club'},
    {'condition': 'home_club_goals < 0 or away_club_goals < 0', 'reason': 'negative_score'},
    {'condition': 'attendance < 0', 'reason': 'negative_attendance'}
] %}
{{ tm_classified_model('stg_tm__games', ['game_id'], rules) }}
