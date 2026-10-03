{% set rules = [
    {'condition': 'appearance_id is null', 'reason': 'missing_appearance_id'},
    {'condition': 'game_id is null', 'reason': 'missing_or_invalid_game_id'},
    {'condition': 'player_id is null', 'reason': 'missing_or_invalid_player_id'},
    {'condition': 'appearance_date is null', 'reason': 'missing_or_invalid_appearance_date'},
    {'condition': 'yellow_cards < 0 or red_cards < 0', 'reason': 'negative_card_count'},
    {'condition': 'goals < 0 or assists < 0', 'reason': 'negative_goal_or_assist_count'},
    {'condition': 'minutes_played < 0', 'reason': 'negative_minutes_played'}
] %}
{{ tm_classified_model('stg_tm__appearances', ['appearance_id'], rules) }}
