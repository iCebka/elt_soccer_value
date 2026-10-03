{% set rules = [
    {'condition': 'player_id is null', 'reason': 'missing_or_invalid_player_id'},
    {'condition': 'valuation_date is null', 'reason': 'missing_or_invalid_valuation_date'},
    {'condition': 'market_value_eur is null', 'reason': 'missing_or_invalid_market_value_eur'},
    {'condition': 'market_value_eur < 0', 'reason': 'negative_market_value_eur'}
] %}
{{ tm_classified_model('stg_tm__player_valuations', ['player_id', 'valuation_date'], rules) }}
