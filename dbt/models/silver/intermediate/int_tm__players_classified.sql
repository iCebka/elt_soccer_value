{% set rules = [
    {'condition': 'player_id is null', 'reason': 'missing_or_invalid_player_id'},
    {'condition': 'player_name is null', 'reason': 'missing_player_name'},
    {'condition': "nullif(trim(raw_record:date_of_birth::varchar), '') is not null and date_of_birth is null", 'reason': 'invalid_date_of_birth'},
    {'condition': 'date_of_birth > current_date()', 'reason': 'date_of_birth_in_future'},
    {'condition': "nullif(trim(raw_record:height_in_cm::varchar), '') is not null and height_in_cm is null", 'reason': 'invalid_height'},
    {'condition': 'height_in_cm <= 0', 'reason': 'nonpositive_height'},
    {'condition': "preferred_foot is not null and preferred_foot not in ('left', 'right', 'both')", 'reason': 'invalid_preferred_foot'},
    {'condition': 'international_caps < 0', 'reason': 'negative_international_caps'},
    {'condition': 'international_goals < 0', 'reason': 'negative_international_goals'},
    {'condition': "nullif(trim(raw_record:market_value_in_eur::varchar), '') is not null and market_value_eur is null", 'reason': 'invalid_market_value_eur'},
    {'condition': 'market_value_eur < 0', 'reason': 'negative_market_value_eur'},
    {'condition': "nullif(trim(raw_record:highest_market_value_in_eur::varchar), '') is not null and highest_market_value_eur is null", 'reason': 'invalid_highest_market_value_eur'},
    {'condition': 'highest_market_value_eur < 0', 'reason': 'negative_highest_market_value_eur'}
] %}
{{ tm_classified_model('stg_tm__players', ['player_id'], rules) }}
