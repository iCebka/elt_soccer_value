{% set rules = [
    {'condition': 'player_id is null', 'reason': 'missing_or_invalid_player_id'},
    {'condition': 'transfer_date is null', 'reason': 'missing_or_invalid_transfer_date'},
    {'condition': "nullif(trim(raw_record:from_club_id::varchar), '') is not null and from_club_id is null", 'reason': 'invalid_from_club_id'},
    {'condition': "nullif(trim(raw_record:to_club_id::varchar), '') is not null and to_club_id is null", 'reason': 'invalid_to_club_id'},
    {'condition': "nullif(trim(raw_record:transfer_fee::varchar), '') is not null and transfer_fee_eur is null", 'reason': 'invalid_transfer_fee_eur'},
    {'condition': 'transfer_fee_eur < 0', 'reason': 'negative_transfer_fee_eur'},
    {'condition': "nullif(trim(raw_record:market_value_in_eur::varchar), '') is not null and market_value_eur is null", 'reason': 'invalid_market_value_eur'},
    {'condition': 'market_value_eur < 0', 'reason': 'negative_market_value_eur'}
] %}
{{ tm_classified_model('stg_tm__transfers', [], rules) }}
