{% set rules = [
    {'condition': 'club_id is null', 'reason': 'missing_or_invalid_club_id'},
    {'condition': 'club_name is null', 'reason': 'missing_club_name'},
    {'condition': 'squad_size < 0', 'reason': 'negative_squad_size'},
    {'condition': 'average_age < 0', 'reason': 'negative_average_age'},
    {'condition': 'foreigners_number < 0', 'reason': 'negative_foreigners_number'},
    {'condition': 'foreigners_percentage < 0 or foreigners_percentage > 100', 'reason': 'foreigners_percentage_out_of_range'},
    {'condition': 'national_team_players < 0', 'reason': 'negative_national_team_players'},
    {'condition': 'stadium_seats < 0', 'reason': 'negative_stadium_seats'},
    {'condition': "nullif(trim(raw_record:total_market_value::varchar), '') is not null and total_market_value_eur is null", 'reason': 'invalid_total_market_value_eur'},
    {'condition': 'total_market_value_eur < 0', 'reason': 'negative_total_market_value_eur'},
    {'condition': 'net_transfer_record_raw is not null and net_transfer_record_eur is null', 'reason': 'invalid_net_transfer_record_eur'}
] %}
{{ tm_classified_model('stg_tm__clubs', ['club_id'], rules) }}
