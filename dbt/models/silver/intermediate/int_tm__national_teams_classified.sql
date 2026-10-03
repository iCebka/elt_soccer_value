{% set rules = [
    {'condition': 'national_team_id is null', 'reason': 'missing_or_invalid_national_team_id'},
    {'condition': 'national_team_name is null', 'reason': 'missing_national_team_name'},
    {'condition': 'country_id is null', 'reason': 'missing_or_invalid_country_id'},
    {'condition': 'squad_size < 0', 'reason': 'negative_squad_size'},
    {'condition': 'average_age < 0', 'reason': 'negative_average_age'},
    {'condition': 'foreigners_number < 0', 'reason': 'negative_foreigners_number'},
    {'condition': 'foreigners_percentage < 0 or foreigners_percentage > 100', 'reason': 'foreigners_percentage_out_of_range'},
    {'condition': "nullif(trim(raw_record:total_market_value::varchar), '') is not null and total_market_value_eur is null", 'reason': 'invalid_total_market_value_eur'},
    {'condition': 'total_market_value_eur < 0', 'reason': 'negative_total_market_value_eur'},
    {'condition': 'fifa_ranking < 1', 'reason': 'nonpositive_fifa_ranking'}
] %}
{{ tm_classified_model('stg_tm__national_teams', ['national_team_id'], rules) }}
