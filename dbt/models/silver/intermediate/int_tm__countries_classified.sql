{% set rules = [
    {'condition': 'country_id is null', 'reason': 'missing_or_invalid_country_id'},
    {'condition': 'country_name is null', 'reason': 'missing_country_name'},
    {'condition': 'total_clubs < 0', 'reason': 'negative_total_clubs'},
    {'condition': 'total_players < 0', 'reason': 'negative_total_players'},
    {'condition': 'average_age < 0', 'reason': 'negative_average_age'}
] %}
{{ tm_classified_model('stg_tm__countries', ['country_id'], rules) }}
