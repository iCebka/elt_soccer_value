{% set rules = [
    {'condition': 'competition_id is null', 'reason': 'missing_competition_id'},
    {'condition': 'competition_name is null', 'reason': 'missing_competition_name'},
    {'condition': 'total_clubs < 0', 'reason': 'negative_total_clubs'}
] %}
{{ tm_classified_model('stg_tm__competitions', ['competition_id'], rules) }}
