{% set rules = [
    {'condition': 'game_event_id is null', 'reason': 'missing_game_event_id'},
    {'condition': 'game_id is null', 'reason': 'missing_or_invalid_game_id'},
    {'condition': 'event_date is null', 'reason': 'missing_or_invalid_event_date'},
    {'condition': 'event_type_raw is null', 'reason': 'missing_event_type'},
    {'condition': "nullif(trim(raw_record:minute::varchar), '') is not null and trim(raw_record:minute::varchar) <> '-1' and event_minute is null", 'reason': 'invalid_event_minute'},
    {'condition': 'event_minute < 0', 'reason': 'negative_event_minute'}
] %}
{{ tm_classified_model('stg_tm__game_events', ['game_event_id'], rules) }}
