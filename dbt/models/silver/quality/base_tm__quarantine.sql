{% set classified_models = {
    'appearances': 'int_tm__appearances_classified',
    'club_games': 'int_tm__club_games_classified',
    'clubs': 'int_tm__clubs_classified',
    'competitions': 'int_tm__competitions_classified',
    'countries': 'int_tm__countries_classified',
    'game_events': 'int_tm__game_events_classified',
    'game_lineups': 'int_tm__game_lineups_classified',
    'games': 'int_tm__games_classified',
    'national_teams': 'int_tm__national_teams_classified',
    'player_valuations': 'int_tm__player_valuations_classified',
    'players': 'int_tm__players_classified',
    'transfers': 'int_tm__transfers_classified'
} %}

{% for asset_name, model_name in classified_models.items() %}
select
    {{ tm_sql_string(asset_name) }} as asset_name,
    business_key as rejection_key,
    record_fingerprint,
    rejection_reasons,
    raw_record as relevant_source_values,
    source_url,
    source_file,
    source_version_checksum,
    source_version,
    source_row_number,
    bronze_ingestion_run_id,
    source_captured_at,
    bronze_loaded_at,
    current_timestamp() as silver_processed_at
from {{ ref(model_name) }}
where record_disposition = 'rejected'
{% if not loop.last %}union all{% endif %}
{% endfor %}
