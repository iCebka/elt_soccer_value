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
    count(*) as input_rows,
    count_if(record_disposition = 'accepted') as accepted_rows,
    count_if(record_disposition = 'duplicate_identical') as identical_duplicate_rows,
    count_if(record_disposition = 'rejected') as rejected_rows,
    count_if(record_disposition = 'rejected' and is_key_conflict) as conflicting_rows,
    min(source_version_checksum) as source_version_checksum,
    min(bronze_ingestion_run_id) as bronze_ingestion_run_id,
    min(source_captured_at) as source_captured_at,
    current_timestamp() as silver_processed_at
from {{ ref(model_name) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
