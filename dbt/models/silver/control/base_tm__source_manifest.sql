{% set required_assets = [
    'appearances',
    'club_games',
    'clubs',
    'competitions',
    'countries',
    'game_events',
    'game_lineups',
    'games',
    'national_teams',
    'player_valuations',
    'players',
    'transfers'
] %}
{% set fixed_manifest = var('transfermarkt_source_manifest', {}) %}

{% if fixed_manifest | length > 0 %}
    {% for asset_name in required_assets %}
        {% if asset_name not in fixed_manifest %}
            {{ exceptions.raise_compiler_error(
                'transfermarkt_source_manifest is fixed but missing asset: ' ~ asset_name
            ) }}
        {% endif %}
        {% if 'ingestion_run_id' not in fixed_manifest[asset_name]
              or 'source_file_sha256' not in fixed_manifest[asset_name] %}
            {{ exceptions.raise_compiler_error(
                'Manifest entry requires ingestion_run_id and source_file_sha256: ' ~ asset_name
            ) }}
        {% endif %}
    {% endfor %}
    {% for asset_name in fixed_manifest.keys() %}
        {% if asset_name not in required_assets %}
            {{ exceptions.raise_compiler_error(
                'Unknown asset in transfermarkt_source_manifest: ' ~ asset_name
            ) }}
        {% endif %}
    {% endfor %}
{% endif %}

with successful_audit as (
    select
        asset as asset_name,
        source_url,
        source_file,
        source_file_sha256 as source_version_checksum,
        run_id as bronze_ingestion_run_id,
        source_version,
        captured_at as source_captured_at,
        source_checked_at,
        finished_at as bronze_finished_at,
        rows_loaded as bronze_rows_loaded
    from {{ source('transfermarkt_bronze', 'transfermarkt_ingestion_files') }}
    where status = 'SUCCESS'
      and source_file_sha256 is not null
      and asset in (
          {% for asset_name in required_assets %}
          {{ tm_sql_string(asset_name) }}{% if not loop.last %}, {% endif %}
          {% endfor %}
      )
),
{% if fixed_manifest | length > 0 %}
requested_manifest as (
    {% for asset_name in required_assets %}
    select
        {{ tm_sql_string(asset_name) }} as asset_name,
        {{ tm_sql_string(fixed_manifest[asset_name]['ingestion_run_id']) }} as bronze_ingestion_run_id,
        {{ tm_sql_string(fixed_manifest[asset_name]['source_file_sha256']) }} as source_version_checksum
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
resolved as (
    select audit.*
    from successful_audit as audit
    inner join requested_manifest as requested
        on audit.asset_name = requested.asset_name
       and audit.bronze_ingestion_run_id = requested.bronze_ingestion_run_id
       and audit.source_version_checksum = requested.source_version_checksum
    qualify row_number() over (
        partition by audit.asset_name
        order by audit.bronze_finished_at desc, audit.bronze_ingestion_run_id desc
    ) = 1
)
{% else %}
resolved as (
    select *
    from successful_audit
    qualify row_number() over (
        partition by asset_name
        order by bronze_finished_at desc, bronze_ingestion_run_id desc
    ) = 1
)
{% endif %}
select
    resolved.*,
    current_timestamp() as manifest_resolved_at,
    {{ 'true' if fixed_manifest | length > 0 else 'false' }} as is_fixed_manifest
from resolved
