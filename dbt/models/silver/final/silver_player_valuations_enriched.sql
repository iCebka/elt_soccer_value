{{ config(tags=['silver_stage_6', 'player_valuations_enriched']) }}

with valuations as (
    select * from {{ ref('base_tm__player_valuations') }}
),
players_current as (
    select * from {{ ref('silver_players_current') }}
),
source_manifest as (
    select * from {{ ref('base_tm__source_manifest') }}
)
select
    valuations.player_id,
    valuations.valuation_date,
    valuations.market_value_eur as historical_market_value_eur,

    valuations.current_club_id as valuation_source_current_club_id,
    valuations.current_club_name as valuation_source_current_club_name,
    valuations.player_club_domestic_competition_id
        as valuation_source_club_domestic_competition_id,

    players_current.player_id as resolved_player_id,
    case
        when players_current.player_id is not null then 'matched_player_id'
        else 'not_found'
    end as player_profile_join_status,
    players_current.player_code as player_code_current_snapshot,
    players_current.player_name as player_name_current_snapshot,
    players_current.date_of_birth as player_date_of_birth,
    players_current.position as player_position_current_snapshot,
    players_current.sub_position as player_sub_position_current_snapshot,
    players_current.preferred_foot as player_preferred_foot_current_snapshot,
    players_current.height_in_cm as player_height_in_cm_current_snapshot,

    case
        when players_current.date_of_birth is null
          or valuations.valuation_date < players_current.date_of_birth then null
        else
            datediff(
                year,
                players_current.date_of_birth,
                valuations.valuation_date
            )
            - iff(
                to_number(to_char(valuations.valuation_date, 'MMDD'))
                    < to_number(to_char(players_current.date_of_birth, 'MMDD')),
                1,
                0
            )
    end as age_at_valuation_years,
    case
        when players_current.player_id is null then 'profile_not_found'
        when players_current.date_of_birth is null then 'missing_birth_date'
        when valuations.valuation_date < players_current.date_of_birth
            then 'valuation_before_birth'
        else 'calculated'
    end as age_at_valuation_status,

    players_current.country_of_birth_source
        as country_of_birth_source_current_snapshot,
    players_current.birth_country_id as birth_country_id_current_snapshot,
    players_current.birth_country_name as birth_country_name_current_snapshot,
    players_current.birth_country_resolution_status
        as birth_country_resolution_status_current_snapshot,
    players_current.country_of_citizenship_source
        as country_of_citizenship_source_current_snapshot,
    players_current.citizenship_country_id
        as citizenship_country_id_current_snapshot,
    players_current.citizenship_country_name
        as citizenship_country_name_current_snapshot,
    players_current.citizenship_country_resolution_status
        as citizenship_country_resolution_status_current_snapshot,

    players_current.current_club_id as player_current_club_id_snapshot,
    players_current.resolved_current_club_id
        as resolved_player_current_club_id_snapshot,
    players_current.current_club_name_source
        as player_current_club_name_source_snapshot,
    players_current.current_club_name as player_current_club_name_snapshot,
    players_current.current_club_code as player_current_club_code_snapshot,
    players_current.current_club_domestic_competition_id
        as player_current_club_domestic_competition_id_snapshot,
    players_current.current_club_resolution_status
        as player_current_club_resolution_status_snapshot,
    case
        when players_current.player_id is null then 'profile_not_found'
        when valuations.current_club_id is null
          and players_current.current_club_id is null then 'missing_both'
        when valuations.current_club_id is null then 'valuation_source_missing'
        when players_current.current_club_id is null
            then 'player_snapshot_missing'
        when valuations.current_club_id = players_current.current_club_id
            then 'match'
        else 'mismatch'
    end as current_club_id_comparison_status,

    players_current.current_national_team_id
        as player_current_national_team_id_snapshot,
    players_current.resolved_current_national_team_id
        as resolved_player_current_national_team_id_snapshot,
    players_current.current_national_team_name
        as player_current_national_team_name_snapshot,
    players_current.current_national_team_code
        as player_current_national_team_code_snapshot,
    players_current.current_national_team_confederation
        as player_current_national_team_confederation_snapshot,
    players_current.current_national_team_resolution_status
        as player_current_national_team_resolution_status_snapshot,
    players_current.national_team_country_id
        as national_team_country_id_current_snapshot,
    players_current.national_team_country_name
        as national_team_country_name_current_snapshot,
    players_current.national_team_country_resolution_status
        as national_team_country_resolution_status_current_snapshot,

    players_current.player_snapshot_last_season,
    players_current.current_market_value_eur
        as player_current_market_value_eur_snapshot,
    players_current.highest_market_value_eur
        as player_highest_market_value_eur_snapshot,
    players_current.current_contract_expiration_date
        as player_current_contract_expiration_date_snapshot,
    players_current.cumulative_international_caps
        as player_cumulative_international_caps_snapshot,
    players_current.cumulative_international_goals
        as player_cumulative_international_goals_snapshot,
    players_current.current_national_team_fifa_ranking
        as current_national_team_fifa_ranking_snapshot,

    valuations_manifest.source_version as valuations_source_version,
    valuations_manifest.source_version_checksum
        as valuations_source_version_checksum,
    valuations_manifest.source_captured_at as valuations_source_captured_at,
    valuations_manifest.bronze_ingestion_run_id
        as valuations_bronze_ingestion_run_id,
    players_manifest.source_version as players_source_version,
    players_manifest.source_version_checksum as players_source_version_checksum,
    players_manifest.source_captured_at as players_source_captured_at,
    players_manifest.bronze_ingestion_run_id as players_bronze_ingestion_run_id,
    teams_manifest.source_version as national_teams_source_version,
    teams_manifest.source_version_checksum
        as national_teams_source_version_checksum,
    teams_manifest.source_captured_at as national_teams_source_captured_at,
    teams_manifest.bronze_ingestion_run_id
        as national_teams_bronze_ingestion_run_id,
    countries_manifest.source_version as countries_source_version,
    countries_manifest.source_version_checksum
        as countries_source_version_checksum,
    countries_manifest.source_captured_at as countries_source_captured_at,
    countries_manifest.bronze_ingestion_run_id
        as countries_bronze_ingestion_run_id,
    clubs_manifest.source_version as clubs_source_version,
    clubs_manifest.source_version_checksum as clubs_source_version_checksum,
    clubs_manifest.source_captured_at as clubs_source_captured_at,
    clubs_manifest.bronze_ingestion_run_id as clubs_bronze_ingestion_run_id,
    players_current.silver_processed_at as players_current_processed_at,
    current_timestamp() as silver_processed_at
from valuations
left join players_current
    on valuations.player_id = players_current.player_id
left join source_manifest as valuations_manifest
    on valuations_manifest.asset_name = 'player_valuations'
left join source_manifest as players_manifest
    on players_manifest.asset_name = 'players'
left join source_manifest as teams_manifest
    on teams_manifest.asset_name = 'national_teams'
left join source_manifest as countries_manifest
    on countries_manifest.asset_name = 'countries'
left join source_manifest as clubs_manifest
    on clubs_manifest.asset_name = 'clubs'
