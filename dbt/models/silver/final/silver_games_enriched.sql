{{ config(tags=['silver_stage_4', 'games_enriched']) }}

with games as (
    select * from {{ ref('base_tm__games') }}
),
competitions as (
    select * from {{ ref('base_tm__competitions') }}
),
clubs as (
    select * from {{ ref('base_tm__clubs') }}
),
source_manifest as (
    select * from {{ ref('base_tm__source_manifest') }}
)
select
    games.game_id,
    games.game_date,
    games.season,
    games.game_round,

    games.competition_id,
    competitions.competition_id as resolved_competition_id,
    competitions.competition_code,
    competitions.competition_name,
    games.competition_type as competition_type_at_game,
    competitions.competition_sub_type as competition_sub_type_snapshot,
    competitions.competition_type as competition_type_snapshot,
    competitions.country_id as competition_country_id,
    competitions.country_name as competition_country_name,
    competitions.confederation as competition_confederation,
    competitions.domestic_league_code as competition_domestic_league_code,
    competitions.competition_url,
    case
        when games.competition_id is null then 'missing_source_id'
        when competitions.competition_id is not null then 'matched_id'
        else 'not_found'
    end as competition_resolution_status,

    games.home_club_id,
    home_club.club_id as resolved_home_club_id,
    games.home_club_name as home_club_name_at_game,
    home_club.club_name as home_club_name_current_snapshot,
    home_club.club_code as home_club_code_current_snapshot,
    home_club.domestic_competition_id
        as home_club_domestic_competition_id_current_snapshot,
    home_club.last_season as home_club_last_season_current_snapshot,
    home_club.club_url as home_club_url_current_snapshot,
    case
        when games.home_club_id is null then 'missing_source_id'
        when home_club.club_id is not null then 'matched_id'
        else 'not_found'
    end as home_club_resolution_status,

    games.away_club_id,
    away_club.club_id as resolved_away_club_id,
    games.away_club_name as away_club_name_at_game,
    away_club.club_name as away_club_name_current_snapshot,
    away_club.club_code as away_club_code_current_snapshot,
    away_club.domestic_competition_id
        as away_club_domestic_competition_id_current_snapshot,
    away_club.last_season as away_club_last_season_current_snapshot,
    away_club.club_url as away_club_url_current_snapshot,
    case
        when games.away_club_id is null then 'missing_source_id'
        when away_club.club_id is not null then 'matched_id'
        else 'not_found'
    end as away_club_resolution_status,

    games.home_club_goals,
    games.away_club_goals,
    games.aggregate_score,
    games.home_club_position as home_club_position_at_game,
    games.away_club_position as away_club_position_at_game,
    games.home_club_manager_name as home_manager_name_at_game,
    games.away_club_manager_name as away_manager_name_at_game,
    games.home_club_formation as home_formation_at_game,
    games.away_club_formation as away_formation_at_game,
    games.stadium_name,
    games.attendance,
    games.referee_name,
    games.game_url,

    games_manifest.source_version as games_source_version,
    games_manifest.source_version_checksum as games_source_version_checksum,
    games_manifest.source_captured_at as games_source_captured_at,
    games_manifest.bronze_ingestion_run_id as games_bronze_ingestion_run_id,
    competitions_manifest.source_version as competitions_source_version,
    competitions_manifest.source_version_checksum
        as competitions_source_version_checksum,
    competitions_manifest.source_captured_at as competitions_source_captured_at,
    competitions_manifest.bronze_ingestion_run_id
        as competitions_bronze_ingestion_run_id,
    clubs_manifest.source_version as clubs_source_version,
    clubs_manifest.source_version_checksum as clubs_source_version_checksum,
    clubs_manifest.source_captured_at as clubs_source_captured_at,
    clubs_manifest.bronze_ingestion_run_id as clubs_bronze_ingestion_run_id,
    current_timestamp() as silver_processed_at
from games
left join competitions
    on games.competition_id = competitions.competition_id
left join clubs as home_club
    on games.home_club_id = home_club.club_id
left join clubs as away_club
    on games.away_club_id = away_club.club_id
left join source_manifest as games_manifest
    on games_manifest.asset_name = 'games'
left join source_manifest as competitions_manifest
    on competitions_manifest.asset_name = 'competitions'
left join source_manifest as clubs_manifest
    on clubs_manifest.asset_name = 'clubs'
