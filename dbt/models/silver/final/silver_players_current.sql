{{ config(tags=['silver_stage_3', 'players_current']) }}

with players as (
    select * from {{ ref('base_tm__players') }}
),
national_teams as (
    select * from {{ ref('base_tm__national_teams') }}
),
countries as (
    select * from {{ ref('base_tm__countries') }}
),
clubs as (
    select * from {{ ref('base_tm__clubs') }}
),
country_resolution as (
    select * from {{ ref('int_tm__country_name_resolution') }}
),
source_manifest as (
    select * from {{ ref('base_tm__source_manifest') }}
)
select
    players.player_id,
    players.player_code,
    players.first_name,
    players.last_name,
    players.player_name,
    players.date_of_birth,
    players.city_of_birth,
    players.position,
    players.sub_position,
    players.preferred_foot,
    players.height_in_cm,
    players.player_url,
    players.image_url,

    players.country_of_birth as country_of_birth_source,
    birth_country.resolved_country_id as birth_country_id,
    birth_country.resolved_country_name as birth_country_name,
    case
        when players.country_of_birth is null then 'missing'
        else coalesce(birth_country.resolution_status, 'not_found')
    end as birth_country_resolution_status,

    players.country_of_citizenship as country_of_citizenship_source,
    citizenship_country.resolved_country_id as citizenship_country_id,
    citizenship_country.resolved_country_name as citizenship_country_name,
    case
        when players.country_of_citizenship is null then 'missing'
        else coalesce(citizenship_country.resolution_status, 'not_found')
    end as citizenship_country_resolution_status,

    players.current_club_id,
    clubs.club_id as resolved_current_club_id,
    players.current_club_name as current_club_name_source,
    clubs.club_name as current_club_name,
    clubs.club_code as current_club_code,
    clubs.domestic_competition_id as current_club_domestic_competition_id,
    clubs.stadium_name as current_club_stadium_name,
    case
        when players.current_club_id is null then 'missing_source_id'
        when clubs.club_id is not null then 'matched_id'
        else 'not_found'
    end as current_club_resolution_status,

    players.current_national_team_id,
    national_teams.national_team_id as resolved_current_national_team_id,
    national_teams.national_team_name as current_national_team_name,
    national_teams.team_code as current_national_team_code,
    national_teams.confederation as current_national_team_confederation,
    case
        when players.current_national_team_id is null then 'missing_source_id'
        when national_teams.national_team_id is not null then 'matched_id'
        else 'not_found'
    end as current_national_team_resolution_status,

    national_teams.country_id as national_team_country_id_source,
    national_team_country.country_id as national_team_country_id,
    national_team_country.country_name as national_team_country_name,
    national_team_country.country_code as national_team_country_code_source,
    case
        when players.current_national_team_id is null then 'not_applicable'
        when national_teams.national_team_id is null then 'national_team_not_found'
        when national_teams.country_id is null then 'missing_source_id'
        when national_team_country.country_id is not null then 'matched_id'
        else 'not_found'
    end as national_team_country_resolution_status,

    players.last_season as player_snapshot_last_season,
    players.market_value_eur as current_market_value_eur,
    players.highest_market_value_eur,
    players.contract_expiration_date as current_contract_expiration_date,
    players.international_caps as cumulative_international_caps,
    players.international_goals as cumulative_international_goals,
    national_teams.fifa_ranking as current_national_team_fifa_ranking,

    players_manifest.source_version as players_source_version,
    players_manifest.source_version_checksum as players_source_version_checksum,
    players_manifest.source_captured_at as players_source_captured_at,
    players_manifest.bronze_ingestion_run_id as players_bronze_ingestion_run_id,
    teams_manifest.source_version as national_teams_source_version,
    teams_manifest.source_version_checksum as national_teams_source_version_checksum,
    teams_manifest.source_captured_at as national_teams_source_captured_at,
    teams_manifest.bronze_ingestion_run_id as national_teams_bronze_ingestion_run_id,
    countries_manifest.source_version as countries_source_version,
    countries_manifest.source_version_checksum as countries_source_version_checksum,
    countries_manifest.source_captured_at as countries_source_captured_at,
    countries_manifest.bronze_ingestion_run_id as countries_bronze_ingestion_run_id,
    clubs_manifest.source_version as clubs_source_version,
    clubs_manifest.source_version_checksum as clubs_source_version_checksum,
    clubs_manifest.source_captured_at as clubs_source_captured_at,
    clubs_manifest.bronze_ingestion_run_id as clubs_bronze_ingestion_run_id,
    current_timestamp() as silver_processed_at
from players
left join national_teams
    on players.current_national_team_id = national_teams.national_team_id
left join countries as national_team_country
    on national_teams.country_id = national_team_country.country_id
left join country_resolution as birth_country
    on {{ tm_country_name_key('players.country_of_birth') }} = birth_country.country_name_key
left join country_resolution as citizenship_country
    on {{ tm_country_name_key('players.country_of_citizenship') }} = citizenship_country.country_name_key
left join clubs
    on players.current_club_id = clubs.club_id
left join source_manifest as players_manifest
    on players_manifest.asset_name = 'players'
left join source_manifest as teams_manifest
    on teams_manifest.asset_name = 'national_teams'
left join source_manifest as countries_manifest
    on countries_manifest.asset_name = 'countries'
left join source_manifest as clubs_manifest
    on clubs_manifest.asset_name = 'clubs'

