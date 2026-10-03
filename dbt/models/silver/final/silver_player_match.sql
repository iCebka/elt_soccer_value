{{ config(tags=['silver_stage_5', 'player_match']) }}

with appearances as (
    select * from {{ ref('base_tm__appearances') }}
),
games as (
    select * from {{ ref('silver_games_enriched') }}
),
club_games as (
    select * from {{ ref('base_tm__club_games') }}
),
source_manifest as (
    select * from {{ ref('base_tm__source_manifest') }}
)
select
    appearances.appearance_id,
    appearances.player_id,
    appearances.player_name as player_name_at_appearance,
    appearances.game_id,
    appearances.player_club_id,
    appearances.player_current_club_id,

    appearances.appearance_date as appearance_date_source,
    games.game_date as game_date_source,
    coalesce(games.game_date, appearances.appearance_date) as match_date,
    case
        when games.game_date is not null then 'games'
        when appearances.appearance_date is not null then 'appearances_fallback'
        else 'missing'
    end as match_date_source,
    case
        when games.game_id is null then 'games_not_found_appearances_used'
        when appearances.appearance_date is null and games.game_date is null
            then 'missing_both'
        when games.game_date is null then 'games_date_missing_appearances_used'
        when appearances.appearance_date is null
            then 'appearance_date_missing_games_used'
        when appearances.appearance_date = games.game_date then 'match'
        else 'mismatch_games_preferred'
    end as date_comparison_status,

    appearances.competition_id as appearance_competition_id,
    games.competition_id as game_competition_id,
    coalesce(games.competition_id, appearances.competition_id) as competition_id,
    case
        when games.competition_id is not null then 'games'
        when appearances.competition_id is not null then 'appearances_fallback'
        else 'missing'
    end as competition_id_source,
    case
        when games.game_id is null then 'games_not_found_appearances_used'
        when appearances.competition_id is null and games.competition_id is null
            then 'missing_both'
        when games.competition_id is null
            then 'games_competition_missing_appearances_used'
        when appearances.competition_id is null
            then 'appearance_competition_missing_games_used'
        when appearances.competition_id = games.competition_id then 'match'
        else 'mismatch_games_preferred'
    end as competition_comparison_status,

    games.season,
    games.game_round,
    games.resolved_competition_id,
    games.competition_code,
    games.competition_name,
    games.competition_type_at_game,
    games.competition_type_snapshot,
    games.competition_country_id,
    games.competition_country_name,
    games.competition_resolution_status
        as competition_catalog_resolution_status,

    games.home_club_id,
    games.home_club_name_at_game,
    games.away_club_id,
    games.away_club_name_at_game,

    club_games.club_id as matched_player_club_id,
    club_games.opponent_id as opponent_club_id,
    club_games.hosting as player_team_hosting,
    case
        when club_games.hosting = 'home' then true
        when club_games.hosting = 'away' then false
    end as is_home,
    case club_games.hosting
        when 'home' then games.home_club_name_at_game
        when 'away' then games.away_club_name_at_game
    end as player_club_name_at_game,
    case club_games.hosting
        when 'home' then games.away_club_name_at_game
        when 'away' then games.home_club_name_at_game
    end as opponent_club_name_at_game,
    case club_games.hosting
        when 'home' then games.home_club_name_current_snapshot
        when 'away' then games.away_club_name_current_snapshot
    end as player_club_name_current_snapshot,
    case club_games.hosting
        when 'home' then games.away_club_name_current_snapshot
        when 'away' then games.home_club_name_current_snapshot
    end as opponent_club_name_current_snapshot,
    case club_games.hosting
        when 'home' then games.home_club_resolution_status
        when 'away' then games.away_club_resolution_status
    end as player_team_catalog_resolution_status,
    case club_games.hosting
        when 'home' then games.away_club_resolution_status
        when 'away' then games.home_club_resolution_status
    end as opponent_catalog_resolution_status,

    club_games.own_goals as player_team_goals,
    club_games.opponent_goals,
    case
        when club_games.own_goals is null or club_games.opponent_goals is null
            then 'unknown'
        when club_games.own_goals > club_games.opponent_goals then 'win'
        when club_games.own_goals = club_games.opponent_goals then 'draw'
        else 'loss'
    end as player_team_result,
    club_games.is_win as club_games_is_win,
    games.home_club_goals,
    games.away_club_goals,
    club_games.own_position as player_team_position_at_game,
    club_games.opponent_position as opponent_position_at_game,
    club_games.own_manager_name as player_team_manager_name_at_game,
    club_games.opponent_manager_name as opponent_manager_name_at_game,

    appearances.minutes_played,
    appearances.goals,
    appearances.assists,
    appearances.yellow_cards,
    appearances.red_cards,

    case
        when games.game_id is not null then 'matched_game_id'
        else 'not_found'
    end as game_join_status,
    case
        when appearances.player_club_id is null then 'missing_player_club_id'
        when club_games.game_id is not null then 'matched_game_and_player_club'
        else 'not_found'
    end as club_game_join_status,
    case
        when games.game_id is null then 'games_not_found'
        when club_games.game_id is null then 'club_game_not_found'
        when club_games.hosting = 'home'
          and club_games.club_id = games.home_club_id
          and club_games.opponent_id = games.away_club_id then 'aligned_home'
        when club_games.hosting = 'away'
          and club_games.club_id = games.away_club_id
          and club_games.opponent_id = games.home_club_id then 'aligned_away'
        else 'inconsistent'
    end as team_context_status,

    appearances_manifest.source_version as appearances_source_version,
    appearances_manifest.source_version_checksum
        as appearances_source_version_checksum,
    appearances_manifest.source_captured_at as appearances_source_captured_at,
    appearances_manifest.bronze_ingestion_run_id
        as appearances_bronze_ingestion_run_id,
    club_games_manifest.source_version as club_games_source_version,
    club_games_manifest.source_version_checksum
        as club_games_source_version_checksum,
    club_games_manifest.source_captured_at as club_games_source_captured_at,
    club_games_manifest.bronze_ingestion_run_id
        as club_games_bronze_ingestion_run_id,
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
from appearances
left join games
    on appearances.game_id = games.game_id
left join club_games
    on appearances.game_id = club_games.game_id
   and appearances.player_club_id = club_games.club_id
left join source_manifest as appearances_manifest
    on appearances_manifest.asset_name = 'appearances'
left join source_manifest as club_games_manifest
    on club_games_manifest.asset_name = 'club_games'
left join source_manifest as games_manifest
    on games_manifest.asset_name = 'games'
left join source_manifest as competitions_manifest
    on competitions_manifest.asset_name = 'competitions'
left join source_manifest as clubs_manifest
    on clubs_manifest.asset_name = 'clubs'
