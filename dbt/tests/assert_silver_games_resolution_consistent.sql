{{ config(tags=['silver_stage_4', 'games_enriched'], severity='error') }}

select game_id
from {{ ref('silver_games_enriched') }}
where
    ((competition_resolution_status = 'matched_id')
        <> (resolved_competition_id is not null))
 or ((home_club_resolution_status = 'matched_id')
        <> (resolved_home_club_id is not null))
 or ((away_club_resolution_status = 'matched_id')
        <> (resolved_away_club_id is not null))
 or (competition_resolution_status = 'missing_source_id'
        <> (competition_id is null))
 or (home_club_resolution_status = 'missing_source_id'
        <> (home_club_id is null))
 or (away_club_resolution_status = 'missing_source_id'
        <> (away_club_id is null))
