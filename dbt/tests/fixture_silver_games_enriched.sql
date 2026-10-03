{{ config(tags=['silver_stage_4', 'fixture', 'games_enriched'], severity='error') }}

with games(
    game_id, competition_id, game_date, home_club_id, away_club_id,
    home_club_name, away_club_name, home_goals, away_goals
) as (
    select * from values
        (1, 'C1', '2024-01-01'::date, 10, 20, 'Historic Home', 'Historic Away', 2, 1),
        (2, 'N1', '2024-01-02'::date, 30, 40, 'Germany', 'Nowhere', 0, 0)
),
competitions(competition_id, competition_name) as (
    select * from values ('C1', 'Catalog Competition')
),
clubs(club_id, club_name) as (
    select * from values
        (10, 'Current Home'),
        (20, 'Historic Away')
),
enriched as (
    select
        games.*,
        competitions.competition_id as resolved_competition_id,
        competitions.competition_name,
        home.club_id as resolved_home_club_id,
        home.club_name as home_club_name_current_snapshot,
        away.club_id as resolved_away_club_id,
        away.club_name as away_club_name_current_snapshot,
        iff(competitions.competition_id is null, 'not_found', 'matched_id')
            as competition_status,
        iff(home.club_id is null, 'not_found', 'matched_id') as home_status,
        iff(away.club_id is null, 'not_found', 'matched_id') as away_status
    from games
    left join competitions
        on games.competition_id = competitions.competition_id
    left join clubs as home
        on games.home_club_id = home.club_id
    left join clubs as away
        on games.away_club_id = away.club_id
),
actual as (
    select
        count(*) as row_count,
        count(distinct game_id) as distinct_games,
        count_if(
            game_id = 1
            and home_club_name = 'Historic Home'
            and home_club_name_current_snapshot = 'Current Home'
            and away_club_name = 'Historic Away'
            and away_club_name_current_snapshot = 'Historic Away'
            and home_status = 'matched_id'
            and away_status = 'matched_id'
            and competition_status = 'matched_id'
        ) as roles_and_historical_name_retained,
        count_if(
            game_id = 2
            and home_club_name = 'Germany'
            and resolved_home_club_id is null
            and resolved_away_club_id is null
            and resolved_competition_id is null
            and home_status = 'not_found'
            and away_status = 'not_found'
            and competition_status = 'not_found'
        ) as unresolved_game_retained
    from enriched
)
select *
from actual
where row_count <> 2
   or distinct_games <> 2
   or roles_and_historical_name_retained <> 1
   or unresolved_game_retained <> 1
