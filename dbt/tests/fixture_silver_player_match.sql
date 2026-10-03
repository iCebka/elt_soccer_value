{{ config(tags=['silver_stage_5', 'fixture', 'player_match'], severity='error') }}

with appearances(
    appearance_id, game_id, player_id, player_club_id,
    player_current_club_id, appearance_date, competition_id, minutes_played
) as (
    select * from values
        ('a_home', 1, 101, 10, 99, '2024-01-01'::date, 'C1', 135),
        ('a_away', 1, 102, 20, 20, '2024-01-02'::date, 'C2', 90),
        ('a_missing_club_game', 2, 103, 30, 30, '2024-02-01'::date, 'C1', 0),
        ('a_missing_game', 3, 104, 40, 50, '2024-03-01'::date, 'C3', 45)
),
games(
    game_id, game_date, competition_id, home_club_id, away_club_id,
    home_name, away_name, home_goals, away_goals
) as (
    select * from values
        (1, '2024-01-01'::date, 'C1', 10, 20, 'Home FC', 'Away FC', 2, 1),
        (2, '2024-02-01'::date, 'C1', 30, 31, 'Third FC', 'Fourth FC', 0, 0)
),
club_games(
    game_id, club_id, opponent_id, hosting, own_goals, opponent_goals, is_win
) as (
    select * from values
        (1, 10, 20, 'home', 2, 1, true),
        (1, 20, 10, 'away', 1, 2, false),
        (3, 40, 41, 'home', 1, 0, true)
),
enriched as (
    select
        appearances.*,
        games.game_id as matched_game_id,
        games.game_date as game_date_source,
        coalesce(games.game_date, appearances.appearance_date) as match_date,
        games.competition_id as game_competition_id,
        coalesce(games.competition_id, appearances.competition_id)
            as canonical_competition_id,
        club_games.club_id as matched_player_club_id,
        club_games.opponent_id,
        club_games.hosting,
        case club_games.hosting
            when 'home' then games.home_name
            when 'away' then games.away_name
        end as player_team_name,
        case club_games.hosting
            when 'home' then games.away_name
            when 'away' then games.home_name
        end as opponent_name,
        case
            when games.game_id is null then 'games_not_found_appearances_used'
            when appearances.appearance_date = games.game_date then 'match'
            else 'mismatch_games_preferred'
        end as date_status,
        case
            when games.game_id is null then 'games_not_found_appearances_used'
            when appearances.competition_id = games.competition_id then 'match'
            else 'mismatch_games_preferred'
        end as competition_status,
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
        end as team_context_status
    from appearances
    left join games
        on appearances.game_id = games.game_id
    left join club_games
        on appearances.game_id = club_games.game_id
       and appearances.player_club_id = club_games.club_id
),
actual as (
    select
        count(*) as row_count,
        count(distinct appearance_id) as distinct_appearances,
        count_if(
            appearance_id = 'a_home'
            and player_club_id = 10
            and player_current_club_id = 99
            and player_team_name = 'Home FC'
            and opponent_name = 'Away FC'
            and team_context_status = 'aligned_home'
            and minutes_played = 135
        ) as changed_club_and_home_role,
        count_if(
            appearance_id = 'a_away'
            and player_team_name = 'Away FC'
            and opponent_name = 'Home FC'
            and team_context_status = 'aligned_away'
            and match_date = '2024-01-01'::date
            and canonical_competition_id = 'C1'
            and date_status = 'mismatch_games_preferred'
            and competition_status = 'mismatch_games_preferred'
        ) as away_role_and_discrepancies,
        count_if(
            appearance_id = 'a_missing_club_game'
            and matched_game_id = 2
            and matched_player_club_id is null
            and team_context_status = 'club_game_not_found'
            and minutes_played = 0
        ) as missing_club_game_retained,
        count_if(
            appearance_id = 'a_missing_game'
            and matched_game_id is null
            and matched_player_club_id = 40
            and match_date = appearance_date
            and canonical_competition_id = competition_id
            and team_context_status = 'games_not_found'
        ) as missing_game_retained
    from enriched
)
select *
from actual
where row_count <> 4
   or distinct_appearances <> 4
   or changed_club_and_home_role <> 1
   or away_role_and_discrepancies <> 1
   or missing_club_game_retained <> 1
   or missing_game_retained <> 1
