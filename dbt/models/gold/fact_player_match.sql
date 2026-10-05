with lineup_by_player_match as (

    select
        game_id,
        player_id,

        case
            when max(
                case
                    when type = 'starting_lineup' then 1
                    else 0
                end
            ) = 1
            then true
            else false
        end as is_starter,

        case
            when count_if(team_captain is not null) = 0 then null
            when max(
                case
                    when team_captain = true then 1
                    else 0
                end
            ) = 1
            then true
            else false
        end as is_captain

    from {{ ref('game_lineups') }}

    group by
        game_id,
        player_id
),

eligible as (

    select
        a.appearance_id,
        a.game_id,
        a.player_id,
        a.player_club_id as club_id,
        g.competition_id,
        g.date as game_date,
        a.minutes_played,
        a.goals,
        a.assists,
        a.yellow_cards,
        a.red_cards

    from {{ ref('appearances') }} a

    inner join {{ ref('games') }} g
        on a.game_id = g.game_id

    inner join {{ ref('dim_player') }} p
        on a.player_id = p.player_id

    where a.game_id is not null
      and a.player_id is not null
      and g.date is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by game_id, player_id
    ) = 1
),

enriched as (

    select
        e.appearance_id,
        e.game_id,
        e.player_id,
        e.game_date,
        to_number(to_char(e.game_date, 'YYYYMMDD')) as game_date_key,
        e.club_id,
        cg.opponent_id as opponent_club_id,
        e.competition_id,
        cg.hosting as home_or_away,
        cg.is_win,
        l.is_starter,
        l.is_captain,
        e.minutes_played,
        e.goals,
        e.assists,
        e.yellow_cards,
        e.red_cards

    from non_duplicated e

    left join {{ ref('club_games') }} cg
        on e.game_id = cg.game_id
       and e.club_id = cg.club_id

    left join lineup_by_player_match l
        on e.game_id = l.game_id
       and e.player_id = l.player_id
)

select *
from enriched
