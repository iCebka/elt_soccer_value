with eligible as (

    select
        e.game_event_id,
        e.game_id,
        e.date as event_date,
        g.competition_id,
        e.minute,
        e.type as event_type,
        e.player_id,
        e.club_id,
        e.player_assist_id,
        e.player_in_id,
        e.description

    from {{ ref('game_events') }} e

    inner join {{ ref('games') }} g
        on e.game_id = g.game_id

    where e.game_event_id is not null
      and e.game_id is not null
      and e.date is not null
      and e.type is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by game_event_id
    ) = 1
)

select
    game_event_id,
    game_id,
    event_date,
    to_number(to_char(event_date, 'YYYYMMDD')) as event_date_key,
    competition_id,
    minute,
    event_type,
    player_id,
    club_id,
    player_assist_id,
    player_in_id,
    description

from non_duplicated
