with recursive

valuation_dates as (

    select
        pv.player_id,
        pv.date as event_date

    from {{ ref('player_valuations') }} pv

    inner join {{ ref('dim_player') }} p
        on pv.player_id = p.player_id

    where pv.player_id is not null
      and pv.date is not null
      and pv.market_value_in_eur is not null

    qualify count(*) over (
        partition by pv.player_id, pv.date
    ) = 1
),

player_match_dates as (

    select
        a.game_id,
        a.player_id,
        g.date as event_date

    from {{ ref('appearances') }} a

    inner join {{ ref('games') }} g
        on a.game_id = g.game_id

    inner join {{ ref('dim_player') }} p
        on a.player_id = p.player_id

    where a.game_id is not null
      and a.player_id is not null
      and g.date is not null

    qualify count(*) over (
        partition by a.game_id, a.player_id
    ) = 1
),

match_event_dates as (

    select
        e.game_event_id,
        e.date as event_date

    from {{ ref('game_events') }} e

    inner join {{ ref('games') }} g
        on e.game_id = g.game_id

    where e.game_event_id is not null
      and e.game_id is not null
      and e.date is not null
      and e.type is not null

    qualify count(*) over (
        partition by e.game_event_id
    ) = 1
),

transfer_dates as (

    select
        t.player_id,
        t.transfer_date as event_date,
        t.from_club_id,
        t.to_club_id

    from {{ ref('transfers') }} t

    inner join {{ ref('dim_player') }} p
        on t.player_id = p.player_id

    where t.player_id is not null
      and t.transfer_date is not null
      and t.transfer_season is not null
      and (
          t.from_club_id is not null
          or t.to_club_id is not null
      )

    qualify count(*) over (
        partition by
            t.player_id,
            t.transfer_date,
            t.from_club_id,
            t.to_club_id
    ) = 1
),

all_dates as (

    select event_date from valuation_dates
    union all
    select event_date from player_match_dates
    union all
    select event_date from match_event_dates
    union all
    select event_date from transfer_dates
),

bounds as (

    select
        min(event_date) as min_date,
        max(event_date) as max_date

    from all_dates
),

date_spine(date) as (

    select min_date
    from bounds
    where min_date is not null

    union all

    select dateadd(day, 1, d.date)
    from date_spine d
    cross join bounds b
    where d.date < b.max_date
)

select
    to_number(to_char(date, 'YYYYMMDD')) as date_key,
    date,
    year(date) as year,
    quarter(date) as quarter,
    month(date) as month,
    monthname(date) as month_name,
    day(date) as day,
    dayofweekiso(date) as day_of_week,
    dayname(date) as day_name,
    weekiso(date) as week_of_year

from date_spine
