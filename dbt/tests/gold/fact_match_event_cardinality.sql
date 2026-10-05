{{ config(tags=['gold']) }}

with eligible as (

    select
        e.game_event_id

    from {{ ref('game_events') }} e

    inner join {{ ref('games') }} g
        on e.game_id = g.game_id

    where e.game_event_id is not null
      and e.game_id is not null
      and e.date is not null
      and e.type is not null
),

expected_rows as (

    select *
    from eligible

    qualify count(*) over (
        partition by game_event_id
    ) = 1
),

expected as (
    select count(*) as row_count
    from expected_rows
),

actual as (
    select count(*) as row_count
    from {{ ref('fact_match_event') }}
)

select
    e.row_count as expected_rows,
    a.row_count as actual_rows

from expected e
cross join actual a

where e.row_count <> a.row_count
