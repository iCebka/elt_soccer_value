{{ config(tags=['gold']) }}

with eligible as (

    select
        a.game_id,
        a.player_id

    from {{ ref('appearances') }} a

    inner join {{ ref('games') }} g
        on a.game_id = g.game_id

    inner join {{ ref('dim_player') }} p
        on a.player_id = p.player_id

    where a.game_id is not null
      and a.player_id is not null
      and g.date is not null
),

expected_rows as (

    select *
    from eligible

    qualify count(*) over (
        partition by game_id, player_id
    ) = 1
),

expected as (
    select count(*) as row_count
    from expected_rows
),

actual as (
    select count(*) as row_count
    from {{ ref('fact_player_match') }}
)

select
    e.row_count as expected_rows,
    a.row_count as actual_rows

from expected e
cross join actual a

where e.row_count <> a.row_count
