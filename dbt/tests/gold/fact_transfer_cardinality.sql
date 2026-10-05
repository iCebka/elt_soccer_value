{{ config(tags=['gold']) }}

with eligible as (

    select
        t.player_id,
        t.transfer_date,
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
),

expected_rows as (

    select *
    from eligible

    qualify count(*) over (
        partition by
            player_id,
            transfer_date,
            from_club_id,
            to_club_id
    ) = 1
),

expected as (
    select count(*) as row_count
    from expected_rows
),

actual as (
    select count(*) as row_count
    from {{ ref('fact_transfer') }}
)

select
    e.row_count as expected_rows,
    a.row_count as actual_rows

from expected e
cross join actual a

where e.row_count <> a.row_count
