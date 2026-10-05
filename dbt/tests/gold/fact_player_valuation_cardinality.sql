{{ config(tags=['gold']) }}

with eligible as (

    select
        pv.player_id,
        pv.date as valuation_date

    from {{ ref('player_valuations') }} pv

    inner join {{ ref('dim_player') }} p
        on pv.player_id = p.player_id

    where pv.player_id is not null
      and pv.date is not null
      and pv.market_value_in_eur is not null
),

expected_rows as (

    select *
    from eligible

    qualify count(*) over (
        partition by player_id, valuation_date
    ) = 1
),

expected as (
    select count(*) as row_count
    from expected_rows
),

actual as (
    select count(*) as row_count
    from {{ ref('fact_player_valuation') }}
)

select
    e.row_count as expected_rows,
    a.row_count as actual_rows

from expected e
cross join actual a

where e.row_count <> a.row_count
