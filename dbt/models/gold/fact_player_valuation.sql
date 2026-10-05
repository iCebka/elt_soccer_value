with eligible as (

    select
        pv.player_id,
        pv.date as valuation_date,
        pv.current_club_id as club_id,
        pv.player_club_domestic_competition_id as competition_id,
        pv.market_value_in_eur

    from {{ ref('player_valuations') }} pv

    inner join {{ ref('dim_player') }} p
        on pv.player_id = p.player_id

    where pv.player_id is not null
      and pv.date is not null
      and pv.market_value_in_eur is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by player_id, valuation_date
    ) = 1
)

select
    player_id,
    valuation_date,
    to_number(to_char(valuation_date, 'YYYYMMDD')) as valuation_date_key,
    club_id,
    competition_id,
    market_value_in_eur

from non_duplicated
