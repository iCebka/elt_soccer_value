with eligible as (

    select
        t.player_id,
        t.transfer_date,
        t.transfer_season,
        t.from_club_id,
        t.to_club_id,
        t.transfer_fee,
        t.market_value_in_eur

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

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by
            player_id,
            transfer_date,
            from_club_id,
            to_club_id
    ) = 1
)

select
    player_id,
    transfer_date,
    to_number(to_char(transfer_date, 'YYYYMMDD')) as transfer_date_key,
    transfer_season,
    from_club_id,
    to_club_id,
    transfer_fee,
    market_value_in_eur

from non_duplicated
