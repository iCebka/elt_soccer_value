with parsed as (
    select
        try_cast(nullif(trim(player_id), '') as integer)
            as player_id,

        try_cast(nullif(trim(transfer_date), '') as date)
            as transfer_date,

        nullif(trim(transfer_season), '')
            as transfer_season,

        try_cast(nullif(trim(from_club_id), '') as integer)
            as from_club_id,

        try_cast(nullif(trim(to_club_id), '') as integer)
            as to_club_id,

        nullif(trim(from_club_name), '')
            as from_club_name,

        nullif(trim(to_club_name), '')
            as to_club_name,

        try_cast(nullif(trim(transfer_fee), '') as number(18, 2))
            as transfer_fee_raw,

        try_cast(nullif(trim(market_value_in_eur), '') as number(18, 2))
            as market_value_in_eur_raw,

        nullif(trim(player_name), '')
            as player_name,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'transfers') }}
),

normalized as (
    select
        player_id,
        transfer_date,
        transfer_season,
        from_club_id,
        to_club_id,
        from_club_name,
        to_club_name,

        case
            when transfer_fee_raw >= 0
            then transfer_fee_raw
        end as transfer_fee,

        case
            when market_value_in_eur_raw >= 0
            then market_value_in_eur_raw
        end as market_value_in_eur,

        player_name,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select t.*
    from normalized t

    where t.player_id is not null
      and t.transfer_date is not null
      and t.transfer_season is not null

      and (
          t.from_club_id is not null
          or t.to_club_id is not null
      )

      and exists (
          select 1
          from {{ ref('players') }} p
          where p.player_id = t.player_id
      )
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            player_id,
            transfer_date,
            transfer_season,
            from_club_id,
            to_club_id,
            from_club_name,
            to_club_name,
            transfer_fee,
            market_value_in_eur,
            player_name
        order by
            _ingested_at desc nulls last,
            _source_file,
            _source_row_number
    ) = 1
),

non_conflicting as (
    select *
    from exact_dedup

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
    transfer_season,
    from_club_id,
    to_club_id,
    from_club_name,
    to_club_name,
    transfer_fee,
    market_value_in_eur,
    player_name,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting