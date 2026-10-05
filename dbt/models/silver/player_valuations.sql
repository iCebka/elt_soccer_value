with parsed as (
    select
        try_cast(nullif(trim(player_id), '') as integer)
            as player_id,

        try_cast(nullif(trim(date), '') as date)
            as date,

        try_cast(nullif(trim(market_value_in_eur), '') as number(18, 2))
            as market_value_in_eur_raw,

        coalesce(
            nullif(trim(current_club_name), ''),
            'Unknown'
        ) as current_club_name,

        try_cast(nullif(trim(current_club_id), '') as integer)
            as current_club_id,

        upper(
            nullif(
                trim(player_club_domestic_competition_id),
                ''
            )
        ) as player_club_domestic_competition_id,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'player_valuations') }}
),

normalized as (
    select
        player_id,
        date,

        case
            when market_value_in_eur_raw >= 0
            then market_value_in_eur_raw
        end as market_value_in_eur,

        current_club_name,
        current_club_id,
        player_club_domestic_competition_id,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select v.*
    from normalized v

    where v.player_id is not null
      and v.date is not null
      and v.market_value_in_eur is not null

      and exists (
          select 1
          from {{ ref('players') }} p
          where p.player_id = v.player_id
      )
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            player_id,
            date,
            market_value_in_eur,
            current_club_name,
            current_club_id,
            player_club_domestic_competition_id
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
        partition by player_id, date
    ) = 1
)

select
    player_id,
    date,
    market_value_in_eur,
    current_club_name,
    current_club_id,
    player_club_domestic_competition_id,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting