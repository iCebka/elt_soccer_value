with parsed as (
    select
        nullif(trim(game_event_id), '') as game_event_id,
        try_cast(nullif(trim(date), '') as date) as date,
        try_cast(nullif(trim(game_id), '') as integer) as game_id,
        try_cast(nullif(trim(minute), '') as integer) as minute_raw,

        lower(nullif(trim(type), '')) as type,

        try_cast(nullif(trim(club_id), '') as integer) as club_id,
        nullif(trim(club_name), '') as club_name,

        try_cast(nullif(trim(player_id), '') as integer) as player_id,
        nullif(trim(description), '') as description,

        try_cast(nullif(trim(player_in_id), '') as integer) as player_in_id,
        try_cast(nullif(trim(player_assist_id), '') as integer) as player_assist_id,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'game_events') }}
),

normalized as (
    select
        game_event_id,
        date,
        game_id,

        case
            when minute_raw = -1 then -1
            when minute_raw >= 0 then minute_raw
            else null
        end as minute,

        type,
        club_id,
        club_name,
        player_id,
        description,
        player_in_id,
        player_assist_id,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where game_event_id is not null
      and date is not null
      and game_id is not null
      and minute is not null
      and type is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            game_event_id,
            date,
            game_id,
            minute,
            type,
            club_id,
            club_name,
            player_id,
            description,
            player_in_id,
            player_assist_id
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
        partition by game_event_id
    ) = 1
)

select
    game_event_id,
    date,
    game_id,
    minute,
    type,
    club_id,
    club_name,
    player_id,
    description,
    player_in_id,
    player_assist_id,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting