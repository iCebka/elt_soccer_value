with parsed as (
    select
        nullif(trim(game_lineups_id), '') as game_lineups_id,
        try_cast(nullif(trim(date), '') as date) as date,

        try_cast(nullif(trim(game_id), '') as integer) as game_id,
        try_cast(nullif(trim(player_id), '') as integer) as player_id,
        try_cast(nullif(trim(club_id), '') as integer) as club_id,

        nullif(trim(player_name), '') as player_name,

        lower(nullif(trim(type), '')) as type_raw,

        nullif(trim(position), '') as position,
        nullif(trim(number), '') as number_raw,
        nullif(trim(team_captain), '') as team_captain_raw,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'game_lineups') }}
),

normalized as (
    select
        game_lineups_id,
        date,
        game_id,
        player_id,
        club_id,
        player_name,

        case
            when type_raw in ('starting_lineup', 'substitutes')
            then type_raw
            else null
        end as type,

        position,

        case
            when number_raw = '-' then null
            else try_cast(number_raw as integer)
        end as number,

        case
            when team_captain_raw = '1' then true
            when team_captain_raw = '0' then false
            else null
        end as team_captain,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where game_lineups_id is not null
      and date is not null
      and game_id is not null
      and player_id is not null
      and club_id is not null
      and type is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            game_lineups_id,
            date,
            game_id,
            player_id,
            club_id,
            player_name,
            type,
            position,
            number,
            team_captain
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
        partition by game_lineups_id
    ) = 1
)

select
    game_lineups_id,
    date,
    game_id,
    player_id,
    club_id,
    player_name,
    type,
    position,
    number,
    team_captain,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting