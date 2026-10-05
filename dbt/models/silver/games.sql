with parsed as (
    select
        try_cast(nullif(trim(game_id), '') as integer) as game_id,

        upper(nullif(trim(competition_id), ''))
            as competition_id,

        try_cast(nullif(trim(season), '') as integer)
            as season,

        nullif(trim(round), '')
            as round,

        try_cast(nullif(trim(date), '') as date)
            as date,

        try_cast(nullif(trim(home_club_id), '') as integer)
            as home_club_id,

        try_cast(nullif(trim(away_club_id), '') as integer)
            as away_club_id,

        try_cast(nullif(trim(home_club_goals), '') as integer)
            as home_club_goals_raw,

        try_cast(nullif(trim(away_club_goals), '') as integer)
            as away_club_goals_raw,

        try_cast(nullif(trim(home_club_position), '') as integer)
            as home_club_position_raw,

        try_cast(nullif(trim(away_club_position), '') as integer)
            as away_club_position_raw,

        nullif(trim(home_club_manager_name), '')
            as home_club_manager_name,

        nullif(trim(away_club_manager_name), '')
            as away_club_manager_name,

        nullif(trim(stadium), '') as stadium,

        try_cast(nullif(trim(attendance), '') as integer)
            as attendance_raw,

        nullif(trim(referee), '') as referee,
        nullif(trim(url), '') as url,

        nullif(trim(home_club_formation), '')
            as home_club_formation,

        nullif(trim(away_club_formation), '')
            as away_club_formation,

        nullif(trim(home_club_name), '')
            as home_club_name,

        nullif(trim(away_club_name), '')
            as away_club_name,

        nullif(trim(aggregate), '')
            as aggregate,

        lower(nullif(trim(competition_type), ''))
            as competition_type,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'games') }}
),

normalized as (
    select
        game_id,
        competition_id,
        season,
        round,
        date,
        home_club_id,
        away_club_id,

        case
            when home_club_goals_raw >= 0
            then home_club_goals_raw
        end as home_club_goals,

        case
            when away_club_goals_raw >= 0
            then away_club_goals_raw
        end as away_club_goals,

        case
            when home_club_position_raw >= 1
            then home_club_position_raw
        end as home_club_position,

        case
            when away_club_position_raw >= 1
            then away_club_position_raw
        end as away_club_position,

        home_club_manager_name,
        away_club_manager_name,
        stadium,

        case
            when attendance_raw >= 0
            then attendance_raw
        end as attendance,

        referee,
        url,
        home_club_formation,
        away_club_formation,
        home_club_name,
        away_club_name,
        aggregate,
        competition_type,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where game_id is not null
      and competition_id is not null
      and season is not null
      and round is not null
      and date is not null
      and home_club_id is not null
      and away_club_id is not null
      and home_club_id <> away_club_id
      and url is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            game_id,
            competition_id,
            season,
            round,
            date,
            home_club_id,
            away_club_id,
            home_club_goals,
            away_club_goals,
            home_club_position,
            away_club_position,
            home_club_manager_name,
            away_club_manager_name,
            stadium,
            attendance,
            referee,
            url,
            home_club_formation,
            away_club_formation,
            home_club_name,
            away_club_name,
            aggregate,
            competition_type
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
        partition by game_id
    ) = 1
)

select
    game_id,
    competition_id,
    season,
    round,
    date,
    home_club_id,
    away_club_id,
    home_club_goals,
    away_club_goals,
    home_club_position,
    away_club_position,
    home_club_manager_name,
    away_club_manager_name,
    stadium,
    attendance,
    referee,
    url,
    home_club_formation,
    away_club_formation,
    home_club_name,
    away_club_name,
    aggregate,
    competition_type,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting