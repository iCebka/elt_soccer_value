with parsed as (
    select
        try_cast(nullif(trim(game_id), '') as integer) as game_id,
        try_cast(nullif(trim(club_id), '') as integer) as club_id,

        try_cast(nullif(trim(own_goals), '') as integer) as own_goals_raw,
        try_cast(nullif(trim(own_position), '') as integer) as own_position_raw,

        nullif(trim(own_manager_name), '') as own_manager_name,

        try_cast(nullif(trim(opponent_id), '') as integer) as opponent_id,
        try_cast(nullif(trim(opponent_goals), '') as integer) as opponent_goals_raw,
        try_cast(nullif(trim(opponent_position), '') as integer) as opponent_position_raw,

        nullif(trim(opponent_manager_name), '') as opponent_manager_name,

        lower(nullif(trim(hosting), '')) as hosting_raw,
        nullif(trim(is_win), '') as is_win_raw,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'club_games') }}
),

normalized as (
    select
        game_id,
        club_id,

        case when own_goals_raw >= 0 then own_goals_raw end as own_goals,
        case when own_position_raw >= 1 then own_position_raw end as own_position,

        own_manager_name,

        opponent_id,

        case when opponent_goals_raw >= 0 then opponent_goals_raw end as opponent_goals,
        case when opponent_position_raw >= 1 then opponent_position_raw end as opponent_position,

        opponent_manager_name,

        case
            when hosting_raw in ('home', 'away') then hosting_raw
            else null
        end as hosting,

        case
            when is_win_raw = '1' then true
            when is_win_raw = '0' then false
            else null
        end as is_win,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where game_id is not null
      and club_id is not null
      and opponent_id is not null

      and not (
          is_win is not null
          and own_goals is not null
          and opponent_goals is not null
          and is_win <> (own_goals > opponent_goals)
      )
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            game_id,
            club_id,
            own_goals,
            own_position,
            own_manager_name,
            opponent_id,
            opponent_goals,
            opponent_position,
            opponent_manager_name,
            hosting,
            is_win
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
        partition by game_id, club_id
    ) = 1
)

select
    game_id,
    club_id,
    own_goals,
    own_position,
    own_manager_name,
    opponent_id,
    opponent_goals,
    opponent_position,
    opponent_manager_name,
    hosting,
    is_win,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting