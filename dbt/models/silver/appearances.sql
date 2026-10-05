with parsed as (
    select
        nullif(trim(appearance_id), '') as appearance_id,
        try_cast(nullif(trim(game_id), '') as integer) as game_id,
        try_cast(nullif(trim(player_id), '') as integer) as player_id,
        try_cast(nullif(trim(player_club_id), '') as integer) as player_club_id,
        try_cast(nullif(trim(player_current_club_id), '') as integer)
            as player_current_club_id,
        try_cast(nullif(trim(date), '') as date) as date,
        nullif(trim(player_name), '') as player_name,
        upper(nullif(trim(competition_id), '')) as competition_id,

        try_cast(nullif(trim(yellow_cards), '') as integer) as yellow_cards_raw,
        try_cast(nullif(trim(red_cards), '') as integer) as red_cards_raw,
        try_cast(nullif(trim(goals), '') as integer) as goals_raw,
        try_cast(nullif(trim(assists), '') as integer) as assists_raw,
        try_cast(nullif(trim(minutes_played), '') as integer) as minutes_played_raw,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'appearances') }}
),

normalized as (
    select
        appearance_id,
        game_id,
        player_id,
        player_club_id,
        player_current_club_id,
        date,
        player_name,
        competition_id,

        case when yellow_cards_raw >= 0 then yellow_cards_raw end as yellow_cards,
        case when red_cards_raw >= 0 then red_cards_raw end as red_cards,
        case when goals_raw >= 0 then goals_raw end as goals,
        case when assists_raw >= 0 then assists_raw end as assists,
        case when minutes_played_raw >= 0 then minutes_played_raw end as minutes_played,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized
    where appearance_id is not null
      and game_id is not null
      and player_id is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            appearance_id,
            game_id,
            player_id,
            player_club_id,
            player_current_club_id,
            date,
            player_name,
            competition_id,
            yellow_cards,
            red_cards,
            goals,
            assists,
            minutes_played
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
        partition by appearance_id
    ) = 1
)

select
    appearance_id,
    game_id,
    player_id,
    player_club_id,
    player_current_club_id,
    date,
    player_name,
    competition_id,
    yellow_cards,
    red_cards,
    goals,
    assists,
    minutes_played,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting