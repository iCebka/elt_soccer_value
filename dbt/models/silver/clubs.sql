with parsed as (
    select
        try_cast(nullif(trim(club_id), '') as integer) as club_id,
        lower(nullif(trim(club_code), '')) as club_code,
        nullif(trim(name), '') as name,

        upper(nullif(trim(domestic_competition_id), ''))
            as domestic_competition_id,

        try_cast(nullif(trim(total_market_value), '') as number(18, 2))
            as total_market_value_raw,

        try_cast(nullif(trim(squad_size), '') as integer)
            as squad_size_raw,

        try_cast(nullif(trim(average_age), '') as number(6, 2))
            as average_age_raw,

        try_cast(nullif(trim(foreigners_number), '') as integer)
            as foreigners_number_raw,

        try_cast(nullif(trim(foreigners_percentage), '') as number(6, 2))
            as foreigners_percentage_raw,

        try_cast(nullif(trim(national_team_players), '') as integer)
            as national_team_players_raw,

        nullif(trim(stadium_name), '') as stadium_name,

        try_cast(nullif(trim(stadium_seats), '') as integer)
            as stadium_seats_raw,

        lower(nullif(trim(net_transfer_record), ''))
            as net_transfer_record_raw,

        nullif(trim(coach_name), '') as coach_name,

        try_cast(nullif(trim(last_season), '') as integer)
            as last_season,

        nullif(trim(filename), '') as filename,
        nullif(trim(url), '') as url,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'clubs') }}
),

normalized as (
    select
        club_id,
        club_code,
        name,
        domestic_competition_id,

        case
            when total_market_value_raw >= 0
            then total_market_value_raw
        end as total_market_value,

        case
            when squad_size_raw >= 0
            then squad_size_raw
        end as squad_size,

        case
            when average_age_raw > 0
            then average_age_raw
        end as average_age,

        case
            when foreigners_number_raw >= 0
            then foreigners_number_raw
        end as foreigners_number,

        case
            when foreigners_percentage_raw between 0 and 100
            then foreigners_percentage_raw
        end as foreigners_percentage,

        case
            when national_team_players_raw >= 0
            then national_team_players_raw
        end as national_team_players,

        stadium_name,

        case
            when stadium_seats_raw >= 0
            then stadium_seats_raw
        end as stadium_seats,

        cast(
            case
                when net_transfer_record_raw is null then null

                when right(net_transfer_record_raw, 1) = 'b' then
                    try_cast(
                        regexp_replace(net_transfer_record_raw, '[^0-9+.-]', '')
                        as number(18, 4)
                    ) * 1000000000

                when right(net_transfer_record_raw, 1) = 'm' then
                    try_cast(
                        regexp_replace(net_transfer_record_raw, '[^0-9+.-]', '')
                        as number(18, 4)
                    ) * 1000000

                when right(net_transfer_record_raw, 1) = 'k' then
                    try_cast(
                        regexp_replace(net_transfer_record_raw, '[^0-9+.-]', '')
                        as number(18, 4)
                    ) * 1000

                else
                    try_cast(
                        regexp_replace(net_transfer_record_raw, '[^0-9+.-]', '')
                        as number(18, 4)
                    )
            end
            as number(18, 2)
        ) as net_transfer_record,

        coach_name,
        last_season,
        filename,
        url,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where club_id is not null
      and club_code is not null
      and name is not null
      and last_season is not null
      and filename is not null
      and url is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            club_id,
            club_code,
            name,
            domestic_competition_id,
            total_market_value,
            squad_size,
            average_age,
            foreigners_number,
            foreigners_percentage,
            national_team_players,
            stadium_name,
            stadium_seats,
            net_transfer_record,
            coach_name,
            last_season,
            filename,
            url
        order by
            _ingested_at desc nulls last,
            _source_file,
            _source_row_number
    ) = 1
),

unique_id as (
    select *
    from exact_dedup

    qualify count(*) over (
        partition by club_id
    ) = 1
),

unique_code as (
    select *
    from unique_id

    qualify count(*) over (
        partition by club_code
    ) = 1
)

select
    club_id,
    club_code,
    name,
    domestic_competition_id,
    total_market_value,
    squad_size,
    average_age,
    foreigners_number,
    foreigners_percentage,
    national_team_players,
    stadium_name,
    stadium_seats,
    net_transfer_record,
    coach_name,
    last_season,
    filename,
    url,
    _source_file,
    _source_row_number,
    _ingested_at

from unique_code