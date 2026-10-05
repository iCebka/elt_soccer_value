with parsed as (
    select
        try_cast(nullif(trim(country_id), '') as integer)
            as country_id,

        nullif(trim(country_name), '')
            as country_name,

        upper(nullif(trim(country_code), ''))
            as country_code,

        lower(nullif(trim(confederation), ''))
            as confederation,

        try_cast(nullif(trim(total_clubs), '') as integer)
            as total_clubs_raw,

        try_cast(nullif(trim(total_players), '') as integer)
            as total_players_raw,

        try_cast(nullif(trim(average_age), '') as number(6, 2))
            as average_age_raw,

        nullif(trim(url), '') as url,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'countries') }}
),

normalized as (
    select
        country_id,
        country_name,
        country_code,
        confederation,

        case when total_clubs_raw >= 0
            then total_clubs_raw
        end as total_clubs,

        case when total_players_raw >= 0
            then total_players_raw
        end as total_players,

        case when average_age_raw > 0
            then average_age_raw
        end as average_age,

        url,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where country_id is not null
      and country_name is not null
      and country_code is not null
      and confederation is not null
      and url is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            country_id,
            country_name,
            country_code,
            confederation,
            total_clubs,
            total_players,
            average_age,
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
    qualify count(*) over (partition by country_id) = 1
),

unique_name as (
    select *
    from unique_id
    qualify count(*) over (partition by country_name) = 1
),

unique_code as (
    select *
    from unique_name
    qualify count(*) over (partition by country_code) = 1
)

select
    country_id,
    country_name,
    country_code,
    confederation,
    total_clubs,
    total_players,
    average_age,
    url,
    _source_file,
    _source_row_number,
    _ingested_at

from unique_code