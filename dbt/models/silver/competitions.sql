with parsed as (
    select
        upper(nullif(trim(competition_id), ''))
            as competition_id,

        lower(nullif(trim(competition_code), ''))
            as competition_code,

        nullif(trim(name), '') as name,

        lower(nullif(trim(sub_type), '')) as sub_type,
        lower(nullif(trim(type), '')) as type,

        try_cast(nullif(trim(country_id), '') as integer)
            as country_id,

        nullif(trim(country_name), '') as country_name,

        upper(nullif(trim(domestic_league_code), ''))
            as domestic_league_code,

        lower(nullif(trim(confederation), ''))
            as confederation,

        try_cast(nullif(trim(total_clubs), '') as integer)
            as total_clubs_raw,

        nullif(trim(url), '') as url,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'competitions') }}
),

normalized as (
    select
        competition_id,
        competition_code,
        name,
        sub_type,
        type,
        country_id,
        country_name,
        domestic_league_code,
        confederation,

        case
            when total_clubs_raw >= 0
            then total_clubs_raw
        end as total_clubs,

        url,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where competition_id is not null
      and competition_code is not null
      and name is not null
      and sub_type is not null
      and type is not null
      and confederation is not null
      and url is not null
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            competition_id,
            competition_code,
            name,
            sub_type,
            type,
            country_id,
            country_name,
            domestic_league_code,
            confederation,
            total_clubs,
            url
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
        partition by competition_id
    ) = 1
)

select
    competition_id,
    competition_code,
    name,
    sub_type,
    type,
    country_id,
    country_name,
    domestic_league_code,
    confederation,
    total_clubs,
    url,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting