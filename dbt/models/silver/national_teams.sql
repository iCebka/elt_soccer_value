with parsed as (
    select
        try_cast(nullif(trim(national_team_id), '') as integer)
            as national_team_id,

        nullif(trim(name), '') as name,

        lower(nullif(trim(team_code), ''))
            as team_code,

        try_cast(nullif(trim(country_id), '') as integer)
            as country_id,

        nullif(trim(country_name), '')
            as country_name,

        upper(nullif(trim(country_code), ''))
            as country_code,

        upper(nullif(trim(confederation), ''))
            as confederation,

        nullif(trim(team_image_url), '')
            as team_image_url,

        try_cast(nullif(trim(squad_size), '') as integer)
            as squad_size_raw,

        try_cast(nullif(trim(average_age), '') as number(6, 2))
            as average_age_raw,

        try_cast(nullif(trim(foreigners_number), '') as integer)
            as foreigners_number_raw,

        try_cast(nullif(trim(foreigners_percentage), '') as number(6, 2))
            as foreigners_percentage_raw,

        try_cast(nullif(trim(total_market_value), '') as number(18, 2))
            as total_market_value_raw,

        nullif(trim(coach_name), '')
            as coach_name,

        try_cast(nullif(trim(fifa_ranking), '') as integer)
            as fifa_ranking_raw,

        try_cast(nullif(trim(last_season), '') as integer)
            as last_season,

        nullif(trim(url), '') as url,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'national_teams') }}
),

normalized as (
    select
        national_team_id,
        name,
        team_code,
        country_id,
        country_name,
        country_code,
        confederation,
        team_image_url,

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
            when total_market_value_raw >= 0
            then total_market_value_raw
        end as total_market_value,

        coach_name,

        case
            when fifa_ranking_raw >= 1
            then fifa_ranking_raw
        end as fifa_ranking,

        last_season,
        url,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select n.*
    from normalized n

    where n.national_team_id is not null
      and n.name is not null
      and n.team_code is not null
      and n.country_id is not null
      and n.country_name is not null
      and n.country_code is not null
      and n.confederation is not null
      and n.last_season is not null
      and n.url is not null

      and exists (
          select 1
          from {{ ref('countries') }} c
          where c.country_id = n.country_id
      )
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            national_team_id,
            name,
            team_code,
            country_id,
            country_name,
            country_code,
            confederation,
            team_image_url,
            squad_size,
            average_age,
            foreigners_number,
            foreigners_percentage,
            total_market_value,
            coach_name,
            fifa_ranking,
            last_season,
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
        partition by national_team_id
    ) = 1
)

select
    national_team_id,
    name,
    team_code,
    country_id,
    country_name,
    country_code,
    confederation,
    team_image_url,
    squad_size,
    average_age,
    foreigners_number,
    foreigners_percentage,
    total_market_value,
    coach_name,
    fifa_ranking,
    last_season,
    url,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting