with parsed as (
    select
        try_cast(nullif(trim(player_id), '') as integer)
            as player_id,

        nullif(trim(first_name), '')
            as first_name,

        nullif(trim(last_name), '')
            as last_name,

        nullif(trim(name), '')
            as name,

        try_cast(nullif(trim(last_season), '') as integer)
            as last_season,

        try_cast(nullif(trim(current_club_id), '') as integer)
            as current_club_id,

        lower(nullif(trim(player_code), ''))
            as player_code,

        nullif(trim(country_of_birth), '')
            as country_of_birth,

        nullif(trim(city_of_birth), '')
            as city_of_birth,

        nullif(trim(country_of_citizenship), '')
            as country_of_citizenship,

        try_cast(
            left(nullif(trim(date_of_birth), ''), 10)
            as date
        ) as date_of_birth_raw,

        nullif(trim(sub_position), '')
            as sub_position,

        nullif(trim(position), '')
            as position,

        lower(nullif(trim(foot), ''))
            as foot_raw,

        try_cast(nullif(trim(height_in_cm), '') as integer)
            as height_in_cm_raw,

        try_cast(
            left(
                nullif(trim(contract_expiration_date), ''),
                10
            )
            as date
        ) as contract_expiration_date,

        nullif(trim(agent_name), '')
            as agent_name,

        nullif(trim(image_url), '')
            as image_url,

        try_cast(nullif(trim(international_caps), '') as integer)
            as international_caps_raw,

        try_cast(nullif(trim(international_goals), '') as integer)
            as international_goals_raw,

        try_cast(nullif(trim(current_national_team_id), '') as integer)
            as current_national_team_id_raw,

        nullif(trim(url), '')
            as url,

        upper(
            nullif(
                trim(current_club_domestic_competition_id),
                ''
            )
        ) as current_club_domestic_competition_id,

        nullif(trim(current_club_name), '')
            as current_club_name,

        try_cast(nullif(trim(market_value_in_eur), '') as number(18, 2))
            as market_value_in_eur_raw,

        try_cast(nullif(trim(highest_market_value_in_eur), '') as number(18, 2))
            as highest_market_value_in_eur_raw,

        _source_file,
        _source_row_number,
        _ingested_at

    from {{ source('bronze', 'players') }}
),

normalized as (
    select
        player_id,
        first_name,
        last_name,
        name,
        last_season,

        current_club_id,

        player_code,
        country_of_birth,
        city_of_birth,
        country_of_citizenship,

        case
            when date_of_birth_raw <= current_date()
            then date_of_birth_raw
            else null
        end as date_of_birth,

        sub_position,
        position,

        case
            when foot_raw in ('left', 'right', 'both')
            then foot_raw
            else null
        end as foot,

        case
            when height_in_cm_raw > 0
            then height_in_cm_raw
        end as height_in_cm,

        contract_expiration_date,

        agent_name,
        image_url,

        case
            when international_caps_raw >= 0
            then international_caps_raw
        end as international_caps,

        case
            when international_goals_raw >= 0
            then international_goals_raw
        end as international_goals,

        case
            when current_national_team_id_raw = -1
            then null
            else current_national_team_id_raw
        end as current_national_team_id,

        url,
        current_club_domestic_competition_id,
        current_club_name,

        case
            when market_value_in_eur_raw >= 0
            then market_value_in_eur_raw
        end as market_value_in_eur,

        case
            when highest_market_value_in_eur_raw >= 0
            then highest_market_value_in_eur_raw
        end as highest_market_value_in_eur,

        _source_file,
        _source_row_number,
        _ingested_at

    from parsed
),

valid as (
    select *
    from normalized

    where player_id is not null
      and name is not null
      and last_season is not null
      and player_code is not null
      and url is not null

      and not (
          date_of_birth is not null
          and contract_expiration_date is not null
          and contract_expiration_date < date_of_birth
      )
),

exact_dedup as (
    select *
    from valid

    qualify row_number() over (
        partition by
            player_id,
            first_name,
            last_name,
            name,
            last_season,
            current_club_id,
            player_code,
            country_of_birth,
            city_of_birth,
            country_of_citizenship,
            date_of_birth,
            sub_position,
            position,
            foot,
            height_in_cm,
            contract_expiration_date,
            agent_name,
            image_url,
            international_caps,
            international_goals,
            current_national_team_id,
            url,
            current_club_domestic_competition_id,
            current_club_name,
            market_value_in_eur,
            highest_market_value_in_eur
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
        partition by player_id
    ) = 1
)

select
    player_id,
    first_name,
    last_name,
    name,
    last_season,
    current_club_id,
    player_code,
    country_of_birth,
    city_of_birth,
    country_of_citizenship,
    date_of_birth,
    sub_position,
    position,
    foot,
    height_in_cm,
    contract_expiration_date,
    agent_name,
    image_url,
    international_caps,
    international_goals,
    current_national_team_id,
    url,
    current_club_domestic_competition_id,
    current_club_name,
    market_value_in_eur,
    highest_market_value_in_eur,
    _source_file,
    _source_row_number,
    _ingested_at

from non_conflicting