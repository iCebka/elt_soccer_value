with eligible as (

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
        url

    from {{ ref('competitions') }}

    where competition_id is not null
      and name is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by competition_id
    ) = 1
)

select *
from non_duplicated
