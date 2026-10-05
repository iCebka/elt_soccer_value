with eligible as (

    select
        club_id,
        club_code,
        name,
        domestic_competition_id,
        stadium_name,
        stadium_seats,
        url

    from {{ ref('clubs') }}

    where club_id is not null
      and name is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by club_id
    ) = 1
)

select *
from non_duplicated
