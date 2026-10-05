with eligible as (

    select
        player_id,
        player_code,
        name,
        first_name,
        last_name,
        date_of_birth,
        country_of_birth,
        city_of_birth,
        country_of_citizenship,
        position,
        sub_position,
        foot,
        height_in_cm,
        contract_expiration_date

    from {{ ref('players') }}

    where player_id is not null
      and name is not null
      and date_of_birth is not null
),

non_duplicated as (

    select *
    from eligible

    qualify count(*) over (
        partition by player_id
    ) = 1
)

select *
from non_duplicated
