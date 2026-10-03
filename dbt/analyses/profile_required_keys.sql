with appearances as (
    select appearance_id
    from {{ ref('base_tm__appearances') }}
),
appearance_profile as (
    select
        count(*) as total_rows,
        count_if(appearance_id is null) as invalid_key_rows,
        count(*)
            - count_if(appearance_id is null)
            - count(distinct appearance_id) as duplicate_excess_rows
    from appearances
),
valuations as (
    select player_id, valuation_date
    from {{ ref('base_tm__player_valuations') }}
),
valuation_profile as (
    select
        count(*) as total_rows,
        count_if(player_id is null or valuation_date is null) as invalid_key_rows
    from valuations
),
valuation_duplicates as (
    select coalesce(sum(group_rows - 1), 0) as duplicate_excess_rows
    from (
        select count(*) as group_rows
        from valuations
        where player_id is not null and valuation_date is not null
        group by player_id, valuation_date
        having count(*) > 1
    )
)
select
    'appearance_id' as candidate_key,
    total_rows,
    invalid_key_rows,
    duplicate_excess_rows
from appearance_profile
union all
select
    'player_id + valuation_date' as candidate_key,
    total_rows,
    invalid_key_rows,
    duplicate_excess_rows
from valuation_profile
cross join valuation_duplicates
