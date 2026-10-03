{{ config(tags=['silver_stage_6', 'fixture', 'player_valuations_enriched'], severity='error') }}

with valuations(
    player_id, valuation_date, market_value_eur,
    valuation_current_club_id, valuation_current_club_name
) as (
    select * from values
        (1, '2020-06-14'::date, 1000000::number(38,2), 10, 'First Club'),
        (1, '2020-06-15'::date, 1200000::number(38,2), 20, 'Second Club'),
        (2, '2021-01-01'::date, 500000::number(38,2), 30, 'Third Club'),
        (3, '2022-01-01'::date, 0::number(38,2), 40, 'Unknown Club')
),
players(
    player_id, date_of_birth, current_club_id, current_national_team_id,
    birth_country_id, birth_country_status, citizenship_value
) as (
    select * from values
        (1, '2000-06-15'::date, 99, 501, 11, 'matched_name', 'Country A'),
        (2, '1995-02-01'::date, 30, null, null, 'not_found', 'Country B')
),
enriched as (
    select
        valuations.*,
        players.player_id as resolved_player_id,
        players.current_national_team_id,
        players.birth_country_id,
        players.birth_country_status,
        players.citizenship_value,
        case
            when players.date_of_birth is null
              or valuations.valuation_date < players.date_of_birth then null
            else
                datediff(year, players.date_of_birth, valuations.valuation_date)
                - iff(
                    to_number(to_char(valuations.valuation_date, 'MMDD'))
                        < to_number(to_char(players.date_of_birth, 'MMDD')),
                    1,
                    0
                )
        end as age_at_valuation,
        case
            when players.player_id is null then 'profile_not_found'
            when players.date_of_birth is null then 'missing_birth_date'
            when valuations.valuation_date < players.date_of_birth
                then 'valuation_before_birth'
            else 'calculated'
        end as age_status,
        case
            when players.player_id is null then 'profile_not_found'
            when valuations.valuation_current_club_id = players.current_club_id
                then 'match'
            else 'mismatch'
        end as club_status
    from valuations
    left join players
        on valuations.player_id = players.player_id
),
actual as (
    select
        count(*) as row_count,
        count(distinct concat_ws(
            '||', to_varchar(player_id), to_varchar(valuation_date)
        )) as distinct_valuations,
        count_if(player_id = 1) as player_one_history_count,
        count_if(
            player_id = 1
            and valuation_date = '2020-06-14'::date
            and age_at_valuation = 19
            and age_status = 'calculated'
            and club_status = 'mismatch'
        ) as before_birthday,
        count_if(
            player_id = 1
            and valuation_date = '2020-06-15'::date
            and age_at_valuation = 20
            and age_status = 'calculated'
        ) as on_birthday,
        count_if(
            player_id = 2
            and current_national_team_id is null
            and birth_country_id is null
            and birth_country_status = 'not_found'
            and citizenship_value = 'Country B'
        ) as no_team_and_absent_country,
        count_if(
            player_id = 3
            and resolved_player_id is null
            and age_status = 'profile_not_found'
            and market_value_eur = 0
        ) as missing_profile_zero_value_retained
    from enriched
)
select *
from actual
where row_count <> 4
   or distinct_valuations <> 4
   or player_one_history_count <> 2
   or before_birthday <> 1
   or on_birthday <> 1
   or no_team_and_absent_country <> 1
   or missing_profile_zero_value_retained <> 1
