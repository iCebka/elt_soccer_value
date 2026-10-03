{{ config(tags=['silver_stage_6', 'player_valuations_enriched'], severity='error') }}

select player_id, valuation_date
from {{ ref('silver_player_valuations_enriched') }}
where
    (age_at_valuation_status = 'calculated' and (
        player_date_of_birth is null
        or valuation_date < player_date_of_birth
        or age_at_valuation_years < 0
        or age_at_valuation_years <>
            datediff(year, player_date_of_birth, valuation_date)
            - iff(
                to_number(to_char(valuation_date, 'MMDD'))
                    < to_number(to_char(player_date_of_birth, 'MMDD')),
                1,
                0
            )
    ))
 or (age_at_valuation_status = 'missing_birth_date'
        and player_date_of_birth is not null)
 or (age_at_valuation_status = 'valuation_before_birth' and (
        player_date_of_birth is null
        or valuation_date >= player_date_of_birth
        or age_at_valuation_years is not null
    ))
 or (age_at_valuation_status = 'profile_not_found'
        and resolved_player_id is not null)
