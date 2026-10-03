with bronze as (
    {{ tm_selected_bronze_rows('players') }}
)
select
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_text('raw_record:first_name') }} as first_name,
    {{ tm_text('raw_record:last_name') }} as last_name,
    {{ tm_text('raw_record:name') }} as player_name,
    {{ tm_integer('raw_record:last_season') }} as last_season,
    {{ tm_integer('raw_record:current_club_id') }} as current_club_id,
    {{ tm_text('raw_record:player_code') }} as player_code,
    {{ tm_text('raw_record:country_of_birth') }} as country_of_birth,
    {{ tm_text('raw_record:city_of_birth') }} as city_of_birth,
    {{ tm_text('raw_record:country_of_citizenship') }} as country_of_citizenship,
    {{ tm_date('raw_record:date_of_birth') }} as date_of_birth,
    {{ tm_text('raw_record:sub_position') }} as sub_position,
    {{ tm_text('raw_record:position') }} as position_raw,
    {{ tm_lower_text('raw_record:foot') }} as preferred_foot,
    {{ tm_decimal('raw_record:height_in_cm', 1) }} as height_in_cm,
    {{ tm_date('raw_record:contract_expiration_date') }} as contract_expiration_date,
    {{ tm_text('raw_record:agent_name') }} as agent_name,
    {{ tm_text('raw_record:image_url') }} as image_url,
    {{ tm_integer('raw_record:international_caps') }} as international_caps,
    {{ tm_integer('raw_record:international_goals') }} as international_goals,
    {{ tm_integer('raw_record:current_national_team_id') }} as current_national_team_id,
    {{ tm_text('raw_record:url') }} as player_url,
    {{ tm_text('raw_record:current_club_domestic_competition_id') }} as current_club_domestic_competition_id,
    {{ tm_text('raw_record:current_club_name') }} as current_club_name,
    {{ tm_euro_amount('raw_record:market_value_in_eur') }} as market_value_eur,
    {{ tm_euro_amount('raw_record:highest_market_value_in_eur') }} as highest_market_value_eur,
    bronze.* exclude (raw_record),
    raw_record
from bronze
