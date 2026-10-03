with bronze as (
    {{ tm_selected_bronze_rows('national_teams') }}
)
select
    {{ tm_integer('raw_record:national_team_id') }} as national_team_id,
    {{ tm_text('raw_record:name') }} as national_team_name,
    {{ tm_upper_text('raw_record:team_code') }} as team_code,
    {{ tm_integer('raw_record:country_id') }} as country_id,
    {{ tm_text('raw_record:country_name') }} as country_name,
    {{ tm_upper_text('raw_record:country_code') }} as country_code,
    {{ tm_upper_text('raw_record:confederation') }} as confederation,
    {{ tm_text('raw_record:team_image_url') }} as team_image_url,
    {{ tm_integer('raw_record:squad_size') }} as squad_size,
    {{ tm_decimal('raw_record:average_age', 2) }} as average_age,
    {{ tm_integer('raw_record:foreigners_number') }} as foreigners_number,
    {{ tm_decimal('raw_record:foreigners_percentage', 2) }} as foreigners_percentage,
    {{ tm_euro_amount('raw_record:total_market_value') }} as total_market_value_eur,
    {{ tm_text('raw_record:coach_name') }} as coach_name,
    {{ tm_integer('raw_record:fifa_ranking') }} as fifa_ranking,
    {{ tm_integer('raw_record:last_season') }} as last_season,
    {{ tm_text('raw_record:url') }} as national_team_url,
    bronze.* exclude (raw_record),
    raw_record
from bronze
