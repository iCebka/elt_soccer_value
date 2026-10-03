with bronze as (
    {{ tm_selected_bronze_rows('countries') }}
)
select
    {{ tm_integer('raw_record:country_id') }} as country_id,
    {{ tm_text('raw_record:country_name') }} as country_name,
    {{ tm_upper_text('raw_record:country_code') }} as country_code,
    {{ tm_upper_text('raw_record:confederation') }} as confederation,
    {{ tm_integer('raw_record:total_clubs') }} as total_clubs,
    {{ tm_integer('raw_record:total_players') }} as total_players,
    {{ tm_decimal('raw_record:average_age', 2) }} as average_age,
    {{ tm_text('raw_record:url') }} as country_url,
    bronze.* exclude (raw_record),
    raw_record
from bronze
