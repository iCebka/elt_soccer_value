with bronze as (
    {{ tm_selected_bronze_rows('competitions') }}
)
select
    {{ tm_text('raw_record:competition_id') }} as competition_id,
    {{ tm_upper_text('raw_record:competition_code') }} as competition_code,
    {{ tm_text('raw_record:name') }} as competition_name,
    {{ tm_lower_text('raw_record:sub_type') }} as competition_sub_type,
    {{ tm_lower_text('raw_record:type') }} as competition_type,
    {{ tm_integer('raw_record:country_id') }} as country_id,
    {{ tm_text('raw_record:country_name') }} as country_name,
    {{ tm_text('raw_record:domestic_league_code') }} as domestic_league_code,
    {{ tm_upper_text('raw_record:confederation') }} as confederation,
    {{ tm_integer('raw_record:total_clubs') }} as total_clubs,
    {{ tm_text('raw_record:url') }} as competition_url,
    bronze.* exclude (raw_record),
    raw_record
from bronze
