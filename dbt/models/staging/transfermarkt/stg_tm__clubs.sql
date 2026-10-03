with bronze as (
    {{ tm_selected_bronze_rows('clubs') }}
)
select
    {{ tm_integer('raw_record:club_id') }} as club_id,
    {{ tm_text('raw_record:club_code') }} as club_code,
    {{ tm_text('raw_record:name') }} as club_name,
    {{ tm_text('raw_record:domestic_competition_id') }} as domestic_competition_id,
    {{ tm_euro_amount('raw_record:total_market_value') }} as total_market_value_eur,
    {{ tm_integer('raw_record:squad_size') }} as squad_size,
    {{ tm_decimal('raw_record:average_age', 2) }} as average_age,
    {{ tm_integer('raw_record:foreigners_number') }} as foreigners_number,
    {{ tm_decimal('raw_record:foreigners_percentage', 2) }} as foreigners_percentage,
    {{ tm_integer('raw_record:national_team_players') }} as national_team_players,
    {{ tm_text('raw_record:stadium_name') }} as stadium_name,
    {{ tm_integer('raw_record:stadium_seats') }} as stadium_seats,
    {{ tm_text('raw_record:net_transfer_record') }} as net_transfer_record_raw,
    {{ tm_euro_text_amount('raw_record:net_transfer_record') }} as net_transfer_record_eur,
    {{ tm_text('raw_record:coach_name') }} as coach_name,
    {{ tm_integer('raw_record:last_season') }} as last_season,
    {{ tm_text('raw_record:filename') }} as source_filename,
    {{ tm_text('raw_record:url') }} as club_url,
    bronze.* exclude (raw_record),
    raw_record
from bronze
