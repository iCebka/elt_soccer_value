with bronze as (
    {{ tm_selected_bronze_rows('player_valuations') }}
)
select
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_date('raw_record:date') }} as valuation_date,
    {{ tm_euro_amount('raw_record:market_value_in_eur') }} as market_value_eur,
    {{ tm_text('raw_record:current_club_name') }} as current_club_name,
    {{ tm_integer('raw_record:current_club_id') }} as current_club_id,
    {{ tm_text('raw_record:player_club_domestic_competition_id') }} as player_club_domestic_competition_id,
    bronze.* exclude (raw_record),
    raw_record
from bronze
