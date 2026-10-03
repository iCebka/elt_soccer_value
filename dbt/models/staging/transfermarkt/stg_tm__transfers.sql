with bronze as (
    {{ tm_selected_bronze_rows('transfers') }}
)
select
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_date('raw_record:transfer_date') }} as transfer_date,
    {{ tm_text('raw_record:transfer_season') }} as transfer_season,
    {{ tm_integer('raw_record:from_club_id') }} as from_club_id,
    {{ tm_integer('raw_record:to_club_id') }} as to_club_id,
    {{ tm_text('raw_record:from_club_name') }} as from_club_name,
    {{ tm_text('raw_record:to_club_name') }} as to_club_name,
    {{ tm_euro_amount('raw_record:transfer_fee') }} as transfer_fee_eur,
    {{ tm_euro_amount('raw_record:market_value_in_eur') }} as market_value_eur,
    {{ tm_text('raw_record:player_name') }} as player_name,
    bronze.* exclude (raw_record),
    raw_record
from bronze
