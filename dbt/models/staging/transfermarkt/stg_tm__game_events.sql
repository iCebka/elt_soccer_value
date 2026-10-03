with bronze as (
    {{ tm_selected_bronze_rows('game_events') }}
)
select
    {{ tm_text('raw_record:game_event_id') }} as game_event_id,
    {{ tm_date('raw_record:date') }} as event_date,
    {{ tm_integer('raw_record:game_id') }} as game_id,
    nullif({{ tm_integer('raw_record:minute') }}, -1) as event_minute,
    {{ tm_text('raw_record:type') }} as event_type_raw,
    {{ tm_integer('raw_record:club_id') }} as club_id,
    {{ tm_text('raw_record:club_name') }} as club_name,
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_text('raw_record:description') }} as event_description,
    {{ tm_integer('raw_record:player_in_id') }} as player_in_id,
    {{ tm_integer('raw_record:player_assist_id') }} as player_assist_id,
    bronze.* exclude (raw_record),
    raw_record
from bronze
