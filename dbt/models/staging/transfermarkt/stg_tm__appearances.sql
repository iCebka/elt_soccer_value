with bronze as (
    {{ tm_selected_bronze_rows('appearances') }}
)
select
    {{ tm_text('raw_record:appearance_id') }} as appearance_id,
    {{ tm_integer('raw_record:game_id') }} as game_id,
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_integer('raw_record:player_club_id') }} as player_club_id,
    {{ tm_integer('raw_record:player_current_club_id') }} as player_current_club_id,
    {{ tm_date('raw_record:date') }} as appearance_date,
    {{ tm_text('raw_record:player_name') }} as player_name,
    {{ tm_text('raw_record:competition_id') }} as competition_id,
    {{ tm_integer('raw_record:yellow_cards') }} as yellow_cards,
    {{ tm_integer('raw_record:red_cards') }} as red_cards,
    {{ tm_integer('raw_record:goals') }} as goals,
    {{ tm_integer('raw_record:assists') }} as assists,
    {{ tm_integer('raw_record:minutes_played') }} as minutes_played,
    bronze.* exclude (raw_record),
    raw_record
from bronze
