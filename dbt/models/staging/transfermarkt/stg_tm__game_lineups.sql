with bronze as (
    {{ tm_selected_bronze_rows('game_lineups') }}
)
select
    {{ tm_text('raw_record:game_lineups_id') }} as game_lineup_id,
    {{ tm_date('raw_record:date') }} as lineup_date,
    {{ tm_integer('raw_record:game_id') }} as game_id,
    {{ tm_integer('raw_record:player_id') }} as player_id,
    {{ tm_integer('raw_record:club_id') }} as club_id,
    {{ tm_text('raw_record:player_name') }} as player_name,
    {{ tm_text('raw_record:type') }} as lineup_type_raw,
    {{ tm_text('raw_record:position') }} as lineup_position,
    {{ tm_text('raw_record:number') }} as shirt_number,
    {{ tm_boolean_01('raw_record:team_captain') }} as is_team_captain,
    bronze.* exclude (raw_record),
    raw_record
from bronze
