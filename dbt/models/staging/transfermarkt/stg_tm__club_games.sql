with bronze as (
    {{ tm_selected_bronze_rows('club_games') }}
)
select
    {{ tm_integer('raw_record:game_id') }} as game_id,
    {{ tm_integer('raw_record:club_id') }} as club_id,
    {{ tm_integer('raw_record:own_goals') }} as own_goals,
    {{ tm_integer('raw_record:own_position') }} as own_position,
    {{ tm_text('raw_record:own_manager_name') }} as own_manager_name,
    {{ tm_integer('raw_record:opponent_id') }} as opponent_id,
    {{ tm_integer('raw_record:opponent_goals') }} as opponent_goals,
    {{ tm_integer('raw_record:opponent_position') }} as opponent_position,
    {{ tm_text('raw_record:opponent_manager_name') }} as opponent_manager_name,
    {{ tm_lower_text('raw_record:hosting') }} as hosting,
    {{ tm_boolean_01('raw_record:is_win') }} as is_win,
    bronze.* exclude (raw_record),
    raw_record
from bronze
