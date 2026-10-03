with bronze as (
    {{ tm_selected_bronze_rows('games') }}
)
select
    {{ tm_integer('raw_record:game_id') }} as game_id,
    {{ tm_text('raw_record:competition_id') }} as competition_id,
    {{ tm_integer('raw_record:season') }} as season,
    {{ tm_text('raw_record:round') }} as game_round,
    {{ tm_date('raw_record:date') }} as game_date,
    {{ tm_integer('raw_record:home_club_id') }} as home_club_id,
    {{ tm_integer('raw_record:away_club_id') }} as away_club_id,
    {{ tm_integer('raw_record:home_club_goals') }} as home_club_goals,
    {{ tm_integer('raw_record:away_club_goals') }} as away_club_goals,
    {{ tm_integer('raw_record:home_club_position') }} as home_club_position,
    {{ tm_integer('raw_record:away_club_position') }} as away_club_position,
    {{ tm_text('raw_record:home_club_manager_name') }} as home_club_manager_name,
    {{ tm_text('raw_record:away_club_manager_name') }} as away_club_manager_name,
    {{ tm_text('raw_record:stadium') }} as stadium_name,
    {{ tm_integer('raw_record:attendance') }} as attendance,
    {{ tm_text('raw_record:referee') }} as referee_name,
    {{ tm_text('raw_record:url') }} as game_url,
    {{ tm_text('raw_record:home_club_formation') }} as home_club_formation,
    {{ tm_text('raw_record:away_club_formation') }} as away_club_formation,
    {{ tm_text('raw_record:home_club_name') }} as home_club_name,
    {{ tm_text('raw_record:away_club_name') }} as away_club_name,
    {{ tm_text('raw_record:aggregate') }} as aggregate_score,
    {{ tm_lower_text('raw_record:competition_type') }} as competition_type,
    bronze.* exclude (raw_record),
    raw_record
from bronze
