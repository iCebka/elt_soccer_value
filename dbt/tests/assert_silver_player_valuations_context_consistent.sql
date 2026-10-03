{{ config(tags=['silver_stage_6', 'player_valuations_enriched'], severity='error') }}

select player_id, valuation_date
from {{ ref('silver_player_valuations_enriched') }}
where
    ((player_profile_join_status = 'matched_player_id')
        <> (resolved_player_id is not null))
 or (current_club_id_comparison_status = 'match' and (
        valuation_source_current_club_id is null
        or player_current_club_id_snapshot is null
        or valuation_source_current_club_id <> player_current_club_id_snapshot
    ))
 or (current_club_id_comparison_status = 'mismatch' and (
        valuation_source_current_club_id is null
        or player_current_club_id_snapshot is null
        or valuation_source_current_club_id = player_current_club_id_snapshot
    ))
 or (player_profile_join_status = 'not_found' and (
        player_name_current_snapshot is not null
        or player_current_national_team_id_snapshot is not null
        or citizenship_country_id_current_snapshot is not null
    ))
