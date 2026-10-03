{{ config(tags=['silver_stage_3', 'players_current'], severity='error') }}

select player_id
from {{ ref('silver_players_current') }}
where
    ((birth_country_resolution_status in ('matched_name', 'matched_alias'))
        <> (birth_country_id is not null and birth_country_name is not null))
 or ((citizenship_country_resolution_status in ('matched_name', 'matched_alias'))
        <> (citizenship_country_id is not null and citizenship_country_name is not null))
 or ((current_club_resolution_status = 'matched_id')
        <> (resolved_current_club_id is not null))
 or ((current_national_team_resolution_status = 'matched_id')
        <> (resolved_current_national_team_id is not null))
 or ((national_team_country_resolution_status = 'matched_id')
        <> (national_team_country_id is not null and national_team_country_name is not null))

