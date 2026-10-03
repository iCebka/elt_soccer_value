{{ config(tags=['silver_stage_3', 'players_current'], severity='error') }}

-- The observed commas belong to the scalar names Korea, South/North. These
-- separators were not observed and would require a bridge before acceptance.
select player_id, country_of_citizenship
from {{ ref('base_tm__players') }}
where country_of_citizenship like '%/%'
   or country_of_citizenship like '%;%'
   or country_of_citizenship like '%|%'
   or (
        country_of_citizenship like '%,%'
        and country_of_citizenship not in ('Korea, South', 'Korea, North')
   )

