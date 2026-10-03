{{ config(tags=['silver_stage_6', 'player_valuations_enriched'], severity='error') }}

select player_id, valuation_date
from {{ ref('silver_player_valuations_enriched') }}
where historical_market_value_eur is null
   or historical_market_value_eur < 0
