{{ config(tags=['silver_stage_6', 'player_valuations_enriched'], severity='error') }}

with base_metrics as (
    select
        count(*) as row_count,
        count(distinct player_id) as player_count,
        sum(market_value_eur) as value_sum,
        min(market_value_eur) as value_min,
        max(market_value_eur) as value_max,
        count_if(market_value_eur = 0) as zero_count
    from {{ ref('base_tm__player_valuations') }}
),
silver_metrics as (
    select
        count(*) as row_count,
        count(distinct player_id) as player_count,
        sum(historical_market_value_eur) as value_sum,
        min(historical_market_value_eur) as value_min,
        max(historical_market_value_eur) as value_max,
        count_if(historical_market_value_eur = 0) as zero_count
    from {{ ref('silver_player_valuations_enriched') }}
),
key_differences as (
    select count(*) as difference_count
    from (
        (
            select player_id, valuation_date
            from {{ ref('base_tm__player_valuations') }}
            minus
            select player_id, valuation_date
            from {{ ref('silver_player_valuations_enriched') }}
        )
        union all
        (
            select player_id, valuation_date
            from {{ ref('silver_player_valuations_enriched') }}
            minus
            select player_id, valuation_date
            from {{ ref('base_tm__player_valuations') }}
        )
    )
)
select
    base_metrics.*,
    silver_metrics.*,
    key_differences.difference_count
from base_metrics
cross join silver_metrics
cross join key_differences
where base_metrics.row_count <> silver_metrics.row_count
   or base_metrics.player_count <> silver_metrics.player_count
   or base_metrics.value_sum <> silver_metrics.value_sum
   or base_metrics.value_min <> silver_metrics.value_min
   or base_metrics.value_max <> silver_metrics.value_max
   or base_metrics.zero_count <> silver_metrics.zero_count
   or key_differences.difference_count <> 0
