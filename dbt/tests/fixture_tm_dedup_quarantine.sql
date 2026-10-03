{{ config(tags=['silver_stage_2', 'fixture'], severity='error') }}

with fixture(source_row_number, business_key, fingerprint, amount_eur, required_name) as (
    select * from values
        (1, 'valid', 'same', 100::number(38,2), 'Valid'),
        (2, 'valid', 'same', 100::number(38,2), 'Valid'),
        (3, 'conflict', 'left', 50::number(38,2), 'Left'),
        (4, 'conflict', 'right', 60::number(38,2), 'Right'),
        (5, null, 'null-key', -1::number(38,2), null)
),
ranked as (
    select
        *,
        row_number() over (partition by fingerprint order by source_row_number) as duplicate_rank,
        array_construct_compact(
            iff(business_key is null, 'missing_key', null),
            iff(required_name is null, 'missing_name', null),
            iff(amount_eur < 0, 'negative_amount', null)
        ) as validation_reasons
    from fixture
),
conflicts as (
    select business_key
    from ranked
    where duplicate_rank = 1 and business_key is not null
    group by business_key
    having count(distinct fingerprint) > 1
),
classified as (
    select
        ranked.*,
        case
            when duplicate_rank > 1 then 'duplicate_identical'
            when array_size(validation_reasons) > 0 or conflicts.business_key is not null then 'rejected'
            else 'accepted'
        end as disposition
    from ranked
    left join conflicts using (business_key)
),
actual as (
    select
        count_if(disposition = 'accepted') accepted_rows,
        count_if(disposition = 'duplicate_identical') duplicate_rows,
        count_if(disposition = 'rejected') rejected_rows,
        max(iff(fingerprint = 'null-key', array_size(validation_reasons), null)) null_key_reason_count
    from classified
)
select *
from actual
where accepted_rows <> 1
   or duplicate_rows <> 1
   or rejected_rows <> 3
   or null_key_reason_count <> 3
