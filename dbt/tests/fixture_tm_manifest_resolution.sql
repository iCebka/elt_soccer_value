{{ config(tags=['silver_stage_2', 'fixture'], severity='error') }}

with audit(asset_name, run_id, checksum, status, finished_at) as (
    select * from values
        ('players', 'old-success', 'sha-old', 'SUCCESS', '2026-01-01'::timestamp_tz),
        ('players', 'new-failed', 'sha-failed', 'FAILED', '2026-03-01'::timestamp_tz),
        ('players', 'new-success', 'sha-new', 'SUCCESS', '2026-02-01'::timestamp_tz),
        ('games', 'games-old', 'sha-games', 'SUCCESS', '2026-01-15'::timestamp_tz)
),
resolved as (
    select *
    from audit
    where status = 'SUCCESS'
    qualify row_number() over (
        partition by asset_name order by finished_at desc, run_id desc
    ) = 1
),
expected(asset_name, run_id, checksum) as (
    select * from values
        ('players', 'new-success', 'sha-new'),
        ('games', 'games-old', 'sha-games')
)
select coalesce(resolved.asset_name, expected.asset_name) as failed_asset
from resolved
full outer join expected using (asset_name, run_id, checksum)
where resolved.asset_name is null or expected.asset_name is null
