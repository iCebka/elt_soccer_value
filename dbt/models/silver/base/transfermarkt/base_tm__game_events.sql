select
    classified.* exclude (
        event_type_raw,
        validation_reasons,
        identical_duplicate_rank,
        is_identical_duplicate,
        is_key_conflict,
        rejection_reasons,
        record_disposition
    ),
    coalesce(aliases.canonical_value, lower(classified.event_type_raw)) as event_type,
    current_timestamp() as silver_processed_at
from {{ ref('int_tm__game_events_classified') }} as classified
left join {{ ref('tm_event_type_aliases') }} as aliases
    on lower(classified.event_type_raw) = lower(aliases.source_value)
where classified.record_disposition = 'accepted'
