select
    classified.* exclude (
        position_raw,
        validation_reasons,
        identical_duplicate_rank,
        is_identical_duplicate,
        is_key_conflict,
        rejection_reasons,
        record_disposition
    ),
    case
        when aliases.source_value is not null then aliases.canonical_value
        else lower(classified.position_raw)
    end as position,
    current_timestamp() as silver_processed_at
from {{ ref('int_tm__players_classified') }} as classified
left join {{ ref('tm_position_aliases') }} as aliases
    on lower(classified.position_raw) = lower(aliases.source_value)
where classified.record_disposition = 'accepted'
