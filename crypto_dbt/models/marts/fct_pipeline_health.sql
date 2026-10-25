select
    source,
    dataset,
    started_at,
    finished_at,
    duration_seconds,
    status,
    status = 'success' as is_success,
    records_fetched,
    records_inserted,
    error_message
from {{ source('ops', 'pipeline_runs') }}