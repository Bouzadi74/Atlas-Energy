select
    run_id,
    provenance_key,
    provenance_value
from {{ source('optimization', 'run_provenance') }}
