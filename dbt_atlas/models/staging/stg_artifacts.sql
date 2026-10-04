select
    run_id,
    artifact_name,
    artifact_format,
    relative_path,
    sha256,
    row_count,
    byte_count
from {{ source('optimization', 'run_artifacts') }}
