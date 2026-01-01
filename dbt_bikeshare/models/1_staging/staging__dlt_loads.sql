SELECT
  load_id AS dlt_load_id,
  schema_name,
  status AS dlt_load_status,
  inserted_at AS dlt_load_inserted_at
FROM {{ source("dlt_bikeshare", "_dlt_loads") }}