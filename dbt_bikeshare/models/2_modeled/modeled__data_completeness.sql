-- This model highlights how complete bikeshare data is as ingested by dlt from the Bikeshare API.

SELECT
  dlt_load_id,
  dlt_load_inserted_at
FROM {{ ref("staging__dlt_loads") }}
