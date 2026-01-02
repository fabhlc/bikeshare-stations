
SELECT
  dlt_load_id,
  dlt_load_inserted_at,
  DATE_PART('day', dlt_load_inserted_at) AS dlt_load_inserted_day,
  DATE_PART('hour', dlt_load_inserted_at) AS dlt_load_inserted_hour,
  DATE_PART('minute', dlt_load_inserted_at) AS dlt_load_inserted_minute,
  DATE_PART('dayofweek', dlt_load_inserted_at) AS dlt_load_inserted_dayofweek
FROM {{ ref("staging__dlt_loads") }}
