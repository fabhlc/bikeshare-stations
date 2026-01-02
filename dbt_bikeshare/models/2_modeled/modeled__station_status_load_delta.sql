
SELECT 
  status.station_id,
  status._dlt_load_id,
  status.last_reported_local_time,
  loads.dlt_load_inserted_at AT TIME ZONE 'America/Toronto' AS dlt_load_inserted_local_time,
  
  ABS(DATE_DIFF('minutes', dlt_load_inserted_local_time, status.last_reported_local_time)) AS minute_diff,
  CASE
      WHEN minute_diff < 30 THEN '1.  15 to 30 mins'
      WHEN minute_diff < 60 THEN '2.  30 mins to 1 hr'
      WHEN minute_diff <= 180 THEN '3.  1 hr to 3 hrs'
      WHEN minute_diff > 180 THEN '4.  > 3 hrs'
  ELSE NULL
  END AS delay_bucket
FROM {{ ref("staging__station_status") }} AS status
LEFT JOIN {{ ref("staging__dlt_loads") }} AS loads
    ON status._dlt_load_id = loads.dlt_load_id
WHERE minute_diff > 15