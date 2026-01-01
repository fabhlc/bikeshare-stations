
SELECT
  to_timestamp(last_reported) AT TIME ZONE 'America/Toronto' AS last_reported_local_time,
  station_id,
  num_bikes_disabled,
  num_bikes_available,
  num_docks_disabled,
  num_docks_available,
  num_bikes_available_types__ebike,
  num_bikes_available_types__mechanical,
  status AS  station_status,
  
  -- metadata
  _dlt_load_id,
  _dlt_id
FROM {{ source("dlt_bikeshare", "station_status") }}