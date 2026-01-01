

SELECT 
  stat.last_reported_local_time,
  stat.station_id,
  stat.num_bikes_disabled,
  stat.num_bikes_available,
  stat.num_docks_disabled,
  stat.num_docks_available,
  stat.num_bikes_available_types__ebike,
  stat.num_bikes_available_types__mechanical,
  stat.station_status,
  station_info.station_name,
  station_info.station_capacity,

  -- metrics
  num_bikes_available/station_capacity::NUMERIC(10, 2) AS bike_availability_rate,
  num_docks_available/station_capacity::NUMERIC(10, 2) AS dock_availability_rate,
  num_bikes_available_types__ebike/station_capacity::NUMERIC(10, 2) AS ebike_availability_rate

FROM {{ ref("staging__station_status") }} AS stat
LEFT JOIN {{ ref("staging__stations_info") }} AS station_info
  ON stat.station_id = station_info.station_id
