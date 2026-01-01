SELECT
  station_id,
  name AS station_name,
  physical_configuration,
  lat,
  lon,
  address AS station_address,
  capacity AS station_capacity,
  is_charging_station::BOOLEAN AS is_charging_station,

  -- metadata
  _dlt_load_id,
  _dlt_id
FROM {{ source("dlt_bikeshare", "stations_info") }}