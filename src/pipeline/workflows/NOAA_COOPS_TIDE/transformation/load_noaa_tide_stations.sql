UPDATE dm_t_noaa_tide_stations_dim 
SET is_active = FALSE,
    dm_updated_time = CURRENT_TIMESTAMP,
    dm_updated_by = 'load_noaa_tide_stations'
WHERE station_id NOT IN (
    SELECT DISTINCT station_id 
    FROM temp_noaa_tide_stations 
    WHERE station_id IS NOT NULL
);

INSERT INTO dm_t_noaa_tide_stations_dim (
    station_id,
    station_name,
    state,
    latitude,
    longitude,
    timezone,
    timezone_offset,
    tidal,
    greatlakes,
    shefcode,
    affiliations,
    tide_type,
    is_active,
    dm_updated_time,
    dm_updated_by,
    dm_hash_key,
    dm_source_filename,
    dm_source_system_code
)
SELECT 
    station_id,
    station_name,
    state,
    latitude,
    longitude,
    timezone,
    timezone_offset,
    tidal,
    greatlakes,
    shefcode,
    affiliations,
    tide_type,
    TRUE as is_active,
    CURRENT_TIMESTAMP as dm_updated_time,
    dm_updated_by,
    dm_hash_key,
    dm_source_filename,
    dm_source_system_code
FROM temp_noaa_tide_stations
ON CONFLICT (station_id) 
DO UPDATE SET
    station_name = EXCLUDED.station_name,
    state = EXCLUDED.state,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    timezone = EXCLUDED.timezone,
    timezone_offset = EXCLUDED.timezone_offset,
    tidal = EXCLUDED.tidal,
    greatlakes = EXCLUDED.greatlakes,
    shefcode = EXCLUDED.shefcode,
    affiliations = EXCLUDED.affiliations,
    tide_type = EXCLUDED.tide_type,
    is_active = TRUE,
    dm_updated_time = CURRENT_TIMESTAMP,
    dm_updated_by = EXCLUDED.dm_updated_by,
    dm_hash_key = EXCLUDED.dm_hash_key,
    dm_source_filename = EXCLUDED.dm_source_filename,
    dm_source_system_code = EXCLUDED.dm_source_system_code
WHERE 
    (dm_t_noaa_tide_stations_dim.dm_hash_key IS DISTINCT FROM EXCLUDED.dm_hash_key)
    OR (dm_t_noaa_tide_stations_dim.is_active = FALSE);