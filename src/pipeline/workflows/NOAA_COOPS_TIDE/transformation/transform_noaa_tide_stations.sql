DROP TABLE IF EXISTS temp_noaa_tide_stations;

CREATE TEMP TABLE temp_noaa_tide_stations AS
WITH cleaned_stations AS (
  SELECT
    NULLIF(TRIM(station_id), '') AS station_id,
    NULLIF(TRIM(station_name), '') AS station_name,
    NULLIF(TRIM(state), '') AS state,
    ROUND(latitude::NUMERIC, 6) AS latitude,
    ROUND(longitude::NUMERIC, 6) AS longitude,
    NULLIF(TRIM(timezone), '') AS timezone,
    timezone_offset::INTEGER AS timezone_offset,
    tidal::BOOLEAN AS tidal,
    greatlakes::BOOLEAN AS greatlakes,
    NULLIF(TRIM(shefcode), '') AS shefcode,
    NULLIF(TRIM(affiliations), '') AS affiliations,
    NULLIF(TRIM(tide_type), '') AS tide_type,
    'load_noaa_tide_stations' AS dm_updated_by,
    MD5(
      COALESCE(station_id, '') ||
      COALESCE(station_name, '') ||
      COALESCE(state, '') ||
      COALESCE(latitude::TEXT, '') ||
      COALESCE(longitude::TEXT, '') ||
      COALESCE(timezone, '') ||
      COALESCE(timezone_offset::TEXT, '') ||
      COALESCE(tidal::TEXT, '') ||
      COALESCE(greatlakes::TEXT, '') ||
      COALESCE(shefcode, '') ||
      COALESCE(affiliations, '') ||
      COALESCE(tide_type, '')
    ) AS dm_hash_key,
    source_filename AS dm_source_filename,
    'NOAA_TIDE' AS dm_source_system_code,
    ROW_NUMBER() OVER (PARTITION BY station_id) as rn
  FROM stg_noaa_tide_stations
  WHERE station_id IS NOT NULL
)
SELECT * FROM cleaned_stations where rn = 1;