DROP TABLE IF EXISTS temp_usgs_earthquake;

CREATE TEMP TABLE temp_usgs_earthquake AS
WITH ranked_events AS (
  SELECT
    NULLIF(TRIM(quake_id::TEXT), '') AS event_id,
    ROUND(latitude::NUMERIC, 6) AS latitude,
    ROUND(longitude::NUMERIC, 6) AS longitude,
    ROUND(depth_km::NUMERIC, 3) AS depth_km,
    ROUND(magnitude::NUMERIC, 2) AS magnitude,
    NULLIF(LOWER(TRIM(magnitude_type::TEXT)), '') AS magnitude_type,
    NULLIF(TRIM(place_name::TEXT), '') AS place_name,
    TO_TIMESTAMP(event_time / 1000) AT TIME ZONE 'utc' AS event_time_utc,
    TO_TIMESTAMP(event_time / 1000) AT TIME ZONE 'Asia/Kolkata' AS event_time_ist,
    TO_TIMESTAMP(updated_time / 1000) AT TIME ZONE 'utc' AS updated_time_utc,
    TO_TIMESTAMP(updated_time / 1000) AT TIME ZONE 'Asia/Kolkata' AS updated_time_ist,
    NULLIF(TRIM(url::TEXT), '') AS url,
    NULLIF(TRIM(detail_url::TEXT), '') AS detail_url,
    CASE
      WHEN LOWER(TRIM(felt_reports::TEXT)) IN ('nan', '') THEN NULL
      ELSE (felt_reports::INTEGER)
    END AS felt_reports,
    CASE
      WHEN LOWER(TRIM(community_intensity::TEXT)) IN ('nan', '') THEN NULL
      ELSE community_intensity
    END AS community_intensity,
    CASE
      WHEN LOWER(TRIM(modified_mercalli_intensity::TEXT)) IN ('nan', '') THEN NULL
      ELSE modified_mercalli_intensity
    END AS modified_mercalli_intensity,
    CASE
      WHEN LOWER(TRIM(alert_level)) IN ('nan', '') THEN NULL
      ELSE LOWER(TRIM(alert_level))
    END AS alert_level,
    CASE
      WHEN LOWER(TRIM(status::TEXT)) IN ('nan', '') THEN NULL
      ELSE LOWER(TRIM(status::TEXT))
    END AS status,
    CASE
      WHEN tsunami IS NULL THEN NULL
      WHEN tsunami = 1 THEN TRUE
      ELSE FALSE
    END AS tsunami,
    CASE
      WHEN LOWER(TRIM(significance::TEXT)) IN ('nan', '') THEN NULL
      ELSE significance
    END AS significance,
    NULLIF(LOWER(TRIM(network::TEXT)), '') AS network,
    CASE LOWER(TRIM(network))
      WHEN 'ak' THEN 'Alaska Earthquake Center'
      WHEN 'av' THEN 'Alaska Volcano Observatory'
      WHEN 'ci' THEN 'Caltech/USGS Southern California'
      WHEN 'hv' THEN 'Hawaiian Volcano Observatory'
      WHEN 'mb' THEN 'Montana Bureau of Mines and Geology'
      WHEN 'nc' THEN 'Northern California Seismic System'
      WHEN 'nm' THEN 'New Madrid Seismic Zone'
      WHEN 'nn' THEN 'Nevada Seismological Laboratory'
      WHEN 'ok' THEN 'Oklahoma Geological Survey'
      WHEN 'pr' THEN 'Puerto Rico Seismic Network'
      WHEN 'pt' THEN 'Pacific Tsunami Warning Center'
      WHEN 'se' THEN 'Southeastern U.S. Seismic Network'
      WHEN 'tx' THEN 'Texas Seismic Network'
      WHEN 'us' THEN 'USGS National Earthquake Information Center'
      WHEN 'uu' THEN 'University of Utah Seismograph Stations'
      WHEN 'uw' THEN 'University of Washington'
      ELSE 'Unknown'
    END AS network_name,
    NULLIF(TRIM(network_event_code::TEXT), '') AS network_event_code,
    CASE
      WHEN LOWER(TRIM(number_of_stations::TEXT)) IN ('nan', '') THEN NULL
      ELSE (number_of_stations::INTEGER)
    END AS number_of_stations,
    CASE
      WHEN LOWER(TRIM(distance_to_nearest_station::TEXT)) IN ('nan', '') THEN NULL
      ELSE distance_to_nearest_station
    END AS distance_to_nearest_station,
    CASE
      WHEN LOWER(TRIM(rms::TEXT)) IN ('nan', '') THEN NULL
      ELSE rms
    END AS rms,
    CASE
      WHEN LOWER(TRIM(azimuthal_gap::TEXT)) IN ('nan', '') THEN NULL
      ELSE azimuthal_gap
    END AS azimuthal_gap,
    NULLIF(LOWER(TRIM(event_type::TEXT)), '') AS event_type,
    MD5(quake_id ||
        COALESCE(latitude::TEXT, '') ||
        COALESCE(longitude::TEXT, '') ||
        COALESCE(depth_km::TEXT, '') ||
        COALESCE(magnitude::TEXT, '') ||
        COALESCE(magnitude_type::TEXT, '') ||
        COALESCE(place_name::TEXT, '') ||
        COALESCE(event_time::TEXT, '') ||
        COALESCE(felt_reports::TEXT, '') ||
        COALESCE(community_intensity::TEXT, '') ||
        COALESCE(modified_mercalli_intensity::TEXT, '') ||
        COALESCE(alert_level::TEXT, '') ||
        COALESCE(status::TEXT, '') ||
        COALESCE(tsunami::TEXT, '') ||
        COALESCE(significance::TEXT, '') ||
        COALESCE(network::TEXT, '') ||
        COALESCE(network_event_code::TEXT, '') ||
        COALESCE(number_of_stations::TEXT, '') ||
        COALESCE(distance_to_nearest_station::TEXT, '') ||
        COALESCE(rms::TEXT, '') ||
        COALESCE(azimuthal_gap::TEXT, '') ||
        COALESCE(event_type::TEXT, '')
    ) AS dm_hash_key,
    source_file_name AS dm_source_filename,
    'USGS' AS dm_source_system_code,
    ROW_NUMBER() OVER (PARTITION BY quake_id ORDER BY updated_time DESC) AS rn
  FROM stg_usgs_earthquake
)
SELECT *
FROM ranked_events
WHERE rn = 1;