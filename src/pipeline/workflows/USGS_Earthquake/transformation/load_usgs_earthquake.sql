UPDATE dm_t_usgs_earthquake_fact tgt
SET
  dm_valid_to       = CURRENT_DATE,
  dm_current_status = FALSE,
  dm_updated_time   = CURRENT_TIMESTAMP,
  dm_updated_by     = 'load_usgs_earthquake'
FROM temp_usgs_earthquake src
WHERE tgt.event_id           = src.event_id
  AND tgt.dm_current_status  = TRUE
  AND src.dm_hash_key IS DISTINCT FROM tgt.dm_hash_key
;

INSERT INTO dm_t_usgs_earthquake_fact (
    event_id,
    latitude,
    longitude,
    depth_km,
    magnitude,
    magnitude_type,
    place_name,
    event_time_utc,
    event_time_ist,
    updated_time_utc,
    updated_time_ist,
    url,
    detail_url,
    felt_reports,
    community_intensity,
    modified_mercalli_intensity,
    alert_level,
    status,
    tsunami,
    significance,
    network,
    network_name,
    network_event_code,
    number_of_stations,
    distance_to_nearest_station,
    rms,
    azimuthal_gap,
    event_type,
    dm_valid_from,
    dm_valid_to,
    dm_current_status,
    dm_current_version,
    dm_ingested_time,
    dm_updated_time,
    dm_updated_by,
    dm_hash_key,
    dm_source_filename,
    dm_source_system_code
)
SELECT
    src.event_id,
    src.latitude,
    src.longitude,
    src.depth_km,
    src.magnitude,
    src.magnitude_type,
    src.place_name,
    src.event_time_utc,
    src.event_time_ist,
    src.updated_time_utc,
    src.updated_time_ist,
    src.url,
    src.detail_url,
    src.felt_reports,
    src.community_intensity,
    src.modified_mercalli_intensity,
    src.alert_level,
    src.status,
    src.tsunami,
    src.significance,
    src.network,
    src.network_name,
    src.network_event_code,
    src.number_of_stations,
    src.distance_to_nearest_station,
    src.rms,
    src.azimuthal_gap,
    src.event_type,
    COALESCE(
      (
        SELECT prev.dm_valid_to
        FROM dm_t_usgs_earthquake_fact prev
        WHERE prev.event_id          = src.event_id
          AND prev.dm_current_status = FALSE
        ORDER BY prev.dm_current_version DESC
        LIMIT 1
      ),
      CURRENT_DATE
    ) AS dm_valid_from,
    '9999-12-31'::DATE  AS dm_valid_to,
    TRUE               AS dm_current_status,
    (
      SELECT COALESCE(MAX(prev.dm_current_version), 0) + 1
      FROM dm_t_usgs_earthquake_fact prev
      WHERE prev.event_id = src.event_id
    ) AS dm_current_version,
    CURRENT_TIMESTAMP AS dm_ingested_time,
    CURRENT_TIMESTAMP AS dm_updated_time,
    'load_usgs_earthquake' AS dm_updated_by,
    src.dm_hash_key,
    src.dm_source_filename,
    src.dm_source_system_code
FROM temp_usgs_earthquake src
LEFT JOIN dm_t_usgs_earthquake_fact tgt
  ON src.event_id = tgt.event_id
 AND tgt.dm_current_status = TRUE
WHERE tgt.event_id IS NULL
   OR src.dm_hash_key IS DISTINCT FROM tgt.dm_hash_key
;