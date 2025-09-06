DROP VIEW IF EXISTS dm_usgs_earthquake_fact;

CREATE VIEW dm_usgs_earthquake_fact AS
SELECT
    f.event_id AS event_id,
    f.event_time_utc AS event_time_utc,
    DATE_PART('year', f.event_time_utc) AS year,
    DATE_PART('month', f.event_time_utc) AS month,
    f.latitude AS latitude,
    f.longitude AS longitude,
    f.depth_km AS depth_km,
    f.magnitude AS magnitude,
    f.magnitude_type AS magnitude_type,
    f.place_name AS place_name,
    f.felt_reports AS felt_reports,
    f.community_intensity AS community_intensity,
    f.modified_mercalli_intensity AS modified_mercalli_intensity,
    f.alert_level AS alert_level,
    f.status AS status,
    f.tsunami AS tsunami,
    f.significance AS significance,
    f.network AS network,
    f.network_name AS network_name,
    f.network_event_code AS network_event_code,
    f.number_of_stations AS number_of_stations,
    f.distance_to_nearest_station AS distance_to_nearest_station,
    f.rms AS rms,
    f.azimuthal_gap AS azimuthal_gap,
    f.event_type AS event_type
FROM dm_t_usgs_earthquake_fact f;