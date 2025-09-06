DROP TABLE IF EXISTS stg_noaa_tide_stations;

CREATE TABLE stg_noaa_tide_stations (
    station_id TEXT,
    station_name TEXT,
    state TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timezone TEXT,
    timezone_offset INTEGER,
    tidal BOOLEAN,
    greatlakes BOOLEAN,
    shefcode TEXT,
    affiliations TEXT,
    tide_type TEXT,
    source_filename TEXT
);