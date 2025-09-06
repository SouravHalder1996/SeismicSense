CREATE TABLE IF NOT EXISTS dm_t_noaa_tide_stations_dim (
    station_id TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    state TEXT,
    latitude DECIMAL(8,6),
    longitude DECIMAL(9,6),
    timezone TEXT,
    timezone_offset INTEGER,
    tidal BOOLEAN,
    greatlakes BOOLEAN,
    shefcode TEXT,
    affiliations TEXT,
    tide_type TEXT,
    is_active BOOLEAN,
    -- Audit Columns
    dm_updated_time TIMESTAMP NOT NULL,
    dm_updated_by TEXT NOT NULL,
    dm_hash_key TEXT NOT NULL,
    dm_source_filename TEXT NOT NULL,
    dm_source_system_code TEXT NOT NULL
);