DROP VIEW IF EXISTS dm_usgs_earthquake_recent;

CREATE VIEW dm_usgs_earthquake_recent AS
SELECT *
FROM dm_t_usgs_earthquake_fact
WHERE event_time_utc >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY event_time_utc DESC;