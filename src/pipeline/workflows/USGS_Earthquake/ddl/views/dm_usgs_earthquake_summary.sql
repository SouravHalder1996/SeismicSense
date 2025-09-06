DROP VIEW IF EXISTS dm_usgs_earthquake_summary;

CREATE VIEW dm_usgs_earthquake_summary AS
SELECT
    DATE_PART('year', f.event_time_ist) AS year,
    DATE_PART('month', f.event_time_ist) AS month,
    f.event_type AS event_type,
    f.alert_level AS alert_level,
    COUNT(*) AS event_count,
    AVG(f.magnitude) AS avg_magnitude,
    MAX(f.magnitude) AS max_magnitude,
    SUM(CASE WHEN f.tsunami THEN 1 ELSE 0 END) AS tsunami_events
FROM dm_t_usgs_earthquake_fact f
GROUP BY
    DATE_PART('year', f.event_time_ist),
    DATE_PART('month', f.event_time_ist),
    f.event_type,
    f.alert_level
ORDER BY
    year DESC,
    month DESC;