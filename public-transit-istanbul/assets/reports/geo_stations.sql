SELECT
    station_name,
    line_name,
    line_type,
    project_phase,
    directorate,
    longitude,
    latitude
FROM `bruin-playground-arsalan.raw.istanbul_geo_stations`
WHERE longitude IS NOT NULL AND latitude IS NOT NULL
ORDER BY line_type, station_name
