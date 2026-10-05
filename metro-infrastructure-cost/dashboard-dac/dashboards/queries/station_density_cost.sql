SELECT
    CONCAT(p.city, ' — ', p.line_name, IF(p.phase_name IS NULL OR p.phase_name = '', '', CONCAT(' — ', p.phase_name))) AS project_label,
    p.stations_per_km,
    p.cost_per_km_2025_usd_m,
    p.length_km
FROM `bruin-playground-arsalan.staging.metro_projects_enriched` AS p
WHERE p.include_in_analysis = TRUE
  AND p.stations_per_km IS NOT NULL
{% if filters.region and filters.region | length > 0 %}
  AND p.analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
ORDER BY p.project_id
