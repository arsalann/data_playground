SELECT
    project_label,
    tunnel_pct,
    cost_per_km_2025_usd_m,
    length_km
FROM `bruin-playground-arsalan.report.metro_project_outliers`
WHERE tunnel_pct IS NOT NULL
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
ORDER BY project_id
