SELECT
    project_label,
    length_km,
    cost_per_km_2025_usd_m,
    tunnel_pct
FROM `bruin-playground-arsalan.report.metro_project_outliers`
WHERE length_km IS NOT NULL
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
ORDER BY project_id
