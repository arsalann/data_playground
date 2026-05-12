/* Top compound-burden NUTS3: ranked by burden_idx_pfas_only (since heat
   attribution requires full temperature ingest); also surfaces excess-deaths
   ranking for the same regions. */
SELECT
    CONCAT(name_latn, ' (', country_code, ')') AS region,
    name_latn,
    country_name_en,
    n_exceed_eu_dwd,
    ROUND(exceedance_density_per_100k, 1) AS exceedances_per_100k,
    ROUND(excess_deaths_total) AS excess_deaths_total,
    ROUND(burden_idx_pfas_only, 2) AS pfas_z_score
FROM `bruin-playground-arsalan.eu_env_dashboard_report.env_burden_nuts3`
WHERE n_exceed_eu_dwd > 0
ORDER BY burden_idx_pfas_only DESC
LIMIT 25
