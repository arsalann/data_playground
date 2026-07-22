/* Top 20 NUTS3 regions by PFAS exceedance count. */
SELECT
    CONCAT(name_latn, ' (', country_code, ')') AS region,
    name_latn,
    country_name_en,
    n_exceed_eu_dwd AS exceedances,
    n_sites_water AS water_samples,
    ROUND(exceedance_density_per_100k, 1) AS exceedances_per_100k,
    ROUND(max_pfas_ng_l_water, 0) AS max_ng_per_l
FROM `bruin-playground-arsalan.eu_env_dashboard_report.env_burden_nuts3`
WHERE n_exceed_eu_dwd > 0
ORDER BY n_exceed_eu_dwd DESC
LIMIT 20
