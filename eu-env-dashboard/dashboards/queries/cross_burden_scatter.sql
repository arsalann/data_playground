/* NUTS3 scatter for the compound-burden chart: heat excess deaths
   vs PFAS exceedances per 100,000 inhabitants. */
SELECT
    CONCAT(name_latn, ' (', country_code, ')') AS region,
    country_name_en,
    GREATEST(IFNULL(excess_deaths_total, 0), 0) AS excess_deaths_total,
    ROUND(IFNULL(exceedance_density_per_100k, 0), 2) AS exceedances_per_100k,
    IFNULL(avg_pop_total, 0) AS avg_pop_total
FROM `bruin-playground-arsalan.eu_env_dashboard_report.env_burden_nuts3`
WHERE excess_deaths_total > 0
  AND exceedance_density_per_100k > 0
ORDER BY exceedances_per_100k DESC
LIMIT 200
