/* Heat mortality and PFAS exposure decomposed by DEGURBA class
   (cities / towns and suburbs / rural). */
SELECT
    e.degurba_label AS degurba,
    COUNT(*) AS n_nuts3,
    ROUND(SUM(IFNULL(e.excess_deaths_total, 0))) AS excess_deaths_total,
    ROUND(SUM(IFNULL(e.n_sites_water, 0))) AS water_samples_total,
    ROUND(SUM(IFNULL(e.n_exceed_eu_dwd, 0))) AS exceedances_total,
    ROUND(AVG(e.avg_share_65plus) * 100, 1) AS avg_pct_over_65
FROM `bruin-playground-arsalan.eu_env_dashboard_report.env_burden_nuts3` e
WHERE e.degurba_label IS NOT NULL
  AND e.degurba_label != 'Unknown'
GROUP BY e.degurba_label
ORDER BY exceedances_total DESC
