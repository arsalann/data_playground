/* Top 10 NUTS3 regions by PFAS EU-DWD exceedance count, sorted descending.
   Limited to 10 so every X-axis label renders without overlap on the headlines
   dashboard chart (VISUALIZATIONS.md section 9: every label must be readable). */
SELECT
    CONCAT(name_latn, ' (', country_code, ')') AS region,
    n_exceed_eu_dwd AS exceedances
FROM `bruin-playground-arsalan.eu_pfas_staging.pf_nuts3_exposure`
WHERE n_exceed_eu_dwd > 0
ORDER BY n_exceed_eu_dwd DESC
LIMIT 10
