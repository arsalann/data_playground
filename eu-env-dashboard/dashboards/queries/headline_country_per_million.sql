/* PFAS DWD exceedance rate per million inhabitants, by country, sorted
   descending. Includes only countries with at least one exceedance. */
WITH country_agg AS (
    SELECT
        country_name_en AS country,
        SUM(n_exceed_eu_dwd) AS exceedances,
        SUM(pop_total_2023) AS pop_total
    FROM `bruin-playground-arsalan.eu_pfas_staging.pf_nuts3_exposure`
    GROUP BY country_name_en
)
SELECT
    country,
    ROUND(exceedances / NULLIF(pop_total, 0) * 1000000, 1) AS exceedances_per_million
FROM country_agg
WHERE exceedances > 0
ORDER BY exceedances_per_million DESC
