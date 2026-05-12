/* PFAS site attribution breakdown. Each row is one attribution class with
   total sites and total exceedances. */
SELECT
    attribution_class AS attribution,
    COUNT(*) AS sites,
    COUNTIF(pfas_sum_ng_l >= 500 AND is_water_sample) AS exceedances
FROM `bruin-playground-arsalan.eu_pfas_staging.pf_source_attribution`
WHERE attribution_class IS NOT NULL
GROUP BY attribution_class
ORDER BY exceedances DESC
