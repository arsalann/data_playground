SELECT
    CAST(year AS STRING) AS year_label,
    COUNT(*) AS comparable_region_count,
    ROUND(APPROX_QUANTILES(housing_cost_pct_disposable_income, 100)[OFFSET(50)], 1) AS median_housing_cost_pct,
    ROUND(APPROX_QUANTILES(disposable_after_housing_ppp_pc, 100)[OFFSET(50)], 0) AS median_after_housing_income
FROM `bruin-playground-arsalan.staging.regional_affordability`
GROUP BY year
ORDER BY year
