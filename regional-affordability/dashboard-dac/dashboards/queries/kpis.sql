SELECT
    COUNT(*) AS comparable_regions,
    COUNT(DISTINCT country_name) AS countries,
    ROUND(AVG(housing_cost_pct_disposable_income), 1) AS avg_housing_cost_pct,
    ROUND(AVG(disposable_after_housing_ppp_pc), 0) AS avg_after_housing_income
FROM `bruin-playground-arsalan.report.regional_affordability_snapshot`
