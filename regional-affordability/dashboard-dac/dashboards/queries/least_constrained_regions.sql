SELECT
    CONCAT(region_name, ', ', country_name) AS region,
    ROUND(disposable_after_housing_ppp_pc, 0) AS after_housing_income_usd_ppp,
    ROUND(housing_cost_pct_disposable_income, 1) AS housing_cost_pct,
    ROUND(affordability_score, 1) AS affordability_score
FROM `bruin-playground-arsalan.report.regional_affordability_snapshot`
ORDER BY affordability_score DESC
LIMIT 12
