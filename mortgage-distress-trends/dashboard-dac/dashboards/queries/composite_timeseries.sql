-- Mortgage Distress Search Index: unweighted mean of the five self-normalised
-- "cannot pay my mortgage" terms, one point per month, 2004-01 to latest full
-- month. Each term is 0-100 vs its own 2004-present peak, so the composite is a
-- 0-100 barometer whose all-time high (Mar 2009 = 86) marks the foreclosure
-- crisis and whose recent level shows how today compares.
SELECT
    FORMAT_DATE('%b %Y', month) AS month_label,
    ROUND(AVG(interest), 1)     AS distress_index
FROM `bruin-playground-arsalan.raw.mortgage_distress_google_trends`
WHERE category = 'core_distress'
GROUP BY month
ORDER BY month
