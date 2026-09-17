-- The five core distress terms pivoted to one column each, monthly, 2004-present.
-- Each column is that term's own 0-100 self-normalised series (100 = its peak
-- month). Purpose: show that every distress query peaks together in 2008-2010
-- and that all sit far below those peaks today.
SELECT
    FORMAT_DATE('%b %Y', month) AS month_label,
    MAX(IF(term = 'foreclosure help',    interest, NULL)) AS foreclosure_help,
    MAX(IF(term = 'loan modification',   interest, NULL)) AS loan_modification,
    MAX(IF(term = 'stop foreclosure',    interest, NULL)) AS stop_foreclosure,
    MAX(IF(term = 'avoid foreclosure',   interest, NULL)) AS avoid_foreclosure,
    MAX(IF(term = 'mortgage assistance', interest, NULL)) AS mortgage_assistance
FROM `bruin-playground-arsalan.raw.mortgage_distress_google_trends`
WHERE category = 'core_distress'
GROUP BY month
ORDER BY month
