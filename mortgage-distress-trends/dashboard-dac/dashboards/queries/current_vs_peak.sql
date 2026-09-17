-- For each core distress term: latest full month's interest expressed as a
-- percentage of that term's 2007-2010 financial-crisis peak. 100% would mean
-- "back to crisis levels"; every bar sits well below that.
WITH base AS (
    SELECT month, term, interest
    FROM `bruin-playground-arsalan.raw.mortgage_distress_google_trends`
    WHERE category = 'core_distress'
),
latest AS (SELECT MAX(month) AS m FROM base),
agg AS (
    SELECT
        term,
        MAX(IF(month BETWEEN '2007-06-01' AND '2010-12-31', interest, NULL))            AS crisis_peak,
        MAX(IF(month = (SELECT m FROM latest), interest, NULL))                          AS current_interest
    FROM base
    GROUP BY term
)
SELECT
    term,
    ROUND(100 * current_interest / NULLIF(crisis_peak, 0), 0) AS pct_of_crisis_peak
FROM agg
ORDER BY pct_of_crisis_peak DESC
