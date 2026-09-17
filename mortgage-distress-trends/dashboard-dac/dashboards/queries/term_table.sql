-- Per-term detail: when the term peaked during the crisis, its latest full
-- month value, its highest month so far in 2026, and today as a share of the
-- crisis peak. All values are 0-100 self-normalised interest.
WITH base AS (
    SELECT month, term, interest
    FROM `bruin-playground-arsalan.raw.mortgage_distress_google_trends`
    WHERE category = 'core_distress'
),
latest AS (SELECT MAX(month) AS m FROM base),
peak AS (
    SELECT term, interest AS crisis_peak, month AS crisis_peak_month
    FROM base
    WHERE month BETWEEN '2007-06-01' AND '2010-12-31'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY term ORDER BY interest DESC, month) = 1
),
cur AS (
    SELECT term, interest AS current_interest
    FROM base WHERE month = (SELECT m FROM latest)
),
hi26 AS (
    SELECT term, MAX(interest) AS high_2026
    FROM base WHERE month >= '2026-01-01' GROUP BY term
)
-- Note: each term is self-normalised so its crisis peak == 100; the latest
-- value therefore already reads as "% of the crisis peak", so no separate
-- percentage column is needed.
SELECT
    p.term,
    FORMAT_DATE('%b %Y', p.crisis_peak_month)  AS crisis_peak_month,
    h.high_2026,
    c.current_interest
FROM peak p
JOIN cur  c USING (term)
JOIN hi26 h USING (term)
ORDER BY c.current_interest DESC
