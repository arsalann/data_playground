/* @bruin
name: staging.edc_county_drought_index
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Per-county drought exposure index built from weekly US Drought Monitor
  statistics, 2015 to 2025. This is the baseline that hypothesis H3 compares
  data centre siting against.

  The USDM publishes cumulative categories: pct_d1_or_worse already includes
  D2, D3 and D4. Two summary measures are derived:

  - mean_pct_d1_or_worse: the average share of county area in moderate drought
    or worse across all weeks. A continuous exposure measure.
  - pct_weeks_majority_drought: the share of weeks where more than half the
    county was in moderate drought or worse. A frequency measure that is less
    sensitive to partial-county coverage.

depends:
  - raw.edc_drought_county
  - raw.edc_county_reference

materialization:
  type: table
  strategy: create+replace

columns:
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS code
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: county_name
    type: VARCHAR
    description: County name
  - name: state
    type: VARCHAR
    description: Two-letter state code
  - name: land_area_sqkm
    type: DOUBLE
    description: County land area in square kilometres
  - name: weeks_observed
    type: INTEGER
    description: Number of weekly USDM observations for this county
  - name: mean_pct_d0_or_worse
    type: DOUBLE
    description: Mean share of county area abnormally dry or worse, across all weeks
  - name: mean_pct_d1_or_worse
    type: DOUBLE
    description: Mean share of county area in moderate drought or worse, across all weeks
  - name: mean_pct_d2_or_worse
    type: DOUBLE
    description: Mean share of county area in severe drought or worse, across all weeks
  - name: pct_weeks_majority_drought
    type: DOUBLE
    description: Share of weeks where over half the county was in moderate drought or worse
  - name: pct_weeks_severe
    type: DOUBLE
    description: Share of weeks where over half the county was in severe drought or worse
  - name: drought_tier
    type: VARCHAR
    description: "National quartile of mean_pct_d1_or_worse: Q1 wettest to Q4 driest"

@bruin */

WITH weekly AS (
  SELECT
    county_fips,
    pct_d0_or_worse,
    pct_d1_or_worse,
    pct_d2_or_worse
  FROM `bruin-playground-arsalan.raw.edc_drought_county`
),

aggregated AS (
  SELECT
    county_fips,
    COUNT(*) AS weeks_observed,
    AVG(pct_d0_or_worse) AS mean_pct_d0_or_worse,
    AVG(pct_d1_or_worse) AS mean_pct_d1_or_worse,
    AVG(pct_d2_or_worse) AS mean_pct_d2_or_worse,
    100 * COUNTIF(pct_d1_or_worse > 50) / COUNT(*) AS pct_weeks_majority_drought,
    100 * COUNTIF(pct_d2_or_worse > 50) / COUNT(*) AS pct_weeks_severe
  FROM weekly
  GROUP BY county_fips
),

tiered AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY mean_pct_d1_or_worse) AS quartile
  FROM aggregated
)

SELECT
  t.county_fips,
  ref.county_name,
  ref.state,
  ROUND(ref.land_area_sqkm, 1) AS land_area_sqkm,
  t.weeks_observed,
  ROUND(t.mean_pct_d0_or_worse, 2) AS mean_pct_d0_or_worse,
  ROUND(t.mean_pct_d1_or_worse, 2) AS mean_pct_d1_or_worse,
  ROUND(t.mean_pct_d2_or_worse, 2) AS mean_pct_d2_or_worse,
  ROUND(t.pct_weeks_majority_drought, 2) AS pct_weeks_majority_drought,
  ROUND(t.pct_weeks_severe, 2) AS pct_weeks_severe,
  CASE t.quartile
    WHEN 1 THEN 'Q1 wettest'
    WHEN 2 THEN 'Q2'
    WHEN 3 THEN 'Q3'
    WHEN 4 THEN 'Q4 driest'
  END AS drought_tier
FROM tiered t
LEFT JOIN `bruin-playground-arsalan.raw.edc_county_reference` ref
  ON ref.county_fips = t.county_fips
