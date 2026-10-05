/* @bruin
name: staging.edc_h3_drought_siting
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H3: "Data centre capacity is disproportionately sited in drought-prone
  counties, and cooling technology does not adapt where water is scarcest."

  Three outputs are unioned:

  - siting rows: mean drought exposure of data centre counties against the
    all-county national baseline, count-weighted and capacity-weighted.
  - tier rows: how facilities and capacity distribute across national drought
    quartiles, against the share of counties each quartile contains.
  - cooling rows: cooling technology mix by drought tier, the adaptation test.

  Confounder, stated plainly: data centres are sited near population, power and
  fibre, and none of those are randomly distributed with respect to drought. A
  difference from the all-county baseline is therefore not evidence that
  drought was ignored as a siting criterion, only that the hosting counties
  differ from the county-average. The all-county baseline treats every county
  equally regardless of size or population.

  The cooling test is small-n: only a minority of facilities report a cooling
  type at all. Read the tier mix, not the absolute counts.

depends:
  - staging.edc_facility_spine
  - staging.edc_county_drought_index

materialization:
  type: table
  strategy: create+replace

columns:
  - name: metric_type
    type: VARCHAR
    description: "One of siting, tier, cooling"
    primary_key: true
  - name: label
    type: VARCHAR
    description: Group name within the metric type
    primary_key: true
  - name: sub_label
    type: VARCHAR
    description: Secondary breakdown, used by cooling rows for the cooling technology
    primary_key: true
  - name: sort_order
    type: INTEGER
    description: Display order, ascending
  - name: facility_count
    type: INTEGER
    description: Facilities in this group
  - name: total_capacity_mw
    type: DOUBLE
    description: Sum of reported capacity in MW for this group
  - name: mean_pct_d1_or_worse
    type: DOUBLE
    description: Mean share of county area in moderate drought or worse, 2015 to 2025
  - name: pct_weeks_majority_drought
    type: DOUBLE
    description: Mean share of weeks with over half the county in moderate drought or worse
  - name: share_pct
    type: DOUBLE
    description: This group's share of its metric type total, as a percentage

@bruin */

WITH facilities AS (
  SELECT
    f.facility_key,
    f.capacity_mw,
    -- The two inventories spell the same technology differently ("closed_loop" vs
    -- "closed loop"), and both use "unknown" where they mean null. Normalise before
    -- any cross-tab, or a single technology splits across two categories.
    CASE
      WHEN LOWER(TRIM(f.cooling_type)) IN ('unknown', 'unspecified', '') THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(f.cooling_type), r'closed') THEN 'Closed loop'
      WHEN REGEXP_CONTAINS(LOWER(f.cooling_type), r'open') THEN 'Open loop'
      WHEN REGEXP_CONTAINS(LOWER(f.cooling_type), r'evapor') THEN 'Evaporative'
      WHEN REGEXP_CONTAINS(LOWER(f.cooling_type), r'hybrid') THEN 'Hybrid'
      WHEN REGEXP_CONTAINS(LOWER(f.cooling_type), r'air|fan') THEN 'Air cooled'
      ELSE INITCAP(LOWER(TRIM(f.cooling_type)))
    END AS cooling_type,
    d.county_fips,
    d.mean_pct_d1_or_worse,
    d.pct_weeks_majority_drought,
    d.drought_tier
  FROM `bruin-playground-arsalan.staging.edc_facility_spine` f
  JOIN `bruin-playground-arsalan.staging.edc_county_drought_index` d
    ON d.county_fips = f.county_fips
),

baseline AS (
  SELECT
    AVG(mean_pct_d1_or_worse) AS mean_d1,
    AVG(pct_weeks_majority_drought) AS mean_weeks,
    COUNT(*) AS county_count
  FROM `bruin-playground-arsalan.staging.edc_county_drought_index`
),

siting AS (
  SELECT
    'All US counties (baseline)' AS label, '' AS sub_label, 1 AS sort_order,
    CAST(NULL AS INT64) AS facility_count, CAST(NULL AS FLOAT64) AS total_capacity_mw,
    (SELECT mean_d1 FROM baseline) AS mean_pct_d1_or_worse,
    (SELECT mean_weeks FROM baseline) AS pct_weeks_majority_drought

  UNION ALL
  SELECT
    'Data centre counties (each county once)', '', 2,
    COUNT(DISTINCT county_fips), CAST(NULL AS FLOAT64),
    AVG(mean_pct_d1_or_worse), AVG(pct_weeks_majority_drought)
  FROM (
    SELECT DISTINCT county_fips, mean_pct_d1_or_worse, pct_weeks_majority_drought
    FROM facilities
  )

  UNION ALL
  SELECT
    'Data centres (each facility once)', '', 3,
    COUNT(*), CAST(NULL AS FLOAT64),
    AVG(mean_pct_d1_or_worse), AVG(pct_weeks_majority_drought)
  FROM facilities

  UNION ALL
  SELECT
    'Data centres (weighted by capacity)', '', 4,
    COUNTIF(capacity_mw > 0), SUM(capacity_mw),
    SUM(mean_pct_d1_or_worse * capacity_mw) / SUM(capacity_mw),
    SUM(pct_weeks_majority_drought * capacity_mw) / SUM(capacity_mw)
  FROM facilities
  WHERE capacity_mw > 0
),

tier_totals AS (
  SELECT COUNT(*) AS n_fac, SUM(capacity_mw) AS n_mw FROM facilities
),

tiers AS (
  SELECT
    f.drought_tier AS label,
    '' AS sub_label,
    CASE f.drought_tier
      WHEN 'Q1 wettest' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 ELSE 4
    END AS sort_order,
    COUNT(*) AS facility_count,
    SUM(f.capacity_mw) AS total_capacity_mw,
    AVG(f.mean_pct_d1_or_worse) AS mean_pct_d1_or_worse,
    AVG(f.pct_weeks_majority_drought) AS pct_weeks_majority_drought,
    100 * COUNT(*) / (SELECT n_fac FROM tier_totals) AS share_pct
  FROM facilities f
  GROUP BY f.drought_tier
),

cooling AS (
  SELECT
    f.drought_tier AS label,
    f.cooling_type AS sub_label,
    CASE f.drought_tier
      WHEN 'Q1 wettest' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 ELSE 4
    END AS sort_order,
    COUNT(*) AS facility_count,
    SUM(f.capacity_mw) AS total_capacity_mw,
    AVG(f.mean_pct_d1_or_worse) AS mean_pct_d1_or_worse,
    AVG(f.pct_weeks_majority_drought) AS pct_weeks_majority_drought,
    100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.drought_tier) AS share_pct
  FROM facilities f
  WHERE f.cooling_type IS NOT NULL
  GROUP BY f.drought_tier, f.cooling_type
)

SELECT 'siting' AS metric_type, label, sub_label, sort_order, facility_count,
       ROUND(total_capacity_mw, 1) AS total_capacity_mw,
       ROUND(mean_pct_d1_or_worse, 2) AS mean_pct_d1_or_worse,
       ROUND(pct_weeks_majority_drought, 2) AS pct_weeks_majority_drought,
       CAST(NULL AS FLOAT64) AS share_pct
FROM siting

UNION ALL
SELECT 'tier', label, sub_label, sort_order, facility_count,
       ROUND(total_capacity_mw, 1), ROUND(mean_pct_d1_or_worse, 2),
       ROUND(pct_weeks_majority_drought, 2), ROUND(share_pct, 2)
FROM tiers

UNION ALL
SELECT 'cooling', label, sub_label, sort_order, facility_count,
       ROUND(total_capacity_mw, 1), ROUND(mean_pct_d1_or_worse, 2),
       ROUND(pct_weeks_majority_drought, 2), ROUND(share_pct, 2)
FROM cooling

ORDER BY metric_type, sort_order, sub_label
