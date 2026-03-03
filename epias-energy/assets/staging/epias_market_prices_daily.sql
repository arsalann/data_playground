/* @bruin
name: staging.epias_market_prices_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates hourly Market Clearing Price (MCP) and System Marginal Price (SMP)
  into daily summary statistics: min, max, average, and weighted metrics.
  Computes the price spread between SMP and MCP as an indicator of real-time
  balancing costs.

depends:
  - raw.epias_mcp
  - raw.epias_smp

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Calendar date
    primary_key: true
    nullable: false
  - name: mcp_avg
    type: DOUBLE
    description: Average Market Clearing Price for the day (TRY/MWh)
  - name: mcp_min
    type: DOUBLE
    description: Minimum hourly MCP for the day (TRY/MWh)
  - name: mcp_max
    type: DOUBLE
    description: Maximum hourly MCP for the day (TRY/MWh)
  - name: mcp_avg_eur
    type: DOUBLE
    description: Average MCP in Euros (EUR/MWh)
  - name: mcp_avg_usd
    type: DOUBLE
    description: Average MCP in US Dollars (USD/MWh)
  - name: smp_avg
    type: DOUBLE
    description: Average System Marginal Price for the day (TRY/MWh)
  - name: smp_min
    type: DOUBLE
    description: Minimum hourly SMP for the day (TRY/MWh)
  - name: smp_max
    type: DOUBLE
    description: Maximum hourly SMP for the day (TRY/MWh)
  - name: spread_avg
    type: DOUBLE
    description: Average daily spread between SMP and MCP (TRY/MWh)
  - name: surplus_hours
    type: INTEGER
    description: Number of hours with energy surplus direction
  - name: deficit_hours
    type: INTEGER
    description: Number of hours with energy deficit direction
  - name: year
    type: INTEGER
    description: Year extracted from date
  - name: month
    type: INTEGER
    description: Month number (1-12)
  - name: day_of_week
    type: VARCHAR
    description: Day of week name (Monday-Sunday)

@bruin */

WITH mcp_deduped AS (
    SELECT *
    FROM raw.epias_mcp
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY extracted_at DESC) = 1
),

smp_deduped AS (
    SELECT *
    FROM raw.epias_smp
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY extracted_at DESC) = 1
),

mcp_daily AS (
    SELECT
        CAST(date AS DATE) AS date,
        ROUND(AVG(price_try), 2) AS mcp_avg,
        ROUND(MIN(price_try), 2) AS mcp_min,
        ROUND(MAX(price_try), 2) AS mcp_max,
        ROUND(AVG(price_eur), 4) AS mcp_avg_eur,
        ROUND(AVG(price_usd), 4) AS mcp_avg_usd
    FROM mcp_deduped
    GROUP BY CAST(date AS DATE)
),

smp_daily AS (
    SELECT
        CAST(date AS DATE) AS date,
        ROUND(AVG(smp), 2) AS smp_avg,
        ROUND(MIN(smp), 2) AS smp_min,
        ROUND(MAX(smp), 2) AS smp_max,
        COUNTIF(UPPER(smp_direction) LIKE '%SURPLUS%') AS surplus_hours,
        COUNTIF(UPPER(smp_direction) LIKE '%DEFICIT%') AS deficit_hours
    FROM smp_deduped
    GROUP BY CAST(date AS DATE)
)

SELECT
    COALESCE(m.date, s.date) AS date,
    m.mcp_avg,
    m.mcp_min,
    m.mcp_max,
    m.mcp_avg_eur,
    m.mcp_avg_usd,
    s.smp_avg,
    s.smp_min,
    s.smp_max,
    ROUND(COALESCE(s.smp_avg, 0) - COALESCE(m.mcp_avg, 0), 2) AS spread_avg,
    s.surplus_hours,
    s.deficit_hours,

    EXTRACT(YEAR FROM COALESCE(m.date, s.date)) AS year,
    EXTRACT(MONTH FROM COALESCE(m.date, s.date)) AS month,
    FORMAT_DATE('%A', COALESCE(m.date, s.date)) AS day_of_week

FROM mcp_daily m
FULL OUTER JOIN smp_daily s ON m.date = s.date
ORDER BY date
