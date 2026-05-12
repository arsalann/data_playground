/* @bruin

name: fifa_reports.h1_match_heat_risk
type: bq.sql
description: |
  H1 — Heat exposure per FIFA-2026 fixture. One row per match (104) with the
  climatological apparent temperature at kickoff (±2h), bound to a 2015-2024
  ERA5 reanalysis window at the venue grid point.

  Augments the staging climatology with two analyst-grade fields:
    - `roof_status_assumed`: 'open' / 'open_likely' / 'closed_likely' / 'unknown'.
      Retractable roofs are treated as CLOSED when expected apparent temp ≥ 30 °C
      OR when precipitation probability ≥ 20 % in the kickoff window.
    - `open_air_effective`: TRUE when a player on the pitch actually experiences
      the climatological conditions (open roof, fixed canopy, or retractable
      assumed open). The "true" heat-risk count uses this flag.

  Bands (US-NWS heat-index thresholds applied to apparent temp):
    Low <27 °C, Caution 27-32, Extreme Caution 32-39, Danger 39-51, Extreme Danger ≥51.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - h1

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.match_climatology
  - fifa_staging.matches_enriched

@bruin */

SELECT
  c.match_id,
  c.stage,
  c.group_id,
  c.venue_id,
  c.venue_city,
  c.venue_country,
  c.stadium,
  c.roof_type,
  c.roof_status_assumed,
  c.open_air_effective,
  c.venue_elevation_m,
  c.kickoff_local,
  c.kickoff_hour_utc,
  c.mean_temp_c                  AS expected_temp_c,
  c.p95_temp_c,
  c.mean_humidity_pct,
  c.mean_wind_speed_kmh,
  c.mean_precip_mm,
  c.prob_precip,
  c.prob_temp_ge30,
  c.apparent_temp_c              AS expected_apparent_temp_c,
  c.heat_band,
  c.methodology_note,
  CURRENT_TIMESTAMP()             AS reported_at
FROM `bruin-playground-arsalan.fifa_staging.match_climatology` c
ORDER BY c.apparent_temp_c DESC
