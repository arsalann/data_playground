/* @bruin

name: fifa_reports.h1_match_heat_risk
type: bq.sql
description: |
  H1 — Heat-risk concentration. One row per FIFA-2026 match (104 rows) with the
  expected apparent temperature at kickoff hour, derived from the 14-year
  June-July ERA5 climatology window at venue + kickoff hour ±2h.

  Apparent temperature is the BoM Steadman approximation, not true WBGT
  (no globe-temperature reanalysis available); the methodology_note column
  carries that caveat through to the dashboard footnote.

  Heat bands (US NWS heat-index thresholds applied to apparent temp):
    Low <27 C, Moderate 27-32 C, High 32-37 C, Extreme >=37 C.
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
  m.match_id,
  m.stage,
  m.group_id,
  m.venue_city,
  m.stadium,
  m.roof_type,
  m.venue_elevation_m,
  m.kickoff_local,
  m.kickoff_hour_utc,
  c.mean_temp_c                                AS expected_temp_c,
  c.p95_temp_c,
  c.mean_humidity_pct,
  c.mean_wind_speed_kmh,
  c.apparent_temp_c                            AS expected_apparent_temp_c,
  c.heat_band,
  c.metar_mean_temp_c                          AS metar_crosscheck_temp_c,
  c.methodology_note,
  CURRENT_TIMESTAMP()                          AS reported_at
FROM `bruin-playground-arsalan.fifa_staging.matches_enriched`   m
JOIN `bruin-playground-arsalan.fifa_staging.match_climatology`  c USING (match_id)
ORDER BY c.apparent_temp_c DESC
