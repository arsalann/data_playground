/* @bruin

name: fifa_reports.h2_team_travel_burden
type: bq.sql
description: |
  H2 — Travel burden across the 48 group-stage teams. One row per qualified
  team summarising the three group-stage legs: total km flown, cumulative
  altitude gained (sum of positive altitude deltas), and max single-leg
  time-zone shift in hours.

  Burden score is a simple z-composite of the three components (equal weights),
  ranked across the 48 teams. group_burden_rank ranks teams within their group
  so dashboards can spot intra-group travel asymmetry.

  Caveat: leg-1 origin is the team's capital-city centroid (per
  qualified_teams.home_lat/lon), not the team's actual training base —
  flagged in the H2 footnote.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - h2

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.team_travel_segments
  - fifa_raw.qualified_teams
  - fifa_raw.groups

@bruin */

WITH per_team AS (
  SELECT
    fifa_code,
    SUM(leg_km)                                                  AS total_km,
    SUM(GREATEST(altitude_delta_m, 0))                           AS altitude_m_gained,
    MAX(COALESCE(tz_shift_h, 0))                                 AS max_tz_shift_h,
    COUNT(*)                                                     AS n_legs
  FROM `bruin-playground-arsalan.fifa_staging.team_travel_segments`
  GROUP BY fifa_code
),
stats AS (
  SELECT
    AVG(total_km)            AS mu_km,    STDDEV_SAMP(total_km)            AS sd_km,
    AVG(altitude_m_gained)   AS mu_alt,   STDDEV_SAMP(altitude_m_gained)   AS sd_alt,
    AVG(max_tz_shift_h)      AS mu_tz,    STDDEV_SAMP(max_tz_shift_h)      AS sd_tz
  FROM per_team
),
scored AS (
  SELECT
    p.fifa_code,
    p.total_km,
    p.altitude_m_gained,
    p.max_tz_shift_h,
    (p.total_km          - s.mu_km)  / NULLIF(s.sd_km,  0) AS z_km,
    (p.altitude_m_gained - s.mu_alt) / NULLIF(s.sd_alt, 0) AS z_alt,
    (p.max_tz_shift_h    - s.mu_tz)  / NULLIF(s.sd_tz,  0) AS z_tz
  FROM per_team p, stats s
)
SELECT
  sc.fifa_code,
  t.name                   AS team_name,
  t.confederation,
  g.group_id,
  ROUND(sc.total_km,          1)  AS total_km,
  ROUND(sc.altitude_m_gained, 0)  AS altitude_m_gained,
  ROUND(sc.max_tz_shift_h,    1)  AS max_tz_shift_h,
  ROUND(sc.z_km,  3)              AS z_km,
  ROUND(sc.z_alt, 3)              AS z_altitude,
  ROUND(sc.z_tz,  3)              AS z_tz_shift,
  ROUND((sc.z_km + sc.z_alt + sc.z_tz) / 3.0, 3)             AS burden_score,
  RANK() OVER (ORDER BY (sc.z_km + sc.z_alt + sc.z_tz) DESC) AS overall_burden_rank,
  RANK() OVER (
    PARTITION BY g.group_id
    ORDER BY (sc.z_km + sc.z_alt + sc.z_tz) DESC
  )                                                          AS group_burden_rank,
  CURRENT_TIMESTAMP() AS reported_at
FROM scored sc
JOIN `bruin-playground-arsalan.fifa_raw.qualified_teams` t USING (fifa_code)
JOIN `bruin-playground-arsalan.fifa_raw.groups`           g USING (fifa_code)
ORDER BY burden_score DESC
