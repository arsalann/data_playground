/* @bruin

name: fifa_reports.r2_travel_segments
type: bq.sql
description: |
  R2 — Per-team travel segments for sankey + path-map widgets. One row per
  (team, leg) pair. Includes origin label, destination label (venue_city),
  leg distance in km, and venue elevation. Group-stage only (3 legs per
  team after the home → first venue hop).

  Origin labels are the team's capital city for leg 1, and the previous
  match's host city for legs 2-3.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - r2

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.team_travel_segments
  - fifa_raw.qualified_teams
  - fifa_raw.host_venues

@bruin */

WITH origins AS (
  SELECT
    s.fifa_code,
    s.leg_idx,
    s.match_id,
    s.origin_lat,
    s.origin_lon,
    s.venue_lat,
    s.venue_lon,
    s.leg_km,
    s.altitude_delta_m,
    s.tz_shift_h,
    /* Origin label: team home city for leg 1, previous venue's city otherwise. */
    LAG(v_dest.city) OVER (PARTITION BY s.fifa_code ORDER BY s.leg_idx) AS prev_venue_city,
    v_dest.city           AS dest_venue_city,
    v_dest.country        AS dest_country,
    v_dest.elevation_m    AS dest_elev_m,
    v_dest.venue_id       AS dest_venue_id
  FROM `bruin-playground-arsalan.fifa_staging.team_travel_segments` s
  /* Re-derive destination venue from match. */
  JOIN `bruin-playground-arsalan.fifa_staging.matches_enriched`     m USING (match_id)
  JOIN `bruin-playground-arsalan.fifa_raw.host_venues`              v_dest ON v_dest.venue_id = m.venue_id
)
SELECT
  o.fifa_code,
  t.name                                            AS team_name,
  t.confederation,
  o.leg_idx,
  COALESCE(o.prev_venue_city, t.name)               AS origin_label,
  o.dest_venue_city                                 AS dest_label,
  o.dest_country,
  o.origin_lat,
  o.origin_lon,
  o.venue_lat,
  o.venue_lon,
  o.dest_elev_m,
  ROUND(o.leg_km, 1)                                AS leg_km,
  o.altitude_delta_m,
  o.tz_shift_h,
  CURRENT_TIMESTAMP()                               AS reported_at
FROM origins o
JOIN `bruin-playground-arsalan.fifa_raw.qualified_teams` t USING (fifa_code)
ORDER BY t.name, o.leg_idx
