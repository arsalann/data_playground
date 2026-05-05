/* @bruin

name: fifa_staging.team_travel_segments
type: bq.sql
description: |
  For each of the 48 group-stage teams, three travel segments corresponding to
  their three group fixtures (in calendar order), with great-circle distance
  in km, altitude delta in metres, and time-zone-offset delta in hours.

  Origin for the first segment is the team's capital-city home coordinate.
  Each subsequent leg starts from the previous match venue. Drives H2 (travel
  burden across teams).

  Caveat: capital-city centroids are a proxy for "team home". Real federations
  have specific training bases. Flagged in H2 footnote.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - geospatial

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.matches_enriched
  - fifa_raw.qualified_teams
  - fifa_raw.groups

@bruin */

WITH team_group_fixtures AS (
  /* For each team, list its 3 group-stage fixtures in calendar order. */
  SELECT
    g.fifa_code,
    g.group_id,
    m.match_id,
    m.kickoff_utc,
    m.venue_lat,
    m.venue_lon,
    m.venue_elevation_m,
    m.venue_timezone,
    ROW_NUMBER() OVER (PARTITION BY g.fifa_code ORDER BY m.kickoff_utc) AS leg_idx
  FROM `bruin-playground-arsalan.fifa_raw.groups`        g
  JOIN `bruin-playground-arsalan.fifa_staging.matches_enriched` m
    ON m.group_id = g.group_id
  WHERE m.stage = 'G'
),
prev_leg AS (
  SELECT
    f.*,
    LAG(f.venue_lat)         OVER (PARTITION BY fifa_code ORDER BY leg_idx) AS prev_lat,
    LAG(f.venue_lon)         OVER (PARTITION BY fifa_code ORDER BY leg_idx) AS prev_lon,
    LAG(f.venue_elevation_m) OVER (PARTITION BY fifa_code ORDER BY leg_idx) AS prev_elev_m,
    LAG(f.venue_timezone)    OVER (PARTITION BY fifa_code ORDER BY leg_idx) AS prev_tz
  FROM team_group_fixtures f
),
with_origin AS (
  SELECT
    p.*,
    t.home_lat,
    t.home_lon,
    COALESCE(p.prev_lat,  t.home_lat) AS origin_lat,
    COALESCE(p.prev_lon,  t.home_lon) AS origin_lon,
    COALESCE(p.prev_elev_m, 0)        AS origin_elev_m
  FROM prev_leg p
  JOIN `bruin-playground-arsalan.fifa_raw.qualified_teams` t USING (fifa_code)
)
SELECT
  fifa_code,
  group_id,
  leg_idx,
  match_id,
  origin_lat,
  origin_lon,
  origin_elev_m,
  venue_lat,
  venue_lon,
  venue_elevation_m,
  /* Haversine distance, km. */
  ROUND(
    2 * 6371 * ASIN(SQRT(
      POWER(SIN((venue_lat - origin_lat) * ACOS(-1) / 360.0), 2)
      + COS(origin_lat * ACOS(-1) / 180.0) * COS(venue_lat * ACOS(-1) / 180.0)
        * POWER(SIN((venue_lon - origin_lon) * ACOS(-1) / 360.0), 2)
    )),
    1
  ) AS leg_km,
  ROUND(venue_elevation_m - origin_elev_m, 0)         AS altitude_delta_m,
  CASE
    WHEN prev_tz IS NULL THEN NULL
    ELSE ABS(
      TIMESTAMP_DIFF(
        TIMESTAMP(DATETIME '2026-06-15 12:00:00', venue_timezone),
        TIMESTAMP(DATETIME '2026-06-15 12:00:00', prev_tz),
        HOUR
      )
    )
  END AS tz_shift_h,
  CURRENT_TIMESTAMP() AS staged_at
FROM with_origin
ORDER BY fifa_code, leg_idx
