/* @bruin

name: fifa_staging.matches_enriched
type: bq.sql
description: |
  Schedule × host_venues × group draw, with kickoff times converted from venue
  local time to UTC, plus group composition (4 fifa_codes per group). One row
  per match. Forms the spine for H1 (heat risk per match), H2 (travel
  segments), and H5 (capacity-vs-demand per fixture).

  Group-stage matches carry the four group members as `group_team_1..4`. KO
  matches carry NULL there — participants are still TBD.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.fifa_schedule
  - fifa_raw.host_venues
  - fifa_raw.qualified_teams
  - fifa_raw.groups

@bruin */

WITH groups_pivot AS (
  SELECT
    group_id,
    MAX(CASE WHEN position = 1 THEN fifa_code END) AS group_team_1,
    MAX(CASE WHEN position = 2 THEN fifa_code END) AS group_team_2,
    MAX(CASE WHEN position = 3 THEN fifa_code END) AS group_team_3,
    MAX(CASE WHEN position = 4 THEN fifa_code END) AS group_team_4,
  FROM `bruin-playground-arsalan.fifa_raw.groups`
  GROUP BY group_id
)
SELECT
  s.match_id,
  s.stage,
  s.group_id,
  s.slot,
  s.venue_id,
  v.city            AS venue_city,
  v.country         AS venue_country,
  v.stadium,
  v.timezone        AS venue_timezone,
  v.latitude        AS venue_lat,
  v.longitude       AS venue_lon,
  v.elevation_m     AS venue_elevation_m,
  v.capacity        AS venue_capacity,
  v.roof_type,
  v.primary_icao,
  /* fifa_schedule.kickoff_local was loaded as a UTC-tagged TIMESTAMP but the
     wall-clock components are venue-local — strip the UTC interpretation,
     then reinterpret in the venue's tz to get true UTC. */
  DATETIME(s.kickoff_local, 'UTC')                                                             AS kickoff_local,
  TIMESTAMP(DATETIME(s.kickoff_local, 'UTC'), v.timezone)                                      AS kickoff_utc,
  EXTRACT(HOUR FROM TIMESTAMP(DATETIME(s.kickoff_local, 'UTC'), v.timezone))                   AS kickoff_hour_utc,
  EXTRACT(DAYOFWEEK FROM DATETIME(s.kickoff_local, 'UTC'))                                     AS kickoff_dow_local,
  g.group_team_1,
  g.group_team_2,
  g.group_team_3,
  g.group_team_4,
  CURRENT_TIMESTAMP() AS staged_at
FROM `bruin-playground-arsalan.fifa_raw.fifa_schedule` s
JOIN `bruin-playground-arsalan.fifa_raw.host_venues`  v USING (venue_id)
LEFT JOIN groups_pivot g ON s.group_id = g.group_id
