/* @bruin
name: fifa_reports.live_venue_load
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Match load by host city and stadium from the live fixture source. This table
  shows where the tournament schedule concentrates fixtures and separates
  group-stage from knockout-stage volume.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: venue_label
    type: VARCHAR
    description: Host city and stadium label.
    primary_key: true
  - name: venue_city
    type: VARCHAR
    description: Host city English display name.
  - name: stadium_name
    type: VARCHAR
    description: Stadium English display name.
  - name: country_label
    type: VARCHAR
    description: Host country English display name.
  - name: group_stage_matches
    type: INTEGER
    description: Number of group-stage fixtures at the venue.
  - name: knockout_matches
    type: INTEGER
    description: Number of knockout fixtures at the venue.
  - name: total_matches
    type: INTEGER
    description: Total number of fixtures at the venue.

@bruin */

SELECT
  CONCAT(COALESCE(venue_city, 'Unknown'), ' - ', COALESCE(stadium_name, 'Unknown stadium')) AS venue_label,
  venue_city,
  stadium_name,
  venue_country AS country_label,
  COUNTIF(match_type = 'group') AS group_stage_matches,
  COUNTIF(match_type != 'group') AS knockout_matches,
  COUNT(*) AS total_matches
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
GROUP BY venue_label, venue_city, stadium_name, country_label
ORDER BY total_matches DESC, venue_city
