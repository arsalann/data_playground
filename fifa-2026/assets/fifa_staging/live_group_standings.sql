/* @bruin
name: fifa_staging.live_group_standings
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest group standings from the worldcup26.ir live tracker source. Raw
  standings snapshots append on every extraction; this table deduplicates to
  the newest group/team row and joins team names/codes for dashboard display.

depends:
  - fifa_raw.live_group_standings
  - fifa_staging.live_teams

materialization:
  type: table
  strategy: create+replace

columns:
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
    primary_key: true
    nullable: false
  - name: team_id
    type: VARCHAR
    description: worldcup26.ir team identifier.
    primary_key: true
    nullable: false
  - name: team_name
    type: VARCHAR
    description: Team English display name.
  - name: fifa_code
    type: VARCHAR
    description: FIFA three-letter team code.
  - name: group_rank
    type: INTEGER
    description: Computed rank within group based on points, goal difference, goals for, and team name.
  - name: matches_played
    type: INTEGER
    description: Matches played.
  - name: wins
    type: INTEGER
    description: Wins.
  - name: draws
    type: INTEGER
    description: Draws.
  - name: losses
    type: INTEGER
    description: Losses.
  - name: points
    type: INTEGER
    description: Group-stage points.
  - name: goals_for
    type: INTEGER
    description: Goals scored.
  - name: goals_against
    type: INTEGER
    description: Goals conceded.
  - name: goal_difference
    type: INTEGER
    description: Goals for minus goals against.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.fifa_raw.live_group_standings`
  WHERE group_id IS NOT NULL
    AND team_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id, team_id ORDER BY extracted_at DESC) = 1
),
joined AS (
  SELECT
    d.group_id,
    d.team_id,
    t.team_name,
    t.fifa_code,
    COALESCE(d.matches_played, 0) AS matches_played,
    COALESCE(d.wins, 0) AS wins,
    COALESCE(d.draws, 0) AS draws,
    COALESCE(d.losses, 0) AS losses,
    COALESCE(d.points, 0) AS points,
    COALESCE(d.goals_for, 0) AS goals_for,
    COALESCE(d.goals_against, 0) AS goals_against,
    COALESCE(d.goal_difference, 0) AS goal_difference,
    d.extracted_at AS source_extracted_at
  FROM deduped d
  LEFT JOIN `bruin-playground-arsalan.fifa_staging.live_teams` t USING (team_id)
)
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY group_id
    ORDER BY points DESC, goal_difference DESC, goals_for DESC, team_name ASC, team_id ASC
  ) AS group_rank
FROM joined
ORDER BY group_id, group_rank
