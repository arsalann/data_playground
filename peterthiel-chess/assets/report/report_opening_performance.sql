/* @bruin
name: chess_peterthiel.report_opening_performance
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes public 3+0 performance by broad opening family and player color.
  Families are transparent groupings of Chess.com's public opening labels,
  intended to make a very granular opening taxonomy readable. Results are
  descriptive and are not adjusted for opponent mix or chess-engine quality.

  Source: https://www.chess.com/news/view/published-data-api

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: opening_family
    type: VARCHAR
    description: Broad opening-family label derived from Chess.com's public opening name.
    primary_key: true
    nullable: false
  - name: opening_chart_label
    type: VARCHAR
    description: Compact, readable label for the opening family in the DAC chart.
  - name: player_color
    type: VARCHAR
    description: Color played by the target account in the grouped games.
    primary_key: true
    nullable: false
  - name: games
    type: INTEGER
    description: Number of public 3+0 games in the opening family and color group.
  - name: score_rate_pct
    type: DOUBLE
    description: Target account score as a percentage of possible points in the group.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent public post-game rating in the group; descriptive context only.
  - name: opening_family_total_games
    type: INTEGER
    description: Total public 3+0 games across both target colors for the opening family.
  - name: opening_family_rank
    type: INTEGER
    description: Descending rank of the opening family by total public 3+0 games.

@bruin */

WITH labeled AS (
    SELECT
        player_color,
        actual_score,
        opponent_rating_after,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^sicilian defense') THEN 'Sicilian Defense'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^alapin sicilian') THEN 'Alapin Sicilian'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^queens gambit declined') THEN 'Queens Gambit Declined'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^queens pawn opening') THEN 'Queens Pawn Opening'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^scandinavian defense') THEN 'Scandinavian Defense'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^french defense') THEN 'French Defense'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^caro kann defense') THEN 'Caro-Kann Defense'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'^petrovs defense') THEN 'Petrovs Defense'
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(opening_name, '')), r'london system') THEN 'London System'
            ELSE 'Other openings'
        END AS opening_family
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    WHERE time_control = '180'
),

aggregated AS (
    SELECT
        opening_family,
        player_color,
        COUNT(*) AS games,
        100 * AVG(actual_score) AS score_rate_pct,
        AVG(opponent_rating_after) AS average_opponent_rating_after
    FROM labeled
    GROUP BY opening_family, player_color
),

with_totals AS (
    SELECT
        *,
        SUM(games) OVER (PARTITION BY opening_family) AS opening_family_total_games
    FROM aggregated
)

SELECT
    opening_family,
    CASE opening_family
        WHEN 'Sicilian Defense' THEN 'Sicilian'
        WHEN 'Alapin Sicilian' THEN 'Alapin'
        WHEN 'Queens Gambit Declined' THEN 'Q. Gambit Declined'
        WHEN 'Queens Pawn Opening' THEN 'Q. Pawn Opening'
        WHEN 'Scandinavian Defense' THEN 'Scandinavian'
        WHEN 'French Defense' THEN 'French'
        WHEN 'Caro-Kann Defense' THEN 'Caro-Kann'
        WHEN 'Petrovs Defense' THEN 'Petrovs'
        WHEN 'London System' THEN 'London'
        ELSE 'Other'
    END AS opening_chart_label,
    player_color,
    games,
    ROUND(score_rate_pct, 2) AS score_rate_pct,
    ROUND(average_opponent_rating_after, 2) AS average_opponent_rating_after,
    opening_family_total_games,
    DENSE_RANK() OVER (ORDER BY opening_family_total_games DESC) AS opening_family_rank
FROM with_totals
ORDER BY opening_family_rank, opening_family, player_color
