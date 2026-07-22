# Argentina–Spain 2026 World Cup Final

Two independently shareable Bruin DAC reports plus three standalone visual stories from the completed World Cup:

- **Final Overview** — tournament baseline, official squads, and complete historical head-to-head context.
- **Hidden Insights** — evidence-led phase, progression, shots, and published passing-connection analysis.
- **Two Roads, One Final** — a custom accessible SVG story of the two cumulative xG-differential paths, including the decisive turning points.
- **England & France: Two Routes, One Bronze Final** — a custom accessible SVG story of their eight-match cumulative xG-differential paths, including the 6–4 bronze-final scoreline and its different xG result.
- **Four Routes, One Tournament** — a custom accessible SVG comparison of Argentina, England, France, and Spain, with their distinct eight-match paths in one chart.

Both dashboards are served from **http://localhost:8321**.

Serve the standalone visual locally:

~~~bash
python3 -m http.server 8330 --directory argentina-spain-final/visuals
# open http://localhost:8330/two-roads-one-final.html
# or   http://localhost:8330/two-roads-england-france.html
# or   http://localhost:8330/four-roads-one-tournament.html
~~~

## Sources

- [FIFA official squad lists PDF](https://fdp.fifa.org/assetspublic/ce281/pdf/SquadLists-English.pdf) — latest tournament 26-player lists, coach, club, DOB, height, caps, and goals.
- [FIFA Training Centre group-stage report hub](https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php) and [knockout-stage hub](https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php) — post-match PDFs for all eight completed Argentina and Spain matches, including the final.
- [11v11 Argentina v Spain record](https://www.11v11.com/teams/argentina/tab/opposingTeams/opposition/Spain/) — 14-match senior-men's historical series through 2018; a secondary source, retained with its source URLs and venue lookup. The pipeline adds the 2026 final from FIFA's primary match report.

## Four-team match record

This is the raw FIFA xG series behind the standalone visuals. Score is team-first; `diff` is xG minus xG conceded, and `cumulative` is its running total. The numbers come from FIFA's model, so do not substitute a single ESPN xG value into this table: different providers can calculate a match differently.

| Team | # | Opponent and stage | Score | xG | xGA | Diff | Cumulative |
|---|---:|---|---:|---:|---:|---:|---:|
| Argentina | 1 | Algeria, group stage | 3–0 | 1.58 | 0.41 | +1.17 | +1.17 |
| Argentina | 2 | Austria, group stage | 2–0 | 2.45 | 0.63 | +1.82 | +2.99 |
| Argentina | 3 | Jordan, group stage | 3–1 | 2.52 | 0.96 | +1.56 | +4.55 |
| Argentina | 4 | Cabo Verde, round of 32 | 3–2 | 2.73 | 0.52 | +2.21 | +6.76 |
| Argentina | 5 | Egypt, round of 16 | 3–2 | 2.63 | 0.76 | +1.87 | +8.63 |
| Argentina | 6 | Switzerland, quarter-final | 3–1 | 1.99 | 0.44 | +1.55 | +10.18 |
| Argentina | 7 | England, semi-final | 2–1 | 1.47 | 0.46 | +1.01 | +11.19 |
| Argentina | 8 | Spain, final | 0–1 | 0.07 | 2.52 | −2.45 | +8.74 |
| England | 1 | Croatia, group stage | 4–2 | 3.33 | 0.82 | +2.51 | +2.51 |
| England | 2 | Ghana, group stage | 0–0 | 1.78 | 0.34 | +1.44 | +3.95 |
| England | 3 | Panama, group stage | 2–0 | 1.61 | 0.72 | +0.89 | +4.84 |
| England | 4 | Congo DR, round of 32 | 2–1 | 1.75 | 0.64 | +1.11 | +5.95 |
| England | 5 | Mexico, round of 16 | 3–2 | 2.12 | 2.01 | +0.11 | +6.06 |
| England | 6 | Norway, quarter-final | 2–1 | 1.64 | 0.87 | +0.77 | +6.83 |
| England | 7 | Argentina, semi-final | 1–2 | 0.46 | 1.47 | −1.01 | +5.82 |
| England | 8 | France, bronze final | 6–4 | 2.34 | 2.99 | −0.65 | +5.17 |
| France | 1 | Senegal, group stage | 3–1 | 1.62 | 0.39 | +1.23 | +1.23 |
| France | 2 | Iraq, group stage | 3–0 | 2.30 | 0.70 | +1.60 | +2.83 |
| France | 3 | Norway, group stage | 4–1 | 1.05 | 1.45 | −0.40 | +2.43 |
| France | 4 | Sweden, round of 32 | 3–0 | 2.90 | 0.71 | +2.19 | +4.62 |
| France | 5 | Paraguay, round of 16 | 1–0 | 1.93 | 0.13 | +1.80 | +6.42 |
| France | 6 | Morocco, quarter-final | 2–0 | 3.52 | 0.16 | +3.36 | +9.78 |
| France | 7 | Spain, semi-final | 0–2 | 0.48 | 2.21 | −1.73 | +8.05 |
| France | 8 | England, bronze final | 4–6 | 2.99 | 2.34 | +0.65 | +8.70 |
| Spain | 1 | Cabo Verde, group stage | 0–0 | 2.26 | 0.13 | +2.13 | +2.13 |
| Spain | 2 | Saudi Arabia, group stage | 4–0 | 3.20 | 0.11 | +3.09 | +5.22 |
| Spain | 3 | Uruguay, group stage | 1–0 | 1.12 | 0.22 | +0.90 | +6.12 |
| Spain | 4 | Austria, round of 32 | 3–0 | 2.25 | 0.25 | +2.00 | +8.12 |
| Spain | 5 | Portugal, round of 16 | 1–0 | 1.72 | 0.91 | +0.81 | +8.93 |
| Spain | 6 | Belgium, quarter-final | 2–1 | 2.20 | 0.34 | +1.86 | +10.79 |
| Spain | 7 | France, semi-final | 2–0 | 2.21 | 0.48 | +1.73 | +12.52 |
| Spain | 8 | Argentina, final | 1–0 | 2.52 | 0.07 | +2.45 | +14.97 |

## Assets

| Layer | Assets | Purpose |
|---|---|---|
| `final_raw` | `official_squads`, `fifa_match_facts`, `h2h_history` | Versioned source retrieval and strict PDF/HTML parsing. |
| `final_staging` | `team_match_metrics`, `team_match_phases`, `shot_events`, `starters`, `player_line_breaks`, `passing_links`, `squad_start_counts`, `h2h_history` | Deduplicated analysis-ready records. |
| `final_reports` | `team_kpis`, `xg_trends`, `cumulative_xg_differential`, `goals_xg_match`, `phase_mix`, `progression_per_100_passes`, `shot_outcomes`, `top_passing_connections`, `official_squads`, `h2h_history`, `evidence_findings` | Dashboard-ready tables and checks-gated findings. |

## Development and validation

```bash
# Static definition checks
bruin validate argentina-spain-final/

# Parser unit test using representative FIFA report text
python3 -m unittest argentina-spain-final/tests/test_fifa_match_facts.py

# Load sources individually, then build their dependents
bruin run argentina-spain-final/assets/final_raw/official_squads.py
bruin run argentina-spain-final/assets/final_raw/fifa_match_facts.py
bruin run argentina-spain-final/assets/final_raw/h2h_history.py
bruin run --downstream argentina-spain-final/assets/final_raw/fifa_match_facts.py
bruin run --downstream argentina-spain-final/assets/final_raw/official_squads.py
bruin run --downstream argentina-spain-final/assets/final_raw/h2h_history.py

# Query the validation invariants after materialization
bruin query --connection bruin-playground-arsalan --query '
  SELECT team_name, COUNT(*) AS players
  FROM `bruin-playground-arsalan.final_reports.official_squads`
  GROUP BY team_name'

# Dashboard validation, query checks, and local review
dac validate --dir argentina-spain-final/dashboard-dac
dac check --dir argentina-spain-final/dashboard-dac
dac serve --dir argentina-spain-final/dashboard-dac --port 8321
# open http://localhost:8321
```

## Important limitations

- This is a completed-tournament descriptive snapshot. It is not an opponent-adjusted strength rating or betting model.
- FIFA’s in- and out-of-possession phase shares can overlap. The dashboard uses grouped zero-baseline bars, not an invalid 100%-stacked composition.
- FIFA publishes only the top five passing links per team-match, not complete passing networks. The report treats them as recurring published links, not absence/presence evidence for every connection.
- The historical H2H series from 11v11 is secondary context; the 2026 final comes directly from FIFA. Neither is used to infer the teams' underlying strength.
- The 11v11 site can block or error on direct programmatic retrieval. The ingestion first tries the cited source and then uses a public reader representation only to retrieve the same source text; output still preserves the original 11v11 URLs.
