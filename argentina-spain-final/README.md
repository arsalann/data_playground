# Argentina–Spain 2026 World Cup Final

Two independently shareable Bruin DAC reports plus one standalone visual story for the Argentina–Spain World Cup final:

- **Final Overview** — tournament baseline, official squads, and complete historical head-to-head context.
- **Hidden Insights** — evidence-led phase, progression, shots, and published passing-connection analysis.
- **Two Roads, One Final** — a custom accessible SVG story of the two cumulative xG-differential paths, including the decisive turning points.

Both dashboards are served from **http://localhost:8321**.

Serve the standalone visual locally:

~~~bash
python3 -m http.server 8330 --directory argentina-spain-final/visuals
# open http://localhost:8330/two-roads-one-final.html
~~~

## Sources

- [FIFA official squad lists PDF](https://fdp.fifa.org/assetspublic/ce281/pdf/SquadLists-English.pdf) — latest tournament 26-player lists, coach, club, DOB, height, caps, and goals.
- [FIFA Training Centre group-stage report hub](https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php) and [knockout-stage hub](https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php) — post-match PDFs for completed Argentina and Spain matches. The final is explicitly excluded.
- [11v11 Argentina v Spain record](https://www.11v11.com/teams/argentina/tab/opposingTeams/opposition/Spain/) — 14-match senior-men's historical series; a secondary source, retained with its source URLs and venue lookup.

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

- This is a **pre-final** descriptive snapshot. It makes no lineup, score, or betting prediction.
- FIFA’s in- and out-of-possession phase shares can overlap. The dashboard uses grouped zero-baseline bars, not an invalid 100%-stacked composition.
- FIFA publishes only the top five passing links per team-match, not complete passing networks. The report treats them as recurring published links, not absence/presence evidence for every connection.
- The historical H2H series is from 11v11 and is secondary context; it is not used to infer the current teams' strength.
- The 11v11 site can put direct programmatic retrieval behind a Cloudflare challenge. The ingestion first tries the cited source and then uses a public reader representation only to retrieve the same source text; output still preserves the original 11v11 URLs.
