# The Cost-of-Living Squeeze

This Bruin pipeline uses Numbeo's public current city and country ranking tables to explore a counter-intuitive question: where do low sticker prices still fail to translate into local affordability?

## Data sources

- [Numbeo current city rankings](https://www.numbeo.com/cost-of-living/rankings_current.jsp) — current city cost, rent, food, restaurant, and local purchasing-power indices.
- [Numbeo current country rankings](https://www.numbeo.com/cost-of-living/rankings_by_country.jsp) — current country and territory equivalents.
- [Numbeo methodology and motivation](https://www.numbeo.com/common/motivation_and_methodology.jsp) — definitions and caveats.

The public pages are captured at modest frequency. Numbeo's API and commercial data license are separate products; do not use this project for commercial or high-volume extraction without checking Numbeo's terms.

## Assets

- `numbeo_raw.numbeo_city_rankings` — current city rankings, appended by snapshot date.
- `numbeo_raw.numbeo_country_rankings` — current country rankings, appended by snapshot date.
- `numbeo_staging.city_cost_metrics` — deduplicated city metrics and affordability signals.
- `numbeo_staging.country_cost_metrics` — deduplicated country metrics and affordability signals.
- `numbeo_report.city_squeeze_top` — cities where combined costs exceed local buying power.
- `numbeo_report.country_squeeze_top` — countries with the largest affordability gaps.
- `numbeo_report.city_rent_burden_top` — cities with the highest indexed rent burden.
- `dashboard-dac/dashboards/numbeo-cost-insights.yml` — Bruin DAC dashboard with Global, Africa, Asia, Europe, North America, South America, and Oceania tabs.

## Run commands

```bash
bruin validate numbeo-cost-insights/
bruin run --start-date 2026-09-16 --end-date 2026-09-16 numbeo-cost-insights/assets/raw/numbeo_city_rankings.py
bruin run --start-date 2026-09-16 --end-date 2026-09-16 numbeo-cost-insights/assets/raw/numbeo_country_rankings.py
bruin run numbeo-cost-insights/assets/staging/city_cost_metrics.sql
bruin run numbeo-cost-insights/assets/staging/country_cost_metrics.sql
bruin run numbeo-cost-insights/assets/report/city_squeeze_top.sql
bruin run numbeo-cost-insights/assets/report/country_squeeze_top.sql
bruin run numbeo-cost-insights/assets/report/city_rent_burden_top.sql

dac validate --dir numbeo-cost-insights/dashboard-dac
dac check --dir numbeo-cost-insights/dashboard-dac
dac serve --dir numbeo-cost-insights/dashboard-dac --port 8321
```

Dashboard: http://localhost:8321

## Known limitations

- Numbeo is crowdsourced; contributor counts, recency, and coverage vary by city and country.
- Indices are relative benchmarks, not official inflation, poverty, or household-budget measures.
- A positive affordability gap is a derived index difference, not a literal percentage of salary spent.
- Continent groupings are broad dashboard slices based on Numbeo country labels; Russia and Turkey are assigned to Europe and Cyprus to Europe.
- The public current pages are snapshots; historical trend analysis requires repeated scheduled captures.
