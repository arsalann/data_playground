# Metro Infrastructure Cost Benchmark

Bruin pipeline and DAC dashboard comparing urban-rail construction cost per corridor kilometer, with tunnel share, station density, construction duration, project length, and country context.

## Data sources

- [Transit Costs Project data](https://transitcosts.com/data/) — project and phase-level urban-rail construction costs, corridor length, alignment type, stations, dates, and source references. The source reports PPP- and inflation-adjusted 2025 USD values. Its cost coverage is not a complete census of every city or country.
- [Transit Costs Project 2026 analysis](https://transitcosts.com/new-data/) — documents the current snapshot and warns that project coverage varies by city and country.
- [World Bank World Development Indicators](https://data.worldbank.org/) — GDP per capita PPP, population, and urban population share by country and year. License: CC BY 4.0.

## Pipeline assets

### Raw

- `raw.metro_costs_project` — public Transit Costs Project spreadsheet export.
- `raw.world_bank_context` — chunked World Bank API ingestion for country-year context.

### Staging

- `staging.metro_projects_clean` — deduplicates source rows, preserves exclusions, classifies comparability, and derives alignment, station, duration, and cost metrics.
- `staging.metro_projects_enriched` — joins each project to the nearest World Bank year around its construction midpoint.

### Reports

- `report.metro_country_cost_summary` — optional country-level context summary.
- `report.metro_city_cost_summary` — city/metropolitan-area averages weighted by route-km and unweighted medians.
- `report.metro_project_outliers` — project-level outlier and source-quality table.

## Run commands

```bash
bruin validate metro-infrastructure-cost/

bruin run metro-infrastructure-cost/assets/raw/metro_costs_project.py
bruin run metro-infrastructure-cost/assets/raw/world_bank_context.py \
  --start-date 1960-01-01 --end-date 2025-12-31

bruin run metro-infrastructure-cost/assets/staging/metro_projects_clean.sql
bruin run metro-infrastructure-cost/assets/staging/metro_projects_enriched.sql
bruin run metro-infrastructure-cost/assets/report/metro_country_cost_summary.sql
bruin run metro-infrastructure-cost/assets/report/metro_city_cost_summary.sql
bruin run metro-infrastructure-cost/assets/report/metro_project_outliers.sql

dac validate --dir metro-infrastructure-cost/dashboard-dac
dac check --dir metro-infrastructure-cost/dashboard-dac
dac serve --dir metro-infrastructure-cost/dashboard-dac --port 8321
```

Dashboard URL: http://localhost:8321

## Classification and limitations

- The comparison includes heavy metro, subway, rapid transit, and light-metro/automated urban rail candidates.
- Regional/mainline, high-speed, BRT, tram, streetcar, trolley, and LRT keyword matches remain in raw/staging data but are excluded from dashboard metrics with an explicit classification.
- Cost/km is not a complete measure of project difficulty. Station size, maintenance facilities, rolling stock treatment, systems, land, financing, taxes, governance, and scope vary between projects.
- Transit Costs Project coverage is selective and source quality varies. The dashboard exposes cost-source type, length-source type, and reference URLs.
- World Bank values are country-level context, not city-level construction cost drivers. The join uses the nearest available year to the construction midpoint and exposes the year gap.
- No causal claims are made from the descriptive comparisons.
