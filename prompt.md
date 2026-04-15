# Pipeline Prompt Template

Generic template for prompting an agent to build a new data pipeline in this repo.
Copy, fill in the bracketed sections, and remove any optional blocks you don't need.

---

## Prompt

Build a new pipeline similar to @stackoverflow-trends for **[DOMAIN / DATA SOURCE]**.

**Context:** utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

[Provide ONE of the following:]

- API documentation link: [URL]
- Relevant endpoints:
  - `[endpoint_1]`
  - `[endpoint_2]`
  - `[endpoint_3]`
- OR: describe the data source if not API-based (e.g. public BigQuery dataset, CSV files, web scraping)

[If credentials are already configured:]
Credentials are in @.bruin.yml ([line range])

### What to extract

[Describe the entities and fields you care about. Be specific about granularity and history depth.]

Examples:

- "daily OHLCV prices + market cap for every S&P 500 ticker, going back to 2012"
- "hourly generation by energy source (MWh), day-ahead forecasts, market clearing prices, for the past 1 year"

### Naming

All asset/table names should have prefix `**[prefix]_`**
Destination: BigQuery

### Dashboard questions

The goal is a final dashboard that answers:

- [Question 1]
- [Question 2]
- [Question 3]
- [Question 4]

### Build order

Start building the pipeline:

1. Create pipeline structure and a README outlining the pipeline, data sources, and processing method
2. Create the Python extraction assets (with Bruin Python materialization), use Bruin's built-in start/end date variables
3. Test each raw asset individually for a small subset of data ([describe subset, e.g. "2-3 days" or "a single quarter"])
4. Build the staging SQL transformation assets, test them
5. Test the entire pipeline end-to-end for a slightly larger window ([describe window, e.g. "1 week"])

### Constraints (optional)

[Any additional constraints, e.g.:]

- "start with a free API for now, we can switch to a paid one later"
- "use append-only strategy and deduplicate in staging"
- "financial statements should fetch all available quarters regardless of date range"

---

## Lessons from Past Pipelines

### Geospatial pipelines (city-pulse)

When building pipelines that compare cities, regions, or geographic entities:

1. **Use identical spatial parameters for all entities.** Never mix `graph_from_place` (admin boundaries) with different scope levels. Use `graph_from_point(center, dist=FIXED_RADIUS)` with hardcoded center coordinates for all cities. Admin boundaries vary wildly — "City of London" is 1 sq mi, "Chicago" is 234 sq mi.
2. **Verify what each query actually returns** before building any charts. Log the bounding box/area, compare across entities.
3. **GHSL GeoPackage** has 16 thematic layers joined on `ID_UC_G0`, Mollweide projection → WGS84. Use `pycountry` with fuzzy matching + manual overrides for country codes.
4. **OSMnx**: Set `ox.settings.timeout = 300`, sleep 2s between cities, use `CITY_LIMIT` env var for testing. Full-city queries for megacities can hang.
5. **Cross-dataset name matching**: Use proximity matching (lat/lon + country code), not string matching. Filter out mismatches from all charts.

### bruin ai enhance

- Always validate after running `bruin ai enhance` — it can add invalid `accepted_values` checks and corrupt YAML column definitions.
- Never do bulk regex cleanups on YAML — rewrite the section manually if needed.

### Dashboard iteration process

1. Start by listing all available data fields and their distributions
2. Find non-obvious insights (correlations, outliers, derived metrics)
3. Build charts iteratively with user feedback — expect to remove boring charts and add analytical ones
4. Filter aggressively (population thresholds, exclude data quality issues)
5. Always include units on metrics and a methodology section

