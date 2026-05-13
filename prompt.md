# Pipeline Build Prompt

Generic prompt for an agent to build a new data pipeline in this repo. Copy, fill in the bracketed sections, and remove any optional blocks you don't need.

---

## Prompt

Build a new pipeline for **[DOMAIN / DATA SOURCE]**.

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding. All dashboards must be built with Bruin DAC (see @DAC.md and the `create-dashboard` skill); do not write a new Streamlit dashboard.

### Data source

[Provide ONE of the following:]

- API documentation link: [URL]
- Relevant endpoints:
  - `[endpoint_1]`
  - `[endpoint_2]`
  - `[endpoint_3]`
- OR: describe the data source if not API-based (e.g. public BigQuery dataset, CSV files, GeoPackage, web scraping)

[If credentials are already configured:]
Credentials are in @.bruin.yml ([line range])

### What to extract

[Describe the entities and fields you care about. Be specific about granularity and history depth.]

Examples:

- "daily OHLCV prices + market cap for every S&P 500 ticker, going back to 2012"
- "hourly generation by energy source (MWh), day-ahead forecasts, market clearing prices, for the past 1 year"
- "street network metrics for 20 cities: orientation entropy, intersection density, bearing distributions"

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
6. Build the DAC dashboard in `dashboard-dac/dashboards/<name>.yml` with per-chart structure (header text → chart → footnote text). Validate with `dac validate`, then `dac check`, then `dac serve --port 8321` and share the `http://localhost:8321` link.

### Constraints (optional)

[Any additional constraints, e.g.:]

- "start with a free API for now, we can switch to a paid one later"
- "use append-only strategy and deduplicate in staging"
- "financial statements should fetch all available quarters regardless of date range"

---

## Tools & How to Use Them

### Bruin CLI

The primary tool for pipeline orchestration. Use it for everything — validation, running assets, checking lineage.

```bash
# Validate before doing anything else — catches YAML/config errors early
bruin validate <pipeline-dir>/

# Run individual assets during development (NEVER run full pipeline until everything works individually)
bruin run <path/to/asset.py>
bruin run <path/to/asset.sql>

# Run with date range for historical backfill
bruin run --start-date 2024-01-01 --end-date 2024-12-31 <path/to/asset>

# Run asset + all downstream dependents
bruin run --downstream <path/to/asset>

# Limit scope during testing via env vars
CITY_LIMIT=3 bruin run <path/to/asset.py>
STOCK_TICKER_LIMIT=5 bruin run <path/to/asset.py>
```

**Key rules:**
- Always validate before running anything.
- Always test raw assets individually with small scope first.
- For `append` strategy raw assets, you MUST specify `--start-date`/`--end-date` — the default interval is "today" which returns nothing for historical APIs.
- Run staging assets separately after raw data exists.

### Bruin MCP

Use Bruin MCP to look up docs, get an overview of the pipeline, and understand asset structure. Reference it when you're unsure about Bruin header syntax, materialization strategies, or column types.

### bruin ai enhance

Use `bruin ai enhance` to add quality checks (not_null, accepted_values, min/max) to asset column definitions. **But be careful:**

- Always run `bruin validate` immediately after — it can add invalid checks.
- Always run `bruin run` on the affected asset to verify the enhanced metadata works with actual data.
- It may corrupt YAML column definitions. If this happens, **rewrite the columns section manually** — never do bulk regex find/replace on YAML.

### BigQuery

The data warehouse. All raw and staging tables land here.

- Connection name: `bruin-playground-arsalan` (configured in `.bruin.yml`)
- Verify data after each raw asset run: check row counts, date ranges, column types, null rates.
- DAC dashboards query BigQuery via `bruin query` using the same `.bruin.yml` connection — no per-dashboard credentials needed. Run `gcloud auth application-default login` once locally.

### Bruin DAC (Dashboards)

Dashboard framework for ALL new dashboards in this repo. Reference implementation: `polymarket-weather/dashboard-dac/`. Detailed working notes (quirks, fork features, conventions): `DAC.md` at the repo root. Widget syntax reference: the `create-dashboard` skill at `.claude/skills/create-dashboard/SKILL.md`.

Layout:

```
<pipeline>/dashboard-dac/
  dashboards/
    <name>.yml                   # one file per dashboard
    queries/
      <widget>.sql               # SQL referenced by `file:` widgets
```

CLI:

```bash
dac validate --dir <pipeline>/dashboard-dac          # fast schema check
dac check    --dir <pipeline>/dashboard-dac          # validate + execute every query
dac serve    --dir <pipeline>/dashboard-dac --port 8321
# → open http://localhost:8321
dac query    --dir <pipeline>/dashboard-dac --dashboard "Name" --widget "Widget"  # debug
```

Always surface the localhost URL when you start `dac serve`. Always run `dac check` before declaring a dashboard done.

Conventions: see `VISUALIZATIONS.md` for the full set (3-row chart structure, color palette, axes, encoding key, methodology section, fork-only fields) and `DAC.md` for DAC-mechanism quirks.

### Domain-Specific Tools (use only when relevant to the pipeline topic)

The following tools are NOT core to every pipeline. Only use them if the data source or analysis requires it.

**Pydeck (maps)** — Only for pipelines with geographic/spatial data that needs interactive maps.
- Use `pdk.Layer("ScatterplotLayer", ...)` for city/point maps.
- Basemap: `https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json`
- Power-scale radius for population: `np.power(pop, 0.45) * 15`
- Use diverging color scales for metrics.

**OSMnx (street networks)** — Only for urban form / street network analysis pipelines.
- **ALWAYS use `graph_from_point(center, dist=RADIUS)`** with a fixed radius for all cities.
- **NEVER use `graph_from_place`** — admin boundaries are inconsistent across cities.
- Set `ox.settings.timeout = 300`, sleep 2s between cities, 3 retries with 30s backoff.
- Use `CITY_LIMIT` env var for testing.
- Full-city queries for megacities (Tokyo, Istanbul) can exceed 2,000 km² and hang.

**GeoPandas / GHSL** — Only for pipelines working with GeoPackage files or geospatial datasets.
- GeoPackages can have multiple thematic layers — list them with `fiona.listlayers()`, read specific layers by name.
- GHSL uses Mollweide projection — convert to WGS84 with `gdf.to_crs(epsg=4326)`.
- Use `pycountry` for country name → ISO code mapping, with fuzzy matching + manual overrides for edge cases.

**World Bank API** — Only for pipelines using World Bank Open Data indicators.
- Chunk requests into 10-year windows (single large requests timeout).
- Use `per_page=20000` to avoid pagination issues.
- 5 retries with exponential backoff — the API is flaky.
- Handle HTTP 400 for invalid country/indicator combinations gracefully (log and skip, don't crash).
- Use `append` strategy and deduplicate in staging.

---

## Data Analysis Process

Follow this process for every pipeline. The goal is analysis-driven dashboards, not data summaries.

### Phase 1: Understand the data

Before writing any chart code:
- List every field available across all data sources.
- Check row counts, null rates, distributions, outliers, duplicates.
- Query staging tables directly to verify what you actually have.
- Compute percentiles, check for join fanouts, verify dedup logic.

### Phase 2: Find non-obvious insights

A dashboard that just shows "here's the data" is not analysis. Look for:
- **Correlations** — do two variables move together? Compute r values.
- **Outliers** — which entities break the pattern? Why?
- **Derived metrics** — ratios, densities, growth rates, entropy-based scores.
- **Cross-domain joins** — combine datasets that weren't designed to go together.
- **Standard domain metrics** — research whether your field has established measurement standards (e.g., intersection density in urban planning, Sharpe ratio in finance).

### Phase 3: Build charts iteratively

Start with the most insightful chart. Show it. Get feedback. Iterate. Expect to:
- **Remove boring charts** — if the takeaway is "the data exists," cut it.
- **Add analytical charts** based on what the data reveals.
- **Filter aggressively** — population thresholds, exclude data quality issues, focus on the story.
- **Adjust visual encoding** — dot sizes, label sizes, axis scales, log vs linear — based on what makes the data readable.
- **Label everything** — units on all metrics, methodology section, data source links, limitations.

### Phase 4: Validate everything

- Cross-check every data claim against the actual data.
- Verify join quality — are matched entities actually correct? (e.g., proximity matching can produce wrong matches)
- Filter out known data quality issues from all charts.
- Include units on all metrics.
- Include a methodology section with: data sources + links, metric definitions, collection method, limitations, what the data cannot tell you.

---

## Visualization Rules

All visualization rules — palette, structure, axes, legends, framework-specific quirks (DAC, Altair, Matplotlib polar) — live in `VISUALIZATIONS.md` at the repo root. Read it before writing any chart code. Every chart MUST strictly follow that document.

---

## Common Pitfalls

Mistakes encountered in past pipelines. Avoid these:

1. **Inconsistent spatial methodology.** When comparing geographic entities, every entity must use identical parameters (same radius, same query type, same resolution). Mixing admin boundaries of different sizes produces meaningless comparisons.

2. **`bruin ai enhance` corrupting YAML.** It adds checks that may be invalid. Regex cleanup of the resulting YAML can strip `- name:` prefixes from column definitions, breaking parsing entirely. Always validate after, rewrite manually if needed.

3. **Altair layer scale mismatch.** Two layers using different field names for the same data get independent axes. Rename DataFrame columns to match across all layers.

4. **World Bank API flakiness.** Single large requests timeout. Always chunk into 10-year windows with retry logic. Handle HTTP 400 errors for invalid combos.

5. **Cross-dataset name matching.** City/entity names vary across datasets ("Mumbai" vs "Bombay", "Côte d'Ivoire" vs "Ivory Coast"). Use proximity matching (coordinates + country code) or ID-based joins, not string matching. Always verify matches and filter out bad ones.

6. **Visualizing data you haven't inspected.** Always query raw/staging tables and check distributions before building charts. Catching issues after the dashboard is built wastes significant time.

7. **Charts that just show data exist.** Every chart must answer a specific question with a non-obvious insight. "Here are the 20 cities" is not analysis. "Grid cities have 30x higher orientation order than organic cities" is.

8. **Missing units.** Every metric in every chart — axis titles, heatmap labels, tooltips, KPIs — must include units. "Building height" is ambiguous; "Building height (m)" is not.

9. **Not filtering for relevance.** Show the user the most meaningful subset, not everything. Filter by population threshold, exclude data quality issues, focus on entities with complete data.

10. **Running full pipeline before testing individual assets.** Always test each raw asset individually with small scope first. Then test staging. Then test the full pipeline. This catches issues early and saves significant time.
