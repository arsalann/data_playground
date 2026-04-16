# Pipeline Build Prompt

---

## Prompt

Build a new pipeline for **Hong Kong Public Transport Network**.

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

There are three data sources covering Hong Kong's public transport system:

**1. Static GTFS Feed (data.gov.hk)**

- Source: Hong Kong government open data portal (`data.gov.hk`)
- Format: ZIP archive containing CSV/text files (`routes.txt`, `stops.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`)
- Coverage: KMB buses, CTB/NWFB Citybus, trams, ferries
- Note: `stop_times.txt` exceeds 100 MB — handle large file ingestion appropriately
- Refresh: full refresh daily (WRITE_TRUNCATE)

**2. MTR Open Data Portal (opendata.mtr.com.hk)**

- Four CSV datasets:
  - `mtr_lines_stations` — heavy rail line and station data
  - `mtr_bus_stops` — MTR feeder bus routes and stops
  - `mtr_fares` — station-to-station fare table
  - `mtr_light_rail_stops` — Light Rail routes and stops
- **Important:** Requests require a `User-Agent` header (e.g. `"Mozilla/5.0"`) or the connection will be dropped
- **Important:** CSVs have UTF-8 BOM — decode with `utf-8-sig` to avoid character corruption

**3. MTR Real-Time Schedule API (rt.data.gov.hk)**

- Real-time train arrival/departure predictions
- JSON format with line, station, and timestamp fields
- Poll every minute for streaming ingestion

Note: MTR does not publish GTFS data. Trip-level data for the heavy rail network is not publicly available — this limits headway and crowding analysis for MTR lines.

Credentials are in @.bruin.yml

### What to extract

- **Stops:** stop ID, name, latitude, longitude — for all bus, tram, ferry, and MTR stops
- **Routes:** route ID, name, type (bus/tram/ferry), operator
- **Trips:** trip ID, route association, service calendar (weekday/weekend patterns)
- **Stop times:** arrival/departure times, stop sequence, per trip — this is the largest dataset
- **MTR stations:** station names, IDs, line associations, fare matrix, Light Rail stop sequences
- **Real-time events:** train arrival/departure predictions with line, station, and timestamp (streaming layer)

Granularity: individual stop and trip level for raw data; aggregated to hourly, daily, and categorical levels in marts.

### Naming

All asset/table names should have prefix **`hk_transit_`**
Destination: BigQuery

### Dashboard questions

The goal is a final dashboard that answers:

- Which stops and routes carry the most traffic (by departure count)?
- How does service volume differ between weekdays and weekends?
- When does the first and last service run on each route?
- Which stops are the top transfer hubs (most distinct routes served)?
- What are the peak hours for departures across the network?
- Which are the longest routes by number of stops?
- How are MTR and bus networks distributed geographically across Hong Kong?

### Build order

Start building the pipeline:

1. Create pipeline structure and a README outlining the pipeline, data sources, and processing method
2. Create the Python extraction assets (with Bruin Python materialization):
   - `ingest_gtfs_static.py` — download GTFS ZIP, extract files, load into BigQuery raw tables (full refresh with WRITE_TRUNCATE)
   - `ingest_mtr_csv.py` — fetch 4 MTR CSVs with User-Agent header and `utf-8-sig` decoding
3. Test each raw asset individually for a small subset of data (single day of GTFS data, verify MTR CSVs load correctly)
4. Build 5 staging SQL transformation assets: type casting, snake_case renaming, null filtering on primary keys (e.g. `CAST(stop_id AS INT64)`, `CAST(stop_lat AS FLOAT64)`, `WHERE stop_id IS NOT NULL`)
5. Build 10 mart SQL aggregation assets:
   - `mart_peak_hour_analysis` — departures grouped by hour
   - `mart_transfer_hubs` — distinct routes per stop
   - `mart_weekday_vs_weekend` — service volume comparison by day type
   - `mart_trip_trajectories` — trips joined with stop coordinates for route paths
   - `mart_busiest_stops` — top stops by departure count
   - `mart_longest_routes` — routes ranked by stop count
   - `mart_first_last_service` — earliest and latest service per route
   - `mart_route_summary` — route-level aggregations
   - `mart_mtr_stations` — MTR station and line reference
   - `mart_mtr_fares` — station-to-station fare lookup
6. Test staging and mart assets individually
7. Test the entire pipeline end-to-end

### Constraints

- Everything should run within GCP free tier limits
- GTFS data uses full refresh (`WRITE_TRUNCATE`) — not append, since the entire static feed is republished daily
- MTR CSVs require `User-Agent` header and `utf-8-sig` decoding — without these, ingestion silently corrupts data
- Always use separate `COUNT(DISTINCT ...)` queries for KPI metrics — do not rely on `LIMIT` clauses on detail queries, which will produce incorrect counts
- The streaming layer (MTR real-time API via Redpanda/Kafka) is optional and can be added after the batch pipeline is complete
- No trip-level analysis is possible for MTR heavy rail (no GTFS published) — document this limitation clearly

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
```

**Key rules:**
- Always validate before running anything.
- Always test raw assets individually with small scope first.
- Run staging assets separately after raw data exists.

### Bruin MCP

Use Bruin MCP to look up docs, get an overview of the pipeline, and understand asset structure. Reference it when you're unsure about Bruin header syntax, materialization strategies, or column types.

### bruin ai enhance

Use `bruin ai enhance` to add quality checks (not_null, accepted_values, min/max) to asset column definitions. **But be careful:**

- Always run `bruin validate` immediately after — it can add invalid checks.
- Always run `bruin run` on the affected asset to verify the enhanced metadata works with actual data.
- It may corrupt YAML column definitions. If this happens, **rewrite the columns section manually** — never do bulk regex find/replace on YAML.

### BigQuery

The data warehouse. All raw, staging, and mart tables land here.

- Connection name: `bruin-playground-arsalan` (configured in `.bruin.yml`)
- Dataset structure: `raw` → `staging` → `marts`
- Verify data after each raw asset run: check row counts, column types, null rates.

### Streamlit + Altair + Pydeck

Dashboard framework for the final visualization layer.

- Use `st.secrets["gcp_service_account"]` for auth.
- Use `@st.cache_resource` for the BigQuery client.
- Use `st.altair_chart(..., use_container_width=True)` for all Altair charts.
- Use Pydeck `ScatterplotLayer` for stop/station location maps.
- Place `.streamlit/secrets.toml` in the reports directory, not the repo root.

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
- **Peak patterns** — when do departures spike? Do different transport modes peak at different times?
- **Transfer connectivity** — which stops connect the most routes? Are there underserved transfer points?
- **Weekend vs weekday shifts** — which routes have the biggest service drop on weekends?
- **Geographic coverage gaps** — overlay bus and MTR networks to find areas served by only one mode.
- **Route efficiency** — which routes have the most stops per km? Which have the longest dwell times?

### Phase 3: Build charts iteratively

Start with the most insightful chart. Show it. Get feedback. Iterate. Expect to:
- **Remove boring charts** — if the takeaway is "the data exists," cut it.
- **Add analytical charts** based on what the data reveals.
- **Filter aggressively** — focus on the busiest stops, the most impactful routes, the clearest patterns.
- **Label everything** — units on all metrics, methodology section, data source links, limitations.

### Phase 4: Validate everything

- Cross-check every data claim against the actual data.
- Use separate `COUNT(DISTINCT ...)` queries for KPI metrics — never derive counts from limited detail queries.
- Document that MTR heavy rail trip-level analysis is not possible (no GTFS).
- Include a methodology section with: data sources + links, metric definitions, collection method, limitations, what the data cannot tell you.

---

## Visualization Rules

These are mandatory. See @AGENTS.md for the full specification.

### Color
- **Wong (2011) palette only**: `#D55E00` vermillion, `#56B4E9` sky blue, `#E69F00` orange, `#009E73` bluish green, `#CC79A7` reddish purple, `#0072B2` blue, `#F0E442` yellow, `#999999` grey.
- Never rely on color alone — always dual-encode with shape, labels, or position.
- Sequential: `blues` or `viridis`. Diverging: `blueorange`. Never `rainbow`/`jet`/`redgreen`.

### Truthful representation
- Bar/area charts: y-axis starts at zero. No exceptions.
- Log scales: label the axis with "(Log Scale)" and explain why.
- No dual y-axes, no 3D, no pie charts.
- Every encoding (size, color, shape) needs a legend or direct label.

### Altair-specific
- **Layered charts must share field names** across layers to share scales.
- **Log scales must be specified on every layer** sharing that axis — they don't propagate.
- **`zero=False`** on scatter y-axes when data clusters in a narrow range. Never on bar charts.
- **Interactive legends**: `alt.selection_point(fields=[...], bind="legend")`.
- **Tooltips mandatory**: format strings on all numeric fields.
- Standard height: 380. Scatter/dense: 450-500.

### Pydeck (stop/station maps)
- Use `pdk.Layer("ScatterplotLayer", ...)` for stop location maps.
- Basemap: `https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json`
- Color-code by transport mode (bus, tram, ferry, MTR).
- Scale dot size by departure count or route count.

### Layout
- KPIs at top with `st.metric()`.
- `st.divider()` between story sections.
- Insight blockquote after every chart: `st.markdown("> ...")` with specific numbers.
- Data source footer on every chart.
- Methodology section at bottom.

---

## Common Pitfalls

Mistakes to avoid in this pipeline:

1. **MTR CSV encoding corruption.** Without `utf-8-sig` decoding, BOM characters corrupt column headers. Always use `encoding='utf-8-sig'` when reading MTR CSVs.

2. **MTR connection drops.** The MTR portal drops connections without a `User-Agent` header. Always set `headers={"User-Agent": "Mozilla/5.0"}` on requests.

3. **KPI count errors from LIMIT clauses.** A `LIMIT 20` on a trips query will show 16 routes instead of 700+. Always use separate `COUNT(DISTINCT ...)` queries for KPI metrics.

4. **`bruin ai enhance` corrupting YAML.** It adds checks that may be invalid. Regex cleanup of the resulting YAML can strip `- name:` prefixes from column definitions, breaking parsing entirely. Always validate after, rewrite manually if needed.

5. **Large GTFS file handling.** `stop_times.txt` exceeds 100 MB. Ensure ingestion handles this without timeouts or memory issues.

6. **Missing MTR trip-level data.** MTR does not publish GTFS — there is no trip-level data for heavy rail. Do not attempt headway or crowding analysis for MTR lines. Document this limitation.

7. **Charts that just show data exist.** Every chart must answer a specific question with a non-obvious insight. "Here are the stops" is not analysis. "The top 10 transfer hubs serve 40% of all route connections" is.

8. **Missing units.** Every metric in every chart — axis titles, heatmap labels, tooltips, KPIs — must include units. "Departures" is ambiguous; "Departures per day" is not.

9. **Not filtering for relevance.** Show the most meaningful subset, not everything. Focus on the busiest stops, the most impactful routes, the clearest patterns.

10. **Running full pipeline before testing individual assets.** Always test each raw asset individually first. Then test staging. Then test marts. Then test the full pipeline.
