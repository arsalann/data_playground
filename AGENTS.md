# AGENTS.md

Utilize Bruin MCP and use Bruin CLI to run assets and query the tables. Reference Bruin docs.

This repository contains data pipelines built with **Bruin**, warehoused in **BigQuery**, with **Streamlit** dashboards visualized using **Altair**. Raw data ingestion is done in **Python**.

## Repository Structure

```
data_playground/
├── .bruin.yml              # Root Bruin config (connections, credentials)
├── .gitignore
├── requirements.txt        # Root-level Python dependencies
├── AGENTS.md               # You are here
├── prompt.md               # Generic template for prompting new pipelines
├── credentials/            # Service account keys (gitignored)
├── <pipeline-name>/        # Each pipeline is a top-level directory
│   ├── pipeline.yml        # Pipeline config (schedule, connections)
│   ├── README.md           # Pipeline-specific docs (data sources, assets, run commands)
│   └── assets/
│       ├── raw/            # Ingestion layer (Python or SQL)
│       ├── staging/        # Transformation layer (SQL)
│       └── reports/        # Dashboards & analytical queries
└── ...
```

## Pipeline Structure

Every pipeline follows the same three-layer pattern. Use `berlin-weather/` as the reference implementation, `stackoverflow-trends/` for advanced patterns (multiple data sources, API ingestion, append + dedup), and `baby-bust/` for a complete example with comprehensive data validation, `bruin ai enhance`, and a 4-chart Streamlit dashboard.

### 1. `pipeline.yml`

Defines the pipeline metadata and default connections.

```yaml
name: <pipeline-name>
schedule: daily
start_date: "2009-01-01"

default_connections:
  google_cloud_platform: "bruin-playground-arsalan"
```

- `name` must match the directory name.
- `schedule` can be `daily`, `hourly`, `weekly`, `monthly`, or a cron expression.
- `default_connections` sets which BigQuery project/connection assets use unless overridden.

### 2. `pipeline-name/README.md`

Every pipeline must have its own README covering:
- Data sources used (with links)
- All assets by layer (raw, staging, reports) with brief descriptions
- Key run commands (`bruin run`, `bruin validate`, `streamlit run`)
- Known limitations or data gaps

### 3. `assets/raw/` — Ingestion Layer (Python or SQL)

Raw assets fetch data from external sources and materialize it into BigQuery.

- **Python** (`type: python`) — for fetching from external APIs or files. Use an embedded Bruin YAML header in a docstring.
- **SQL** (`type: bq.sql`) — for querying existing BigQuery datasets (e.g. public datasets). Use an embedded Bruin YAML header in a SQL comment.

**File naming**: the file name (without extension) must match the table name in the asset's `name` field. For example, asset `raw.stackoverflow_api_monthly` lives in `stackoverflow_api_monthly.py`.

**Table naming**: `raw.<descriptive_name>` — all raw tables live in the `raw` schema.

**Template**:

```python
"""@bruin
name: raw.<table_name>
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Brief description of what this asset ingests.
  Include the data source URL and license if applicable.

materialization:
  type: table
  strategy: append

columns:
  - name: <column_name>
    type: <VARCHAR|INTEGER|DOUBLE|TIMESTAMP|BOOLEAN|DATE>
    description: <what this column contains, including units>
    primary_key: true  # only on the natural key

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    # Fetch from API / file / etc.
    ...


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "<default>")
    end_date = os.environ.get("BRUIN_END_DATE", "<default>")

    logger.info("Interval: %s to %s", start_date, end_date)
    df = fetch_data(start_date, end_date)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Fetched %d rows", len(df))
    return df
```

**Rules**:
- Always use `image: python:3.11`.
- Always include an `extracted_at` timestamp column (use `datetime.now(timezone.utc)`).
- Always include structured logging (`logging.basicConfig` + `logger = logging.getLogger(__name__)`).
- Document every column with a description including units where applicable.
- Mark exactly one column (or composite key) as `primary_key: true`.
- Use `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables for date-bounded fetches.
- Place a `requirements.txt` alongside the Python file with only the dependencies needed for that layer.

#### Materialization Strategy

| Strategy | Use when |
|---|---|
| `create+replace` | Small/immutable reference data (e.g. ticker lists, tag catalogs) |
| `append` | Large/incremental data (e.g. daily prices, hourly readings). **Always deduplicate in staging.** |
| `merge` | When you need upsert behavior on the raw table itself |
| `delete+insert` | When re-processing a date partition should replace old rows |

**Prefer `append` for most raw ingestion assets.** This allows safe re-runs and backfills without losing previously ingested data. Deduplication is handled in staging SQL using `ROW_NUMBER() ... ORDER BY extracted_at DESC`.

#### API Ingestion Best Practices

When fetching from external APIs:

- **Chunk large date ranges**: Break requests into 30-day (or smaller) windows to avoid timeouts and stay within API limits.
- **Retry with backoff**: Use exponential backoff for transient errors (429, 502, 503, timeouts). 5 retries is a good default.
- **Handle rate limits gracefully**: If you hit a rate limit mid-run, return whatever data was already fetched rather than crashing. Log a warning about the partial result.
- **Add delay between requests**: Use `time.sleep(0.5)` (or more) between API calls to avoid throttling.
- **Log progress**: Log each chunk/batch with counts so you can monitor long-running ingestions.
- **Environment variables for testing**: Use env vars like `STOCK_TICKER_LIMIT` to limit scope during development, so you don't need to fetch all 500+ tickers every test run.

#### Secrets

When an asset needs API credentials, declare them in the Bruin header under `secrets`. The keys must match the secret names in `.bruin.yml`:

```yaml
secrets:
  - key: epias_username
  - key: epias_password
```

Access them in Python via `os.environ["epias_username"]`.

**Dependencies** (`assets/raw/requirements.txt`):

```
pandas
requests
```

Add additional packages as needed (e.g. `yfinance`, `lxml`, `python-dateutil`).

### 4. `assets/staging/` — Transformation Layer (SQL)

Staging assets transform raw data into analysis-ready tables using BigQuery SQL. They reference upstream raw tables via `depends`.

**File naming**: `<entity>_<grain>.sql` — e.g. `weather_daily.sql`, `games_monthly.sql`.

**Table naming**: `staging.<descriptive_name>` — all staging tables live in the `staging` schema.

**Template**:

```sql
/* @bruin
name: staging.<table_name>
type: bq.sql
connection: bruin-playground-arsalan
description: |
  What this transformation does and why.
  Link to any reference material (e.g. code tables, standards).

depends:
  - raw.<upstream_table>

materialization:
  type: table
  strategy: create+replace

columns:
  - name: <column_name>
    type: <DATE|INTEGER|VARCHAR|DOUBLE|BOOLEAN>
    description: <description>
    primary_key: true
    nullable: false

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.<upstream_table>
    WHERE <primary_key> IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY <natural_key_columns> ORDER BY extracted_at DESC) = 1
)

SELECT
    ...
FROM deduped
ORDER BY <primary_key>
```

**Rules**:
- Always declare `depends` listing every upstream asset by its full `schema.table` name.
- **Always deduplicate raw data** using a `deduped` CTE with `ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY extracted_at DESC) = 1`. This is critical when raw assets use `append` strategy.
- Add derived columns: temporal dimensions (year, month, season, day_of_week), human-readable labels, boolean flags, unit conversions, derived ratios.
- Use `COALESCE` for nullable numeric fields to default to 0.
- Use `CASE` expressions for categorization and code-to-label mappings.
- Document every output column.
- SQL should be pure `SELECT` — let Bruin handle the DDL via `materialization`.
- Staging always uses `create+replace` strategy (it rebuilds from raw each time).

#### Multiple Data Sources

When the same entity comes from multiple sources (e.g. BigQuery public data + API supplement):
- Keep each source as a separate raw asset.
- In staging, `UNION ALL` the sources and deduplicate with `ROW_NUMBER()`, preferring the richer source for overlapping periods.
- Use `CAST(NULL AS INT64)` for columns that only exist in one source.

#### Common Staging Patterns

- **Unpivot**: Turn wide-format raw tables (one column per category) into long format using `UNION ALL` with `source_name` / `category` columns. E.g. energy sources, financial statement line items.
- **Enrichment joins**: Join with reference/dimension tables (e.g. `raw.stock_tickers` for sector/industry).
- **Window functions**: Moving averages (`AVG ... ROWS BETWEEN N PRECEDING`), rolling highs/lows, daily returns, period-over-period growth.
- **Ratio derivation**: Margins, ROE, debt-to-equity, share percentages — compute in staging, not in reports.

### 5. `assets/reports/` — Reports Layer (Streamlit + Altair)

This layer contains Streamlit dashboards and their supporting SQL queries.

**Files**:
- `streamlit_app.py` — the main dashboard application.
- `<analysis_name>.sql` — standalone analytical queries loaded at runtime by the Streamlit app. These are plain SQL files (no Bruin header) that query the BigQuery staging tables directly using fully-qualified table names (`project.schema.table`).
- `requirements.txt` — dependencies for the dashboard.

**Dependencies** (`assets/reports/requirements.txt`):

```
streamlit
google-cloud-bigquery
db-dtypes
altair
pandas
```

**Streamlit App Conventions**:

```python
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="<Dashboard Title>", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()
```

- Use `st.secrets["gcp_service_account"]` for credentials (stored in `.streamlit/secrets.toml`, gitignored).
- Load main data with `run_raw()` for inline SQL, `run_query()` for SQL files in the same directory.
- Use `@st.cache_resource` for the BigQuery client.

**Data Visualization Standards (Altair + Streamlit)**:

These rules are **mandatory** for every chart in every dashboard. They encode best practices from Tufte, Cleveland, Few, Munzner, and the ethical data visualization principles from the Data Visualization Society. Violations should be treated like bugs.

#### Before You Build Any Chart

- **Validate the data first.** Before writing any visualization code, query the data like a data analyst: check row counts, null rates, distributions, outliers, duplicates, and correlations. Understand what you actually have. Compute percentiles, check for join fanouts, verify dedup logic. Never visualize data you haven't inspected.
- **Analyze, don't summarize.** A dashboard that just shows "here's the data" is not analysis. Find correlations, compute derived metrics (e.g. price/ELO, gap ratios), identify Pareto frontiers, test hypotheses. Each chart must prove or disprove something non-obvious. If the takeaway is "the data exists," the chart has failed.
- **Follow the narrative arc.** Every dashboard tells a story structured as: (1) hypothesis/question, (2) evidence that the phenomenon exists, (3) evidence that it is systematic or repeatable, (4) quantification of the magnitude, (5) implications and limitations. If a chart doesn't advance this arc, cut it.
- **Fewer charts, more narrative.** 2–4 well-chosen charts with clear explanatory text beats 8 charts that overwhelm the viewer. Every chart must earn its place by answering a specific question. If you can say it in a sentence, don't make a chart.
- **Enrich aggressively.** Before building the dashboard, check what other datasets exist in BigQuery that could be joined. Cross-domain correlations (pricing + stock prices, quality rankings + prediction markets) are what make analysis interesting.
- **Be honest about sample sizes.** If a data point is based on 3 observations, say so. Annotate sample sizes directly on charts. Small-n medians are noise, not signal.
- **Explain the data.** Every dashboard must include: where the data comes from (with links), how it was collected and transformed, what the key metrics mean (with units), and what the limitations are. Put this in a dedicated methodology section, not hidden in tooltips. State explicitly what the data cannot tell you.
- **Don't make claims the data doesn't support.** If only 18 of 348 models have quality rankings, don't title a chart "Every Arena-Ranked Model" — say "The 18 models we can actually rank." If early time periods have tiny samples, caveat the trend explicitly.
- **Tables can be better than charts.** A 10-row dataset does not need a chart. Use `st.dataframe()` and let the reader scan the numbers. Charts are for patterns in data too large to read as a table.
- **Interactive legends.** Use `alt.selection_point(fields=[...], bind="legend")` so viewers can click legend entries to isolate series. This replaces overcrowded charts with focused exploration.
- **Quantify inline.** After every chart, include a `st.markdown("> ...")` blockquote that states the specific finding with numbers (e.g. "r = 0.23", "18x price difference for 5% quality gap"). The chart shows the pattern; the text states the magnitude.

#### Color and Accessibility

- **Colorblind-safe palette only.** Use the Wong (2011) palette from *Nature Methods*: `#D55E00` vermillion, `#56B4E9` sky blue, `#E69F00` orange, `#009E73` bluish green, `#CC79A7` reddish purple, `#0072B2` blue, `#F0E442` yellow, `#999999` grey. These 8 colors are the **maximum** for categorical encoding. If you need more categories, aggregate or facet — do not invent new colors.
- **Never rely on color alone.** Every color-encoded dimension must also be conveyed through a second channel: shape (`alt.Shape`), stroke dash pattern (`strokeDash`), direct text labels, or spatial position (faceting). A viewer who cannot distinguish any two colors in the palette must still be able to read the chart.
- **Highlight vs default.** For binary emphasis (e.g. "this item" vs "everything else"), use `HIGHLIGHT = "#D55E00"` and `DEFAULT = "#56B4E9"` with `alt.condition`. Never use red/green for binary states — use vermillion/sky-blue or vermillion/grey.
- **Sequential and diverging scales.** For continuous color scales use `blues` or `viridis` (sequential) and `blueorange` (diverging). Never use `redgreen`, `redblue`, or `rainbow`/`jet` — all are colorblind-hostile or perceptually non-uniform.
- **Overlaid series must have a legend.** If two series share the same positional axes and are distinguished only by color, a legend is required on the chart — not just in a `st.caption()`. Captions supplement; they do not replace legends.

#### Truthful Representation

- **Y-axis baseline.** Bar charts and area charts must start the quantitative axis at zero. A truncated axis exaggerates differences and misleads the viewer. Use `alt.Scale(zero=True)` (default for bars). If zero-baseline makes the data unreadable (e.g. ELO scores clustered in 1300–1500), switch to a dot plot or line chart — do not truncate a bar chart.
- **Log scales must be labeled.** If you use `scale=alt.Scale(type="log")`, the axis title must include "(Log Scale)" and the chart title or a caption must explain *why* the log scale is used (e.g. "Log scale used because values span 3+ orders of magnitude"). Never use a log scale to make a trend look more dramatic.
- **No dual Y-axes.** They are virtually always misleading — the viewer cannot compare magnitudes across two unrelated scales. Use faceted charts (side-by-side or vertically stacked) instead.
- **No 3D, no pie charts.** 3D adds no information and distorts area/length perception. Pie charts are inferior to bar charts for comparing quantities (Cleveland & McGill 1984). Use horizontal bar charts sorted by value instead.
- **No gradient fills for quantitative data.** Gradient fills (e.g. fading from color to white) obscure the data boundary, create phantom visual weight, and serve no analytical purpose. Use solid fills with reduced opacity (0.3–0.4 for area charts behind a line) or solid line charts.
- **Aspect ratio matters.** Time-series should use a wide aspect ratio (~3:1). Use the banking-to-45-degrees heuristic: slopes of ~45 degrees maximize readability.

#### Encoding Discipline

- **Every visual encoding must be explained.** If a chart uses size, color, shape, opacity, or stroke as a data channel, each must have either a visible legend or a direct label on the chart. `legend=None` is only acceptable when the channel is redundant with another fully-explained encoding (e.g. color matching x-axis categories that are already labeled).
- **Limit encodings to 3 channels max per chart.** Position (x, y) + one of {color, size, shape}. Adding a fourth channel (e.g. color + size + shape simultaneously) overloads working memory. If you need more dimensions, use faceting or a table.
- **Tooltips are mandatory.** Every chart must include `alt.Tooltip` entries for all encoded fields plus any context fields the viewer would want on hover. Numeric tooltips must have format strings (e.g. `format=",.0f"` for integers, `format="$.3f"` for prices).
- **Consistent chart heights.** Standard: `height=380`. Taller (scatter, dense): `height=480`. Never vary heights arbitrarily within a dashboard.
- **Sort bars by value.** Categorical bar charts must be sorted by the quantitative axis (largest to smallest or vice versa) unless there is a natural order (e.g. time, tiers). Alphabetical sort is almost never useful.

#### Annotation and Context

- **Reference lines must be labeled on the chart.** Use `mark_rule` + `mark_text` layered with `+`. The label text should state *what* the line represents (e.g. "GPT-4 Launch Price") and be positioned to avoid overlapping data points.
- **Chart titles state the insight, not the data.** Prefer "Median AI prices fell 98% in 3 years" over "Median Price by Quarter". The title tells the reader what to see; the axis labels tell them what is plotted.
- **Axis titles are required** on both axes unless the meaning is unambiguous from context (e.g. a single-series time chart where the x-axis is clearly dates). Include units in axis titles (e.g. "Price ($/MTok)", not just "Price").
- **Captions explain methodology.** Use `st.caption()` below the chart to explain filtering choices, what outliers were excluded and why, or caveats about the data. Never use captions as a substitute for a missing legend.

#### Streamlit Layout

- Use `st.altair_chart(..., use_container_width=True)` to render all charts.
- Use `cornerRadiusTopLeft=4, cornerRadiusTopRight=4` on `mark_bar()` for rounded bar tops.
- Place KPI metrics above the first chart. Use `st.metric()` with explicit delta values when comparing periods.
- Use `st.columns(2)` for side-by-side comparisons of related charts. Never put unrelated charts side-by-side.
- Use `st.divider()` between story sections, not between every chart.
- **Data tables complement charts.** Show the underlying data (top N, summary) below the chart so the viewer can verify what they see. Use `st.dataframe(..., hide_index=True)`.

## Bruin Asset Metadata Reference

All Bruin metadata lives inside a comment block at the top of the file:
- **Python**: `"""@bruin ... @bruin"""`
- **SQL**: `/* @bruin ... @bruin */`

### Key Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | `schema.table_name` — determines the destination table |
| `type` | Yes | `python` for Python assets, `bq.sql` for BigQuery SQL |
| `image` | Python only | Python Docker image, use `python:3.11` |
| `connection` | Yes | BigQuery connection name from `.bruin.yml` |
| `description` | Yes | Multi-line description of what the asset does |
| `depends` | SQL only | List of upstream asset names (`schema.table`) |
| `materialization.type` | Yes | `table` or `view` |
| `materialization.strategy` | Yes | `create+replace`, `merge`, `delete+insert`, `append`, etc. |
| `columns` | Yes | Full column definitions with `name`, `type`, `description` |

### Column Types

Use these types for column definitions: `VARCHAR`, `INTEGER`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`.

## Bruin CLI Quick Reference

```bash
bruin run <path/to/asset>                                    # Run a single asset
bruin run <path/to/pipeline/>                                # Run entire pipeline
bruin run --downstream <path>                                # Run asset + all downstream
bruin run --start-date 2024-07-01 --end-date 2024-12-31 <path>  # Run with date range
bruin validate <path>                                        # Validate asset/pipeline definitions
bruin format <path>                                          # Auto-format asset files
bruin lineage <path>                                         # Show asset dependency graph
bruin connections list                                       # List configured connections
bruin connections ping <name>                                # Test a connection
```

- Always run individual assets during development, not entire pipelines.
- Use `--start-date` / `--end-date` for backfilling specific date ranges without re-ingesting everything.
- Use `--downstream` when you want to run a raw asset and automatically rebuild its staging dependents.

## Testing & Development Workflow

Follow this order when building or modifying a pipeline:

1. **Validate first**: `bruin validate <pipeline-dir>/` — catches header/config errors before any execution.
2. **Test raw assets individually** with a small date range (2-3 days) or a limited scope (e.g. `STOCK_TICKER_LIMIT=5`).
3. **Verify data in BigQuery** after each raw asset — check row counts, date ranges, column types.
4. **Test staging SQL** once raw tables exist — run individually, then check output row counts and derived metrics.
5. **Test full pipeline** with a slightly larger window (e.g. 1 week) using `bruin run <pipeline-dir>/`.
6. **Run Streamlit** locally to verify the dashboard renders: `streamlit run <pipeline>/assets/reports/streamlit_app.py`.

For financial or quarterly data: the source API may only return recent quarters regardless of date range. Test with whatever the API actually provides rather than forcing specific dates.

## Creating a New Pipeline — Checklist

1. Create a new top-level directory named after the pipeline.
2. Add `pipeline.yml` with name, schedule, start_date, and default_connections.
3. Add a `README.md` documenting data sources, all assets, and run commands.
4. Create `assets/raw/` with Python ingestion scripts and a `requirements.txt`.
5. Create `assets/staging/` with SQL transformations that `depends` on the raw assets. Always deduplicate.
6. Create `assets/reports/` with a Streamlit app, supporting SQL files, and `requirements.txt`.
7. Validate with `bruin validate <pipeline-dir>/`.
8. Test each raw asset individually with a small subset of data.
9. Test staging assets once raw data exists.
10. Test the full pipeline end-to-end.

## Dependency Resolution

Bruin resolves Python dependencies by walking up the file tree from the asset to find the nearest `requirements.txt`. Keep a separate `requirements.txt` per layer (`raw/`, `reports/`) so dependencies stay isolated.

## Secrets Management

**No secrets, credentials, API keys, tokens, or passwords may ever be committed to this repository.** This is a hard rule with no exceptions.

### What is gitignored

The following are excluded via `.gitignore` and must never be committed:
- `.bruin.yml` — contains connection credentials (API keys, passwords, service account paths)
- `**/secrets.toml` — Streamlit secrets files in any directory
- `credentials/` — service account JSON files
- `*.pem`, `*.key`, `*.p12`, `*.pfx` — private key files
- `.env`, `.env.*` — environment variable files

### Rules

- **Never hardcode secrets in Python or SQL.** Use `os.environ["KEY_NAME"]` in Python and Bruin `secrets:` declarations in the asset YAML header.
- **Never commit `.streamlit/secrets.toml`** — generate it locally from the service account JSON in `credentials/`. See the Streamlit section below.
- **Never commit `.bruin.yml`** — this file contains all connection credentials. It stays local.
- **Never create service account JSON files outside `credentials/`** — that directory is gitignored.
- **If you accidentally commit a secret, the credential must be rotated immediately.** Removing the file from git tracking does not remove it from history. Use `git-filter-repo` to rewrite history and force-push.

### Streamlit secrets setup

Each dashboard that queries BigQuery needs a `.streamlit/secrets.toml` in its reports directory. Create it from the GCP service account JSON:

```bash
# From the pipeline's reports directory:
mkdir -p .streamlit
# Then manually create secrets.toml with the service account fields
# DO NOT copy the JSON file directly — use the TOML format shown in AGENTS.md
```

The `.streamlit/secrets.toml` file is gitignored globally via `**/secrets.toml`. Verify before committing: `git status` should never show a `secrets.toml` file.

## Things to Avoid

- Do not put non-asset files inside `assets/`. Use separate directories for ad-hoc queries or analyses.
- Do not hardcode dates in SQL — use Bruin templating or `BRUIN_START_DATE`/`BRUIN_END_DATE` in Python.
- Do not use `.yml` extension for asset definitions — use `.asset.yml` if defining assets in YAML.
- **Do not commit `.bruin.yml`, credentials, `secrets.toml`, `.env`, private keys, or any file containing secrets** — they are all gitignored. If `git status` shows any of these files as untracked or modified, do not stage them.
- Do not write `CREATE TABLE` or `INSERT` in SQL assets — let Bruin's `materialization` handle DDL.
- Do not give the asset file a different name than the asset name (file name and asset name must match — asset name is `<dataset>.<table_name>` which is `<parent_folder_name>.<asset_file_name>`).
- Do not use `create+replace` for large incremental data — use `append` and deduplicate in staging.
- Do not crash on API rate limits — return partial data and log a warning.
- Do not skip logging — every Python asset must have structured logging with progress output.
- Do not leave throwaway test scripts in the root directory or inside `assets/`.
- When running `bruin run <pipeline>/` on pipelines with `append` raw assets, the default date interval is today — which may return no data for historical APIs. Run raw assets explicitly with `--start-date`/`--end-date` for initial loads, then run staging assets separately.
- For flaky APIs (e.g., World Bank), use chunked requests (10-year windows) with high `per_page` values and retry logic with exponential backoff. Single large requests are more likely to timeout.
- When pivoting long-to-wide in staging (e.g., indicator rows → columns), validate every pivoted column against raw by joining on natural key and checking for zero diff. This catches silent data loss from incorrect indicator codes or join fanout.
- After running `bruin ai enhance`, always re-run `bruin validate` and `bruin run` on the affected assets to verify the enhanced metadata doesn't break anything. The enhance command adds quality checks (not_null, accepted_values, min/max) that may fail if the data has edge cases. **Never do bulk regex edits on YAML column definitions** — if `bruin ai enhance` corrupts the YAML, rewrite the section manually.
- For Streamlit secrets: create `.streamlit/secrets.toml` in the reports directory (not root). Generate it from the GCP service account JSON in `credentials/`. This file is gitignored.
- Use `python3 -m streamlit run` instead of bare `streamlit run` if the streamlit binary isn't on PATH.

## Geospatial Data Rules

These rules are **mandatory** when working with geospatial data (OSMnx, GeoPandas, GHSL, etc.):

- **Consistent spatial methodology.** When comparing cities or regions, every entity MUST use identical spatial parameters: same query function, same radius, same resolution, same projection. Never mix `graph_from_place` (admin boundaries) across cities — admin boundary sizes vary wildly (e.g., "City of London" = 1 sq mi vs "Chicago" = 234 sq mi). Use `graph_from_point(center, dist=RADIUS)` with a fixed radius for all cities.
- **Verify query scope before charting.** Before building any visualization, verify what each geospatial query actually returned. Log the bounding box or area. Compare areas across all entities to catch inconsistencies. A query for "Barcelona" might return the city, the province, or a single neighborhood depending on the API and query string.
- **Document the methodology explicitly.** State the exact spatial parameters (radius, center coordinates, projection, data version) in the README and in the dashboard's methodology section. Future users must be able to reproduce the analysis.
- **GHSL GeoPackage handling.** The GHSL R2024A release contains 16 thematic layers in a single GeoPackage that must be joined on `ID_UC_G0`. It uses Mollweide projection — convert centroids to WGS84 (EPSG:4326) for lat/lon coordinates. Use `pycountry` with fuzzy matching + manual overrides for country name → ISO code mapping.
- **OSMnx Overpass API.** Set `ox.settings.timeout = 300` for large network downloads. Add `time.sleep(2)` between city downloads to respect rate limits. Use `CITY_LIMIT` env var for testing. Full-city queries for megacities (Tokyo, Istanbul) can exceed 2,000 km² and hang — always use bounded queries.
- **Name matching across datasets.** When joining datasets from different sources (e.g., GHSL city names to OSMnx queries), use proximity matching (lat/lon within threshold + country code match) rather than string matching. City names vary across datasets (e.g., "Mumbai" vs "Bombay", "Brasília" vs "Lago Norte"). Filter out known mismatches from all charts.

## Data Analysis Process

This is the process for building analysis-driven dashboards. Follow this order:

### Phase 1: Data inventory
Before writing any chart code, list every field available across all data sources. Understand the data range, granularity, null rates, and distributions. Query the staging tables directly to verify what you actually have.

### Phase 2: Find non-obvious insights
A dashboard that just shows "here's the data" is not analysis. Look for:
- **Correlations** — do two variables move together? (e.g., GDP vs grid-ness, temperature vs street length)
- **Outliers** — which cities/countries break the pattern? Why?
- **Derived metrics** — intersection density (intersections / area), orientation order (entropy-derived), population growth rates
- **Cross-domain joins** — combine datasets that weren't designed to go together (GHSL urban data + OSMnx street analysis + World Bank economic indicators)

### Phase 3: Build charts iteratively
Start with the most insightful chart. Show it to the user. Get feedback. Iterate. Each chart must answer a specific question — if the takeaway is "the data exists," cut the chart. Expect to:
- Remove boring or redundant charts
- Add new charts based on what the data reveals
- Filter aggressively (e.g., cities over 5M population only, exclude data mismatches)
- Adjust visual encoding (dot sizes, label sizes, axis scales) based on what makes the data readable

### Phase 4: Validate everything
- Cross-check data claims against the actual data
- Verify join quality (are matched cities actually correct?)
- Filter out known data quality issues (e.g., GHSL proximity mismatches)
- Include units on all metrics
- Include methodology section with data source links and limitations

## Altair Gotchas

Lessons learned from building dashboards with Altair:

- **Layered charts must share field names.** If a dot layer uses `population_2015` and a line layer uses `y`, Altair creates independent scales. Rename DataFrame columns to match across layers.
- **Angle values must be 0-360.** `angle=-33` raises a validation error in newer Altair. Use `angle=327` instead (equivalent rotation).
- **Log scales on all layers.** When using `alt.Scale(type="log")`, every layer sharing that axis must also specify the log scale explicitly — it does not propagate across layers.
- **`zero=False` on Y-axis.** When data is clustered in a narrow range (e.g., building heights 10-50m), use `alt.Scale(zero=False)` on scatter/point charts to spread the data. Never do this on bar charts.
- **Interactive legends.** Use `alt.selection_point(fields=[...], bind="legend")` so viewers can click legend entries to filter. Encode both color and shape on the same field for dual encoding (accessibility).

## Matplotlib Polar Plot Gotchas

- **Default orientation is wrong for compass bearings.** Matplotlib's default: 0° at East (right), counter-clockwise. For street orientation plots, set `ax.set_theta_zero_location("N")` and `ax.set_theta_direction(-1)` to get North at top, clockwise. Do this before drawing bars.