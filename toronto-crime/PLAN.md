# Toronto Crime Dashboard - Technical Requirements

## Goal

Build a Bruin pipeline and Bruin DAC dashboard for analyzing Toronto Community Safety Indicator crime patterns by year, category, neighbourhood, premises type, time of occurrence, and location density.

The implementation must use Bruin for ingestion, transformation, validation, and BigQuery materialization. The dashboard must use Bruin DAC. Do not create a Streamlit dashboard.

The built dashboard must be served locally at:

```bash
dac serve --dir toronto-crime/dashboard-dac --port 8321
```

Local review URL:

```text
http://localhost:8321
```

## Source Data

### Toronto Police Community Safety Indicators

Primary crime source:

```text
https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Major_Crime_Indicators_Open_Data/FeatureServer/0
```

Use the ArcGIS REST query endpoint:

```text
https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Major_Crime_Indicators_Open_Data/FeatureServer/0/query
```

Required query characteristics:

- Format: `geojson`
- Output spatial reference: `4326`
- Page size: `2000`
- Pagination: use `resultOffset` until `exceededTransferLimit` is false or no rows are returned
- Date filtering: use `BRUIN_START_DATE` and `BRUIN_END_DATE` to filter by occurrence date whenever possible
- Retry policy: exponential backoff for connection errors, timeouts, HTTP 429, and 5xx responses
- Rate limiting: short delay between page requests

Important source caveats to preserve in documentation and dashboard footnotes:

- The dataset includes selected Community Safety Indicator categories: Assault, Break and Enter, Auto Theft, Robbery, and Theft Over. It excludes sexual violations.
- Data starts in 2014.
- Rows are at offence and/or victim level. One occurrence may have multiple rows.
- Locations are deliberately offset to nearby road intersections for privacy.
- Location data must be treated as approximate and must not be interpreted as exact addresses or individuals.
- Counts by division and neighbourhood may not exactly match other police geographies or publications.
- The data is preliminary and may change after publication.
- Rows with `NSA` neighbourhoods or invalid coordinates must be retained for citywide counts but excluded from neighbourhood rate and spatial analyses where appropriate.

Expected source fields:

- `OBJECTID`
- `EVENT_UNIQUE_ID`
- `REPORT_DATE`
- `OCC_DATE`
- `REPORT_YEAR`
- `REPORT_MONTH`
- `REPORT_DAY`
- `REPORT_DOY`
- `REPORT_DOW`
- `REPORT_HOUR`
- `OCC_YEAR`
- `OCC_MONTH`
- `OCC_DAY`
- `OCC_DOY`
- `OCC_DOW`
- `OCC_HOUR`
- `DIVISION`
- `LOCATION_TYPE`
- `PREMISES_TYPE`
- `UCR_CODE`
- `UCR_EXT`
- `OFFENCE`
- `CSI_CATEGORY`
- `HOOD_158`
- `NEIGHBOURHOOD_158`
- `HOOD_140`
- `NEIGHBOURHOOD_140`
- `LONG_WGS84`
- `LAT_WGS84`
- GeoJSON point coordinates

### City of Toronto Neighbourhood Profiles

Profile source:

```text
https://open.toronto.ca/dataset/neighbourhood-profiles/
```

Use the City of Toronto CKAN API to discover resources:

```text
https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=neighbourhood-profiles
```

Required resources:

- `neighbourhood-profiles-2021-158-model`
- `neighbourhood-profiles-2016-140-model`

Minimum required profile fields:

- Neighbourhood ID
- Neighbourhood name
- Population
- Land area in square kilometres
- Census/profile year
- Neighbourhood model: `158` or `140`

The 158-neighbourhood model is the default dashboard geography. The 140-neighbourhood model is retained for historical comparability and for rows where only 140 geography is needed.

### City of Toronto Neighbourhood Boundaries

Boundary source:

```text
https://open.toronto.ca/dataset/neighbourhoods/
```

Use the City of Toronto CKAN API to discover resources:

```text
https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=neighbourhoods
```

Required resources:

- Current 158-neighbourhood boundaries
- Historical 140-neighbourhood boundaries

Minimum required boundary fields:

- Neighbourhood ID
- Neighbourhood name
- Neighbourhood model: `158` or `140`
- GeoJSON geometry
- BigQuery `GEOGRAPHY` geometry
- Polygon area in square kilometres, derived from geometry when possible

## Pipeline Structure

Create the following directory structure:

```text
toronto-crime/
├── pipeline.yml
├── README.md
├── PLAN.md
├── assets/
│   ├── raw/
│   │   ├── toronto_csi_events.py
│   │   ├── toronto_neighbourhood_profiles.py
│   │   ├── toronto_neighbourhood_boundaries.py
│   │   └── requirements.txt
│   ├── staging/
│   │   ├── crime_events.sql
│   │   ├── neighbourhood_profiles.sql
│   │   ├── neighbourhood_boundaries.sql
│   │   ├── crime_neighbourhood_yearly.sql
│   │   └── crime_temporal_patterns.sql
│   └── report/
│       ├── dashboard_kpis.sql
│       ├── category_trends.sql
│       ├── neighbourhood_rankings.sql
│       ├── neighbourhood_category_mix.sql
│       ├── temporal_heatmap.sql
│       └── spatial_summary.sql
└── dashboard-dac/
    ├── dashboards/
    │   ├── toronto-crime.yml
    │   └── queries/
    │       ├── kpi_total_crimes.sql
    │       ├── kpi_latest_year.sql
    │       ├── kpi_yoy_change.sql
    │       ├── kpi_citywide_rate.sql
    │       ├── category_trends.sql
    │       ├── neighbourhood_rankings.sql
    │       ├── neighbourhood_category_mix.sql
    │       ├── temporal_heatmap.sql
    │       └── neighbourhood_table.sql
    └── semantic/
```

## `pipeline.yml`

Required pipeline metadata:

```yaml
name: toronto-crime
schedule: daily
start_date: "2014-01-01"

default_connections:
  google_cloud_platform: "bruin-playground-arsalan"
```

## Raw Assets

### `raw.toronto_csi_events`

File:

```text
toronto-crime/assets/raw/toronto_csi_events.py
```

Bruin asset requirements:

- `type: python`
- `image: python:3.11`
- `connection: bruin-playground-arsalan`
- `materialization.type: table`
- Preferred initial implementation: `strategy: append`
- Include a natural/composite primary key. Preferred key: `objectid`; if instability is detected, use `(event_unique_id, offence, occ_date, latitude, longitude)`.
- Include `extracted_at TIMESTAMP`.
- Include structured logging with page counts, row counts, retry attempts, and date interval.
- Use `BRUIN_START_DATE` and `BRUIN_END_DATE`.
- Use chunked date windows for large historical backfills.
- Return partial data with a warning if the source rate-limits after some successful pages.

Required raw output columns:

- `objectid INTEGER`
- `event_unique_id VARCHAR`
- `report_date TIMESTAMP`
- `occurrence_date TIMESTAMP`
- `report_year INTEGER`
- `report_month VARCHAR`
- `report_day INTEGER`
- `report_day_of_year INTEGER`
- `report_day_of_week VARCHAR`
- `report_hour INTEGER`
- `occurrence_year INTEGER`
- `occurrence_month VARCHAR`
- `occurrence_day INTEGER`
- `occurrence_day_of_year INTEGER`
- `occurrence_day_of_week VARCHAR`
- `occurrence_hour INTEGER`
- `division VARCHAR`
- `location_type VARCHAR`
- `premises_type VARCHAR`
- `ucr_code VARCHAR`
- `ucr_ext VARCHAR`
- `offence VARCHAR`
- `csi_category VARCHAR`
- `hood_158 VARCHAR`
- `neighbourhood_158 VARCHAR`
- `hood_140 VARCHAR`
- `neighbourhood_140 VARCHAR`
- `longitude DOUBLE`
- `latitude DOUBLE`
- `source_url VARCHAR`
- `extracted_at TIMESTAMP`

### `raw.toronto_neighbourhood_profiles`

File:

```text
toronto-crime/assets/raw/toronto_neighbourhood_profiles.py
```

Bruin asset requirements:

- `type: python`
- `image: python:3.11`
- `connection: bruin-playground-arsalan`
- `materialization.type: table`
- `strategy: create+replace`
- Fetch resource metadata from CKAN rather than hardcoding file URLs only.
- Include `extracted_at TIMESTAMP`.
- Preserve enough source metadata to audit the resource used.

Required raw output columns:

- `neighbourhood_model INTEGER`
- `profile_year INTEGER`
- `neighbourhood_id VARCHAR`
- `neighbourhood_name VARCHAR`
- `population INTEGER`
- `land_area_km2 DOUBLE`
- `source_resource_name VARCHAR`
- `source_resource_url VARCHAR`
- `extracted_at TIMESTAMP`

### `raw.toronto_neighbourhood_boundaries`

File:

```text
toronto-crime/assets/raw/toronto_neighbourhood_boundaries.py
```

Bruin asset requirements:

- `type: python`
- `image: python:3.11`
- `connection: bruin-playground-arsalan`
- `materialization.type: table`
- `strategy: create+replace`
- Fetch current 158 and historical 140 GeoJSON resources from CKAN.
- Include `extracted_at TIMESTAMP`.

Required raw output columns:

- `neighbourhood_model INTEGER`
- `neighbourhood_id VARCHAR`
- `neighbourhood_name VARCHAR`
- `geojson_geometry VARCHAR`
- `source_resource_name VARCHAR`
- `source_resource_url VARCHAR`
- `extracted_at TIMESTAMP`

### Raw Requirements File

File:

```text
toronto-crime/assets/raw/requirements.txt
```

Minimum dependencies:

```text
pandas
requests
openpyxl
python-dateutil
```

Add `shapely` only if geometry validation is needed in Python. Prefer BigQuery geography functions for spatial derivations.

## Staging Assets

All staging assets must:

- Use `type: bq.sql`.
- Use `connection: bruin-playground-arsalan`.
- Use `materialization.type: table`.
- Use `strategy: create+replace`.
- Declare all upstream `depends`.
- Deduplicate append-based raw data with `ROW_NUMBER() OVER (...) ORDER BY extracted_at DESC`.
- Use only `SELECT` SQL. Do not write DDL or DML.
- Document every output column.

### `staging.crime_events`

Purpose:

Create one cleaned, typed crime event/offence table.

Key transformations:

- Deduplicate raw CSI rows.
- Trim all string dimensions.
- Normalize `Theft Over` to `Theft Over $5k`.
- Parse and expose date fields:
  - `occurrence_date DATE`
  - `occurrence_timestamp TIMESTAMP`
  - `report_date DATE`
  - `report_timestamp TIMESTAMP`
- Derive:
  - `occurrence_year`
  - `occurrence_month_num`
  - `occurrence_month_name`
  - `occurrence_day`
  - `occurrence_day_of_week`
  - `occurrence_day_of_week_num`
  - `occurrence_hour`
  - `is_weekend`
  - `season`
- Parse neighbourhood labels into clean names and IDs.
- Add `valid_toronto_coordinate BOOLEAN`.
- Add `crime_point GEOGRAPHY`, null when coordinates are missing, `0,0`, or outside plausible Toronto bounds.
- Add `is_neighbourhood_known BOOLEAN`, false for `NSA`, null, or blank neighbourhood values.

### `staging.neighbourhood_profiles`

Purpose:

Create a clean profile table for 158 and 140 geography models.

Key transformations:

- Deduplicate by `(neighbourhood_model, profile_year, neighbourhood_id)`.
- Normalize neighbourhood IDs to left-padded strings where appropriate.
- Validate positive `population`.
- Validate positive `land_area_km2`.
- Add `population_density_per_km2`.

### `staging.neighbourhood_boundaries`

Purpose:

Create a geography table for 158 and 140 neighbourhood models.

Key transformations:

- Convert GeoJSON to BigQuery `GEOGRAPHY` with `ST_GEOGFROMGEOJSON`.
- Add `boundary_area_km2` with `ST_AREA(geometry) / 1000000`.
- Add `centroid` with `ST_CENTROID`.
- Deduplicate by `(neighbourhood_model, neighbourhood_id)`.

### `staging.crime_neighbourhood_yearly`

Purpose:

Aggregate crime by year, neighbourhood model, neighbourhood, category, and premises type.

Required metrics:

- `crime_count`
- `distinct_event_count`
- `crime_count_per_1000_people`
- `crime_count_per_km2_profile`
- `crime_count_per_km2_boundary`
- `population`
- `land_area_km2`
- `boundary_area_km2`

Rules:

- Use 158 geography by default.
- Exclude `NSA` and unknown neighbourhoods from neighbourhood rankings.
- Keep a citywide aggregate separately for KPI context.
- Use profile land area for parity with City profile metrics, and boundary area as a QA/comparison field.

### `staging.crime_temporal_patterns`

Purpose:

Aggregate crime by year, category, day of week, hour, and weekend flag.

Required metrics:

- `crime_count`
- `share_of_year_category`
- `hour_rank_within_category`

## Report Assets

Report assets are the stable interface consumed by DAC. They should be narrow, chart-ready, and avoid repeating dashboard-specific SQL in many widgets.

### `report.dashboard_kpis`

Required columns:

- `metric_name`
- `metric_value`
- `metric_label`
- `latest_complete_year`
- `comparison_year`
- `year_over_year_change_pct`
- `source_updated_at`

Required metrics:

- Total CSI rows in selected period
- Latest complete year count
- Year-over-year change
- Citywide crimes per 1,000 people

### `report.category_trends`

Required grain:

One row per `occurrence_year`.

Required columns:

- `occurrence_year`
- `assault`
- `auto_theft`
- `break_and_enter`
- `robbery`
- `theft_over_5k`
- `total_crimes`

### `report.neighbourhood_rankings`

Required grain:

One row per neighbourhood and latest complete year.

Required columns:

- `rank_by_rate`
- `rank_by_count`
- `neighbourhood_id`
- `neighbourhood_name`
- `crime_count`
- `crime_count_per_1000_people`
- `crime_count_per_km2`
- `population`
- `land_area_km2`
- `latest_complete_year`

### `report.neighbourhood_category_mix`

Required grain:

One row per top neighbourhood, category, and latest complete year.

Required columns:

- `neighbourhood_name`
- `csi_category`
- `crime_count`
- `category_share`
- `latest_complete_year`

### `report.temporal_heatmap`

Required grain:

One row per day of week and hour for the latest complete year.

Required columns:

- `day_of_week`
- `day_of_week_num`
- `hour`
- `crime_count`
- `share_of_week`
- `latest_complete_year`

### `report.spatial_summary`

Purpose:

Support an optional "near a coordinate" workflow using BigQuery geography functions.

Required parameters if implemented in dashboard query SQL:

- `latitude`
- `longitude`
- `radius_km`

Recommended default:

- Center: Toronto City Hall or another documented Toronto point
- Radius: `0.833 km`, representing a 10-minute walk at `5 km/h`

Rules:

- Use `ST_DISTANCE(crime_point, ST_GEOGPOINT(longitude, latitude)) <= radius_km * 1000`.
- Never imply exact address-level precision.
- Include a clear privacy caveat in footnotes.

## Dashboard Requirements

Dashboard file:

```text
toronto-crime/dashboard-dac/dashboards/toronto-crime.yml
```

Required top-level settings:

```yaml
name: Toronto Crime Dashboard
description: Community Safety Indicator trends, neighbourhood rates, and time patterns for Toronto.
connection: bruin-playground-arsalan
theme: ibm-cb-dark
refresh:
  interval: "5m"
```

Recommended filters:

- `date_range`, type `date-range`, default `this_year` or `year_to_date` only if data is current enough; otherwise use `all_time`.
- `neighbourhood_model`, type `select`, default `158`.
- `category`, type `select`, default all categories if DAC supports the required semantics cleanly.

If filters create fragile SQL or unclear claims, start with a static latest-complete-year dashboard and add filters later.

## Dashboard Story

The dashboard should answer:

1. How has Toronto's reported CSI crime mix changed since 2014?
2. Which neighbourhoods have the highest reported crime rates after adjusting for population?
3. Are high-count neighbourhoods also high-rate neighbourhoods?
4. Which categories dominate in the highest-rate neighbourhoods?
5. When during the week and day are reported CSI crimes most concentrated?

## Required Dashboard Sections

### Overview KPIs

Use a KPI row with four metric widgets:

- Total CSI rows
- Latest complete year CSI rows
- Year-over-year change
- Citywide CSI rows per 1,000 residents

Each KPI must include units in the label or suffix.

### Annual CSI Counts by Category

Chart type:

- `line` using the local DAC fork fields when available

Required structure:

- Header text widget
- Chart widget with `hideName: true`
- Footnote text widget

Required title:

```text
# Annual Community Safety Indicator rows by category, Toronto, 2014-present
```

Required description:

- State the strongest observed trend with magnitude after data is available.
- Include latest complete year and comparison baseline.

Required encoding key:

- Lines encode CSI categories.
- Y-axis is offence/victim-level rows.

Required chart fields:

- `x: occurrence_year`
- `y: [assault, auto_theft, break_and_enter, robbery, theft_over_5k]`
- `yLabel: CSI rows`
- `seriesNames` mapping snake_case fields to display labels

### Highest-Rate Neighbourhoods

Chart type:

- `bar`

Required structure:

- Header text widget
- Chart widget with `hideName: true`
- Footnote text widget
- Table widget with the same ranked data

Required title:

```text
# Highest reported CSI rates by neighbourhood, latest complete year
```

Required description:

- State the top neighbourhood, its rate per 1,000 residents, and how many times higher it is than the citywide rate.

Required encoding key:

- Bars show CSI rows per 1,000 residents.
- Tooltip/table must expose raw count, population, and land area.

Rules:

- Sort descending by rate.
- Limit to top 15 or fewer to keep labels readable.
- Exclude unknown/NSA neighbourhoods.
- Do not use a truncated zero baseline.

### Count vs Rate Ranking Table

Widget type:

- `table`

Purpose:

Show that high raw counts and high population-adjusted rates are different rankings.

Required columns:

- Rank by rate
- Rank by count
- Neighbourhood
- CSI rows
- CSI rows per 1,000 residents
- CSI rows per km2
- Population
- Land area km2

### Category Mix in Top Neighbourhoods

Chart type:

- `combo` or stacked `bar`

Purpose:

Compare the category composition of the highest-rate neighbourhoods.

Rules:

- Use only the top 10 neighbourhoods by rate.
- Include a visible legend or an exhaustive encoding key.
- Use Wong colorblind-safe palette via the DAC theme.
- Do not use pie charts.

### Day-of-Week and Hour Pattern

Chart type:

- `heatmap` if DAC renders it legibly; otherwise use a table plus grouped bar chart.

Purpose:

Identify the strongest time blocks for reported CSI rows.

Required title:

```text
# Reported CSI rows by day of week and hour, latest complete year
```

Required description:

- State the highest-volume day/hour cell and its share of the week's rows.

Required encoding key:

- X-axis is hour of day.
- Y-axis is day of week.
- Color intensity encodes CSI row count.

### Methodology

The dashboard must end with a full-width text widget titled:

```text
# Methodology
```

Include:

- Source links for Toronto Police CSI, City neighbourhood profiles, and City neighbourhood boundaries.
- Date range.
- Latest complete year definition.
- Neighbourhood model used by default.
- Rate formulas:
  - `crime_count_per_1000_people = crime_count / population * 1000`
  - `crime_count_per_km2 = crime_count / land_area_km2`
- Explanation that rows are offence/victim-level, not unique incidents.
- Explanation that locations are privacy-offset and approximate.
- Explanation that unknown/NSA neighbourhoods are excluded from neighbourhood rankings but retained in citywide counts.
- Tooling: Bruin cli, BigQuery, Bruin dac.

## Visualization Standards

Every analytical chart must follow this exact DAC row pattern:

```text
header text widget -> chart widget -> footnote text widget
```

Each widget should use `col: 12`.

Every chart header must include:

- A clear Markdown `#` title stating entity, metric, units, and time range.
- A bold 1-3 sentence description with a quantified insight.
- An encoding key explaining color, position, line, bar, or heatmap intensity.

Every chart footnote must include:

- `Sources:`
- `Tools:`
- `Limitations:`

Color and accessibility requirements:

- Use the Wong colorblind-safe palette exposed through `theme: ibm-cb-dark`.
- Do not rely on color alone; add legends, encoding keys, line style, ordering, or direct labels.
- Multi-series charts must have a native legend or an explicit encoding key.
- Use `seriesNames` for line charts when supported by the local DAC fork.
- Keep all SQL output columns in `snake_case`.

Truthfulness requirements:

- Bar charts must start at zero.
- Do not use pie charts.
- Do not use 3D charts.
- Do not use dual y-axes unless there is a clear time-alignment need and the local line-chart fork fields are used with explicit labels.
- Do not make exact-address claims from privacy-offset crime coordinates.

## Data Quality Requirements

Add Bruin column checks and custom checks where supported.

Minimum checks:

- `raw.toronto_csi_events.objectid` is not null.
- `staging.crime_events.objectid` is unique.
- `staging.crime_events.csi_category` is one of:
  - `Assault`
  - `Auto Theft`
  - `Break and Enter`
  - `Robbery`
  - `Theft Over $5k`
- `staging.crime_events.occurrence_year >= 2014`.
- `staging.crime_events.occurrence_hour BETWEEN 0 AND 23`.
- `staging.neighbourhood_profiles.population > 0`.
- `staging.neighbourhood_profiles.land_area_km2 > 0`.
- `staging.neighbourhood_boundaries.boundary_area_km2 > 0`.
- Neighbourhood yearly rollup row counts reconcile to staging crime rows after documented exclusions.

Recommended custom checks:

- Count of invalid coordinates is tracked and does not unexpectedly spike.
- Count of `NSA` neighbourhood rows is tracked and does not unexpectedly spike.
- Latest occurrence date is within an expected freshness window after the source is known to publish current data.
- Category totals in `report.category_trends` equal total rows by year in `staging.crime_events`.

## Development Workflow

Initial setup:

```bash
bruin validate toronto-crime/
```

Test raw CSI ingestion with a small date range:

```bash
bruin run --start-date 2014-01-01 --end-date 2014-01-03 toronto-crime/assets/raw/toronto_csi_events.py
```

Run reference assets:

```bash
bruin run toronto-crime/assets/raw/toronto_neighbourhood_profiles.py
bruin run toronto-crime/assets/raw/toronto_neighbourhood_boundaries.py
```

Run staging after raw tables exist:

```bash
bruin run toronto-crime/assets/staging/crime_events.sql
bruin run toronto-crime/assets/staging/neighbourhood_profiles.sql
bruin run toronto-crime/assets/staging/neighbourhood_boundaries.sql
bruin run toronto-crime/assets/staging/crime_neighbourhood_yearly.sql
bruin run toronto-crime/assets/staging/crime_temporal_patterns.sql
```

Run report assets:

```bash
bruin run toronto-crime/assets/report/
```

Validate DAC:

```bash
dac validate --dir toronto-crime/dashboard-dac
```

Execute all dashboard queries:

```bash
dac check --dir toronto-crime/dashboard-dac
```

Serve dashboard:

```bash
dac serve --dir toronto-crime/dashboard-dac --port 8321
```

Local URL:

```text
http://localhost:8321
```

## Backfill Strategy

For initial load, backfill in yearly or quarterly windows to avoid ArcGIS timeouts:

```bash
bruin run --start-date 2014-01-01 --end-date 2014-12-31 toronto-crime/assets/raw/toronto_csi_events.py
bruin run --start-date 2015-01-01 --end-date 2015-12-31 toronto-crime/assets/raw/toronto_csi_events.py
```

After raw backfill, rebuild staging and report assets:

```bash
bruin run toronto-crime/ --selector 'path:assets/staging'
bruin run toronto-crime/ --selector 'path:assets/report'
```

For daily scheduled runs, use the pipeline schedule and a narrow date interval. Because raw CSI uses append strategy, staging must always deduplicate by latest `extracted_at`.

## README Requirements

Create `toronto-crime/README.md` during implementation.

It must include:

- Project purpose.
- Source datasets with links and licenses.
- Pipeline asset list by layer.
- Required dependencies.
- Exact run commands.
- Exact DAC commands.
- Local dashboard URL: `http://localhost:8321`.
- Known limitations and privacy caveats.
- Explanation of 158 vs 140 neighbourhood models.
- Explanation of row-level counting methodology.

## Acceptance Criteria

The work is complete when:

- `toronto-crime/pipeline.yml` exists and validates.
- All raw assets run successfully for a small date range or reference load.
- Staging assets deduplicate raw data correctly.
- Report assets expose stable dashboard-ready tables.
- `bruin validate toronto-crime/` passes.
- `dac validate --dir toronto-crime/dashboard-dac` passes.
- `dac check --dir toronto-crime/dashboard-dac` passes.
- `dac serve --dir toronto-crime/dashboard-dac --port 8321` starts successfully.
- The dashboard is available at `http://localhost:8321`.
- Every chart follows the required title, description, encoding key, chart, and footnote structure.
- Every chart uses accessible color/encoding and truthful axes.
- The dashboard ends with a methodology section.
- Source caveats about offence/victim-level rows and privacy-offset locations are visible in the dashboard.

## Open Questions

- Whether the first production dashboard should default to all years or latest complete year.
- Whether dashboard filters should be implemented in the first version or deferred until static chart queries are stable.
- Whether a map should be delivered as a phase-two DAC fork/custom widget or as a static generated artifact. Current DAC schema does not provide a first-class choropleth map widget.
- Whether rates should use profile land area or boundary-derived area as the primary denominator. Initial recommendation: use profile land area for published profile parity and expose boundary-derived area for QA.
