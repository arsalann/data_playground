# Toronto Crime Dashboard

Bruin pipeline and Bruin DAC dashboard for Toronto Community Safety Indicator crime patterns by year, category, neighbourhood, premises type, and occurrence time.

Local dashboard URL when served:

```text
http://localhost:8321
```

## Data Sources

- Toronto Police Service Community Safety Indicators: https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Major_Crime_Indicators_Open_Data/FeatureServer/0
- City of Toronto Neighbourhood Profiles: https://open.toronto.ca/dataset/neighbourhood-profiles/
- City of Toronto Neighbourhoods: https://open.toronto.ca/dataset/neighbourhoods/

The CSI source contains selected categories: Assault, Auto Theft, Break and Enter, Robbery, and Theft Over. It excludes sexual violations. Rows are offence/victim-level records, not guaranteed unique incidents. Coordinates are privacy-offset and approximate.

## Assets

Raw:

- `raw.toronto_csi_events`: paginated ArcGIS CSI ingestion with date bounds, retries, logging, and append materialization.
- `raw.toronto_neighbourhood_profiles`: CKAN-discovered 2021 158-model and 2016 140-model profile rows.
- `raw.toronto_neighbourhood_boundaries`: CKAN-discovered 158-model and 140-model GeoJSON boundaries.

Staging:

- `staging.crime_events`: deduplicated, typed CSI rows with temporal dimensions, normalized categories, neighbourhood keys, and approximate geography points.
- `staging.neighbourhood_profiles`: cleaned population and land-area denominators; 158 land area is filled from official boundary geometry.
- `staging.neighbourhood_boundaries`: BigQuery geography boundaries, areas, and centroids.
- `staging.crime_neighbourhood_yearly`: yearly neighbourhood/category/premises aggregates and rates.
- `staging.crime_temporal_patterns`: day-of-week and hour aggregates.

Report:

- `report.dashboard_kpis`
- `report.category_trends`
- `report.neighbourhood_rankings`
- `report.neighbourhood_category_mix`
- `report.temporal_heatmap`
- `report.spatial_summary`

DAC:

- `dashboard-dac/dashboards/toronto-crime.yml`
- `dashboard-dac/dashboards/queries/*.sql`

## Run Commands

Validate definitions:

```bash
bruin validate toronto-crime/
```

Test CSI ingestion with a small interval:

```bash
bruin run --start-date 2014-01-01 --end-date 2014-01-03 toronto-crime/assets/raw/toronto_csi_events.py
```

Run reference assets:

```bash
bruin run toronto-crime/assets/raw/toronto_neighbourhood_profiles.py
bruin run toronto-crime/assets/raw/toronto_neighbourhood_boundaries.py
```

Run transformations and reports after raw data exists:

```bash
bruin run toronto-crime/ --selector 'path:assets/staging'
bruin run toronto-crime/ --selector 'path:assets/report'
```

Backfill CSI in yearly chunks:

```bash
bruin run --start-date 2014-01-01 --end-date 2014-12-31 toronto-crime/assets/raw/toronto_csi_events.py
bruin run --start-date 2015-01-01 --end-date 2015-12-31 toronto-crime/assets/raw/toronto_csi_events.py
```

## Dashboard Commands

Validate the DAC project:

```bash
dac validate --dir toronto-crime/dashboard-dac
```

Execute every widget query:

```bash
dac check --dir toronto-crime/dashboard-dac
```

Serve locally:

```bash
dac serve --dir toronto-crime/dashboard-dac --port 8321
```

Open:

```text
http://localhost:8321
```

## Methodology Notes

- The 158-neighbourhood model is the default dashboard geography.
- The 140-neighbourhood model is retained for historical comparability and source auditability.
- Neighbourhood rate formula: `crime_count / population * 1000`.
- Area density formula: `crime_count / land_area_km2`.
- Unknown and `NSA` neighbourhood rows are retained in citywide counts but excluded from neighbourhood rankings.
- Current-model profile land area is filled from official boundary geometry because the 2021 profile workbook does not expose a land-area field in the profile sheet used here.
- Spatial summaries use BigQuery `ST_DISTANCE` against privacy-offset points and should not be interpreted as exact address-level crime counts.
