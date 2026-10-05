# The Disaster Summer Atlas 2019-2026

Where did extreme heat, wildfire and flood exposure overlap with vulnerable populations? This pipeline scores the 471 urban centres with at least 1 million residents against three hazards for June 1 - September 20 of every summer 2019-2026, and weights the result by each city's subnational Human Development Index. 2019 is the first summer for which GDACS publishes flood polygons and impact reports.

## Data sources

- [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/api/area/) - MODIS Collection 6.1 active-fire detections (Terra + Aqua). Standard Processing (`MODIS_SP`) where available, Near Real-Time (`MODIS_NRT`) after. Needs a free MAP_KEY, stored in `.bruin.yml` as the generic connection `nasa-firms-map-key`.
- [GDACS](https://www.gdacs.org/) - flood events, affected-area polygons and Sendai-framework impact reports (UN OCHA + European Commission JRC). Public API, no key.
- [ERA5 reanalysis](https://cds.climate.copernicus.eu/) (ECMWF / Copernicus) via the [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) - daily Tmax, apparent Tmax and precipitation per city. CC BY 4.0.
- [GHSL Urban Centre Database R2024A](https://ghsl.jrc.ec.europa.eu/) - city points, 2015 population and 2020 subnational HDI, read from `raw.ghsl_urban_centers` (ingested by `city-pulse/`).
- [Overture Maps](https://overturemaps.org/) country areas (`bigquery-public-data.overture_maps.division_area`) - simplified coastline for the map.

NOAA station data was evaluated first and rejected: NOAA GSOD 2026 is not published (NCEI and AWS), and GHCN-Daily 2026 in BigQuery public data (through 2026-09-12) has a qualifying TMAX station within 50 km of only 16% of the cities.

## Assets

Raw
- `raw.atlas_cities` (SQL) - the 471 GHSL urban centres >= 1M people, with HDI cleaning (values > 0.20 below the country median are replaced by it: Dammam, Xiamen).
- `raw.firms_modis_fires` (Python, append) - one row per MODIS detection, fetched day by day for the whole world.
- `raw.gdacs_flood_events` (Python, append) - one row per GDACS flood event with affected-area GeoJSON and impact-report JSON.
- `raw.era5_city_daily` (Python, append) - daily ERA5 weather per city (batched Open-Meteo requests).

Staging
- `staging.fire_detections` - deduplicated detections with low-confidence, FIRMS static-source and persistent-cell (>= 10 days in a ~2 km cell) masks; `is_vegetation_fire` is the fire definition.
- `staging.flood_events` - deduplicated events, parsed `affected_area` GEOGRAPHY, deaths and displaced (max per region, summed).
- `staging.city_weather_daily` - deduplicated ERA5 days with TXge35, apparent >= 40 C and R50mm flags.
- `staging.city_exposure` - one row per city: hazard metrics, exposure flags, HDI tier, exposure index and risk score.

Report
- `report.atlas_basemap` - coastline vertices (union, 30 km simplification, antimeridian split).
- `report.atlas_map_layers` - long table for the Vega-Lite map (coast, fire cells, flood centroids, heat cities, labels).
- `report.atlas_weekly_exposure` - weekly population exposed per hazard.
- `report.atlas_vulnerability_exposure` - share of population with 0 / 1 / 2+ hazards by summer and HDI tier.
- `report.atlas_yearly_trend` - one row per summer: population exposed per hazard and 2+ hazard share by HDI tier.

Dashboard
- `dashboard-dac/dashboards/disaster-summer-atlas.yml` - Bruin DAC dashboard: 2019-2026 trend charts, then a `season_year` filter (default 2026) driving KPIs, event map, exposure by HDI tier, weekly timeline and city risk ranking; methodology.
- `dashboard-dac/themes/wong-cb-dark.yml` - Wong (2011) palette on bruin-dark.

## Definitions

- Window: June 1 - September 20 of each year 2019-2026 (112 days, 16 ISO weeks). Grain of `staging.city_exposure`: city x season_year.
- Heat-exposed: >= 28 days (1 in 4) with ERA5 Tmax >= 35 C at the city grid cell.
- Fire-exposed: vegetation-fire detections within 50 km of the GHSL centroid on >= 28 days.
- Flood-exposed: centroid inside a GDACS flood affected-area polygon that overlaps the window.
- Risk score = 100 x mean(min(hot_days/56,1), min(fire_days/56,1), flood alert rank/3) x (1 - HDI).

## Run commands

```bash
bruin validate disaster-summer-atlas/

# raw: assets fetch only the June 1 - September 20 season of each year in the interval
# (SEASON_START / SEASON_END env vars) and skip days / city-seasons already loaded
bruin run disaster-summer-atlas/assets/raw/atlas_cities.sql
bruin run --start-date 2019-06-01 --end-date 2026-09-20 disaster-summer-atlas/assets/raw/gdacs_flood_events.py
# FIRMS and ERA5: run one year at a time so each summer commits on its own
for y in 2019 2020 2021 2022 2023 2024 2025 2026; do
  bruin run --start-date $y-06-01 --end-date $y-09-20 disaster-summer-atlas/assets/raw/firms_modis_fires.py
  bruin run --start-date $y-06-01 --end-date $y-09-20 disaster-summer-atlas/assets/raw/era5_city_daily.py
done

# staging + report
bruin run disaster-summer-atlas/assets/report/atlas_basemap.sql
bruin run --downstream disaster-summer-atlas/assets/staging/fire_detections.sql
bruin run --downstream disaster-summer-atlas/assets/staging/flood_events.sql
bruin run --downstream disaster-summer-atlas/assets/staging/city_weather_daily.sql
bruin run disaster-summer-atlas/assets/report/atlas_weekly_exposure.sql

# dashboard
dac validate --dir disaster-summer-atlas/dashboard-dac
dac check --dir disaster-summer-atlas/dashboard-dac
dac serve --dir disaster-summer-atlas/dashboard-dac --port 8321 \
  --template disaster-summer-atlas/dashboard-dac/themes/wong-cb-dark.yml
```

Dashboard: http://localhost:8321

Test-scope env vars: `CITY_LIMIT` (ERA5 cities), `ERA5_BATCH_SIZE`, `ERA5_SLEEP_S`. FIRMS uses ~38 transactions per world-day; the asset waits when the MAP_KEY nears 4,500 of its 5,000-per-10-minute limit (one summer takes ~10 min). ERA5 sleeps through Open-Meteo's minutely / hourly limits and stops on the daily limit; re-run to resume. Empty runs return typed zero-row frames - an untyped empty DataFrame makes the loader rewrite the raw table schema as STRING.

## Known limitations

- Heat uses a fixed 35 C threshold (exposure, not anomaly); hot-climate cities qualify every summer, and Southern Hemisphere cities are in winter.
- ERA5 (~25 km) smooths urban heat-island peaks.
- MODIS NRT has no fire-type field; static sources are masked with June SP types plus the persistence rule. Agricultural burning still counts as fire. The June (SP) to July (NRT) switch is a processing change.
- GDACS affected areas can be basin-scale (up to 5.3M km2 in 2020); a single alert can cover many cities.
- GDACS polygons became much smaller from 2025 (90th-percentile area 22-31k km2 vs 120-490k km2 in 2019-2024), so flood and overlap exposure from 2025 is not directly comparable with earlier summers. GDACS also lists more events over time (98 in summer 2019, 274 in 2026), and some 2019-2021 events have no polygon.
- MODIS Terra has been drifting from its original orbit since about 2020; detection counts across years may shift for that reason.
- Population (2015) and HDI (2020) are fixed across all summers.
- GDACS impact reports are cumulative updates; deaths/displaced use max-per-region, which can undercount distinct incidents.
- Population is GHS-POP 2015; HDI is a regional proxy for vulnerability. 9 cities have no HDI and are not ranked.
- The Low-HDI tier is small (31 cities); its 2+ hazard share swings from 5% to 38% depending on whether GDACS flood alerts cover Pakistan's Punjab / Khyber Pakhtunkhwa cities (HDI 0.549, just under the 0.550 tier cut).
- The map is Vega-Lite (DAC has no MapLibre widget); the basemap is drawn from BigQuery because DAC blocks Vega-Lite `data.url`.
