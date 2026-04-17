# Hong Kong Public Transport Network Pipeline

A Bruin pipeline that ingests, transforms, and aggregates Hong Kong public transport data for network analysis and dashboarding.

## Data Sources

### 1. Static GTFS Feed (data.gov.hk)
- **Source:** Hong Kong Transport Department via [data.gov.hk](https://data.gov.hk)
- **Format:** ZIP archive containing CSV files (routes.txt, stops.txt, trips.txt, stop_times.txt, calendar.txt)
- **Coverage:** KMB buses, CTB/NWFB Citybus, trams, ferries
- **Refresh:** Full refresh daily (WRITE_TRUNCATE)
- **Note:** `stop_times.txt` exceeds 100 MB — loaded via BigQuery streaming upload

### 2. MTR Open Data Portal (opendata.mtr.com.hk)
- **Source:** [MTR Open Data](https://opendata.mtr.com.hk)
- **Datasets:** mtr_lines_stations, mtr_bus_stops, mtr_fares, mtr_light_rail_stops
- **Requires:** `User-Agent` header and `utf-8-sig` encoding for CSV decoding

### 3. MTR Real-Time Schedule API (rt.data.gov.hk) — *not yet implemented*
- Real-time train arrival/departure predictions (JSON)
- Streaming layer to be added after batch pipeline is complete

## Known Limitations

- **MTR does not publish GTFS data.** Trip-level analysis (headway, crowding) is not possible for MTR heavy rail lines. Only station-level reference data and fare tables are available.
- **stop_times.txt is large** (>100 MB). Ingestion uses BigQuery load jobs for memory efficiency.
- **MTR CSVs require special handling:** `User-Agent` header to avoid connection drops, `utf-8-sig` decoding to avoid BOM character corruption.

## Assets

### Raw Layer (Python ingestion)
| Asset | Description |
|---|---|
| `raw.hk_transit_gtfs_static` | Downloads GTFS ZIP, loads stops/routes/trips/stop_times/calendar into BigQuery |
| `raw.hk_transit_mtr_csv` | Fetches 4 MTR CSV datasets into BigQuery |

### Staging Layer (SQL transformations)
| Asset | Description |
|---|---|
| `staging.hk_transit_stops` | Cleaned stop locations with type casting and null filtering |
| `staging.hk_transit_routes` | Cleaned routes with route type labels (bus/tram/ferry) |
| `staging.hk_transit_trips` | Cleaned trips joined with service calendar patterns |
| `staging.hk_transit_stop_times` | Cleaned stop times with parsed arrival/departure times |
| `staging.hk_transit_mtr_stations` | Unified MTR station reference (heavy rail + light rail) |

### Mart Layer (SQL aggregations)
| Asset | Description |
|---|---|
| `marts.hk_transit_mart_peak_hour_analysis` | Departures grouped by hour of day |
| `marts.hk_transit_mart_transfer_hubs` | Stops ranked by distinct routes served |
| `marts.hk_transit_mart_weekday_vs_weekend` | Service volume comparison by day type |
| `marts.hk_transit_mart_trip_trajectories` | Trip stop sequences with coordinates |
| `marts.hk_transit_mart_busiest_stops` | Top stops by departure count |
| `marts.hk_transit_mart_longest_routes` | Routes ranked by stop count |
| `marts.hk_transit_mart_first_last_service` | First/last departure per route |
| `marts.hk_transit_mart_route_summary` | Route-level aggregated metrics |
| `marts.hk_transit_mart_mtr_stations` | MTR station and line reference |
| `marts.hk_transit_mart_mtr_fares` | Station-to-station fare lookup |

## Run Commands

```bash
# Validate pipeline
bruin validate hong-kong-transit/

# Run raw ingestion (test individually first)
bruin run hong-kong-transit/assets/raw/hk_transit_gtfs_static.py
bruin run hong-kong-transit/assets/raw/hk_transit_mtr_csv.py

# Run staging transformations
bruin run hong-kong-transit/assets/staging/hk_transit_stops.sql
bruin run hong-kong-transit/assets/staging/hk_transit_routes.sql
bruin run hong-kong-transit/assets/staging/hk_transit_trips.sql
bruin run hong-kong-transit/assets/staging/hk_transit_stop_times.sql
bruin run hong-kong-transit/assets/staging/hk_transit_mtr_stations.sql

# Run mart aggregations
bruin run hong-kong-transit/assets/marts/

# Run entire pipeline (only after individual assets are tested)
bruin run hong-kong-transit/
```
