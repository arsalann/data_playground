# Istanbul Public Transit — Ridership Patterns & Metro Expansion Impact

Analysis of Istanbul's public transit ridership across metro, bus, ferry, and Marmaray systems using Istanbulkart tap data from the IBB Open Data Portal (2020-2025).

## Data Sources

| Source | Format | Size | Period |
|--------|--------|------|--------|
| [Hourly Public Transport Data](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set) | 60 monthly CSVs | ~60 GB total | Jan 2020 - Dec 2024 |
| [Rail Station Ridership](https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) | Annual CSVs | ~11-101 MB/yr | 2021-2025 |
| [Rail Ridership by Age Group](https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) | CSV/XLSX | ~11-101 MB/yr | 2021-2025 |
| [Ferry Pier Passengers](https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari) | Annual CSVs | ~72 KB/yr | 2021-2025 |
| [Rail Station GeoJSON](https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi) | GeoJSON | ~1 MB | June 2025 |
| [Istanbul Traffic Index](https://data.ibb.gov.tr/en/dataset/istanbul-trafik-indeksi) | CSV | ~200 KB | 2015-2024 |

All data under the Istanbul Metropolitan Municipality Open Data License.

## Assets

### Raw (Python)

| Asset | Table | Strategy | Description |
|-------|-------|----------|-------------|
| `istanbul_hourly_transport.py` | `raw.istanbul_hourly_transport` | append | Hourly Istanbulkart tap data, processed monthly via CKAN API |
| `istanbul_rail_stations.py` | `raw.istanbul_rail_stations` | create+replace | Daily station ridership with coordinates |
| `istanbul_rail_age_group.py` | `raw.istanbul_rail_age_group` | create+replace | Station ridership by age group |
| `istanbul_ferry_piers.py` | `raw.istanbul_ferry_piers` | create+replace | Monthly ferry pier passenger counts |
| `istanbul_geo_stations.py` | `raw.istanbul_geo_stations` | create+replace | Station points from GeoJSON |
| `istanbul_traffic_index.py` | `raw.istanbul_traffic_index` | create+replace | Daily traffic congestion index |

### Staging (SQL)

| Asset | Table | Depends On | Description |
|-------|-------|------------|-------------|
| `istanbul_daily_ridership.sql` | `staging.istanbul_daily_ridership` | hourly_transport | Daily aggregation by mode/line/district |
| `istanbul_hourly_patterns.sql` | `staging.istanbul_hourly_patterns` | hourly_transport | Hour x day-of-week ridership matrix |
| `istanbul_station_growth.sql` | `staging.istanbul_station_growth` | rail_stations, geo_stations | Station YoY growth rates with coordinates |
| `istanbul_district_summary.sql` | `staging.istanbul_district_summary` | hourly_transport | District-level mode splits |
| `istanbul_transit_traffic.sql` | `staging.istanbul_transit_traffic` | hourly_transport, traffic_index | Transit-traffic correlation |

### Reports (Streamlit)

Dashboard with 5 charts and 2 interactive maps analyzing rail ridership growth, top lines, station expansion impact, rider demographics, and ferry trends.

## Run Commands

```bash
# Validate the pipeline
bruin validate public-transit-istanbul/

# Run individual raw assets (small datasets first)
bruin run public-transit-istanbul/assets/raw/istanbul_ferry_piers.py
bruin run public-transit-istanbul/assets/raw/istanbul_traffic_index.py
bruin run public-transit-istanbul/assets/raw/istanbul_geo_stations.py
bruin run public-transit-istanbul/assets/raw/istanbul_rail_stations.py
bruin run public-transit-istanbul/assets/raw/istanbul_rail_age_group.py

# Run hourly transport with date range (large dataset)
ISTANBUL_MONTH_LIMIT=3 bruin run --start-date 2024-01-01 --end-date 2024-03-31 \
  public-transit-istanbul/assets/raw/istanbul_hourly_transport.py

# Run staging assets
bruin run public-transit-istanbul/assets/staging/istanbul_daily_ridership.sql
bruin run public-transit-istanbul/assets/staging/istanbul_station_growth.sql

# Run the dashboard
python3 -m streamlit run public-transit-istanbul/assets/reports/streamlit_app.py
```

## Known Limitations

- 2022 rail station data shows anomalously high ridership (1.55B vs ~1.1B in 2023/2024) — likely a data collection methodology change
- 2021 age group data is monthly aggregated (17K rows vs 500K+ daily in other years)
- Some 2023/2025 CSV files use semicolons as delimiters and Turkish locale number formatting
- Station coordinates in 2023/2025 data used dots as thousands separators — corrected during ingestion
- Hourly transport CSVs are 1-1.8 GB each; full backfill takes several hours
- "Unknown" age group (23% of 2024 ridership) = unregistered Istanbulkart holders
