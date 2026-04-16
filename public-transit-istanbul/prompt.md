Build a new pipeline for **Istanbul Public Transit — Ridership Patterns & Metro Expansion Impact**.

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

All data comes from the Istanbul Metropolitan Municipality (IBB) Open Data Portal: https://data.ibb.gov.tr/en/
No API keys or authentication required for CSV downloads. All data is under the Istanbul Metropolitan Municipality Open Data License (permissive for analysis).

**1. Hourly Public Transport Data (Istanbulkart tap data)**
- URL: https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set
- Publisher: BELBIM Inc. (Istanbulkart operator)
- Format: 60 monthly CSV files, ~1–1.8 GB each (~60 GB total uncompressed)
- Time range: January 2020 – December 2024
- Fields: `transition_date`, `transition_hour`, `transport_type_id`, `road_type`, `line`, `transfer_type`, `number_of_passage`, `number_of_passenger`, `product_kind`, `transaction_type_desc`, `town`, `line_name`, `station_poi_desc_cd`
- Coverage: all modes — IETT bus, metro, metrobus, Marmaray, IDO ferries, maritime services
- Includes: transfer vs direct trips, fare type (full/discounted), district, line name, station
- This is the core dataset. Due to size (~60 GB), process month-by-month in the Python asset and load incrementally.

**2. Rail Station-Level Ridership (with coordinates)**
- URL: https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
- Format: CSV (annual files, 11–101 MB per year)
- Time range: 2021–2025
- Fields: `passage_cnt`, `passanger_cnt`, `transaction_year`, `transaction_month`, `transaction_day`, `line`, `station_name`, `station_number`, `town`, `longitude`, `latitude`
- Daily station-level ridership with geographic coordinates — enables spatial analysis and mapping.

**3. Rail Ridership by Age Group**
- URL: https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
- Format: CSV (2022–2025), XLSX (2021)
- Same structure as rail station data but segmented by age group (based on Istanbulkart holder registration data).

**4. Ferry Pier Passengers**
- URL: https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari
- Format: CSV (~72 KB/year)
- Time range: 2021–2025
- Fields: `yil` (year), `ay` (month), `otorite_adi` (operator), `istasyon_adi` (pier name), `tekil_yolcu_sayisi` (unique passengers), `toplam_yolculuk_sayisi` (total journeys)
- Monthly pier-level ferry ridership with operator attribution.

**5. Rail System GeoJSON Layers**
- Rail station points: https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi (GeoJSON, June 2025)
- Rail lines: https://data.ibb.gov.tr/en/dataset/rayli-ulasim-hatlari-vektor-verisi (GeoJSON, June 2025)
- Use these for the station map visualization. Do NOT use the GTFS feeds — they are frozen and outdated.

**6. Istanbul Traffic Index**
- URL: https://data.ibb.gov.tr/en/dataset (search "traffic index")
- Format: CSV, 2015–present, daily
- Fields: `trafficindexdate`, `minimum_traffic_index`, `maximum_traffic_index`, `average_traffic_index`
- Use as a contextual overlay — when transit ridership rises, does road congestion change?

### What to extract

**From Hourly Public Transport Data (source 1):**
- Aggregate to daily level by mode (`transport_type_id`), line, district (`town`), and transfer type. Keep hourly granularity in raw, aggregate in staging.
- Due to the ~60 GB total size, download and process one month at a time. Use `append` strategy with deduplication in staging.
- Extract month files by iterating the dataset resource URLs on the IBB portal page.

**From Rail Station Ridership (source 2):**
- Daily ridership per station with coordinates. Load all years (2021–2025). Use `create+replace` per year file (small enough to reload).
- This is the primary dataset for the metro expansion impact analysis — compare ridership at stations before/after new lines open.

**From Rail Ridership by Age Group (source 3):**
- Daily ridership per station per age group. Same structure as source 2 with age dimension added.

**From Ferry Pier Passengers (source 4):**
- Monthly ridership per pier. Small dataset, load all years with `create+replace`.

**From GeoJSON layers (source 5):**
- Parse GeoJSON into a stations table with: station name, line, lat, lon, geometry. Load once with `create+replace`.
- Parse rail lines GeoJSON for map overlay.

**From Traffic Index (source 6):**
- Daily traffic congestion index, 2015–present. Load with `create+replace`.

### Naming

All asset/table names should have prefix `**istanbul_`**
Destination: BigQuery

### Dashboard questions

The goal is a final dashboard that answers:

1. **How has Istanbul's transit ridership changed from 2020 to 2024?** Monthly ridership by mode (metro, bus, metrobus, Marmaray, ferry). Show the COVID crash and recovery trajectory. Which mode recovered fastest? Which grew beyond pre-COVID levels?
2. **What does Istanbul's daily transit rhythm look like?** Heatmap of ridership by hour-of-day and day-of-week. Do weekday/weekend patterns differ by mode? When are the peak hours? How do Ramadan, national holidays, and summer affect patterns?
3. **How did new metro line openings redistribute ridership?** Map of rail stations colored by ridership growth rate (2022 vs 2024). Identify stations near new line openings that saw surges. Did parallel bus corridors see declines? Focus on specific line openings during the 2021–2025 period.
4. **Which districts are underserved?** Map of ridership per capita by district (`town`), overlaid with rail network. Identify high-population districts with low transit access. Cross-reference with the Asian vs European side divide.

### Build order

Start building the pipeline:

1. Create pipeline structure (`pipeline.yml`, `README.md`) with schedule `weekly` and start_date `"2020-01-01"`
2. Create raw Python assets:
   - `istanbul_hourly_transport.py` — download monthly CSVs from IBB portal, one month at a time. Use `append` strategy. Parse CSV (check encoding — may need `utf-8` or `latin-1`). Process each month's ~1 GB file in chunks with pandas `chunksize` parameter to avoid memory issues. Deduplicate in staging.
   - `istanbul_rail_stations.py` — download annual rail station ridership CSVs (2021–2025). Use `create+replace`. Include lat/lon columns.
   - `istanbul_rail_age_group.py` — download rail ridership by age group files. Use `create+replace`. Handle mixed CSV/XLSX formats (2021 is XLSX, rest are CSV).
   - `istanbul_ferry_piers.py` — download ferry pier passenger CSVs (2021–2025). Use `create+replace`.
   - `istanbul_geo_stations.py` — download and parse rail station points GeoJSON + rail lines GeoJSON. Extract to flat table with lat/lon. Use `create+replace`.
   - `istanbul_traffic_index.py` — download traffic index CSV. Use `create+replace`.
3. Test each raw asset individually — start with the smallest datasets first (ferry piers, traffic index, geo stations). For the hourly transport data, test with a single month (e.g., January 2024) before attempting the full 60-month backfill.
4. Build staging SQL assets:
   - `istanbul_daily_ridership.sql` — aggregate hourly data to daily by mode, line, district. Deduplicate overlapping months from append strategy. Add day-of-week, is_weekend, month name.
   - `istanbul_hourly_patterns.sql` — aggregate to hour-of-day x day-of-week matrix by mode. Average across the full time range for the rhythm heatmap.
   - `istanbul_station_growth.sql` — join rail station ridership across years. Calculate year-over-year growth rate per station. Join with GeoJSON station coordinates for mapping.
   - `istanbul_district_summary.sql` — aggregate ridership by district and mode. Calculate per-district totals and mode splits.
   - `istanbul_transit_traffic.sql` — join daily ridership totals with traffic index. Enables correlation analysis between transit usage and road congestion.
5. Test the entire pipeline end-to-end for a 3-month window (e.g., January–March 2024) before running the full backfill.

### Constraints

- The hourly transport CSVs are ~1–1.8 GB each. **Do not load entire files into memory at once.** Use pandas `read_csv(..., chunksize=100000)` and process in chunks. Alternatively, use `pyarrow` for faster CSV parsing.
- IBB portal CSV encoding may be `utf-8` or `latin-1` — test both. Turkish characters (ş, ı, ö, ü, ç, ğ) must render correctly. If they don't, try `encoding='utf-8-sig'` or `encoding='iso-8859-9'` (Turkish Latin-5).
- The `transport_type_id` field is numeric. You'll need to determine the mapping to mode names (metro, bus, metrobus, Marmaray, ferry) from the data itself — check the `transaction_type_desc` or `road_type` fields for labels, or look for a reference table on the IBB portal.
- Field names may be in Turkish or English depending on the dataset. Standardize all column names to English in the raw asset.
- The GeoJSON files use WGS84 (EPSG:4326) — no projection conversion needed.
- For the 60-month backfill of hourly data, implement a download loop that: (a) checks which months are already loaded in BigQuery, (b) only downloads missing months, (c) has retry logic for failed downloads, (d) logs progress. This avoids re-downloading 60 GB on every run.
- Use `append` strategy for hourly transport (incremental monthly loads). Use `create+replace` for everything else (small enough to reload fully).
- Do NOT use the IETT GTFS feeds — they are frozen, use semicolons as delimiters, and have Turkish character encoding corruption (UTF-8 mojibake). The ridership CSVs are the authoritative source.
- For the station map in the dashboard, use Pydeck with the dark-matter basemap (`https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json`). Color stations by ridership growth rate using a diverging color scale.
