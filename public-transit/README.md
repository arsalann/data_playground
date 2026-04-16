# Public Transit — Global Urban Mobility Analysis

A data pipeline comparing public transportation systems across major world cities and countries, combining ridership trends, infrastructure metrics, modal split, and fare data from multiple open sources.

## Data Sources

### Tier 1: Primary Sources (Programmatic, rich, well-maintained)

#### 1. US National Transit Database (NTD) — Federal Transit Administration
- **URL**: https://www.transit.dot.gov/ntd/ntd-data
- **What it contains**: The most comprehensive open transit dataset in the world. Covers ~900 US transit agencies with:
  - Monthly and annual ridership (unlinked passenger trips)
  - Vehicle revenue miles/hours
  - Operating and capital expenses
  - Fare revenue
  - Fleet inventory (vehicle count, age, type)
  - Infrastructure (route miles, track miles, station count)
  - Service area population and area
- **Coverage**: All US transit agencies receiving federal funds
- **Time range**: Annual data from 1991, monthly from 2002
- **Access**: Bulk CSV/Excel downloads, free, public domain
- **Key files**:
  - Monthly module (adjusted): ridership by agency/mode/month
  - TS1.1: Total ridership time series
  - Annual database: full operational/financial data
- **Limitations**:
  - US only
  - Agency-level, not route-level
  - "Unlinked passenger trips" counts each boarding (a transfer = 2 UPTs)
  - ~1 year publication lag for annual data, ~3 months for monthly

#### 2. World Bank Open Data — Transport Indicators
- **URL**: https://api.worldbank.org/v2/
- **Relevant indicators**:
  - `IS.RRS.TOTL.KM` — Rail lines, total route-km
  - `IS.RRS.PASG.KM` — Railways, passengers carried (million passenger-km)
  - `IS.ROD.TOTL.KM` — Roads, total network km
  - `SP.URB.TOTL.IN.ZS` — Urban population (% of total)
  - `SP.POP.TOTL` — Total population
  - `NY.GDP.PCAP.CD` — GDP per capita (current US$)
- **Coverage**: 200+ countries, annual
- **Time range**: 1960s–2023 (varies by indicator)
- **Access**: REST API, no key required, CC BY 4.0
- **Limitations**: National-level only (no city breakdown), rail-focused (no bus/metro/tram detail), flaky API (needs chunked requests with retry)

#### 3. Eurostat — Transport Statistics
- **URL**: https://ec.europa.eu/eurostat/api/
- **Relevant tables**:
  - `tran_hv_psmod` — Modal split of passenger transport (% by car, bus, rail)
  - `tran_hv_pstra` — Passenger transport by rail (passenger-km)
  - `urb_ctran` — Urban audit transport data (PT network km, number of stops)
  - `tran_r_vehst` — Vehicle stock by type
- **Coverage**: EU 27 + EEA countries (~35 countries), some city-level via Urban Audit
- **Time range**: 1990–2022 (varies)
- **Access**: SDMX REST API, free, no key required
- **Limitations**: Country-level modal split (not city-level), metro/tram often lumped together, Urban Audit city data has significant gaps

#### 4. US Census ACS — Commuting Mode Share (Table B08301)
- **URL**: https://data.census.gov/ / Census API
- **What it contains**: "Means of Transportation to Work" — mode share for commuting (drove alone, carpool, public transit, walked, bicycle, work from home)
- **Coverage**: National, state, metro area, county, city, census tract
- **Time range**: 2005–present (1-year ACS), 2009–present (5-year)
- **Access**: Census API (free key) or CSV download
- **Limitations**: Commute trips only (not all trips), US only, 1-year lag

### Tier 2: City-Level APIs (Variable quality, good for enrichment)

#### 5. Transport for London (TfL) Unified API
- **URL**: https://api.tfl.gov.uk
- **What it contains**: Real-time arrivals, line status, timetables, annual station entry/exit counts
- **Coverage**: All London transport modes
- **Access**: Free, API key via portal registration
- **Historical ridership**: Station usage spreadsheets back to ~2007
- **Rate limits**: 500 req/min with key

#### 6. MTA (New York) Open Data
- **URL**: https://new.mta.info/developers + https://data.ny.gov
- **What it contains**: GTFS schedules, GTFS-RT real-time, weekly turnstile counts per station (4-hour blocks), monthly/annual ridership by station
- **Coverage**: NYC subway, bus, LIRR, Metro-North
- **Historical**: Turnstile data back to 2010+
- **Access**: Free, API key for real-time feeds

#### 7. CTA (Chicago) Ridership via City Data Portal
- **URL**: https://data.cityofchicago.org
- **Dataset**: CTA Ridership — L Station Entries — Daily Totals
- **What it contains**: Daily station-level subway entries
- **Time range**: 2001–present — one of the deepest historical series
- **Access**: Free Socrata API, no key needed
- **Format**: CSV/JSON

#### 8. Singapore LTA DataMall
- **URL**: https://datamall.lta.gov.sg
- **What it contains**: Bus arrivals, bus/train routes, monthly origin-destination passenger volumes (tap-in/tap-out), traffic analytics
- **Access**: Free, API key required
- **Notable**: OD matrix data is unusually rich for a public dataset

#### 9. Hong Kong MTR Open Data
- **URL**: https://data.gov.hk + https://opendata.mtr.com.hk
- **What it contains**: Monthly station entry/exit data, next train times, fares
- **Access**: Free, no key for most endpoints
- **Limitation**: No official GTFS

#### 10. Seoul Open Data Plaza
- **URL**: https://data.seoul.go.kr
- **What it contains**: Subway station entry/exit by hour (very granular), T-money card aggregate data
- **Access**: Free registration, interface in Korean
- **Rate limits**: 1,000 calls/day free tier
- **Notable**: Surprisingly granular hourly ridership, but language barrier

### Tier 3: Supplementary / Reference Sources

#### 11. GTFS Aggregators
- **Mobility Database**: https://mobilitydatabase.org — catalog of 1,700+ GTFS feed URLs across 70+ countries. Not data itself but pointers to agency feeds. GitHub catalog is fully open.
- **Transitland**: https://transit.land — ingests and indexes GTFS data. 2,500+ operators, 55+ countries. REST + GraphQL API. Free tier rate-limited.
- **Use case**: Measuring transit service supply (routes, stops, frequency) rather than demand (ridership)

#### 12. EPOMM TEMS — European Modal Split
- **URL**: http://www.epomm.eu/tems/
- **What it contains**: Modal split (walking, cycling, PT, car) for 400+ European cities
- **Limitations**: Self-reported, not standardized, snapshot years only (not time series)

#### 13. Numbeo — Transit Fare Prices
- **URL**: https://www.numbeo.com/cost-of-living/
- **What it contains**: "Monthly Pass" and "One-way Ticket" prices for 500+ cities
- **Limitations**: User-contributed, API requires paid subscription, restrictive license

#### 14. OpenStreetMap — Transit Infrastructure
- **URL**: https://overpass-turbo.eu/ or BigQuery `bigquery-public-data.geo_openstreetmap`
- **What it contains**: Bus stops, metro stations, tram stops, rail lines, route geometries (global)
- **Limitations**: Schedule/ridership not included, quality varies by region

#### 15. Wikidata — Metro System Facts
- **URL**: https://query.wikidata.org/sparql
- **What it contains**: System-level data: opening year, number of lines/stations, annual ridership, system length
- **Coverage**: Global metro/light rail/BRT systems
- **Access**: SPARQL endpoint, free, CC0

### Data Availability by City (5M+ population)

| City | Real-time | GTFS | Ridership | Historical | Rating |
|------|:---------:|:----:|:---------:|:----------:|:------:|
| New York | Yes | Yes | Station daily | 10+ years | **A+** |
| London | Yes | Yes | Station annual | 15+ years | **A+** |
| Chicago | Yes | Yes | Station daily | 20+ years | **A** |
| Singapore | Yes | Yes | OD monthly | Several years | **A** |
| Toronto | Yes | Yes | Annual + delays | Several years | **A-** |
| Los Angeles | Yes | Yes | Monthly by line | Several years | **B+** |
| Paris | Yes | Yes | Aggregate | Moderate | **B+** |
| Hong Kong | Yes | Unofficial | Monthly station | Several years | **B** |
| Seoul | Partial | Partial | Hourly station | Several years | **B** |
| Berlin | Community | Yes | Aggregate annual | Limited | **B** |
| Madrid | Bus only | Yes | Annual | Limited | **B** |
| Sao Paulo | Bus only | Bus only | Aggregate | Limited | **B-** |
| Mexico City | No | Partial | Annual reports | Moderate | **B-** |
| Istanbul | No | Yes | Aggregate | Limited | **C+** |
| Delhi | Partial | Yes | Reports only | Limited | **C+** |
| Tokyo | Partial | Partial | Limited | Limited | **C** |
| Jakarta | No | BRT only | Aggregate | Limited | **C** |
| Moscow | No | Unofficial | Annual only | Limited | **C-** |
| Bangkok | No | Unofficial | Annual reports | Minimal | **D+** |
| Mumbai | No | Intermittent | Reports only | Minimal | **D** |
| Shanghai/Beijing | No | Unofficial | None open | None | **D-** |
| Cairo | No | No | None | None | **F** |
| Lagos/Dhaka/Karachi | No | No | None | None | **F** |

### Sources Not Available as Open Data

- **UITP World Metro Figures** — behind paywall (membership organization)
- **Moovit Global Transit Index** — web-only, no API or download
- **INRIX / TomTom Traffic Index** — PDF reports, no data export
- **Deloitte City Mobility Index** — web profiles only
- **Apple/Google COVID Mobility** — discontinued (archived copies exist on GitHub)

## Key Methodology Notes

### Metric Comparability
- **US (NTD)**: Reports "unlinked passenger trips" (UPT) — each boarding counts as one trip. A ride with a transfer = 2 UPTs.
- **Europe (Eurostat)**: Reports "passenger-km" — distance-weighted. Not directly comparable to UPT.
- **UK (TfL)**: Reports "journeys" — linked trips (a ride with a transfer = 1 journey).
- **Conversion**: No universal formula. For cross-region comparison, normalize by population (trips per capita) or service area, and note the metric difference explicitly.

### Population Data
- Use World Bank `SP.POP.TOTL` and `SP.URB.TOTL.IN.ZS` for country-level.
- For US metro areas, use NTD service area population or Census metro area estimates.
- For individual cities, GHSL Urban Centre Database (already used in city-pulse pipeline) has 10K+ city populations.

### COVID Impact
- NTD monthly data is the canonical source for US ridership drop/recovery.
- Google Community Mobility Reports (archived) provide relative transit station visits globally through Oct 2022.

## Assets

### Raw Layer
| Asset | Source | Description |
|-------|--------|-------------|
| `raw.transit_ntd_monthly` | NTD | Monthly US transit ridership by agency and mode (2002–present) |
| `raw.transit_ntd_annual` | NTD | Annual US transit operations: ridership, finances, fleet, infrastructure |
| `raw.transit_worldbank` | World Bank API | National transport indicators (rail km, rail passengers, urbanization, GDP) |
| `raw.transit_eurostat_modal` | Eurostat API | European modal split by country |
| `raw.transit_wikidata_metros` | Wikidata SPARQL | Global metro system facts: lines, stations, length, ridership, opening year |

### Staging Layer
| Asset | Source | Description |
|-------|--------|-------------|
| `staging.transit_us_ridership` | NTD monthly + annual | Deduplicated US ridership with per-capita normalization and COVID recovery metrics |
| `staging.transit_global_rail` | World Bank + Eurostat | Combined international rail passenger data with GDP/urbanization context |
| `staging.transit_metro_systems` | Wikidata + NTD | Unified metro system comparison table (length, stations, ridership, year opened) |

### Reports Layer
| Asset | Description |
|-------|-------------|
| `streamlit_app.py` | Dashboard: global transit comparison, US ridership trends, COVID recovery, infrastructure vs ridership |

## Dashboard Questions

1. How has US transit ridership recovered post-COVID, and which cities/modes bounced back fastest?
2. Which metro systems carry the most passengers per km of track (efficiency)?
3. How does transit investment (operating expense per capita) correlate with ridership?
4. How does modal split vary across European countries, and is PT share growing or shrinking?
5. What is the relationship between GDP per capita and rail infrastructure investment?

## Run Commands

```bash
# Validate pipeline
bruin validate public-transit/

# Run raw assets individually (test with small scope first)
bruin run --start-date 2023-01-01 --end-date 2023-12-31 public-transit/assets/raw/transit_ntd_monthly.py
bruin run --start-date 2020-01-01 --end-date 2023-12-31 public-transit/assets/raw/transit_worldbank.py
bruin run public-transit/assets/raw/transit_eurostat_modal.py
bruin run public-transit/assets/raw/transit_wikidata_metros.py

# Run staging
bruin run public-transit/assets/staging/transit_us_ridership.sql
bruin run public-transit/assets/staging/transit_global_rail.sql
bruin run public-transit/assets/staging/transit_metro_systems.sql

# Run full pipeline
bruin run public-transit/

# Launch dashboard
streamlit run public-transit/assets/reports/streamlit_app.py
```

## Known Limitations

- **No single global ridership dataset exists.** Must assemble from NTD (US) + Eurostat (EU) + individual city portals. This pipeline starts with NTD + World Bank + Eurostat + Wikidata as the core.
- **Metric incomparability.** UPT (US) vs passenger-km (EU) vs journeys (UK) are fundamentally different units. Cross-region comparisons must be normalized and caveated.
- **City-level data outside US/EU is sparse.** Most of Asia, Africa, and South America lack open transit data. Wikidata metro facts are the best available proxy for global coverage.
- **NTD lag.** Monthly data has ~3 month lag; annual data has ~1 year lag.
- **World Bank transport indicators are national-level.** No city-level breakdown from this source.
- **GTFS measures service supply, not demand.** Having a GTFS feed tells you what service is scheduled, not how many people use it.
