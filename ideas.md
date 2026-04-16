- Polymarket: https://github.com/pmxt-dev/pmxt

- Google's Jobs API

# Pipeline Ideas

## Completed

### 1. "The Hormuz Effect" — Global Energy Crisis Dashboard
Track how the Strait of Hormuz closure is rippling through oil prices, gas prices, and commodity markets. Compare to the 1973 oil embargo and 2022 Russia-Ukraine shock.

- **Data sources**: FRED API (Brent DCOILBRENTEU, WTI DCOILWTICO, CPI, consumer sentiment, yield curve), FMP API (S&P 500 sector stocks)
- **Pipeline**: `hormuz-effect/`
- **Dashboard**: "The Double Squeeze" — oil vs stock sector performance

### 2. "AI Price Wars" — Model Pricing vs Quality
AI model pricing vs quality analysis across major providers.

- **Pipeline**: `ai-price-wars/`

### 3. "Polymarket Insights" — Prediction Market Analysis
Prediction market analysis for Q1 2026 using Polymarket data.

- **Pipeline**: `polymarket-insights/`

### 4. "Baby Bust — The Price Tag on the Next Generation"
Global fertility decline correlated with economic development. Shows that as countries get richer and more urban, fertility drops below replacement — universally and irreversibly.

- **Data sources**: World Bank Open Data API (10 indicators, 217 countries, 1960-2024)
- **Pipeline**: `baby-bust/`
- **Dashboard**: 4 charts — income group convergence, GDP-fertility scatter, country trajectories, demographic stage shift
- **Key findings**: 54% of countries below replacement, world avg fertility halved (5.4 → 2.4), South Korea at 0.75

### 5. "City Pulse — Decoding Urban Form"
Street network fingerprint analysis comparing how cities are physically structured. Uses real urban planning tools (OSMnx, NetworkX) to compute orientation entropy, intersection density, circuity, and bearing distributions for 20 global cities, combined with GHSL Urban Centre Database (~10K cities) for population, GDP, building heights, climate.

- **Data sources**: GHSL Urban Centre Database R2024A (European Commission JRC, 5 GeoPackage layers), OpenStreetMap (via OSMnx Overpass API), World Bank Open Data API (6 urbanization indicators)
- **Pipeline**: `city-pulse/`
- **Dashboard**: 6 charts — interactive world map (10K cities), street orientation polar plots (9 cities), grid order vs population scatter, building heights scatter (5M+ cities), 20-city normalized heatmap (7 metrics), climate vs urban design explorer (12 indicators)
- **Key findings**: Grid cities (Chicago 0.58, NYC 0.41) have measurably higher orientation order than organic cities (Rome 0.02, Istanbul 0.01). Building height correlates with economic development. Intersection density is a standard urban planning walkability metric (EPA, Walk Score, LEED-ND).
- **Methodology note**: All 20 cities analyzed with consistent 10km radius from city center using `graph_from_point` — never use `graph_from_place` for cross-city comparison (admin boundaries vary wildly in size).

### 6. "Public Transit" — Global Urban Mobility Analysis
Comparative analysis of public transportation systems across major world cities and countries, combining ridership trends, infrastructure metrics, modal split, and fare data.

- **Pipeline**: `public-transit/`
- **Dashboard questions**: US COVID ridership recovery by city/mode, metro system efficiency (passengers per track-km), transit investment vs ridership correlation, European modal split trends, GDP vs rail infrastructure

#### Data Sources — International / Global

- **US National Transit Database (NTD)** — transit.dot.gov/ntd/ntd-data
  - The single richest open transit dataset in the world. ~900 US transit agencies.
  - Fields: monthly/annual ridership (unlinked passenger trips), vehicle revenue miles/hours, operating & capital expenses, fare revenue, fleet inventory (vehicle count, age, type), infrastructure (route miles, track miles, station count), service area population.
  - Time range: annual from 1991, monthly from 2002. Update: monthly ~3 month lag, annual ~1 year lag.
  - Access: bulk CSV/Excel download, free, US public domain.
  - Key files: Monthly Module (adjusted ridership), TS1.1 (total ridership time series), Annual Database (full operational/financial data).
  - Metric: "unlinked passenger trips" (UPT) — each boarding = 1 trip. A transfer = 2 UPTs.
  - Limitation: US only, agency-level not route-level.

- **World Bank Open Data API** — api.worldbank.org/v2
  - National-level transport & context indicators for 200+ countries.
  - Indicators: `IS.RRS.TOTL.KM` (rail route-km), `IS.RRS.PASG.KM` (rail passenger million-km), `IS.ROD.TOTL.KM` (road network km), `SP.URB.TOTL.IN.ZS` (urban pop %), `SP.POP.TOTL` (population), `NY.GDP.PCAP.CD` (GDP per capita).
  - Time range: 1960s–2023 (varies by indicator). Access: REST API, no key, CC BY 4.0.
  - Limitation: country-level only (no city breakdown), rail-focused (no bus/metro/tram detail), API can be flaky (use chunked 10-year windows with retry).

- **Eurostat API** — ec.europa.eu/eurostat/api
  - European transport statistics for EU 27 + EEA (~35 countries).
  - Tables: `tran_hv_psmod` (modal split % by car/bus/rail), `tran_hv_pstra` (rail passenger-km), `urb_ctran` (Urban Audit: PT network km, stops), `tran_r_vehst` (vehicle stock).
  - Time range: 1990–2022 (varies). Access: SDMX REST API, free, no key.
  - Limitation: country-level modal split (not city), metro/tram often lumped. Urban Audit city data has significant gaps.

- **US Census ACS — Table B08301** — data.census.gov
  - "Means of Transportation to Work" — commuting mode share (drove alone, carpool, public transit, walked, bicycle, WFH).
  - Granularity: national, state, metro area, county, city, census tract.
  - Time range: 2005–present (1-year ACS), 2009–present (5-year). Access: Census API (free key) or CSV.
  - Limitation: commute trips only (not all trips), US only, 1-year publication lag.

- **Wikidata SPARQL** — query.wikidata.org/sparql
  - Global metro/BRT/light rail system facts: opening year, number of lines/stations, route-km, annual ridership, system length.
  - Coverage: all notable transit systems worldwide. Access: SPARQL endpoint, free, CC0.
  - Limitation: crowdsourced (may lag reality), system-level only (no station/route granularity).

- **Mobility Database (MobilityData)** — mobilitydatabase.org / github.com/MobilityData/mobility-database-catalogs
  - Catalog of 1,700+ GTFS feed URLs across 70+ countries. Not transit data itself — pointers to agency feeds.
  - Access: GitHub CSV catalog (fully open, Apache 2.0) + REST API (free key via registration).
  - Limitation: catalog only; you must download/parse each agency's GTFS zip. Feed quality varies. Some feeds require agency-side auth.

- **Transitland** — transit.land
  - Ingests and indexes GTFS data. 2,500+ operators in 55+ countries. Queryable stops, routes, schedules, geometries, operators, historical feed versions.
  - Access: REST + GraphQL API. Free tier rate-limited; higher usage needs Interline account.
  - Limitation: free tier best for targeted queries, not bulk download. Feed licensing varies by agency.

- **OpenStreetMap (OSM)** — overpass-turbo.eu / BigQuery `bigquery-public-data.geo_openstreetmap`
  - Global transit infrastructure: bus stops, metro stations, tram stops, rail lines, route geometries. Volunteer-maintained.
  - Access: Overpass API (free, rate-limited), planet file bulk download (50+ GB), or BigQuery.
  - Limitation: infrastructure locations only — no schedule or ridership data. Quality varies by region.

- **EPOMM TEMS** — epomm.eu/tems
  - Modal split (walking, cycling, PT, car) for 400+ European cities.
  - Limitation: self-reported by cities, not standardized, snapshot years only (not time series). Download availability inconsistent.

- **Numbeo** — numbeo.com/cost-of-living
  - Transit fare prices ("Monthly Pass" and "One-way Ticket") for 500+ cities globally. User-contributed.
  - Limitation: API requires paid subscription, restrictive commercial license. Web scraping may violate ToS.

- **OECD/ITF Transport Statistics** — stats.oecd.org
  - Passenger transport by mode (passenger-km) for OECD countries, modal split trends, infrastructure investment, CO2 from transport.
  - Time range: 1970s–2022. Access: OECD.Stat explorer + SDMX API.
  - Limitation: country-level only, definitions vary across countries, public transport not always separated from intercity rail.

- **UN Habitat SDG 11.2.1** — data.unhabitat.org
  - "Proportion of population with convenient access to public transport" for ~100 countries.
  - Time range: 2015–2023 (sparse). Access: CSV download. Limitation: high-level national percentages, methodology varies.

- **Global BRT Data** — bfrtonline.brtdata.org
  - BRT system characteristics: corridor length, daily ridership, stations, fleet size. 170+ cities.
  - Access: web interface, no formal API. Limitation: BRT systems only.

#### Data Sources — City-Level APIs

**Tier 1: Excellent** (mature APIs, rich historical ridership)

- **New York — MTA** — new.mta.info/developers + data.ny.gov
  - Real-time: GTFS-RT for subway/bus. Static: GTFS for subway, bus, LIRR, Metro-North.
  - Ridership: weekly turnstile counts per station (4-hour blocks, back to 2010+), monthly/annual station ridership.
  - Access: free, API key for real-time feeds. Format: GTFS, GTFS-RT, CSV, JSON.
  - Notable: turnstile data is uniquely granular — 4-hour blocks per individual turnstile.

- **London — TfL** — api.tfl.gov.uk
  - Real-time: arrival predictions all modes, line status, disruptions. Static: timetables, station locations, route geometry.
  - Ridership: annual station entry/exit counts (back to ~2007), Oyster/contactless aggregate data (NUMBAT), Rolling OD Survey (RODS).
  - Access: free, API key via portal. Rate limits: 500 req/min with key.
  - Notable: TfL is a global leader in open transit data. Very well-documented Swagger API.

- **Chicago — CTA** — transitchicago.com/developers + data.cityofchicago.org
  - Real-time: Train Tracker API, Bus Tracker API. Static: GTFS.
  - Ridership: daily station-level L entries and bus route totals on Socrata portal. **Back to 2001** — one of the deepest historical series anywhere.
  - Access: free, API key for real-time. Format: XML/JSON (real-time), GTFS, CSV (ridership).

- **Singapore — LTA DataMall** — datamall.lta.gov.sg
  - Real-time: bus arrivals, traffic conditions. Static: bus/train routes and stops.
  - Ridership: monthly origin-destination passenger volumes (tap-in/tap-out). Traffic analytics.
  - Access: free, API key required. Rate limits: 500 calls/min.
  - Notable: OD matrix data is unusually rich for a public dataset.

**Tier 2: Good** (usable APIs, some gaps)

- **Paris — Île-de-France Mobilités** — data.iledefrance-mobilites.fr + transport.data.gouv.fr + prim.iledefrance-mobilites.fr
  - Real-time: next departures, disruptions (PRIM/Siri-Lite). Static: GTFS for all IDF operators (RATP, SNCF Transilien, buses).
  - Ridership: station validation counts (aggregate), published periodically. Access: free, PRIM requires registration.
  - Format: GTFS, GTFS-RT, Siri, NeTEx, JSON, CSV. Limitation: multiple overlapping portals, fragmented.

- **Toronto — TTC** — open.toronto.ca
  - Real-time: NextBus-based vehicle positions. Static: GTFS. Ridership: annual stats, subway/bus delay datasets (several years).
  - Access: free, no key for most. Notable: delay datasets useful for reliability analysis.

- **Los Angeles — LA Metro** — developer.metro.net
  - Real-time: GTFS-RT for bus/rail. Static: GTFS. Ridership: monthly/annual by line.
  - Access: free. GTFS archives via Transitland.

- **Hong Kong — MTR** — data.gov.hk + opendata.mtr.com.hk
  - Real-time: next train times, line status. Ridership: monthly station entry/exit (several years).
  - Access: free, no key for most. Limitation: no official GTFS (community-generated only).

- **Seoul — Seoul Open Data** — data.seoul.go.kr
  - Ridership: subway station entry/exit **by hour** (very granular, several years), T-money card aggregate data.
  - Access: free registration, interface in Korean. Rate limits: 1,000 calls/day free.
  - Notable: surprisingly granular hourly ridership. Main barrier is language (Korean portal/docs/responses).

- **Berlin — BVG/VBB** — vbb.de + v6.vbb.transport.rest (community REST API)
  - Static: GTFS for all Berlin-Brandenburg transit. Community REST API by derhuerst (excellent, no key needed).
  - Ridership: annual aggregate statistics only. Limitation: no granular ridership data.

- **Madrid — EMT/CRTM** — opendata.emtmadrid.es + datos.crtm.es
  - Real-time: EMT bus arrivals/positions. Static: GTFS for CRTM (metro, bus, cercanías).
  - Ridership: annual stats only. Access: free, EMT API requires registration.

- **São Paulo — SPTrans** — sptrans.com.br/desenvolvedores (Olho Vivo API)
  - Real-time: bus GPS positions for ~15,000 buses (Olho Vivo — excellent coverage). Static: GTFS for bus.
  - Ridership: aggregate from municipal data portal. Limitation: metro/CPTM rail data is limited.

- **Mexico City — CDMX** — datos.cdmx.gob.mx
  - Static: GTFS for Metrobús (incomplete for other modes). Ridership: metro ridership in annual reports (often PDF, not machine-readable).
  - Also: Ecobici bike-share trip data back to 2010+.

**Tier 3: Limited** (some data, significant gaps)

- **Istanbul — IBB** — data.ibb.gov.tr — **177 mobility datasets, much richer than expected**
  - See detailed Istanbul section below.

- **Tokyo — ODPT** — developer-tokyochallenge.odpt.org + odpt.org
  - Real-time: train location/delay for some operators (JR East, Tokyo Metro, Toei). Static: timetables, station data.
  - Access: free registration. Format: JSON-LD (unique). Limitation: fragmented across many private operators, incomplete coverage, very limited ridership data.

- **Delhi — OTD** — otd.delhi.gov.in
  - GTFS for DMRC (metro) and DTC (buses), real-time bus GPS. Access: free with registration.
  - Ridership: annual reports (PDF) only. Strongest open transit data in India.

- **Jakarta — Transjakarta** — data.jakarta.go.id
  - Static: GTFS for Transjakarta BRT. Ridership: aggregate daily, published periodically.
  - Limitation: broader Jakarta transit (KRL, MRT, LRT) has less standardized data.

- **Moscow** — data.mos.ru
  - Some route data, annual metro ridership figures. Portal in Russian.
  - Limitation: no standard GTFS from official sources, access/updates may be affected by geopolitical situation.

**Tier 4: Minimal or No Open Data**

- **Bangkok** (BTS/MRT/BMTA): no official API, community GTFS may be outdated, annual ridership in corporate report PDFs only.
- **Mumbai** (BEST): intermittent GTFS for buses, ridership in reports only.
- **Shanghai/Beijing**: no internationally accessible open API. Community GTFS exists but unofficial/outdated. National aggregate stats in Ministry of Transport yearbooks.
- **Cairo**: no open data portal, no GTFS, ridership only in news/press releases.
- **Lagos/Dhaka/Karachi**: no machine-readable transit data exists. Transit is largely informal.

#### Data Sources — Istanbul Deep Dive

Istanbul's IBB Open Data Portal hosts **177 mobility datasets** — one of the richest transit data ecosystems outside Western Europe/North America. Upgraded from initial C+ to **B+** rating.

**High-value ridership datasets** (all free, CSV download from data.ibb.gov.tr):

- **Hourly Public Transport Data** (Saatlik Toplu Ulasim Veri Seti)
  - URL: data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set
  - Publisher: BELBIM Inc. (Istanbulkart operator). 60 monthly CSV files, ~1–1.8 GB each (~60 GB total).
  - Time range: January 2020 – December 2024. Last updated: April 2025.
  - Fields: `transition_date`, `transition_hour`, `transport_type_id`, `road_type`, `line`, `transfer_type`, `number_of_passage`, `number_of_passenger`, `product_kind`, `transaction_type_desc`, `town`, `line_name`, `station_poi_desc_cd`.
  - Coverage: all modes — IETT bus, metro, metrobus, Marmaray, IDO ferries, maritime. Includes transfer vs direct trips, fare type (full/discounted), district, line, station.
  - This is the single most valuable Istanbul dataset — hourly multi-modal ridership with 5 years of history.

- **Rail Station-Level Ridership** (Rayli Sistemler Istasyon Bazli Yolcu ve Yolculuk Sayilari)
  - URL: data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
  - Format: CSV (annual files, 11–101 MB). Time range: 2021–2025. Last updated: February 2026.
  - Fields: `passage_cnt`, `passanger_cnt`, `transaction_year/month/day`, `line`, `station_name`, `station_number`, `town`, `longitude`, `latitude`.
  - Includes coordinates — excellent for spatial analysis and mapping.

- **Rail Ridership by Age Group** (Yas Grubuna Gore...)
  - URL: data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
  - Same as above but with demographic (age group) segmentation based on Istanbulkart holder data.
  - Format: CSV (2022–2025), XLSX (2021). Time range: 2021–2025. Last updated: February 2026.

- **Ferry Pier Passengers** (Istanbul Deniz Iskeleleri Yolcu Sayilari)
  - URL: data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari
  - Format: CSV (~72 KB/year). Time range: 2021–2025. Last updated: February 2026.
  - Fields: `yil` (year), `ay` (month), `otorite_adi` (operator), `istasyon_adi` (pier), `tekil_yolcu_sayisi` (unique passengers), `toplam_yolculuk_sayisi` (total journeys).

- **Istanbulkart Usage by Card Type** — data.ibb.gov.tr/en/dataset/istanbulkart-yillara-ve-kart-tiplerine-gore-kullanim-verisi. XLSX, updated March 2026.
- **Electronic Ticket Pass Data** — data.ibb.gov.tr/en/dataset/toplu-tasimada-kullanilan-elektronik-bilet-gecis-verileri. XLSX, updated March 2026.
- **Non-Istanbulkart Passage Data** — data.ibb.gov.tr/en/dataset/toplu-ulasimda-istanbul-kart-harici-gecis-verisi. CSV (2023–2025). Fields: date, operator, card type, journeys, passengers.
- **Number of Passengers by Transport Mode** — data.ibb.gov.tr/en/dataset/yolculuk-turu-bazinda-yolcu-sayisi. XLSX, annual through 2019. Historical mode split.
- **Daily Maximum Rail System Journeys** — data.ibb.gov.tr/en/dataset/rayli-sistem-gunluk-maksimum-yolculuk-sayilari. XLSX, through Nov 2019. Pre-COVID peak records by line.

**Geospatial layers** (all GeoJSON, updated June 2025 from data.ibb.gov.tr):
  - Rail station points, rail station areas, rail lines
  - IETT bus stops, IETT bus routes
  - Ferry piers, ferry/maritime lines
  - Minibus stops, minibus lines
  - Taxi minibus stops, taxi lines, taxi stands
  - Bicycle paths, pedestrianized roads
  - EV charging stations
  - Main arterial roads (CSV + KML, December 2025)

**Metro Istanbul operational data** (XLSX, actively updated through April 2026):
  - Daily/monthly/annual timetables by line
  - Wagon-kilometer data by line
  - Line-based voyage numbers (monthly)
  - Line length information
  - Transfer information between lines
  - Vehicle features (weight, capacity, acceleration)
  - Energy consumption per line (kWh/100km/day)
  - Station infrastructure (escalators, elevators)

**Traffic data**:
  - Istanbul Traffic Index — CSV, 2015–present, daily (`min/max/avg_traffic_index`). Updated September 2025.
  - Hourly Traffic Density — 61 monthly CSVs, January 2020 – January 2025. Updated April 2025.

**Real-time SOAP APIs** (legacy protocol, requires IBB Login account + Python `zeep` library):
  - Base URL: api.ibb.gov.tr/iett
  - Live bus GPS positions (entire fleet or by route): `GetFiloAracKonum_json`, `GetHatOtoKonum_json`
  - Bus stop information with accessibility data: `GetDurak_json`
  - Trip completion rates, service alerts, planned timetables
  - Response fields include: vehicle ID, lat/lon, speed, route code, direction, nearest stop, timestamp.
  - Existing client libraries: .NET (github.com/AydinAdn/IBB.Api), Python (use `zeep`).
  - Metro Istanbul also publishes 6 web service APIs (station info, line/direction lookups).

**GTFS feeds** (available but problematic):
  - IETT Bus GTFS: data.ibb.gov.tr/en/dataset/iett-gtfs-verisi — **frozen, will not be updated**. Uses semicolons as delimiters (nonstandard). Turkish character encoding is corrupted (UTF-8 mojibake).
  - Multi-modal GTFS: data.ibb.gov.tr/en/dataset/public-transport-gtfs-data — covers metro, ferry, Marmaray, minibus but **data frozen at July 2020**. 8 standard GTFS files.
  - Istanbul is **not indexed on Transitland**. Not in MobilityData catalog independently either.

**Planning & survey data**:
  - Transportation Demand Forecast Model: 18 files (XLSX + KMZ), base year 2020, projection to 2040, 18.6M population, demand matrices. Updated February 2026.
  - Household Travel Surveys: 2006 and 2012 surveys (PDF questionnaires + CSV responses).
  - "34 Minutes Istanbul" accessibility indices (shelter, work, needs) — GeoJSON.

**Istanbul caveats**:
  - Hourly transport CSVs are very large (1+ GB each, ~60 GB total) — needs BigQuery or chunked processing.
  - GTFS feeds are frozen/outdated — do not rely on them for current schedules.
  - SOAP APIs are legacy protocol (not REST). Use Python `zeep`.
  - Portal is bilingual (Turkish/English) but API documentation is mostly Turkish.
  - Turkey has no national open transport data mandate — this is entirely an IBB municipal initiative.
  - All data uses Istanbul Metropolitan Municipality Open Data License (permissive for analysis).

#### Key Limitations & Methodology Notes

- **No single global ridership dataset exists.** Must assemble from NTD (US) + Eurostat (EU) + individual city portals.
- **Metrics are incomparable across regions**: US uses "unlinked passenger trips" (UPT, each boarding = 1), Europe uses "passenger-km" (distance-weighted), UK uses "journeys" (linked trips, transfer = 1). Normalize by population (trips per capita) and note metric differences.
- **Asia/Africa/South America data is sparse.** Most 5M+ cities in the Global South have no open transit data. Istanbul is a notable exception.
- **GTFS measures service supply, not demand.** Having a GTFS feed tells you what's scheduled, not how many people use it.
- **City data availability** (5M+ pop): A+ tier: NYC, London, Chicago. A tier: Singapore, Toronto. B+ tier: Istanbul, LA, Paris. B tier: Hong Kong, Seoul, Berlin, Madrid. B- tier: São Paulo, Mexico City. C tier: Delhi, Tokyo, Jakarta, Moscow. D/F tier: Bangkok, Mumbai, Shanghai, Beijing, Cairo, Lagos, Dhaka, Karachi.

#### Not Available as Open Data

- **UITP World Metro Figures** — membership paywall.
- **Moovit Global Transit Index** — web-only, no API or download.
- **INRIX / TomTom Traffic Index** — PDF reports, no data export.
- **Deloitte City Mobility Index** — web profiles only.
- **Apple/Google COVID Mobility** — discontinued (archived on GitHub).

## Backlog

### 6. "The Tariff Tax" — What Americans Are Actually Paying
Visualize effective tariff rates over time, consumer price increases by product category, and the ~$1,500/household annual cost.

- **Data sources**: FRED (CPI series, import/export prices), BLS CPI microdata, Yale Budget Lab tariff tracker
- **Status**: Skipped — too political

### 7. "AI Ate My Job" — White-Collar Hiring Collapse
Track job postings and employment by sector over time, showing where AI is replacing human roles.

- **Data sources**: FRED (JOLTS job openings, unemployment by sector), BLS employment data
- **Status**: Skipped — too political

### 8. "The Loneliness Epidemic" — Social Isolation in Numbers
Combine time-use surveys, marriage/friendship data, and mental health indicators to visualize increasing social isolation.

- **Data sources**: World Bank (life expectancy, urbanization), OECD Better Life Index, WHO mental health data
- **Potential**: Cross-reference with baby-bust data (urbanization → isolation → lower fertility?)

### 9. "The Education Premium Is Shrinking"
Track college degree ROI over time — rising tuition vs stagnant wage premium by field of study.

- **Data sources**: FRED (tuition CPI, wage data by education), BLS, College Scorecard API
- **Potential**: Combine with baby-bust data (female tertiary enrollment already ingested)

## Notes

- **Preference**: Avoid politically charged topics (tariffs, immigration, partisan issues)
- **Data quality**: Prioritize gold-standard, reliable, complete datasets (World Bank, FRED, BigQuery public datasets)
- **Cross-pipeline enrichment**: Look for opportunities to join across existing pipelines (e.g., FRED macro data from hormuz-effect + World Bank from baby-bust)
- **World Bank API**: Can be flaky — use chunked requests (10-year windows), high per_page (20000), and retry with exponential backoff
- **Geospatial comparison**: When comparing cities/regions, always use identical spatial parameters (same radius, same query type, same resolution). Never mix admin boundaries of different sizes — `graph_from_place` returns wildly different areas for different cities.
- **GHSL GeoPackage**: The R2024A release has 16 thematic layers that must be merged on `ID_UC_G0`. Uses Mollweide projection — convert to WGS84 for lat/lon. Use `pycountry` with fuzzy matching + manual overrides for country name → ISO code mapping.
- **OSMnx best practice**: Use `graph_from_point(center, dist=radius)` with a fixed radius for all cities instead of `graph_from_place`. Set `ox.settings.timeout = 300` for large downloads. Add 2s sleep between cities to respect Overpass rate limits.
