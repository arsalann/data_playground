# City Pulse — Decoding Urban Form

A data pipeline analyzing how the world's cities are physically structured using real urban planning tools and datasets. The centerpiece: street network "fingerprints" that reveal each city's DNA — is it a grid (NYC), organic (London), or radial (Paris)?

Combines the GHSL Urban Centre Database (~10K global cities with population, GDP, building height, HDI, climate) with OSMnx street network analysis for ~20 cities and World Bank urbanization time series.

## Build Plan

### Tools & Datasets

| Tool/Dataset | Role |
|---|---|
| **OSMnx** | Street network download + orientation/connectivity analysis from OpenStreetMap |
| **NetworkX** | Graph metrics: intersection density, dead-end ratio, circuity |
| **GeoPandas / Shapely** | Spatial data handling, geometry operations |
| **Matplotlib** | Street orientation polar plots ("city fingerprints") |
| **Pydeck** | Interactive world maps (ScatterplotLayer, ColumnLayer) |
| **GHSL Urban Centre Database** | Pre-computed metrics for ~10K global cities |
| **OpenStreetMap** (via OSMnx) | Street network graphs for ~20 cities |
| **World Bank API** | Country-level urbanization time series (2000-2024) |

### Build Checklist

- [x] **Step 1: Scaffold** — pipeline.yml, README.md, requirements.txt files
- [x] **Step 2: Raw — ghsl_urban_centers.py** — GHSL Urban Centre Database download + parse
- [x] **Step 3: Raw — street_networks.py** — OSMnx street network analysis for ~20 cities
- [x] **Step 4: Raw — worldbank_urban.py** — World Bank urbanization indicators
- [x] **Step 5: Staging — city_profiles.sql** — Join GHSL + street metrics, derive tiers/zones
- [x] **Step 6: Staging — urban_trends.sql** — Country-level urbanization trajectories
- [x] **Step 7: Streamlit dashboard** — 4 visualizations: world map, polar plots, scatter, spaghetti
- [x] **Step 8: Validate & test** — bruin validate, test assets, verify dashboard

## Data Sources

**GHSL Urban Centre Database** (European Commission JRC)
- URL: https://ghsl.jrc.ec.europa.eu/
- License: Creative Commons Attribution 4.0 (CC BY 4.0)
- Coverage: ~10,000 urban centers globally, population epochs 1975/1990/2000/2015

**OpenStreetMap** (via OSMnx Overpass API)
- URL: https://www.openstreetmap.org
- License: Open Data Commons Open Database License (ODbL)
- Coverage: ~20 selected cities representing diverse urban forms

**World Bank Open Data API**
- URL: https://data.worldbank.org
- License: Creative Commons Attribution 4.0 (CC BY 4.0)
- Coverage: 217 countries, 2000-2024

### World Bank Indicators

| Code | Indicator | Purpose |
|---|---|---|
| `SP.URB.TOTL.IN.ZS` | Urban population (% of total) | Core urbanization metric |
| `SP.URB.GROW` | Urban population growth (annual %) | Growth velocity |
| `EN.URB.LCTY.UR.ZS` | Population in largest city (% of urban) | Urban primacy |
| `NY.GDP.PCAP.CD` | GDP per capita (current US$) | Economic development |
| `SP.POP.TOTL` | Population, total | Size context |
| `EN.POP.DNST` | Population density (people/sq km) | Density comparison |

## Assets

### Raw
- **`raw.ghsl_urban_centers`** — Downloads GHSL Urban Centre Database R2024A GeoPackage. Reads 5 thematic layers and merges on urban center ID. Extracts city name, country, coordinates, population (multi-epoch), built-up area, GDP, building height, HDI, climate, elevation. ~11.4K rows. Create+replace strategy.
- **`raw.street_networks`** — Uses OSMnx to download and analyze street network graphs for 20 cities using `graph_from_point` with a consistent 10km radius from city center. Computes orientation entropy, intersection density, dead-end proportion, circuity, bearing distributions. Create+replace strategy.
- **`raw.worldbank_urban`** — Fetches 6 urbanization indicators from World Bank API in 10-year chunks with retry logic. Append strategy with deduplication in staging.

### Staging
- **`staging.city_profiles`** — Joins GHSL urban centers with street network metrics. Derives population tier, climate zone, continent, population growth rate, network analysis flag.
- **`staging.urban_trends`** — Country-level urbanization time series. Pivots indicators to columns, derives urbanization stage, velocity, decade, region, income group.

### Reports
- **`streamlit_app.py`** — Interactive dashboard: "City Pulse — Decoding Urban Form"
  - **Chart 1**: "Where the World Lives" — Pydeck ScatterplotLayer, ~10K cities, sized by population, colored by selectable metric
  - **Chart 2**: "City Fingerprints" — 3x3 grid of Matplotlib polar plots showing street orientation distributions (North at top)
  - **Chart 3**: "Grid order vs. population" — Altair scatter, 19 analyzed cities, dot size = intersection count, log-scale population axis
  - **Chart 4**: "Building heights across major cities" — Altair scatter, filtered to 5M+ population, all cities labeled
  - **Chart 5**: "Comparing the 20 analyzed cities" — Normalized heatmap of 7 metrics with units (pop density, grid order, building height, street length, intersections/km², dead ends %, route directness)
  - **Chart 6**: "Climate vs. urban design" — Dual-dropdown explorer with all 12 indicators on both axes, city labels, GHSL mismatches filtered

## Run Commands

```bash
# Validate
bruin validate city-pulse/

# Run raw assets individually
bruin run city-pulse/assets/raw/ghsl_urban_centers.py
bruin run city-pulse/assets/raw/street_networks.py
bruin run city-pulse/assets/raw/worldbank_urban.py --start-date 2000-01-01 --end-date 2024-12-31

# Run staging
bruin run city-pulse/assets/staging/city_profiles.sql
bruin run city-pulse/assets/staging/urban_trends.sql

# Launch dashboard
streamlit run city-pulse/assets/reports/streamlit_app.py

# Quick test (limited scope)
CITY_LIMIT=3 bruin run city-pulse/assets/raw/street_networks.py
```

## Street Network Cities

| Category | Cities | Expected Pattern |
|---|---|---|
| Grid | New York, Chicago, Barcelona, Buenos Aires | Sharp 4-directional spikes |
| Organic | London, Tokyo, Istanbul, Rome | Uniform/random distribution |
| Radial/Planned | Paris, Moscow, Washington DC, Brasilia | Radial patterns |
| Developing Megacities | Lagos, Mumbai, Jakarta, Cairo | Mixed/irregular |
| Compact | Amsterdam, Singapore, Hong Kong, Seoul | Dense, variable patterns |

## Street Network Methodology

All 20 cities are analyzed with **identical spatial parameters** to ensure fair comparison:
- **Query method**: `osmnx.graph_from_point(center, dist=10000, network_type="drive")`
- **Radius**: 10 km from city center for all cities
- **Center coordinates**: Hardcoded lat/lon for each city's urban center
- **Network type**: Drivable roads only

This replaces the original `graph_from_place` approach, which used admin boundaries of wildly different sizes (e.g., "City of London" = 1 sq mi vs "Chicago" = 234 sq mi), making cross-city comparison invalid.

## Known Limitations

- GHSL population data has epochs (1975/1990/2000/2015) — not annual
- GHSL GDP estimates are modeled, not directly measured
- Street network analysis covers only 20 cities (OSMnx/Overpass rate limits)
- OSMnx Overpass API can be slow; full 20-city analysis takes ~26 minutes
- World Bank data has ~2 year publication lag
- Climate zone classification is simplified (based on temperature + precipitation thresholds)
- GHSL proximity matching can produce mismatches (e.g., Buenos Aires → "San Nicolás de los Arroyos", Brasilia → "Lago Norte") — these are filtered out in the dashboard
- 10km radius captures the core urban area but may miss suburban patterns in sprawling cities
