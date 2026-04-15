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
- **Reddit targets**: r/dataisbeautiful, r/economics, r/collapse, r/sociology

### 5. "City Pulse — Decoding Urban Form"
Street network fingerprint analysis comparing how cities are physically structured. Uses real urban planning tools (OSMnx, NetworkX) to compute orientation entropy, intersection density, circuity, and bearing distributions for 20 global cities, combined with GHSL Urban Centre Database (~10K cities) for population, GDP, building heights, climate.

- **Data sources**: GHSL Urban Centre Database R2024A (European Commission JRC, 5 GeoPackage layers), OpenStreetMap (via OSMnx Overpass API), World Bank Open Data API (6 urbanization indicators)
- **Pipeline**: `city-pulse/`
- **Dashboard**: 6 charts — interactive world map (10K cities), street orientation polar plots (9 cities), grid order vs population scatter, building heights scatter (5M+ cities), 20-city normalized heatmap (7 metrics), climate vs urban design explorer (12 indicators)
- **Key findings**: Grid cities (Chicago 0.58, NYC 0.41) have measurably higher orientation order than organic cities (Rome 0.02, Istanbul 0.01). Building height correlates with economic development. Intersection density is a standard urban planning walkability metric (EPA, Walk Score, LEED-ND).
- **Methodology note**: All 20 cities analyzed with consistent 10km radius from city center using `graph_from_point` — never use `graph_from_place` for cross-city comparison (admin boundaries vary wildly in size).
- **Reddit targets**: r/dataisbeautiful, r/urbanplanning, r/geography, r/mapporn

## Backlog

### 6. "The Tariff Tax" — What Americans Are Actually Paying
Visualize effective tariff rates over time, consumer price increases by product category, and the ~$1,500/household annual cost.

- **Data sources**: FRED (CPI series, import/export prices), BLS CPI microdata, Yale Budget Lab tariff tracker
- **Reddit targets**: r/dataisbeautiful, r/economics, r/personalfinance
- **Why it's viral**: Largest US tax increase as % of GDP since 1993, directly impacts every household
- **Status**: Skipped — too political

### 7. "AI Ate My Job" — White-Collar Hiring Collapse
Track job postings and employment by sector over time, showing where AI is replacing human roles.

- **Data sources**: FRED (JOLTS job openings, unemployment by sector), BLS employment data
- **Reddit targets**: r/dataisbeautiful, r/technology, r/cscareerquestions
- **Why it's viral**: AI anxiety is at peak, white-collar hiring visibly slowing, personally relatable
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
