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

### 4. "Baby Bust" - Global Fertility Trends
Global fertility rate trends across 160+ countries (after microstate filtering), examining relationships with GDP, female labor participation, and demographic stages.

- **Data sources**: World Bank Open Data API (10 indicators, 160+ countries after filtering, 1960-2024), World Bank SP.POP.TOTL for population weighting
- **Pipeline**: `baby-bust/`
- **Dashboard**: 10+ interactive Altair charts - income group trends, GDP-fertility scatter, demographic stage shifts, ex-communist trajectories, population-weighted sub-region bubble chart, largest TFR declines, fertility reversals, Sub-Saharan Africa analysis
- **Features**: Microstate filtering (55 countries <1M pop), population-weighted sub-region aggregation, colorblind-safe Wong 2011 palette, interactive legend filtering, standardized data source/limitations footnotes on every chart
- **Reddit targets**: r/dataisbeautiful, r/economics, r/sociology

## Backlog

### 5. "The Tariff Tax" — What Americans Are Actually Paying
Visualize effective tariff rates over time, consumer price increases by product category, and the ~$1,500/household annual cost.

- **Data sources**: FRED (CPI series, import/export prices), BLS CPI microdata, Yale Budget Lab tariff tracker
- **Reddit targets**: r/dataisbeautiful, r/economics, r/personalfinance
- **Why it's viral**: Largest US tax increase as % of GDP since 1993, directly impacts every household
- **Status**: Skipped — too political

### 6. "AI Ate My Job" — White-Collar Hiring Collapse
Track job postings and employment by sector over time, showing where AI is replacing human roles.

- **Data sources**: FRED (JOLTS job openings, unemployment by sector), BLS employment data
- **Reddit targets**: r/dataisbeautiful, r/technology, r/cscareerquestions
- **Why it's viral**: AI anxiety is at peak, white-collar hiring visibly slowing, personally relatable
- **Status**: Skipped — too political

### 7. "The Loneliness Epidemic" — Social Isolation in Numbers
Combine time-use surveys, marriage/friendship data, and mental health indicators to visualize increasing social isolation.

- **Data sources**: World Bank (life expectancy, urbanization), OECD Better Life Index, WHO mental health data
- **Potential**: Cross-reference with baby-bust data (urbanization → isolation → lower fertility?)

### 8. "The Education Premium Is Shrinking"
Track college degree ROI over time — rising tuition vs stagnant wage premium by field of study.

- **Data sources**: FRED (tuition CPI, wage data by education), BLS, College Scorecard API
- **Potential**: Combine with baby-bust data (female tertiary enrollment already ingested)

## Notes

- **Preference**: Avoid politically charged topics (tariffs, immigration, partisan issues)
- **Data quality**: Prioritize gold-standard, reliable, complete datasets (World Bank, FRED, BigQuery public datasets)
- **Cross-pipeline enrichment**: Look for opportunities to join across existing pipelines (e.g., FRED macro data from hormuz-effect + World Bank from baby-bust)
- **World Bank API**: Can be flaky — use chunked requests (10-year windows), high per_page (20000), and retry with exponential backoff
