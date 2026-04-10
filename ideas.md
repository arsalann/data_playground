Polymarket: https://github.com/pmxt-dev/pmxt

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
