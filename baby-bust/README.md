# Baby Bust - Global Fertility Trends

A data pipeline analyzing global fertility rate trends using World Bank Open Data. Covers 160+ countries (after excluding microstates under 1M population) from 1960-2024, examining relationships between fertility, GDP, income level, female labor participation, and demographic stages.

## Data Source

**World Bank Open Data API** (free, no authentication required)
- URL: https://data.worldbank.org
- License: Creative Commons Attribution 4.0 (CC BY 4.0)
- Coverage: 217 countries before microstate filtering, ~160+ after excluding 55 countries with population under 1 million

### Indicators

| Code | Indicator | Purpose | Coverage |
|---|---|---|---|
| `SP.DYN.TFRT.IN` | Fertility rate (births per woman) | Core dependent variable | 1960-2024, 262 entities |
| `SP.DYN.LE00.IN` | Life expectancy at birth (years) | Health/development proxy | 1960-2024, 262 entities |
| `NY.GDP.PCAP.PP.CD` | GDP per capita, PPP (current intl $) | Wealth proxy, PPP-adjusted | 1990-2024, 248 entities |
| `SP.URB.TOTL.IN.ZS` | Urban population (% of total) | Urbanization driver | 1960-2024, 262 entities |
| `SE.TER.ENRR.FE` | Female tertiary enrollment (% gross) | Education effect | 1970-2024, 246 entities |
| `SL.TLF.CACT.FE.ZS` | Female labor force participation (%) | Economic participation | 1990-2024, 232 entities |
| `SP.DYN.IMRT.IN` | Infant mortality (per 1,000 births) | Demographic transition signal | 1960-2024, 241 entities |
| `SH.XPD.CHEX.GD.ZS` | Health expenditure (% of GDP) | Healthcare investment | 2000-2024, 238 entities |
| `FP.CPI.TOTL` | Consumer price index (2010=100) | Inflation/cost pressure | 1960-2024, 192 entities |
| `NY.GNP.PCAP.CD` | GNI per capita (current US$) | Income group classification | 1962-2024, 252 entities |

## Assets

### Raw
- **`raw.worldbank_indicators`** - Python ingestion from World Bank API. Fetches all 10 indicators for all countries in 10-year chunks with retry logic. Append strategy with deduplication in staging.

### Staging
- **`staging.fertility_squeeze`** - Deduplicates raw data, pivots from long to wide format (one row per country-year), adds derived columns (income group, demographic stage, region, 5-year change metrics). Filters out World Bank aggregate entities.

### Reports
- **`streamlit_app.py`** - Interactive Streamlit dashboard with 10+ Altair charts:
  1. **Fertility trajectories after dropping below 1.5 births per woman** - Spaghetti chart of post-threshold trajectories for all countries that crossed below 1.5
  2. **Number of countries first falling below 1.5 TFR, by 5-year period** - Stacked bar chart by World Bank region
  3. **GDP per capita (PPP) vs total fertility rate** - Scatter plot by region with shape encoding
  4. **Mean total fertility rate by World Bank income group, 1960-2024** - Line chart with dash patterns
  5. **Country distribution by demographic stage, 1960-2022** - Stacked bar with region dropdown filter
  6. **Fertility trends in former communist states, 1985-2022** - Spaghetti chart with 10 highlighted countries and collapse-zone shading
  7. **Female labor force participation vs GDP per capita by sub-region** - Bubble chart with population-weighted averages, ~20 sub-regions
  8. **Largest 5-year declines in total fertility rate** - Horizontal bar chart colored by context (conflict/coercion vs voluntary)
  9. **Countries with higher TFR in latest data than in 2000** - Dumbbell chart
  10. **Largest TFR declines in Sub-Saharan Africa, 2000-2022** - Dumbbell chart

  Dashboard features:
  - Population-weighted sub-region aggregation (SP.POP.TOTL via World Bank API)
  - Colorblind-safe Wong 2011 palette with redundant encoding (color + shape + dash)
  - Interactive legend filtering on all charts
  - Microstate exclusion (55 countries under 1M population)
  - Standardized data source, tools, and limitations footnote on every chart
  - Factual, non-sensational titles and descriptions

## Run Commands

```bash
# Validate
bruin validate baby-bust/

# Run raw ingestion (full historical data)
bruin run baby-bust/assets/raw/worldbank_indicators.py --start-date 1960-01-01 --end-date 2024-12-31

# Run staging transformation only
bruin run baby-bust/assets/staging/fertility_squeeze.sql

# Launch dashboard
streamlit run baby-bust/assets/reports/streamlit_app.py
```

## Data Validation

### Row Counts
| Layer | Rows | Countries/Entities | Years |
|---|---|---|---|
| Raw (`raw.worldbank_indicators`) | 116,276 | 262 (incl. aggregates) | 1960-2024 |
| Staging (`staging.fertility_squeeze`) | 14,075 | 217 (countries only) | 1960-2024 |
| Dashboard (after microstate filter) | ~12,500 | ~162 | 1960-2024 |

### Entity Filtering
- 262 raw entities -> 217 staging countries: 48 World Bank aggregates removed (WLD, HIC, SSF, EUU, NAC, etc.)
- 217 staging countries -> ~162 dashboard countries: 55 microstates (population under 1M) excluded
- 4 entities with blank country codes also filtered (income group totals)

### Automated Quality Checks (Bruin)
9 checks pass on every run: `not_null` on PK columns (country_code, year), `accepted_values` on demographic_stage and income_group, `min`/`max` bounds on year and urbanization_pct, `non_negative` on fertility_rate.

## Known Limitations

- World Bank data may lag 1-2 years for some countries; 2024 data may be preliminary
- TFR is a period measure that can be affected by tempo effects (shifts in timing of births)
- Income group classification uses latest available GNI thresholds and is applied retroactively
- Female labor participation includes both formal and informal employment (including subsistence agriculture), limiting cross-region comparability
- Population weights for sub-region aggregation use most recent available year from SP.POP.TOTL
- Female tertiary enrollment data is sparse before 1990
- Health expenditure data begins in 2000
- GDP per capita (PPP) data begins in 1990
- World Bank API can be flaky - the ingestion script uses 10-year chunks with 5x retry and exponential backoff
