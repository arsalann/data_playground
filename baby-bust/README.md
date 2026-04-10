# Baby Bust — The Price Tag on the Next Generation

A data pipeline analyzing the global fertility decline and its correlation with economic development. Uses World Bank Open Data to show that as countries get richer and more urban, fertility drops below replacement — and this pattern is universal, accelerating, and irreversible.

## Key Findings

- **54% of countries** (117 of 217) are now below the 2.1 replacement level (2022)
- World average fertility dropped from **5.4 in 1960** to **2.4 in 2024** — more than halved
- **52 countries** are in "demographic crisis" with fertility below 1.5 (South Korea: 0.75, China: 1.03)
- Strong negative correlation between GDP and fertility (**r = -0.75** with log GDP)
- Perfect income-fertility ladder: Low (4.67) > Lower-middle (3.04) > Upper-middle (2.02) > High (1.53)
- Only **7 countries** remain in pre-transition (>5 births/woman), down from 155 in 1960

## Data Source

**World Bank Open Data API** (free, no authentication required)
- URL: https://data.worldbank.org
- License: Creative Commons Attribution 4.0 (CC BY 4.0)
- Coverage: 217 countries (after filtering aggregates), 1960-2024

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
- **`raw.worldbank_indicators`** — Python ingestion from World Bank API. Fetches all 10 indicators for all countries in 10-year chunks with retry logic. 116,276 rows. Append strategy with deduplication in staging.

### Staging
- **`staging.fertility_squeeze`** — Deduplicates raw data, pivots from long to wide format (one row per country-year), and adds derived columns:
  - **Income group**: World Bank 2024 GNI thresholds (Low < $1,145, Lower-middle < $4,516, Upper-middle < $14,006, High)
  - **Demographic stage**: Pre-transition (>5), Early (3-5), Late (2.1-3), Below replacement (1.5-2.1), Crisis (<1.5)
  - **Region**: 7 geographic regions mapped from country codes
  - **5-year change metrics**: fertility_change_5yr, gdp_growth_5yr_pct, urbanization_change_5yr
  - Filters out 50+ World Bank aggregate entities (regions, income groups, world totals)
  - 14,075 rows, 217 countries

### Reports
- **`streamlit_app.py`** — Interactive dashboard: "The Price Tag on the Next Generation"
  - **Chart 1**: "Every Income Group Is Converging Below Replacement" — fertility rate over time by income group
  - **Chart 2**: "The Richer You Get, The Fewer Kids You Have" — GDP vs fertility scatter (log scale)
  - **Chart 3**: "South Korea Is Everyone's Future" — country trajectory lines (urbanization vs fertility)
  - **Chart 4**: "The World Is Running Out of High-Fertility Countries" — demographic stage shift by decade

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

### Entity Filtering
- 262 raw entities → 217 staging countries: **48 World Bank aggregates removed** (WLD, HIC, SSF, EUU, NAC, etc.)
- 4 entities with blank country codes also filtered (income group totals)
- 20 territories classified as "Other" region (Bermuda, Greenland, Guam, Channel Islands, etc.)

### Value Integrity (raw vs staging, zero tolerance)
| Column | Rows Matched | Mismatches |
|---|---|---|
| `fertility_rate` (SP.DYN.TFRT.IN) | 14,073 | **0** |
| `gdp_per_capita_ppp` (NY.GDP.PCAP.PP.CD) | 6,831 | **0** |
| `urbanization_pct` (SP.URB.TOTL.IN.ZS) | 14,075 | **0** |
| `gni_per_capita` (NY.GNP.PCAP.CD) | 10,428 | **0** |

### Derived Column Validation
| Column | Validation | Result |
|---|---|---|
| `demographic_stage` | Boundary check (no overlaps between stages) | Pass |
| `income_group` | GNI thresholds match World Bank 2024 definitions | Pass |
| `above_replacement` | Flag matches fertility_rate > 2.1 for all rows | **0 errors** |

### Column Completeness (% non-null in staging)
| Column | Coverage |
|---|---|
| fertility_rate, life_expectancy, urbanization_pct | 100% |
| infant_mortality | 83.9% |
| gni_per_capita | 74.1% |
| cpi | 64.7% |
| gdp_per_capita_ppp | 48.5% |
| female_labor_participation | 46.4% |
| female_tertiary_enrollment | 32.6% |
| health_expenditure_pct_gdp | 32.4% |

### Automated Quality Checks (Bruin)
9 checks pass on every run: `not_null` on PK columns (country_code, year), `accepted_values` on demographic_stage and income_group, `min`/`max` bounds on year and urbanization_pct, `non_negative` on fertility_rate.

## Known Limitations

- World Bank data has a ~2 year lag for some indicators (fertility rate 2024 data may be preliminary)
- Female tertiary enrollment data is sparse before 1990
- Health expenditure data begins in 2000
- GDP per capita (PPP) data begins in 1990
- CPI coverage varies significantly by country (192 vs 262 entities)
- World Bank API can be flaky — the ingestion script uses 10-year chunks with 5x retry and exponential backoff
