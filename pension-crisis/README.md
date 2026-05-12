# pension-crisis

Global retirement and pension crisis analysis — a Bruin/BigQuery/Streamlit pipeline
covering the 38 OECD member countries. Every indicator is sourced from a single
harmonized international methodology (UN, OECD, or Mercer CFA) so cross-country
comparisons are apples-to-apples by construction.

## Scope

**38 OECD member countries only.** Non-OECD economies are excluded because we could
not guarantee that their national-source pension statistics would use definitions
compatible with the OECD frame.

## Data sources

| Source | Vintage | What we use |
|---|---|---|
| UN World Population Prospects 2024 | Released 11 July 2024 | Population by age, old-age dependency, life expectancy at 65 (1950-2100) |
| OECD Pensions at a Glance | Most recent biennial edition (2023; 2025 pending public release) | Normal statutory retirement age, net pension replacement rate |
| OECD Social Expenditure Database (SOCX) | Annual | Public pension spending as % of GDP |
| OECD Global Pension Statistics (GPS) | Annual | Pension fund assets as % of GDP |
| Mercer CFA Global Pension Index | 2025 edition, October 2025 | Overall + Adequacy / Sustainability / Integrity sub-indices |
| World Bank WDI | Annual, through 2024 | GDP per capita PPP, fertility, LFP (contextual) |

## Pipeline layout

```
pension-crisis/
├── pipeline.yml
├── README.md
└── assets/
    ├── raw/
    │   ├── requirements.txt
    │   ├── pc_un_wpp_population.py         # population by age, 1950-2100
    │   ├── pc_un_wpp_life_expectancy.py    # life expectancy at 65
    │   ├── pc_oecd_retirement_age.py       # statutory retirement age
    │   ├── pc_oecd_replacement_rate.py     # net pension replacement rate
    │   ├── pc_oecd_pension_spending.py     # public pension spending %GDP
    │   ├── pc_oecd_pension_assets.py       # pension fund assets %GDP
    │   ├── pc_worldbank_indicators.py      # contextual WB indicators
    │   ├── pc_mercer_index_2025.py         # Mercer GPI 2025 scores
    │   └── seeds/
    │       └── mercer_gpi_2025.csv         # fill in from Mercer report
    ├── staging/
    │   ├── pc_country_dim.sql              # canonical OECD-38 dimension
    │   ├── pc_demographics_annual.sql      # UN WPP country-year panel
    │   ├── pc_pension_system.sql           # pivoted OECD indicators, latest year
    │   ├── pc_mercer_scores.sql            # Mercer joined to country_dim
    │   └── pc_country_pension_profile.sql  # mart: one row per country
    └── reports/
        ├── requirements.txt
        ├── streamlit_app.py
        ├── dependency_trajectory.sql
        ├── retirement_gap.sql
        ├── spending_vs_dependency.sql
        └── mercer_sustainability.sql
```

## Running

```bash
# validate everything first
bruin validate pension-crisis/

# run raw assets individually
bruin run pension-crisis/assets/raw/pc_un_wpp_population.py
bruin run pension-crisis/assets/raw/pc_un_wpp_life_expectancy.py
bruin run pension-crisis/assets/raw/pc_oecd_retirement_age.py
bruin run pension-crisis/assets/raw/pc_oecd_replacement_rate.py
bruin run pension-crisis/assets/raw/pc_oecd_pension_spending.py
bruin run pension-crisis/assets/raw/pc_oecd_pension_assets.py
bruin run pension-crisis/assets/raw/pc_worldbank_indicators.py
bruin run pension-crisis/assets/raw/pc_mercer_index_2025.py

# then staging in dependency order
bruin run pension-crisis/assets/staging/

# finally the Streamlit dashboard
python3 -m streamlit run pension-crisis/assets/reports/streamlit_app.py
```

## OECD SDMX endpoints

The four OECD raw assets fetch from OECD Data Explorer SDMX CSV endpoints. OECD has
migrated endpoints several times; if a fetch fails, open
[data-explorer.oecd.org](https://data-explorer.oecd.org/), find the indicator, use
the "Download → Filtered data, CSV" option, and paste the URL into the relevant
environment variable:

- `OECD_RETIREMENT_AGE_URL`
- `OECD_REPLACEMENT_RATE_URL`
- `OECD_PENSION_SPENDING_URL`
- `OECD_PENSION_ASSETS_URL`

## Mercer 2025 seed

`assets/raw/seeds/mercer_gpi_2025.csv` ships as a template. Populate it from the
[Mercer CFA Institute Global Pension Index 2025](https://www.mercer.com/insights/investments/market-outlook-and-ideas/mercer-cfa-institute-global-pension-index/)
report with one row per OECD country. Required columns:

```
iso3_code,country_name,overall_index,adequacy_sub_index,sustainability_sub_index,integrity_sub_index,grade
```

Alternatively, set `MERCER_GPI_2025_URL` to a CSV URL and the asset will fetch it
directly.

## Known limitations

- **UN WPP medium variant only.** High- and low-variant projections diverge
  substantially by 2050; we chose the central scenario.
- **Statutory ≠ effective retirement age.** The statutory value is legislated; the
  average age at which people actually retire is typically 1-3 years lower in most
  OECD countries.
- **Public pension spending excludes private payouts.** Countries with large funded
  systems (Netherlands, Denmark, Iceland) will look "cheaper" on this metric than
  their true total pension outlay.
- **Mercer scoring is opinionated.** The sustainability sub-index bundles coverage,
  asset accumulation, demographics, and contribution levels with published but
  non-neutral weights.
