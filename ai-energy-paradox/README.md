# AI Energy Paradox

**Is AI eating the clean energy transition?**

Renewables overtook coal globally for the first time in H1 2025 (34.3% vs 33.1%). But AI data center energy demand is projected to double by 2030 (415 TWh to 945 TWh). Ireland's data centers already consume 21% of national electricity. This pipeline quantifies the paradox: are AI's energy demands canceling out the gains from the renewable transition?

## Data Sources

| Source | What | License | Strategy |
|--------|------|---------|----------|
| [Ember Global Electricity](https://ember-climate.org/data-catalogue/yearly-electricity-data/) | Annual electricity generation by source, capacity, demand, emissions for 215 countries | CC-BY-4.0 | create+replace |
| [Our World in Data — Energy](https://github.com/owid/energy-data) | Pre-cleaned energy CSV with GDP, population, per-capita metrics | CC-BY | create+replace |
| IEA Data Center & AI Reports | Published projections on data center and AI-specific electricity demand | Manual seed | create+replace |
| [IEA Global EV Data Explorer](https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer) | EV sales, stock, charging, electricity demand by country | CC-BY-4.0 | create+replace |
| [FRED](https://fred.stlouisfed.org/) | Energy commodity prices (Brent, WTI, gas, electricity CPI) | Public domain | append |

## Assets

### Raw (Python ingestion)

| Asset | Description |
|-------|-------------|
| `raw.aep_ember_yearly` | Ember annual electricity data (API with CSV fallback) |
| `raw.aep_owid_energy` | Our World in Data energy CSV (~40MB) |
| `raw.aep_datacenter_demand` | IEA data center projections seed table |
| `raw.aep_ev_demand` | IEA EV sales, stock, and electricity demand |
| `raw.aep_energy_prices` | FRED energy price series (6 series) |

### Staging (SQL transformations)

| Asset | Description |
|-------|-------------|
| `staging.aep_electricity_by_source` | Ember pivoted wide, shares, YoY growth |
| `staging.aep_energy_overview` | OWID with per-capita and energy intensity metrics |
| `staging.aep_demand_growth` | Unified DC + EV + total demand timeline |
| `staging.aep_country_paradox` | Per-country renewable share vs DC intensity classification |
| `staging.aep_emissions_impact` | Estimated DC emissions by country grid intensity |

### Reports

| Asset | Description |
|-------|-------------|
| `streamlit_app.py` | 6-chart dashboard: crossover moment, AI consumption gap, country paradox, EVs vs AI, emissions irony, price signal |

## Run Commands

```bash
# Validate
bruin validate ai-energy-paradox/

# Run individual raw assets
bruin run ai-energy-paradox/assets/raw/aep_datacenter_demand.py
bruin run ai-energy-paradox/assets/raw/aep_energy_prices.py --start-date 2015-01-01 --end-date 2026-04-15
bruin run ai-energy-paradox/assets/raw/aep_owid_energy.py
bruin run ai-energy-paradox/assets/raw/aep_ember_yearly.py
bruin run ai-energy-paradox/assets/raw/aep_ev_demand.py

# Run staging
bruin run ai-energy-paradox/assets/staging/aep_electricity_by_source.sql
bruin run ai-energy-paradox/assets/staging/aep_energy_overview.sql
bruin run ai-energy-paradox/assets/staging/aep_demand_growth.sql
bruin run ai-energy-paradox/assets/staging/aep_country_paradox.sql
bruin run ai-energy-paradox/assets/staging/aep_emissions_impact.sql

# Run full pipeline
bruin run ai-energy-paradox/

# Dashboard
streamlit run ai-energy-paradox/assets/reports/streamlit_app.py
```

## Limitations

- Ember API may be undocumented; falls back to bulk CSV download
- IEA data center projections are manually structured from published reports (not a live API)
- IEA EV Excel download URL may change; supports manual file path fallback via `AEP_EV_DATA_PATH` env var
- Country-level data center electricity shares are only available for a handful of countries (Ireland, Singapore, US)
- Emissions impact estimates distribute global DC demand proportionally by generation share — actual DC locations may differ
