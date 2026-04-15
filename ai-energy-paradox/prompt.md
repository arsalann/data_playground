Build a new pipeline for **AI Energy Paradox — Is AI eating the clean energy transition?**

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Thesis

Renewables overtook coal globally for the first time in H1 2025 (34.3% vs 33.1%). But AI data center energy demand is projected to double by 2030 (415 TWh → 945 TWh). Ireland's data centers already consume 21% of national electricity. AI-optimized servers alone are forecast to go from 93 TWh to 432 TWh. The question: are AI's energy demands canceling out the gains from the renewable transition?

This pipeline combines global electricity generation data, AI/data center energy consumption estimates, EV adoption figures, and energy commodity prices to quantify the paradox and show where the tipping points are — by country, by year, and by energy source.

---

### Data sources

#### 1. Ember Global Electricity Data (primary)

- **What:** Annual and monthly electricity generation by source, capacity, demand, and emissions for 215 countries
- **Access:** Free API at `https://ember-energy.org/data/api/` — also available as bulk CSV downloads from `https://ember-climate.org/data-catalogue/yearly-electricity-data/`
- **License:** CC-BY-4.0
- **Freshness:** Updated biweekly; 2024 complete for 88 countries covering 93% of global demand
- **Key fields:**
  - Country, year (annual data back to 2000+)
  - Generation by source (TWh): coal, gas, oil, nuclear, hydro, wind, solar, bioenergy, other renewables
  - Total generation, total demand
  - Capacity (GW) by source
  - CO2 emissions from electricity (MtCO2)
  - Emissions intensity (gCO2/kWh)
  - Share of generation by source (%)
- **Endpoints to try:**
  - `/electricity-generation/yearly` — generation by country, year, source
  - `/electricity-generation/monthly` — monthly granularity for recent years
  - `/carbon-intensity/yearly` — emissions intensity by country
  - `/electricity-demand/yearly` — demand data
- **Fallback:** If the API is undocumented or rate-limited, download the bulk yearly CSV from `https://ember-climate.org/data-catalogue/yearly-electricity-data/` and ingest via Python

#### 2. Our World in Data — Energy Dataset (supplement/cross-check)

- **What:** Pre-cleaned, well-documented energy CSVs combining Ember, BP Statistical Review, and EIA data
- **Access:** GitHub CSV at `https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv`
- **License:** CC-BY
- **Freshness:** Continuously updated, mirrors Ember + other sources
- **Key fields:**
  - Country, year, ISO code, population, GDP
  - Primary energy consumption (TWh)
  - Electricity generation by source (TWh and share %)
  - Per capita energy and electricity metrics
  - Carbon intensity of electricity (gCO2/kWh)
  - Energy mix shares (fossil, nuclear, renewables)
- **Why include:** Provides GDP, population, and per-capita normalization that Ember alone doesn't have. Also has longer historical series (back to 1900 for some countries). Useful for cross-checking Ember values.
- **Ingestion:** Direct CSV download via Python `requests`, parse with pandas

#### 3. IEA Data Center and AI Energy Estimates (contextual/derived)

- **What:** IEA projections on data center and AI-specific electricity demand
- **Access:** The IEA publishes projections in reports (not a live API). Key figures from "Energy and AI" report (Jan 2025):
  - Global data center electricity demand: 415 TWh (2024) → 945 TWh (2030 projected)
  - AI-optimized servers specifically: 93 TWh (2024) → 432 TWh (2030 projected)
  - Data centers as share of global electricity: ~1.5% (2024) → ~3.4% (2030)
  - US data centers: ~4% of national electricity (2024) → projected 6-12% by 2030
  - Ireland: 21% of national electricity consumed by data centers (2024)
  - Singapore: ~7% of electricity to data centers
- **Ingestion approach:** Create a reference/seed table with these published IEA projections as structured data (year, metric, value, source). This acts as the "AI demand" overlay on top of Ember's supply-side data.
- **Additional data points to include in seed table:**
  - Goldman Sachs estimate: US data center power demand to grow 160% by 2030
  - EPRI estimate: US data centers 4.4% of electricity by 2030 (up from 1.5% in 2023)
  - Individual hyperscaler energy usage where publicly reported (Google: 25.3 TWh in 2024, Microsoft: ~24 TWh, Meta: ~14 TWh)

#### 4. IEA Global EV Data Explorer (cross-domain)

- **What:** EV sales, stock, charging infrastructure, and battery demand by country
- **Access:** Excel download from `https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer` — file: `EVDataExplorer2025.xlsx`
- **License:** CC-BY-4.0
- **Freshness:** 2024 actuals, 2025-2030 projections (Global EV Outlook 2025)
- **Key fields:**
  - Country, year
  - EV sales (BEV + PHEV), total car sales, EV sales share (%)
  - EV stock (fleet), charging points
  - Electricity demand from EVs (TWh)
- **Why include:** EVs are the OTHER major new electricity demand driver alongside data centers. Combine with Ember to show: how much new renewable generation is being consumed by EVs vs. data centers vs. displacing fossil fuels. China hit 50% EV sales share in 2025.
- **Ingestion:** Download Excel, parse with pandas/openpyxl

#### 5. FRED — Energy Prices (macro context)

- **What:** Energy commodity prices and related economic indicators
- **Access:** FRED API at `https://api.stlouisfed.org/fred/series/observations` — free API key required (already configured from hormuz-effect pipeline)
- **Freshness:** Updated daily/monthly, data through March/April 2026
- **Series to pull:**
  - `DCOILBRENTEU` — Brent crude oil (daily)
  - `DCOILWTICO` — WTI crude oil (daily)
  - `GASREGW` — US regular gasoline price (weekly)
  - `DHHNGSP` — Henry Hub natural gas spot price (daily)
  - `CPIENGSL` — CPI Energy component (monthly)
  - `PCU2211--2211--` — Electric power price index (monthly, if available)
- **Ingestion:** Python with `requests`, same pattern as hormuz-effect pipeline. Use append strategy.

Credentials are in @.bruin.yml (the FRED API key should be set as env var `FRED_API_KEY` — check hormuz-effect for the pattern)

---

### What to extract

**From Ember (annual):**
- Electricity generation by source (TWh) for all available countries, 2000-2024
- Electricity demand (TWh) by country
- CO2 emissions and emissions intensity by country
- Installed capacity (GW) by source and country

**From Our World in Data:**
- Full energy dataset CSV — all countries, all years available
- Key columns: country, year, iso_code, population, gdp, electricity generation by source, per capita metrics, carbon intensity

**From IEA Data Center estimates:**
- Structured seed table of published projections (year, category, metric_name, value, unit, source)
- Include both historical actuals and forward projections (2020-2030)

**From IEA EV Data:**
- EV sales, stock, and electricity demand by country and year (2010-2024 actuals, 2025-2030 projections)

**From FRED:**
- Daily/weekly/monthly energy price series, 2015-present

---

### Naming

All asset/table names should have prefix **`aep_`** (ai-energy-paradox)
Destination: BigQuery

---

### Dashboard questions

The goal is a final Streamlit dashboard that answers:

1. **The crossover moment** — When exactly did renewables overtake coal globally? Show the trajectory: how fast is the gap widening (or narrowing)?
2. **The AI consumption gap** — How much of the new renewable generation is being consumed by data centers? Overlay IEA data center demand projections onto Ember's renewable generation growth to show the "gap" — is AI demand growing faster than renewables?
3. **The country paradox** — Which countries are winning vs. losing the race? Show per-country renewable share vs. data center electricity share. Highlight paradox countries: Ireland (high renewables AND high DC consumption), Singapore, Netherlands, Virginia (US state-level if data available).
4. **EVs vs. AI — who eats more?** — Compare electricity demand growth from EVs vs. data centers over time. Which new demand source is larger? Which grows faster?
5. **The emissions irony** — Are data centers in coal-heavy grids (India, Poland, Indonesia) actually increasing emissions despite the global renewable trend? Show emissions intensity by country overlaid with data center growth.
6. **The price signal** — How do energy prices correlate with the renewable buildout? Are countries with cheaper electricity building more data centers?

---

### Build order

Start building the pipeline:

1. Create pipeline structure (`pipeline.yml`) and a README outlining the pipeline, data sources, and processing method
2. Create the Python extraction assets:
   - `aep_ember_yearly.py` — Ember annual electricity data (try API first, fall back to CSV)
   - `aep_owid_energy.py` — Our World in Data energy CSV download
   - `aep_datacenter_demand.py` — IEA data center projections seed table
   - `aep_ev_demand.py` — IEA EV data (Excel download and parse)
   - `aep_energy_prices.py` — FRED energy price series
3. Test each raw asset individually for a small subset (e.g., 2 countries for Ember, 3 FRED series)
4. Build the staging SQL transformation assets:
   - `aep_electricity_by_source.sql` — clean Ember data, pivot to wide format, compute shares and YoY growth
   - `aep_energy_overview.sql` — join OWID data for GDP/population normalization, compute per-capita metrics
   - `aep_demand_growth.sql` — combine data center projections + EV demand + total demand growth into unified timeline
   - `aep_country_paradox.sql` — per-country renewable share vs. data center intensity, emissions intensity, classify countries
   - `aep_emissions_impact.sql` — compute emissions with/without data center demand by country
5. Test staging assets, verify joins and derived metrics
6. Build the Streamlit dashboard with 6 visualizations matching the dashboard questions
7. Test the entire pipeline end-to-end for full historical range

---

### Constraints

- Ember API may be undocumented or have rate limits — start with the API, but be ready to fall back to bulk CSV download
- The IEA data center projections are NOT from a live API — they must be manually structured into a seed table from published report figures. Cite the source report and publication date for each data point.
- IEA EV Excel download may require navigating their data explorer UI to get the file — document the exact download steps in the asset README or comments
- FRED API key: reuse the same env var pattern from `hormuz-effect` pipeline (`FRED_API_KEY`)
- Use `append` strategy for FRED time series, `create+replace` for Ember/OWID (they provide full snapshots)
- The OWID CSV is ~40MB — be mindful of memory when parsing. Filter to relevant columns early.
- For the seed table (IEA projections), include a `source` column with report name and date so all numbers are traceable
- When comparing countries, normalize by population or GDP where appropriate — absolute TWh comparisons between China and Ireland are meaningless without context
