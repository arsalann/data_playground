Build a new pipeline for **US Public Transit — COVID Recovery & Spending Efficiency**.

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

This pipeline uses bulk CSV downloads (not APIs) from the US National Transit Database and the US Census Bureau.

**1. US National Transit Database (NTD) — Federal Transit Administration**
- Download page: https://www.transit.dot.gov/ntd/ntd-data
- Key files to download:
  - **Monthly Module (Adjusted Data Release)**: https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release — monthly ridership (UPT) and vehicle revenue miles (VRM) by agency and mode. CSV. ~900 agencies, 2002–present.
  - **TS1.1 Total Ridership Time Series**: https://www.transit.dot.gov/ntd/data-product/ts11-total-ridership-time-series — pre-aggregated national ridership totals by mode and month.
  - **Annual Database — Service**: https://www.transit.dot.gov/ntd/data-product/2022-annual-database-service — annual operational data: ridership, vehicle revenue hours/miles, route miles, stations. CSV.
  - **Annual Database — Operating Expenses**: https://www.transit.dot.gov/ntd/data-product/2022-annual-database-operating-expenses — operating expenses by agency and mode. CSV.
  - **Annual Database — Capital Expenses**: capital spending by agency. CSV.
  - **Agency Information**: https://www.transit.dot.gov/ntd/data-product/2022-annual-database-agency-information — agency metadata: name, city, state, UZA (urbanized area), service area population, service area sq miles. CSV.
- All files are free, public domain, no authentication required.
- Metric: "Unlinked Passenger Trips" (UPT) — each boarding counts as one trip. A ride with a transfer = 2 UPTs. This is the standard US transit ridership metric.
- NTD mode codes: `HR` (heavy rail/subway), `LR` (light rail), `CR` (commuter rail), `MB` (bus), `DR` (demand response/paratransit), `CB` (commuter bus), `TB` (trolleybus), `FB` (ferryboat), `SR` (streetcar), `IP` (inclined plane), `CC` (cable car).

**2. US Census American Community Survey (ACS) — Table B08301**
- API: https://api.census.gov/data/{year}/acs/acs1?get=NAME,B08301_001E,B08301_010E,B08301_019E,B08301_021E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*
- Free API key required: https://api.census.gov/data/key_signup.html
- Fields: `B08301_001E` (total workers), `B08301_010E` (public transit commuters), `B08301_019E` (walked), `B08301_021E` (worked from home).
- Granularity: metropolitan statistical area (MSA). Time range: 2005–2023 (1-year ACS).
- Use this to get transit mode share (% of commuters using transit) and WFH rate by metro area.

No credentials needed beyond a free Census API key. NTD data is direct CSV download.

### What to extract

**From NTD Monthly Module:**
- Monthly ridership (UPT) and vehicle revenue miles (VRM) by NTD agency ID and mode, January 2002 – latest available.
- This is the core dataset for COVID recovery analysis — compare each agency/mode's monthly ridership to its 2019 baseline.

**From NTD Annual Database:**
- Per agency per year: ridership (UPT), vehicle revenue miles (VRM), vehicle revenue hours (VRH), operating expenses, fare revenue, directional route miles, number of stations, fleet size (vehicles operated in maximum service, VOMS).
- Agency metadata: name, city, state, UZA name, UZA population, service area population, service area sq miles.
- Derive: operating cost per trip, fare recovery ratio (fare revenue / operating expense), trips per capita, trips per route-mile, cost per vehicle revenue hour.

**From Census ACS:**
- Transit commuter share and WFH rate by MSA, 2005–2023. One row per MSA per year.

### Naming

All asset/table names should have prefix `**transit_`**
Destination: BigQuery

### Dashboard questions

The goal is a final dashboard that answers:

1. **How has US transit ridership recovered post-COVID by mode?** Show monthly ridership indexed to January 2019 = 100 for each mode (heavy rail, light rail, commuter rail, bus). Which modes have recovered? Which are permanently lower? Bus vs rail divergence.
2. **Which metro areas bounced back the fastest, and which are still lagging?** Rank the top 50 US metro areas by % recovery (2024 vs 2019 ridership). Correlate recovery rate with WFH rate (from Census ACS) — do cities with higher WFH rates have worse transit recovery?
3. **Does spending more on transit get more riders?** Scatter plot of operating expense per capita vs trips per capita across US metro areas. Identify outliers — which systems are most/least cost-efficient? Is there a diminishing returns curve?
4. **How does farebox recovery vary across systems?** What % of operating costs does fare revenue cover? Which agencies are most/least self-sustaining? How has this changed post-COVID?

### Build order

Start building the pipeline:

1. Create pipeline structure (`pipeline.yml`, `README.md`) with schedule `weekly` and start_date `"2002-01-01"`
2. Create raw Python assets:
   - `transit_ntd_monthly.py` — download and parse NTD Monthly Module CSVs into BigQuery. Use `create+replace` strategy (the NTD publishes complete updated files, not incremental). Parse the wide-format CSV (months as columns) into long format (one row per agency/mode/month).
   - `transit_ntd_annual.py` — download and parse NTD Annual Database files (service, operating expenses, agency info). Join them by NTD ID. Use `create+replace`.
   - `transit_census_acs.py` — fetch ACS Table B08301 by MSA for each year 2010–2023. Use `create+replace`. Requires Census API key (store in `.bruin.yml` or use environment variable).
3. Test each raw asset individually for a small subset — for monthly, just verify the CSV parses and loads correctly. For ACS, test with a single year (2023).
4. Build staging SQL assets:
   - `transit_ridership_recovery.sql` — monthly ridership indexed to 2019 baseline by agency and mode. Calculate recovery % = (current month UPT / same month 2019 UPT) * 100.
   - `transit_agency_efficiency.sql` — annual agency-level efficiency metrics: cost per trip, fare recovery ratio, trips per capita, trips per route-mile. Join with agency metadata for city/state/UZA.
   - `transit_metro_comparison.sql` — aggregate to metro area level (sum agencies in same UZA), join with Census ACS for WFH rate. One row per metro area per year.
5. Test the entire pipeline end-to-end.

### Constraints

- NTD Monthly Module CSV is in **wide format** (one column per month). You must pivot it to long format (one row per agency/mode/month) during ingestion. Check the actual CSV header structure before writing the parser — it changes slightly between releases.
- NTD Annual Database has **separate files** for service, operating expenses, and agency info. Join them by NTD ID in staging, not in the raw asset.
- The NTD uses "UZA" (Urbanized Area) as its geographic unit, while Census uses "MSA" (Metropolitan Statistical Area). These don't map 1:1. For the metro-level comparison, use UZA name as the join key and document which metros couldn't be matched.
- Use `create+replace` strategy for all raw assets — NTD publishes complete snapshots, not incremental updates.
- Download NTD CSV files directly from their URLs in Python (use `requests` + `pandas`). Don't require the user to manually download files.
- For the Census API, handle the free API key via an environment variable `CENSUS_API_KEY`. Document this in the README.
