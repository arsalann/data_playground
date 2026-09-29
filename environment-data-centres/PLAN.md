# US Data Centres and the Environment - Research Plan

Research groundwork for a visualisation project on US data centres and their environmental impact.
This document maps the data sources available, states the hypotheses worth testing, and records the
methodology traps to avoid.

**Compiled:** 2026-08-14. All "as of" dates are the source's own; verify freshness before ingestion.

> **Build status.** The first tier is implemented: see `README.md` for the pipeline and
> `dashboard-dac/` for the dashboard. Results so far:
>
> | Item | Outcome |
> |---|---|
> | Count disagreement | Built. Inventories range 937 to 5,115. Overlap between two of them varies from 15.5% at a 250 m match radius to 84.6% at 20 km, so "how much do inventories agree" is a threshold choice, not a fact |
> | H1 dirty siting | **Rejected.** Hosting grids land between 6.7% below and 1.0% above the US average of 349.7 gCO2e/kWh across six subsets and both weightings |
> | H9 announcement attrition | **Not supported, heavily caveated.** Cancellation runs 4-10% per cohort, but a curated tracker cannot observe quiet abandonment, so this is a floor |
> | H3 water-stress collision | **Split.** No drought skew by facility count; capacity-weighted exposure 18% above baseline. Cooling adaptation half **rejected** - evaporative cooling is least common where it is driest |
>
> H1's rejection contradicts arXiv 2411.09786 (548 vs 369 gCO2e/kWh). The differences are the
> inventory, the eGRID vintage and the region-assignment method; the two are not reconciled here.

---

## 1. Framing

### The question

US data centre electricity demand roughly tripled over the decade to 2023 and is projected to double
or triple again by 2028 ([LBNL 2024 US Data Center Energy Usage Report](https://eta-publications.lbl.gov/publications/2024-lbnl-data-center-energy-usage-report)).
That growth lands somewhere physical: on a specific grid, drawing from a specific watershed, on a
specific parcel of land, in a specific county whose residents pay a specific electricity rate.

This project asks **where the burden actually falls, and whether it falls evenly.**

### Four impact channels

The document is organised around the four ways a data centre touches its environment:

| Channel | Mechanism | Primary measurable |
|---|---|---|
| **Electricity to emissions** | Grid draw is met by the local generation mix | gCO2e/kWh, tonnes CO2e, marginal vs average rate |
| **Water** | Evaporative cooling (direct) + thermoelectric generation (indirect) | Gallons withdrawn/consumed, water stress overlap |
| **Land and air** | Site construction, land cover conversion, backup diesel generators | Hectares converted, NDVI change, permitted NOx |
| **Community and economics** | Rate base, tax abatements, employment, siting equity | $/kWh change, subsidy per job, EJ index percentile |

### What this project is not

- Not a complete census of US data centres. No public inventory is complete; every one has a coverage bias.
- Not a capacity forecast. Forward projections come from paid analyst databases (S&P 451, CBRE, JLL,
  Wood Mackenzie) that are outside scope. Where forecasts appear they are cited, not produced.
- Not a claim about any individual operator's compliance. Site-level attribution from public records
  carries real uncertainty and will be labelled as such.

---

## 2. Data source catalogue

Grouped A-G. Each table records what the source gives, its granularity, how to get it, and its
licence or terms. The **Confidence** column flags coverage bias and access risk.

### A. Facility inventory - who, where, how big

The hardest layer. There is no authoritative government register of US data centres. Every source
below is a partial reconstruction, and they disagree on counts by a factor of five.

| Source | Gives | Granularity | Access | Licence / terms | Confidence |
|---|---|---|---|---|---|
| [PNNL IM3 Open Source Data Center Atlas](https://im3.pnnl.gov/datacenter-atlas) | Name, lat/lon, state, county, square footage, operator, geometry type (point / building / campus) | Facility, with footprint polygons where available | [MSD-LIVE record](https://data.msdlive.org/records/65g71-a4731) as GeoPackage + CSV; DOI 10.57931/2550666. Processing notebook at [IMMM-SFA/datacenter-atlas](https://github.com/IMMM-SFA/datacenter-atlas) | ODbL, attribution required | Derived from OpenStreetMap, so coverage follows crowd-sourced tagging. Repository login may be required. DOE-backed and the cleanest open starting point |
| [OpenStreetMap Overpass API](https://wiki.openstreetmap.org/wiki/Tag:telecom%3Ddata_center) | `telecom=data_center` and `building=data_center` features, with real building polygons | Feature-level geometry | Overpass QL query, free, no key | ODbL | Same underlying data as PNNL but queryable live and refreshable. Known under-tagging of smaller and non-obvious facilities |
| [FracTracker US Data Centers Tracker](https://fractracker.org/data-centers/) | Coordinates, operator/tenant, reported MW, estimated square footage and acreage, cooling method (air / water / closed / open loop), power sourcing (grid vs dedicated plant), status (proposed / permitted / under construction / expanding / operating / suspended / cancelled), backup-generator and local-official-NDA flags | Facility | Download from the [tracker dashboard](https://fractracker.org/2026/04/open-u-s-data-centers-tracker/) | Free for non-commercial use with credit to FracTracker | 1,400+ sites, 803 in pre-development or construction as of April 2026. Compiled from permits, parcel data, FOIA responses and media. Updated continuously. **The single richest source for status and cooling method.** Advocacy-organisation provenance, so cross-check MW figures |
| [Compute Atlas](https://www.compute-atlas.com/) | The richest single open record shape found. Per facility: `status` (operational / under_construction / permitted / proposed / cancelled), `confidence` (confirmed / reported / rumored), `location` (lat, lon, city, county, state, precision), `capacityMw` (planned, operational), `energy` (source, utility, onSiteGenerationMw), `water` (coolingType, reportedMgd), `landAcres`, `subsidies`, `jobs` (construction, permanent), `investmentUsd`, `environmental` (pue, wue, gridCarbonIntensity, renewablePercent, waterStress), `community` (status), `statusHistory`, and a `sources` array on every record | Facility | Public unauthenticated JSON API: `GET /api/facilities`, `/api/facilities/{id}`, `/api/stats`, `/api/schema`. Open CORS. Verified working 2026-08-14 | CC-BY-4.0. Note `robots.txt` disallows `/api/` to crawlers while the API docs state public reads are unauthenticated - use politely and rate-limit | **Verified 2026-08-14:** 937 facilities across 50 states; 20,731 MW operational, 90,193 MW under construction, 219,593 MW planned. Status mix 462 operational / 193 under construction / 187 proposed / 56 permitted / 39 cancelled. **Field population is uneven** (see below). Smaller count than the listing sites, but every record is source-cited |
| [DataCenterMap](https://www.datacentermap.com/usa/), [Baxtel](https://baxtel.com/data-center/united-states), [datacenters.com](https://www.datacenters.com/) | Operator, address, sometimes square footage and MW | Facility | Scraping. DataCenterMap lists 4,767 US facilities; Baxtel lists ~5,115 | Commercial sites. Check `robots.txt` and ToS per site; rate-limit | Highest raw counts, but colocation-marketing oriented: many listings are small suites inside shared buildings, not independent facilities. Double-counting risk is severe |
| Business Insider air-permit database | ~1,000 facilities; power estimated from permitted backup generator nameplate capacity | Facility | [Methodology published](https://www.yahoo.com/news/business-insider-investigated-true-cost-081301902.html); dataset behind their interactive map | Editorial content | 322 facilities estimated at 40+ MWh/hour. The **method** is the valuable part and is reproducible from primary sources |
| State air-permit registries + [EPA ECHO](https://echo.epa.gov/) | Permitted generator count and nameplate kW per site, permit conditions, runtime limits | Facility, per permit | Per-state portals; ECHO has a REST API | Public records | The primary, reproducible route to per-site power capacity. Labour-intensive: 50 states with different systems. Virginia has permitted roughly 9,000 data centre generators, per [PEC](https://www.pecva.org/work/energy-work/proposed-increase-to-data-center-diesel-generator-use/) |
| Operator location pages ([Google](https://www.google.com/about/datacenters/locations/), Microsoft, Meta, AWS) + Wikipedia | Campus names, launch years, investment figures | Campus | Web pages | Public | Ground truth for the largest hyperscale campuses only. Useful for validating the open inventories |

**Compute Atlas field population, measured 2026-08-14 over all 937 records.** The schema promises far
more than the data delivers, so plan around the sparse fields rather than assuming them:

| Field | Populated | Field | Populated |
|---|---|---|---|
| `location.lat/lon` | 100% | `landAcres` | 42% |
| `location.county` | 100% | `community.status` | 39% |
| `sources` | 100% | `statusHistory` with >1 entry | 22% |
| `energy.utility` | 54% | `water.coolingType` | 15% |
| `capacityMw` (either) | 46% | `subsidies` | 10% |
| `energy.source` | 46% | `jobs.permanent` | 10% |
| | | `water.reportedMgd` | 2% |
| | | `environmental.pue` | <1% |

Location precision is `exact` for 561 records and `approximate` for 375, which matters for any
block-group or parcel-level join.

**Recommended spine:** Compute Atlas as the attribute backbone (status, capacity, operator, county,
provenance), PNNL/OSM for actual footprint geometry, FracTracker for additional status and cooling
coverage, air permits for independently derived capacity. Join on proximity plus operator, not on name
strings. Expect the three inventories to disagree; measure and report the overlap rate.

### B. Electricity - demand, supply, price

| Source | Gives | Granularity | Access | Licence |
|---|---|---|---|---|
| [EIA API v2](https://www.eia.gov/opendata/) `electricity/retail-sales` | Price, sales, revenue, customers by sector (RES/COM/IND) | State x sector, monthly and annual | REST, free API key | Public domain |
| EIA API v2 `electricity/rto/region-data` (Form EIA-930) | Hourly demand, day-ahead forecast, net generation by fuel, interchange | Balancing authority, hourly | REST, free API key. Mirrors in [PUDL](https://docs.catalyst.coop/pudl/en/stable/data_sources/eia930.html) and [Zenodo](https://zenodo.org/records/16262491) | Public domain |
| [Form EIA-861 / 861M](https://www.eia.gov/electricity/data/eia861/) | Utility-level annual and monthly sales, revenue, customer counts | Utility x state x sector | Bulk XLS/CSV | Public domain |
| Forms EIA-860 / EIA-923 | Generator inventory (nameplate, fuel, location) and monthly plant generation | Plant and generator | Bulk download | Public domain |
| [LBNL Queued Up 2026](https://emp.lbl.gov/queues) | Project-level interconnection queue: capacity, fuel, status, queue date, region. ~8,200 active projects, 1,312 GW generation + 749 GW storage as of end-2025, from 50+ grid operators covering ~98% of installed capacity | Project | Excel with codebook and 36 summary tabs | CC-BY-4.0, attribute LBNL and GridTracker |
| [ERCOT Large Load Integration](https://www.ercot.com/services/rq/large-load-integration) | Large-load interconnection requests, the closest thing to a public data-centre demand queue | Request, aggregated | PDF/XLS board and TAC reports | Public |
| PJM load forecast, capacity auction (BRA) results | Zonal load forecasts, capacity clearing prices | Zone | PJM site | Public |
| [FERC Form 714](https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric/data) | Hourly planning-area load by respondent | Planning area, hourly | Bulk download | Public |
| [LBNL 2024 US Data Center Energy Usage Report](https://eta-publications.lbl.gov/publications/2024-lbnl-data-center-energy-usage-report) | National data centre electricity 2014-2028 by equipment and space type | National | PDF, figures and tables | Public |
| [LBNL Large Load Literature Review: Data Sources](https://emp.lbl.gov/publications/2026-large-load-literature-review) | A monthly-updated meta-catalogue of large-load and data-centre data sources | n/a | PDF series | Public. **Check the latest edition before finalising this catalogue** |

### C. Emissions and air quality

| Source | Gives | Granularity | Access | Notes |
|---|---|---|---|---|
| [EPA eGRID](https://www.epa.gov/egrid) | Output emission rates (CO2, CH4, N2O, NOx, SO2) in lb/MWh, plus generation mix | Plant, state, eGRID subregion, BA | XLSX download | eGRID2023 released January 2025 is the latest edition as of that release. EPA recommends the **subregion** rate for electricity-use footprinting |
| [EPA GHGRP / FLIGHT](https://www.epa.gov/ghgreporting) | Reported facility GHG emissions, 32 industry types | Facility | [Envirofacts REST API](https://www.epa.gov/enviro/greenhouse-gas-restful-data-service) | Threshold is 25,000 t CO2e/yr, so it captures power plants but generally not data centres themselves |
| [EPA National Emissions Inventory](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei) | Criteria and hazardous pollutants including stationary internal combustion engines | County and facility, triennial | Bulk download | The route to county-level NOx/PM baselines for comparing generator burden |
| [EPA AQS](https://www.epa.gov/aqs) / [AirData](https://www.epa.gov/outdoor-air-quality-data) | Monitored ozone (parameter 44201) and PM2.5 (88101): annual and daily summaries, NAAQS exceedances | Monitor, county, CBSA | REST API + prepared CSV files, updated twice yearly | Observed air quality near data centre clusters |
| EPA [Green Book](https://www.epa.gov/green-book) nonattainment areas | NAAQS attainment status | County / partial county | Download | Needed for hypothesis 4. Northern Virginia is in ozone nonattainment |
| [NREL Cambium](https://www.nrel.gov/analysis/cambium) | Hourly long-run marginal emission rates and average rates, modelled to 2050 | Region, hourly, scenario | Scenario Viewer download | Answers "what does *additional* load emit", which average rates cannot |
| [Electricity Maps free tier](https://www.electricitymaps.com/free-tier-api) / [WattTime](https://watttime.org/) | Hourly grid carbon intensity, 200+ zones | Zone, hourly | REST API | Electricity Maps free tier gives consumption-based average intensity; WattTime's marginal endpoints are subscriber-only |

### D. Water

| Source | Gives | Granularity | Access | Notes |
|---|---|---|---|---|
| [USGS Water Use Data for the Nation](https://www.usgs.gov/mission-areas/water-resources/science/water-use-data-america) | Withdrawals by category (public supply, thermoelectric, industrial) | County (since 1985), state (since 1950), 5-year | NWIS download + API | The baseline against which data centre water use is compared |
| USGS monthly water use / Data Companion | Monthly public-supply and irrigation withdrawals and consumptive use 2000-2020; thermoelectric by plant 2008-2020 | HUC-12, county, monthly | ScienceBase | Enables seasonal analysis, which annual data hides |
| USGS 2020 industrial water use | Industrial withdrawals | County | [doi:10.5066/P14WTLVD](https://doi.org/10.5066/P14WTLVD) | Data centres sit inside the industrial category, not broken out |
| Operator sustainability reports | Water withdrawal and consumption, WUE (L/kWh), replenishment claims | Varies: [Google](https://sustainability.google/reports/) publishes city-level for 36 cities in 2024 (23 US); Microsoft and Meta report totals for owned sites; Amazon reports per-unit-of-power rather than totals | PDF and web | Voluntary, non-standardised, mostly not site-level, and generally excludes indirect water from electricity generation. Not comparable across companies without adjustment |
| [US Drought Monitor](https://droughtmonitor.unl.edu/DmData/DataDownload.aspx) | Weekly drought category D0-D4 by area and population | County, weekly, back to 2000 | CSV/JSON download + REST services | Builds a per-county drought-frequency index for siting analysis |
| [NOAA nClimDiv / nClimGrid](https://www.drought.gov/data-maps-tools/climate-division-datasets-nclimdiv) | Temperature, precipitation, heating and cooling degree days | County (since Nov 2018), climate division, monthly | NCEI download; [nClimGrid on AWS Open Data](https://registry.opendata.aws/noaa-nclimgrid/) | CDD drives cooling load and therefore both power and water intensity |

**Critical caveat:** LBNL estimates indirect water consumption from data centre electricity use was
roughly 12x the direct cooling water use in 2023. Any water analysis that stops at cooling towers
understates the footprint by an order of magnitude.

### E. Land and remote sensing (optional workstream)

| Source | Gives | Granularity | Access | Notes |
|---|---|---|---|---|
| [Annual NLCD](https://www.mrlc.gov/data/project/annual-nlcd) (MRLC/USGS) | Annual land cover, land cover change, fractional impervious surface, impervious descriptor | 30 m, annual, 1985-2025 (Collection 1.2) | EarthExplorer, ScienceBase, AWS, MRLC mosaic download | The cheapest way to answer "what was there before". No satellite processing required |
| Sentinel-2 L2A | Red and NIR bands at 10 m for NDVI; Scene Classification Layer for cloud masking | 10 m, ~5-day revisit, 2017-present | [AWS Earth Search STAC](https://earth-search.aws.element84.com/v1) or [Microsoft Planetary Computer STAC](https://planetarycomputer.microsoft.com/api/stac/v1), both free and anonymous via `pystac-client` | Enables per-site vegetation change. Requires phenology matching to avoid seasonal artefacts (see §4) |
| Landsat Collection 2 Level-2 | Surface temperature band, plus long historical record | 30 m (100 m thermal resampled), 1984-present | Same STAC catalogues, or USGS EarthExplorer | For any local heat-signature analysis |
| OSM building footprints / [Microsoft US Building Footprints](https://github.com/microsoft/USBuildingFootprints) | Building polygons | Building | GeoJSON download | Site area where the inventories lack square footage |

### F. Community, equity, economics

| Source | Gives | Granularity | Access | Notes |
|---|---|---|---|---|
| EPA EJScreen | 2-factor EJ indexes and 5-factor supplemental indexes combining environmental and demographic indicators | Census block group | **Public EPA access was discontinued 5 February 2025.** Mirrors: [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/RLR5AX), [screening-tools.com](https://screening-tools.com/epa-ejscreen) | Record the vintage explicitly; a mirror is a snapshot, not a live product |
| [Census ACS API](https://www.census.gov/data/developers/data-sets/acs-5year.html) | Population, income, race, housing | Block group, tract, county | REST, free key | Independent demographic layer if EJScreen vintage is a problem |
| [BLS QCEW](https://www.bls.gov/cew/downloadable-data-files.htm) | Establishments, employment, average weekly wage for NAICS 518210 (data processing, hosting) | County x industry, quarterly | Open CSV files + API | The reality check on job-creation claims |
| [Good Jobs First Subsidy Tracker](https://subsidytracker.goodjobsfirst.org/) | Company-level subsidy awards: programme, year, amount, location. 670,000+ entries across 1,400+ programmes, with a published data-centre deep dive | Award | Web search interface; bulk access terms to be confirmed | Many states do not disclose data centre sales-tax exemptions at all, so totals are a floor |
| County assessor / parcel records, local permit dockets | Assessed value, tax abatement terms, construction permits | Parcel | Per-county portals | Case-study depth only; not nationally scalable |
| [Sierra Club, Data Center State Policies 2026](https://www.sierraclub.org/sites/default/files/2026-01/policies-for-data-centers-2026.pdf) | State-by-state policy inventory | State | PDF | Policy context layer; advocacy provenance |

### G. Literature to build on

| Work | Relevance |
|---|---|
| [Environmental Burden of United States Data Centers in the AI Era](https://arxiv.org/abs/2411.09786) (arXiv 2411.09786) | The closest prior art. 2,132 facilities, Sep 2023-Aug 2024: 192.64 TWh (4.59% of US electricity), 105.59 Mt CO2e, carbon intensity 548 gCO2e/kWh against a 369 gCO2e/kWh national average, 56% fossil-sourced, Virginia 27% of demand. Uses Baxtel + scraped datacenters.com + OSM + eGRID 2022 + WattTime BA polygons, with a gradient-boosted tree (R2 = 0.77) imputing missing capacity. **Analyses carbon only, not water or air.** Code under the [NSAPH-Projects](https://github.com/NSAPH-Projects) org |
| [Health-Informed Computing](https://arxiv.org/pdf/2412.06288) (arXiv 2412.06288) | Public health impact framing and cost attribution |
| [Have Data Centers Raised Your Electric Bill?](https://arxiv.org/pdf/2606.19777) (arXiv 2606.19777) | Econometric approach to hypothesis 2 |
| [Environmental impact and net-zero pathways for AI servers in the USA](https://www.nature.com/articles/s41893-025-01681-y) (Nature Sustainability) | Projects a 731-1,125 million m3/yr water footprint and 24-44 Mt CO2e/yr additional emissions for US AI server deployment 2024-2030 |
| [The environmental footprint of data centers in the United States](https://iopscience.iop.org/article/10.1088/1748-9326/abfba1) (IOP, 2021) | Established the carbon-water-land triple-footprint framing |
| [Quantifying Vegetation Loss Around the Largest US Data Centers](https://milanjanosov.substack.com/p/quantifying-vegetation-loss-around) (Janosov) | Method reference for hypothesis 5: 42 largest sites by estimated consumption, phenology-matched Sentinel-2 NDVI composites 2017-2026, differenced against concentric reference rings, NLCD 2018 baseline. Reports ~51 km2 of footprint as ~60% cropland, ~26% already-developed, ~6% forest, with 23 of 42 sites showing no measurable vegetation transition. **Treated here as a claim to test independently, not a settled result** - it is a single-author blog analysis over a hand-traced 42-site sample |
| [FracTracker national tracker writeups](https://www.fractracker.org/2025/07/national-data-centers-tracker/), [WRI on community impacts](https://www.wri.org/insights/us-data-center-growth-impacts) | Qualitative grounding for which local impacts communities actually raise |

---

## 3. Hypotheses

Ten candidates. Each states the claim, the test, the data needed, what would falsify it, and difficulty.
The intent is that most of these get tested and some get rejected: a rejected hypothesis with a clear
chart is a better result than a confirmed one with a vague one.

### H1. Dirty siting

**Claim.** Capacity-weighted carbon intensity of electricity consumed by US data centres exceeds the
national grid average, because siting follows cheap land and cheap power rather than clean power.

**Test.** Map each facility to its balancing authority and eGRID subregion. Weight subregion output
emission rates by estimated facility capacity. Compare to the national average rate.

**Data.** Facility inventory (A) + eGRID subregion rates (C) + BA boundaries.

**Falsified if** the capacity-weighted rate is at or below the national average, or if the gap
disappears once Virginia is excluded (making it a one-state story, not a siting pattern).

**Difficulty.** Low-medium. arXiv 2411.09786 found 548 vs 369 gCO2e/kWh for 2023-24; replicating with
current eGRID and an independent inventory is a meaningful check rather than a novel result.

### H2. Ratepayer transfer

**Claim.** States and balancing authorities with the fastest data centre load growth have seen faster
residential electricity price growth than comparable peers.

**Test.** Build a state-year panel of residential price (EIA-861) and commercial sales growth as a
data-centre-load proxy (EIA notes Virginia commercial sales rose by nearly 30 million MWh from 2019 to
2025, largely data centres). Control for natural gas price and generation mix. Compare high-growth to
low-growth states.

**Data.** EIA retail-sales and EIA-861 (B) + facility inventory for the load proxy + Henry Hub gas prices.

**Falsified if** price growth is uncorrelated with load growth once gas prices are controlled for, which
is the finding of at least one prior study.

**Difficulty.** Medium. The identification problem is real: prices rose nationally over the same period
for reasons unrelated to data centres. Present as correlation with explicit confounders named, or not at all.

### H3. Water-stress collision

**Claim.** Data centre capacity is disproportionately sited in counties with high drought frequency,
and evaporative (water-consuming) cooling is not less common in those counties.

**Test.** Build a per-county drought-frequency index from US Drought Monitor weekly categories
(2000-2025). Compare the capacity-weighted drought exposure of data centre counties to a
population-weighted or all-county baseline. Cross-tabulate FracTracker cooling method against drought tier.

**Data.** Facility inventory with cooling method (A) + US Drought Monitor (D) + USGS county water use (D).

**Falsified if** data centre counties are no drier than baseline, or if closed-loop and air cooling are
significantly more common in high-drought counties (which would show the industry adapting).

**Difficulty.** Medium. The siting half is easy: county is populated for essentially every Compute Atlas
record. The cooling half is hard: `water.coolingType` is populated for only 15% of Compute Atlas records
and partially in FracTracker, so the cooling cross-tab is a small-n analysis. Pool both sources, state
the n on the chart, and be prepared to report the siting result alone.

### H4. Backup-generator air burden

**Claim.** Permitted diesel backup generator capacity is concentrated in counties that already fail
ozone standards, and worst-case runtime NOx would be material relative to county totals.

**Test.** Sum permitted generator nameplate capacity by county from state air permits. Convert to
worst-case annual NOx at permitted runtime hours and applicable emission factors. Compare to county NEI
NOx totals. Overlay EPA nonattainment designations and AQS monitor trends.

**Data.** State air permits / ECHO (A) + EPA NEI (C) + Green Book nonattainment (C) + AQS (C).

**Falsified if** permitted capacity is spread evenly across attainment and nonattainment counties, or if
worst-case NOx is a trivial share of county totals.

**Difficulty.** High. Requires state-by-state permit extraction. Northern Virginia is the natural
starting scope: it is in ozone nonattainment and Virginia has permitted roughly 9,000 data centre
generators. **This is a worst-case bound, not an emissions estimate** - emergency generators run few
hours per year in practice, and the chart must say so.

### H5. Cropland, not forest

**Claim.** New large data centre sites convert cropland and already-developed land far more often than
forest, contrary to the dominant public narrative.

**Test.** Intersect facility footprints with Annual NLCD land cover from the year before construction.
Tabulate converted area by class. Optionally corroborate with phenology-matched Sentinel-2 NDVI
composites differenced against a surrounding reference ring, per the Janosov method.

**Data.** Facility footprints (A) + Annual NLCD (E), optionally Sentinel-2 via STAC (E).

**Falsified if** forest is a substantially larger share than the ~6% previously reported, or if the
result flips when the sample is expanded beyond the largest sites (the prior analysis covered 42 sites,
which is small and skewed toward hyperscale rural campuses).

**Difficulty.** Low for the NLCD-only version, high with satellite processing. **The NLCD version alone
tests the claim** and needs no imagery pipeline. Start there.

### H6. Jobs per megawatt

**Claim.** Public subsidy per permanent data centre job is far higher than for typical economic
development incentives, and county-level NAICS 518210 employment barely moves after a large facility opens.

**Test.** Join Subsidy Tracker awards to facilities by company and location. Compute subsidy per
permanent job using announced job figures. Separately, build county quarterly employment series for
NAICS 518210 and look at the change around known facility opening dates.

**Data.** Good Jobs First Subsidy Tracker (F) + BLS QCEW (F) + facility inventory with opening dates (A).
Compute Atlas already carries `subsidies` (10% of records), `jobs.permanent` (10%) and `investmentUsd`,
which gives a pre-joined starting sample of roughly 90 facilities before touching Subsidy Tracker.

**Falsified if** employment does rise materially in host counties, or if subsidy-per-job is in line with
manufacturing incentives.

**Difficulty.** Medium. QCEW suppresses cells with few establishments, which will hit exactly the rural
counties of interest. Announced job counts are promotional figures, not audited. The ~90-facility
pre-joined sample is self-selected toward projects that made news, which biases upward.

### H7. Marginal-hour carbon

**Claim.** Data-centre-heavy balancing authorities have flatter load shapes, but the generation serving
their incremental load is disproportionately fossil, so marginal emissions exceed what average grid
rates imply.

**Test.** Compute load factor and hourly load shape by BA from EIA-930, split by data centre capacity
density. Compare Cambium long-run marginal emission rates to eGRID average rates for the same regions.

**Data.** EIA-930 hourly (B) + NREL Cambium (C) + eGRID (C) + facility inventory (A).

**Falsified if** marginal and average rates converge in high-density BAs, or if load shapes are
indistinguishable from low-density BAs.

**Difficulty.** Medium-high. The average-vs-marginal distinction is the single most under-reported
element of the public debate and is worth a chart of its own regardless of the outcome.

### H8. Equity gradient

**Claim.** Data centre capacity is disproportionately sited in census block groups above the median
environmental-justice burden.

**Test.** Spatial join facilities to block groups. Compare the EJScreen index distribution of host block
groups to the national distribution, capacity-weighted and unweighted.

**Data.** Facility inventory (A) + EJScreen mirror (F) + Census ACS (F).

**Falsified if** the host distribution matches national. Note that a prior review of ~700 facilities
found nearly half in tracts with above-median environmental burden - which is close to what random
siting would produce, so the framing must be careful.

**Difficulty.** Low-medium. The main risk is over-reading a weak effect. Report the full distribution,
not just the share above median.

### H9. Announcement attrition (stretch)

**Claim.** A large share of announced data centre megawatts is never built, so queue and announcement
totals systematically overstate future demand.

**Test.** Track FracTracker status transitions over time (proposed to cancelled/suspended vs operating).
Compare announced MW cohorts to completions. Cross-reference against interconnection queue withdrawal
rates from LBNL Queued Up, where withdrawal is the historical norm for generation projects.

**Data.** Compute Atlas `statusHistory` and `announcedDate` (A) + FracTracker status snapshots (A) +
LBNL Queued Up (B) + ERCOT large-load reports (B).

**Falsified if** completion rates are high, or if the status history is too thin to measure.

**Difficulty.** Medium, upgraded from high. Compute Atlas ships a `statusHistory` array, populated with
more than one entry for 22% of records (about 210 facilities), plus 39 records already marked
`cancelled`. That is a usable retrospective sample without waiting to accumulate snapshots. The scale
context is stark: 20,731 MW operational against 219,593 MW planned, so the attrition rate on that
planned pipeline is the whole question. Start capturing daily snapshots anyway to make the forward
measurement clean.

### H10. Local heat signature (stretch)

**Claim.** Large data centre campuses show a measurable land-surface-temperature uplift relative to
matched control areas.

**Test.** Extract Landsat Collection 2 surface temperature over site footprints and matched control
polygons of similar prior land cover and elevation, seasonally matched, before and after construction.

**Data.** Landsat C2 L2 via STAC (E) + facility footprints (A) + Annual NLCD for control matching (E).

**Falsified if** the uplift is within the noise of the control distribution, which is the likely outcome
given that any built surface warms and the question is whether data centres differ from generic
development.

**Difficulty.** High, and the null result is likely. Lowest priority.

### Suggested priority

| Tier | Hypotheses | Rationale |
|---|---|---|
| First | H1, H3, H9 | High signal, tractable data. H9 moves up because Compute Atlas `statusHistory` makes it testable today |
| Second | H5, H7, H8 | Strong analytical interest, moderate data assembly |
| Third | H6, H2, H4 | Real identification, sparsity and extraction difficulty; high payoff if done honestly |
| Stretch | H10 | Likely to return a null result; lowest priority |

---

## 4. Methodology cautions

These are the traps that make data centre analysis wrong. Each must be handled explicitly and
documented in the dashboard methodology section.

**MW is not MWh.** Nearly every public figure is a capacity number. Converting to energy requires
assumptions about utilisation and PUE, and those assumptions dominate the result. State them or do not
publish the number. LBNL's national report assumes data centres operate at roughly 50% of maximum.

**Nameplate is not consumption.** Backup generator capacity, IT nameplate capacity, and utility service
capacity are three different numbers. Business Insider's method assumes maximum use is 50-80% of
generator capacity and then halves it. Any capacity-derived energy estimate carries at least a 2x uncertainty band.

**Double counting across inventories.** DataCenterMap and Baxtel list colocation suites, which can put
five "facilities" in one building. PNNL/OSM tags campuses and individual buildings inconsistently.
Deduplicate spatially (proximity plus operator), never by name string. Report the dedup rate.

**Campus vs building.** A "site" may be one building or twenty on one parcel. Footprint area, land
conversion, and per-site capacity all change by an order of magnitude depending on the choice. Pick one
definition, apply it identically to every site, and log the resulting area distribution before charting
anything - the repo's geospatial rules require verifying query scope before visualising.

**Attributional vs consequential emissions.** Assigning a facility the average emission rate of its grid
region (attributional) answers a different question from asking what emissions its added load causes
(consequential/marginal). They can differ by a factor of two. Never mix them in one chart.

**Average vs marginal grid intensity.** eGRID gives average output rates. Cambium and WattTime give
marginal rates. The marginal rate is the right one for "should this data centre be built here" and the
average rate is the right one for corporate Scope 2 reporting. Label which is in use, every time.

**Direct vs indirect water.** Cooling water is the visible number. Water consumed generating the
electricity is the larger one - LBNL put the indirect figure at roughly 12x direct in 2023. A chart
showing only cooling water understates the footprint dramatically and must say so.

**Corporate disclosure is not comparable.** Google reports city-level water for owned and leased sites
but not third-party operated ones; Microsoft reports totals without site detail; Amazon reports water
per unit of power rather than absolute volume; Meta covers owned sites only. These cannot be summed or
ranked without adjustment, and the adjustment is itself an assumption.

**Crowd-sourced coverage bias.** OSM-derived inventories (and therefore PNNL) under-represent facilities
in areas with fewer OSM contributors and over-represent visually obvious hyperscale campuses. This
biases any geographic comparison. Quantify it by comparing counts against a commercial listing count per state.

**Advocacy and commercial provenance.** FracTracker is an environmental advocacy organisation; Baxtel
and DataCenterMap are industry marketing platforms. Both have incentives that shape what gets listed and
what MW figure gets attached. Cross-check headline numbers across at least two provenance types.

**Vintage drift.** eGRID2023 reflects 2023 grid conditions; EJScreen mirrors are frozen at their 2025
snapshot; NLCD runs to 2025; the facility trackers update daily. Joining them produces a mixed-vintage
result. Record every source's as-of date on the chart footnote.

---

## 5. Proposed project shape

Not built yet. Described here so the source catalogue maps onto an executable structure, following the
repo conventions in `AGENTS.md`.

```
environment-data-centres/
├── PLAN.md                    # this file
├── pipeline.yml
├── README.md
├── assets/
│   ├── raw/                   # one Python asset per source
│   └── staging/               # SQL: facility spine, county rollups, hypothesis tables
└── dashboard-dac/
    └── dashboards/
        ├── <dashboard>.yml
        └── queries/*.sql
```

**Suggested build order.**

1. **Inventory spine.** Compute Atlas API for attributes, PNNL/OSM Atlas and Overpass for footprint
   geometry, FracTracker for extra status and cooling coverage. Deduplicate to one facility table with a
   provenance column per field. Everything else joins to this. Nothing downstream is trustworthy until
   the dedup and cross-source agreement rates are measured. Snapshot the Compute Atlas API daily from
   day one so H9 gets a forward series.
2. **Grid layer.** eGRID subregion rates, BA boundaries, EIA-930 hourly demand, EIA retail prices.
   Unlocks H1, H2, H7.
3. **Water and land layer.** US Drought Monitor county index, USGS county water use, Annual NLCD.
   Unlocks H3, H5.
4. **Community layer.** EJScreen mirror, QCEW, Subsidy Tracker. Unlocks H6, H8.
5. **Dashboard.** Bruin DAC, per `DAC.md` and `VISUALIZATIONS.md`. MapLibre for the siting map.

**First three sources to ingest**, chosen for signal per unit of effort: the Compute Atlas API (one
unauthenticated call gives 937 facilities with county, status, capacity and provenance), EPA eGRID, and
the US Drought Monitor. Those three test H1, H3 and H9 end to end without a single scrape or satellite
tile. Add PNNL/OSM next when footprint geometry is needed for H5.

---

## 6. Open questions

1. **Which hypotheses to prioritise.** The suggested first tier is H1, H3, H5. Confirm or reorder.
2. **Scraped commercial inventories in phase 1?** DataCenterMap/Baxtel/datacenters.com give the highest
   raw counts but carry the worst double-counting risk. They can be deferred to a validation role rather
   than being part of the spine.
3. **Headline geographic unit.** Facility, county, or balancing authority. This determines the shape of
   every chart and should be settled before staging tables are written.
4. **Time scope.** A snapshot of current state, or a time series showing the buildout? The latter needs
   historical tracker snapshots that mostly do not exist, and would lean on NLCD and interconnection
   queue vintages instead.
5. **Inventory count discrepancy.** Compute Atlas has 937 source-cited facilities, FracTracker 1,400+,
   PNNL/OSM a different set again, and the commercial listings 4,700-5,100. That five-fold spread is
   itself a finding worth charting, and the project should decide early whether to present one
   reconciled spine or to show the disagreement explicitly as a data-quality story.
