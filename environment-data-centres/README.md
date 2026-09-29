# Environment: US Data Centres

Tests four claims about US data centres against open facility, grid-emissions and
drought data. Most are rejected or only partly supported by the evidence; that is
reported as the result rather than reframed.

`PLAN.md` holds the full research groundwork: a catalogue of roughly 40 candidate data
sources across seven domains, ten hypotheses, and the methodology traps that make data
centre analysis go wrong. This pipeline implements the first tier of that plan.

## What is tested

| ID | Claim | Result |
|---|---|---|
| Count disagreement | Public inventories agree on how many US data centres exist | **Rejected.** Counts range from 937 to 5,115, a factor of 5.5, driven by definition rather than data quality |
| H1 | Data centres are sited on dirtier-than-average grids | **Rejected.** Hosting grids land between 6.7% below and 1.0% above the US average of 349.7 gCO2e/kWh across every subset tested |
| H9 | A large share of announced capacity is never built | **Not supported, heavily caveated.** Cancellation runs 4-10% per cohort, but a curated tracker cannot see quiet abandonment, so this is a floor |
| H3 | Capacity concentrates in drought-prone counties, and cooling does not adapt | **Split.** Facility counts show no drought skew, but capacity-weighted exposure runs 18% above the all-county baseline - the largest projects skew dry. The cooling half is **rejected**: evaporative cooling is least common in the driest quartile (3.4% vs 12.8% in the wettest) |

## Data sources

| Source | What | Licence | Access |
|---|---|---|---|
| [Compute Atlas](https://www.compute-atlas.com/) | 937 source-cited US facilities: status, capacity, operator, county, cooling, subsidies, jobs, status history | CC-BY-4.0 | Public JSON API, no auth |
| [FracTracker Alliance](https://fractracker.org/data-centers/) | 1,668 facilities: MW range, cooling type, generators, acreage, community pushback | Free for non-commercial use with credit | ArcGIS FeatureServer |
| [OpenStreetMap](https://www.openstreetmap.org/) | 1,758 US features tagged `telecom=data_center` or `building=data_center` | ODbL | Overpass API |
| [EPA eGRID2023](https://www.epa.gov/egrid) | Subregion and plant-level output emission rates, generation mix | Public domain | XLSX, file `egrid2023_data_rev2.xlsx` (June 2025 revision) |
| [US Drought Monitor](https://droughtmonitor.unl.edu/) | Weekly county drought severity by area percentage, 2015-2025 | Free with attribution to NDMC, USDA and NOAA | REST API, one request per county |
| [US Census gazetteer](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html) | County FIPS, names, land area, internal points | Public domain | 2024 national county file |

`PLAN.md` catalogues the sources not yet ingested, including EIA, USGS water use, NLCD,
Sentinel-2, EJScreen, QCEW and the Good Jobs First Subsidy Tracker.

## Assets

### Raw

| Asset | Description |
|---|---|
| `raw.edc_compute_atlas_facilities` | Compute Atlas facilities, flattened from nested JSON |
| `raw.edc_fractracker_facilities` | FracTracker tracker, paginated from ArcGIS |
| `raw.edc_osm_data_centres` | OSM data centre features via Overpass, centroids for ways and relations |
| `raw.edc_egrid_subregion_rates` | eGRID sheet SRL23, subregion emission rates plus a g/kWh conversion |
| `raw.edc_egrid_plants` | eGRID sheet PLNT23, plant coordinates used for subregion assignment |
| `raw.edc_drought_county` | Weekly USDM county statistics, 2015-2025. Roughly 40 minutes to run |
| `raw.edc_county_reference` | Census county gazetteer with a normalised name for joining |

### Staging

| Asset | Description |
|---|---|
| `staging.edc_facility_spine` | 2,372 deduplicated facilities from Compute Atlas plus FracTracker, with county FIPS |
| `staging.edc_facility_grid` | Each facility assigned to an eGRID subregion by nearest generating plant |
| `staging.edc_inventory_comparison` | Count disagreement, plus overlap as a function of match radius |
| `staging.edc_h1_carbon_intensity` | H1 headline: six subsets and two weightings against the national average |
| `staging.edc_h1_subregion_detail` | Per-subregion capacity share against grid intensity |
| `staging.edc_h9_attrition` | H9: cohort conversion by first-proposed year, plus the capacity pipeline |
| `staging.edc_county_drought_index` | Per-county drought exposure index and national quartile |
| `staging.edc_h3_drought_siting` | H3: siting exposure, tier distribution, cooling mix by drought tier |

### Dashboard

`dashboard-dac/` - Bruin DAC, nine charts each wrapped in a header and a footnote
carrying sources, tools and limitations. 29 widgets, all passing `dac check`.

## Run

```bash
bruin validate environment-data-centres/

# Raw. The drought asset takes ~40 minutes; the US Drought Monitor API
# rate-limits at roughly 1.4 counties per second regardless of concurrency.
bruin run environment-data-centres/assets/raw/edc_compute_atlas_facilities.py
bruin run environment-data-centres/assets/raw/edc_fractracker_facilities.py
bruin run environment-data-centres/assets/raw/edc_osm_data_centres.py
bruin run environment-data-centres/assets/raw/edc_county_reference.py
bruin run environment-data-centres/assets/raw/edc_egrid_subregion_rates.py
bruin run environment-data-centres/assets/raw/edc_egrid_plants.py
bruin run environment-data-centres/assets/raw/edc_drought_county.py

# Everything
bruin run environment-data-centres/

# Dashboard
dac validate --dir environment-data-centres/dashboard-dac
dac check --dir environment-data-centres/dashboard-dac
dac serve --dir environment-data-centres/dashboard-dac --port 8321
```

Dashboard at [http://localhost:8321](http://localhost:8321).

### Useful environment variables

| Variable | Purpose |
|---|---|
| `EDC_COUNTY_LIMIT` | Cap the number of counties in the drought asset for a smoke test |
| `EDC_DROUGHT_START` / `EDC_DROUGHT_END` | Drought date range, `M/D/YYYY` |
| `EDC_DROUGHT_WORKERS` | Thread count, default 8. Raising it does not help; the API rate-limits server-side |
| `EDC_EGRID_URL` | Override when EPA publishes a new eGRID edition |
| `EDC_OVERPASS_URL` | Alternative Overpass mirror |

## Limitations

- **No authoritative facility census exists.** Every inventory used here is a partial
  reconstruction with its own coverage bias, and they overlap far less than expected:
  only 164 of 937 Compute Atlas facilities match a FracTracker record under the spine's
  1 km rule.
- **Field coverage is uneven.** Across the 937 Compute Atlas records: coordinates and
  county 100%, capacity 46%, cooling type 15%, subsidies 10%, reported water 2%, PUE
  under 1%. Capacity-weighted results rest only on the reporting subset.
- **Capacity is not energy.** MW figures are announced or nameplate capacity. Converting
  to MWh needs utilisation and PUE assumptions that would dominate the result, so no
  energy or absolute emissions figure is published here.
- **Average, not marginal, emission rates.** eGRID answers how dirty a grid region is,
  not what additional load would emit. The marginal question needs NREL Cambium and is
  not addressed.
- **Grid assignment is a proxy.** Facilities are mapped to an eGRID subregion by nearest
  generating plant, which is unreliable near subregion boundaries. A subset excluding
  sites over 25 km from their assigned plant is reported and does not change H1.
- **H9 measures a survivorship-biased sample.** A curated tracker records projects that
  reached public attention; projects announced and quietly dropped before curation began
  are absent, so the measured cancellation rate is a floor.
- **Independent cities.** Virginia, Maryland and Missouri have independent cities sharing
  a name with a neighbouring county. Bare county names resolve to the county.
- **Cooling analysis is small-n and possibly self-selecting.** Cooling type is reported
  for 191 of 2,372 facilities (~8%), and facilities under permit scrutiny or community
  opposition are likelier to have it documented - which may correlate with water-stressed
  areas and could itself generate the observed pattern.
- **Drought exposure is time-mismatched.** The 2015-2025 drought index is applied to
  facilities regardless of build date, so a 2024 facility is scored on drought that
  largely predates it.
- **No water-use, land-cover or equity analysis yet.** Those sources are catalogued in
  `PLAN.md` but not ingested.
