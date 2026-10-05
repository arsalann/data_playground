# Cities Shops Map - retail site selection on official registers

> **This document is the pre-implementation appraisal and plan, kept as written.** Building it
> turned up eight corrections to the figures below, and the score matrix in this file is
> superseded as a result. The most consequential: London has no bakery class at all, so the 74
> scored below credits a universe that does not exist; and Paris does have a dedicated nightclub
> class (`SA404`) and bookshop class (`CE101`), both of which sit outside the `CH` restauration
> block this appraisal searched. See `README.md` § "Corrections found while building" for the
> full list, and `report.coverage_matrix` for the current scores.

## Why this project

The reference project, [AzeemPatil/kadikoy-cafe-site-selection](https://github.com/AzeemPatil/kadikoy-cafe-site-selection), ranks candidate cafe sites in one Istanbul district using only OpenStreetMap: 744 competitor cafes, 626 candidate points, a distance-decay competition metric and an OSM-derived demand proxy, blended 55% demand / 45% low-competition.

The method is sound. The data is the weak point. OSM completeness is unmeasured and, for a given district, unmeasurable, so the ranking cannot be audited: a cell may score well because it is genuinely underserved, or because nobody mapped the bars on that street.

This project rebuilds the same analysis on **official administrative and statistical registers**, across **five cities and every shop type each register can actually resolve**.

The data-quality appraisal is the first deliverable, not a preamble. Whether a site score means anything depends entirely on two things: whether the competitor universe is complete, and whether the target shop type is separable in the source taxonomy. Both are measurable, and both are measured below.

**Output**: a Bruin/BigQuery pipeline plus a standalone MapLibre map with a city and shop-type selector, where every city x shop-type combination carries its own verified caveats. No DAC dashboard.

---

## Part 1 - Dataset appraisal

### Scoring rubric

| Criterion | Weight | Score 0 | Score 10 |
|---|---:|---|---|
| Source authority and collection method | 20 | scraped listing, crowd-sourced | national statistics institute or statutory register, field-verified |
| Spatial coverage | 20 | one neighbourhood | entire municipality or metro, every premise including vacant |
| **Category granularity for that shop type** | **25** | shop type not separable from others | dedicated code, sub-types distinguished |
| Freshness and cadence | 15 | more than 5 years stale | updated daily |
| Geocoding quality | 10 | address text only | per-premise coordinates plus a small-area unit |
| Access and licence | 10 | scraping required, restrictive licence | open licence, direct bulk or API, no auth |

Granularity carries the most weight because it is the criterion that most often silently invalidates this kind of study. London's register is otherwise close to perfect, and it cannot tell a cafe from a restaurant.

Every figure in this document was verified against the live source in August 2026. Where a number is a count, it came from querying the API or streaming the file, not from documentation.

### City base scores

All criteria except granularity, out of 75.

| City | Source | Authority | Coverage | Freshness | Geocoding | Access | Base |
|---|---|---:|---:|---:|---:|---:|---:|
| **Madrid** | [Censo de locales](https://datos.madrid.es/dataset/200085-0-censo-locales), Ayuntamiento de Madrid | 9 | 10 | 10 | 10 | 8 | **71** |
| **Mexico City** | [INEGI DENUE 05/2026](https://www.inegi.org.mx/app/mapa/denue/) | 10 | 8 | 9 | 10 | 9 | **68.5** |
| **Chicago** | [Business Licenses](https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses-Current-Active/uupf-x98q), BACP | 9 | 8 | 10 | 8 | 10 | **67** |
| **Paris** | [APUR BDCOM 2023](https://opendata.apur.org/datasets/bdcom-2023) | 10 | 10 | 6 | 10 | 8 | **67** |
| **London** | [FSA Food Hygiene Rating Scheme API](https://api.ratings.food.gov.uk/help) | 9 | 8 | 10 | 7 | 10 | **66** |

**Madrid.** 203,612 premises and 225,461 activity rows across 455 distinct `desc_epigrafe` values. `fx_carga` was 2026-08-13 when checked, confirming the daily cadence. Carries `coordenada_x/y_local` (EPSG:25830) plus census section, barrio and district; `desc_situacion_local` gives open/closed; per-premise opening hours; and a companion terraces file with terrace surface area and hours. CC BY 4.0. Docked on access because the files are 85-119 MB CSVs with no filtered API and the CKAN endpoint returns 404, so resource URLs have to be hardcoded. Docked on authority because the portal itself warns the data is "an administrative registry without statistical processing" that "may contain inconsistencies".

**Mexico City.** 462,732 establishments across all 16 alcaldías, and **100% carry lat/lon** (verified row by row, not sampled), plus AGEB and manzana identifiers, a `per_ocu` employee-size band and `fecha_alta`. Direct 45 MB bulk zip, no auth, `Last-Modified: 2026-05-20`. Docked on coverage: fixed establishments only, so informal and street commerce is excluded, which in Mexico City is a large share of food and drink.

**Chicago.** All 77 community areas, daily, Socrata API with no key required. One business can hold several licences, so records must be deduplicated on `account_number` plus `site_number`. The `business_activity` field is considerably more informative than `license_description`.

**Paris.** 83,154 premises, of which 60,845 are active shops, from a June 2023 door-to-door field survey by roughly 20 surveyors. ODbL, exposed as a queryable ArcGIS REST layer. It is the only source in the shortlist that ships **vacant premises** (`Local vacant`) as records, which means real candidate sites at real addresses rather than synthetic grid points. It also carries `surfexacte` floor area, a `reseau` chain-membership flag, and per-premise activity codes for 2000, 2003, 2005, 2007, 2011, 2014 and 2017, giving premise-level churn history. `idcar_200m` pre-joins each premise to the INSEE 200 m statistical grid. Docked on freshness: the survey is triennial and the 2026 wave is not yet published.

**London.** 81,170 food establishments across all 33 boroughs, with `LastPublishedDate` of 2026-08-14 when checked. Open REST API, no key, Open Government Licence. Docked on geocoding because coordinates are address-derived with postcode-centroid fallbacks and known nulls. Docked on coverage because it is a food register: no retail, no vacancy, no floor area.

### The city x shop-type matrix

Cell score = city base + (granularity x 2.5). A dash means the source has no coverage of that shop type at all.

| City | bar / pub | nightclub | cafe | restaurant | fast food / takeaway | bakery / pastry | bookstore |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Madrid** | **96** | **96** | **96** | **96** | 93 | **96** | 93 |
| **Mexico City** | 93 | 93 | 89 | 93 | 91 | 89 | 91 |
| **Paris** | 80 | 75 | 85 | 92 | 92 | 90 | 90 |
| **Chicago** | 90 | 87 | 72 | 82 | 82 | 72 | 72 |
| **London** | 91 | 74 | 71 | 76 | 91 | 74 | - |

### Granularity evidence per city

This section becomes the caveat text in `report.coverage_matrix`, which the map reads directly. Nothing here is asserted without a count behind it.

**Madrid - 10/10 on every type.** Open-premise counts from the full activity file:

| Class | Open premises |
|---|---:|
| `BAR RESTAURANTE` | 4,548 |
| `BAR CON COCINA` | 3,946 |
| `CAFETERIA` | 3,199 |
| `RESTAURANTE` | 2,809 |
| `BAR SIN COCINA` | 1,555 |
| `BAR ESPECIAL SIN ACTUACIONES` | 730 |
| `RESTAURANTES DE COMIDA RAPIDA` | 726 |
| `COMERCIO AL POR MENOR DE LIBROS` | 449 |
| `DISCOTECAS Y SALAS DE BAILE` | 235 |
| `CAFE ESPECTACULO` | 161 |
| `TABERNA` | 131 |
| `CHOCOLATERIA/SALON DE TE Y HELADERIA` | 104 |
| `SALAS DE FIESTA` (with and without food service) | 94 |
| `BAR ESPECIAL CON ACTUACIONES` | 82 |

Bakery and pastry get five separate classes split by whether the premise has an on-site oven (`obrador`) and a tasting counter (`barra de degustación`).

> **Caveat.** `BAR RESTAURANTE` (4,548) is a genuine hybrid class, larger than any pure bar class. Assigning it to bars or to restaurants is an analyst decision that changes the competitor count by up to 70%. It must be stated on the map, not buried.

**Mexico City.** SCIAN 6-digit codes are clean and unambiguous:

| SCIAN | Class | Establishments |
|---|---|---:|
| `722515` | Cafeterías, fuentes de sodas, neverías, refresquerías | 9,860 |
| `722412` | Bares, cantinas y similares | 1,043 |
| `465312` | Comercio al por menor de libros | 557 |
| `722411` | Centros nocturnos, discotecas y similares | 71 |
| `722511`-`722518` | Restaurants, eight classes by cuisine and service model | 42,227 |

> **Caveats.** `722515` merges cafes with juice bars and ice cream parlours, so the cafe universe is inflated. More seriously, 1,043 registered bars for 9.2M residents against Madrid's 6,444 for 3.3M is a 17-fold per-capita gap. That is not a real difference in drinking culture of that magnitude; it is under-registration and informality. Flag prominently on the bar and nightclub maps, and do not compare CDMX bar density to the other four cities without saying so.

**Paris.** APUR's 220-post nomenclature, resolved from [BDCOM_2017_CODACT_OD.xlsx](https://www.apur.org/open_data/BDCOM_2017_CODACT_OD.xlsx):

| Code | Label | Premises |
|---|---|---:|
| `CH101`-`CH109` | Restaurants, nine classes by cuisine | 6,273 |
| `CH303` | Restauration rapide assise | 2,956 |
| `CH201` / `CH202` | Brasserie, restauration continue, without / with tobacco | 2,913 |
| `CH403` | **Bar ou Café sans tabac** | 1,394 |
| `CH302` | Restauration rapide debout | 1,048 |
| `CH401` | Salon de thé | 405 |
| `CH402` | Café - Tabac | 239 |
| `CH501` | Cabaret - Diner-Spectacle | 53 |
| `CH301` | Cafétéria | 23 |

> **Caveat.** `CH403` is literally "Bar ou Café sans tabac". Bars and cafes share one code and cannot be separated, and there is no pub or nightclub class at all - `CH501` (53 premises) is the nearest thing. This is the single reason Paris scores 80 for bars rather than 90, and it is the correction that came out of resolving the nomenclature rather than trusting the level-18 label "Café et Restaurant".

**Chicago.** Active-licence counts, deduplicated on `account_number`:

| Field | Value | Count |
|---|---|---:|
| `license_description` | `Retail Food Establishment` | 9,091 |
| `license_description` | `Consumption on Premises - Incidental Activity` | 2,334 |
| `license_description` | `Package Goods` | 813 |
| `license_description` | `Tavern` | 751 |
| `license_description` | `Public Place of Amusement` | 717 |
| `license_description` | `Outdoor Patio` | 640 |
| `license_description` | `Late Hour` | 114 |
| `business_activity` | `Preparation of Food and Dining on Premises With Seating` | 2,107 |
| `business_activity` | `Sale of Food Prepared Onsite With Dining Area` | 1,342 |
| `business_activity` | `Sale of Food Prepared Onsite Without Dining Area` | 794 |
| `business_activity` | `Tavern - Consumption of Liquor on Premises` | 741 |

> **Caveat.** `Tavern` plus `business_activity = 'Tavern - Consumption of Liquor on Premises'` gives a clean bar universe, and `Consumption on Premises - Incidental Activity` (restaurants with a liquor licence) must be excluded from it. Everything else is coarse: `Retail Food Establishment` is one bucket in which cafes, bakeries and grocers are indistinguishable, and `Limited Business License` plus `Regulated Business License` (20,465 combined) hide all non-food retail including bookshops. Chicago is a strong bar and nightclub source and a weak everything-else source.

**London.** FHRS `BusinessTypeId` values, verified against the live API:

| Id | Business type |
|---|---|
| `1` | Restaurant/Cafe/Canteen |
| `7843` | Pub/bar/nightclub |
| `7844` | Takeaway/sandwich shop |
| `4613` | Retailers - other |
| `7840` | Retailers - supermarkets/hypermarkets |

> **Caveat.** `BusinessTypeId 1` is one bucket for restaurants, cafes and canteens, so cafes are not separable without name heuristics. `7843` merges nightclubs into pubs and bars. FHRS is a food-hygiene register, so bookshops have no coverage whatsoever, which is why that cell is a dash rather than a low score.

### Cities appraised and set aside

Kept here because the rejection reasons are part of the finding.

| City | Source | Best score | Why set aside |
|---|---|---:|---|
| Barcelona | [Cens de locals en planta baixa](https://opendata-ajuntament.barcelona.cat/data/en/dataset/cens-locals-planta-baixa-act-economica), 2024 wave | 87 (restaurant) | Content is excellent: roughly 61k premises with lat/lon, 2024 wave published Feb 2025. But the portal serves a BunkerWeb/hCaptcha bot wall, and both the CKAN API and direct resource downloads were verified blocked, so ingestion cannot be automated |
| Melbourne | [City of Melbourne CLUE](https://data.melbourne.vic.gov.au/explore/dataset/business-establishments-with-address-and-industry-classification/) | 90 (bar/pub) | Covers only 37 km², the City of Melbourne LGA, and ANZSIC 4511 merges cafes with restaurants. Strong runner-up otherwise: 19,672 establishments in the 2024 census, plus jobs, floor space and residents per block from the same survey, and dedicated seat and patron-capacity datasets |
| Seoul | [LOCALDATA](https://www.localdata.go.kr/), Ministry of the Interior and Safety | 89 (restaurant) | Separates 휴게음식점 (cafes) from 일반음식점 (restaurants) and 단란/유흥주점 (bars) cleanly, but requires API key registration, has Korean-only documentation, EUC-KR encoding, EPSG:5174 coordinates and no retail coverage |
| New York City | [DOHMH inspections](https://data.cityofnewyork.us/resource/43nn-pn8j) | 88 (restaurant) | `Coffee/Tea` covers 2,217 distinct establishments and is usable, but the field is *cuisine*, not premise type, so a bakery-cafe lands in `Bakery Products/Desserts`. No bar category, no non-food retail |
| Vancouver | [Business licences](https://opendata.vancouver.ca/explore/dataset/business-licences/) | 92 (restaurant) | 204,809 records refreshed daily, but the post-2024 categories fold cafes into `Limited Service Food Establishment` (4,827) alongside takeaways |
| Toronto | [DineSafe](https://open.toronto.ca/dataset/dinesafe/) | 85 (restaurant) | Daily, verified refreshed 2026-08-13, with an establishment-type field, but food premises only and the portal reports "License not specified" |
| Milan | [Pubblici esercizi](https://dati.comune.milano.it/dataset/ds58_economia_pubblici_esercizi_in_piano) and esercizi di vicinato | 79 (restaurant) | GeoJSON under CC-BY, but the published file is a December 2023 vintage despite recent portal metadata |
| Amsterdam | [Horeca exploitation permits](https://api.data.amsterdam.nl/v1/wfs/horeca/) | 77 (restaurant) | Terrace polygons are a useful extra, but these are permits rather than premises, horeca only, no retail |
| Istanbul | OpenStreetMap, the reference project's source | 71 | Kept as the **baseline to beat**. Tagging is fine (`amenity=cafe`, `amenity=bar`); completeness is unmeasured and uneven. Worth running an OSM-versus-register comparison for one city to quantify what the reference method missed |

---

## Part 2 - Build plan

### Repository layout

Follows `AGENTS.md` conventions - top-level pipeline directory, three asset layers, `bruin-playground-arsalan` connection - with `map/` in place of `dashboard-dac/`.

```
cities-shops-map/
├── PLAN.md, README.md, pipeline.yml
├── assets/
│   ├── raw/          requirements.txt + one Python asset per source
│   ├── staging/      crosswalk, unified shops, demand grid, quality scores
│   └── report/       map layers + coverage matrix
└── map/              index.html, app.js, style.css, export_geojson.py
```

### Phase A - supply side and the coverage matrix

Raw assets, all `type: python`, `image: python:3.11`, `append` strategy, `extracted_at` timestamp, structured logging, per `AGENTS.md`.

| Asset | Source | Implementation notes |
|---|---|---|
| `raw.madrid_premises` | `datos.madrid.es/dataset/200085-0-censo-locales/resource/200085-1-censo-locales/download/200085-1-censo-locales.csv` | UTF-8, semicolon-delimited, 85 MB. Resource URL must be hardcoded, the CKAN API returns 404 |
| `raw.madrid_activities` | resource `200085-5` | Carries `id_epigrafe` and `desc_epigrafe`; joins to premises on `id_local` |
| `raw.madrid_terraces` | resource `200085-6` | Terrace surface area and hours; annotation layer only |
| `raw.paris_bdcom_premises` | `carto2.apur.org/apur/rest/services/BDCOM/bdcom2023/MapServer/1/query` | 83,154 rows. Paginate with `resultOffset`, `maxRecordCount` is 1000. Pass `outSR=4326` to get WGS84 instead of Lambert-93 |
| `raw.paris_bdcom_nomenclature` | `apur.org/open_data/BDCOM_2017_CODACT_OD.xlsx` | The 220-post `codact` lookup. The sheet stores labels beside unevaluated `VLOOKUP` formulas, so read with `openpyxl` and take columns A and B (`CODACT`, `LIBACT`) only |
| `raw.cdmx_denue_units` | `inegi.org.mx/contenidos/masiva/denue/denue_09_csv.zip` | latin-1. Read `conjunto_de_datos/denue_inegi_09_.csv` from the archive - the first CSV inside is the data dictionary, not the data |
| `raw.london_fhrs_establishments` | `api.ratings.food.gov.uk/Establishments` | Requires header `x-api-version: 2`. Loop the 33 London `LocalAuthorityId`s taken from `/Authorities` filtered on `RegionName == 'London'`; page at 5,000 |
| `raw.chicago_business_licenses` | Socrata `uupf-x98q` | Use `type: ingestr` with a `socrata-chicago-open-data` connection if one exists in `.bruin.yml`, otherwise Python with `$limit` and `$offset` |

Staging:

- `staging.shop_type_crosswalk` - the hand-authored map from each city's native code to a canonical type (`bar_pub`, `nightclub`, `cafe`, `restaurant`, `fast_food`, `bakery`, `bookstore`), carrying a separability grade and the caveat text per city x type. Written as a SQL `VALUES` list so every classification decision is reviewable in the diff.
- `staging.shops_unified` - one row per establishment across all five cities: `city`, `establishment_id`, `name`, `native_code`, `native_label`, `shop_type`, `lat`, `lon`, `is_open`, `small_area_id`, `floor_area_m2` (Paris and Madrid only), `source_vintage`. Deduplicate every raw table with `ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY extracted_at DESC) = 1`; Chicago additionally on `account_number` plus `site_number`.
- `staging.data_quality_scores` - the Part 1 rubric stored as data, so published scores are computed from sub-scores rather than typed into prose twice.

Report:

- `report.shop_points` - the point map layer.
- `report.coverage_matrix` - city x shop_type with score, sub-scores, verified counts and caveat text. Drives the map's caveat panel.

**Validation gate before Phase B**, per the `AGENTS.md` geospatial rules: log each city's bounding box and area, confirm every establishment falls inside the expected administrative boundary, and compare per-capita density per shop type across cities. Investigate anything anomalous - the Mexico City bar count is already a known flag.

### Phase B - grid, demand and site scoring

**Grid.** One 250 m square grid built identically for all five cities, so spatial methodology stays constant. Cell id from an equirectangular metric approximation with a per-city latitude reference, computed in SQL:

```
cell_x = FLOOR(lon * 111320 * COS(lat_ref_rad) / 250)
cell_y = FLOOR(lat * 110540 / 250)
```

`lat_ref` per city is documented in the README and on the map footnote. Paris's native `idcar_200m` is retained as a second column purely to join INSEE data, never as the analysis grid.

**Demand inputs.** The same three for every city, so the five remain comparable:

1. **Resident population density.** Madrid: population by census section (Ayuntamiento de Madrid / INE Padrón). Paris: [INSEE Filosofi 200 m grid](https://www.insee.fr/fr/statistiques/8735162), 2021, GeoPackage, which also supplies income. Mexico City: INEGI Censo 2020 AGEB results. London: ONS LSOA mid-year estimates. Chicago: ACS 5-year tract estimates. Areal-weighted onto the 250 m grid.
2. **Complementary-premise density.** Count of establishments within 400 m that are *not* the target type, taken from `shops_unified` itself, so it is available for all five cities by construction.
3. **Rail and metro station proximity.** CRTM (Madrid), IDFM (Paris), Metro and Metrobús (Mexico City), TfL/NaPTAN (London), CTA `8pix-ypme` (Chicago).

City-specific richness - Madrid terrace areas, Paris floor area and churn history, Mexico City `per_ocu` bands - becomes map annotation and narrative, never a score input. Otherwise the five cities are not comparable.

**Score**, adapting the reference project's weighting:

```
competition = Σ exp(-d / 200m) over same-type establishments within 800 m
demand      = mean of the three z-scored demand inputs
site_score  = 0.55 * percentile(demand) + 0.45 * (1 - percentile(competition))
```

Percentile-normalised within city x shop_type, so cells are ranked against their own city and never across cities. Output to `report.site_scores`.

**Paris gets what no other city can offer.** Intersect the top-scoring cells with BDCOM `Local vacant` premises to surface actual available units at actual addresses, and use the 2000-2017 per-premise activity codes to flag high-churn addresses as elevated risk. This is the analytical payoff of choosing a field-surveyed census over a licence register, and it should be called out as such.

### Phase C - MapLibre map

`cities-shops-map/map/`, standalone MapLibre GL JS, no DAC. Per `VISUALIZATIONS.md` §11:

- City selector (5) crossed with shop-type selector (7), driving three toggleable layers: establishment points, site-score choropleth over the 250 m grid, and Paris vacant-premise candidates.
- Title, insight-led description, always-visible legend, and a per-combination footnote naming the source, its vintage and the verified caveats read from `report.coverage_matrix`. The map must be readable without hovering.
- Wong (2011) palette, colour always paired with a second channel - shape for point layers, hatch or explicit bin labels for the choropleth.
- A coverage-matrix view rendering the Part 1 table, so the reader sees the data-quality grade before the site scores.
- `export_geojson.py` runs `bruin query` and writes GeoJSON into `map/data/`. Gitignore anything over a few MB.

Serve with:

```bash
python3 -m http.server 8080 --directory cities-shops-map/map
```

Then open **http://localhost:8080**.

### Known limitation to state in the methodology

The five cities' population denominators come from five different vintages: Madrid padrón, INSEE 2021, INEGI 2020, ONS mid-year, ACS 5-year. Cross-city per-capita comparisons therefore carry a vintage mismatch of up to six years. Per-capita figures stay as context only and never enter the score.

---

## Verification

1. `bruin validate cities-shops-map/` once the assets exist.
2. Run each raw asset individually and assert row counts against the figures verified in Part 1: Madrid 203,612 premises and 225,461 activity rows, Paris 83,154, Mexico City 462,732, London roughly 81,170, Chicago roughly 50k active licences. A material deviation means the source moved, not that the pipeline is fine.
3. Assert per-type counts against the granularity evidence above - Madrid `BAR CON COCINA` 3,946 open, Mexico City `722412` 1,043, Paris `CH403` 1,394, London `7843` per borough. These are regression tests on the crosswalk.
4. Null-rate check on `lat` and `lon` per city. Mexico City must stay at 100%; London is expected to have nulls, so quantify and report rather than silently dropping rows.
5. Log bounding box and area per city and confirm all five grids use the same 250 m resolution.
6. Run the OSM-versus-register comparison for Madrid, the strongest register, using Overpass `amenity=bar`, and report the gap. This quantifies what an OSM-only method would have missed and is the empirical justification for the whole approach.
7. Serve the map, screenshot every city x shop-type combination with Playwright and review each one, per `VISUALIZATIONS.md` §9. Delete the screenshots afterwards.
