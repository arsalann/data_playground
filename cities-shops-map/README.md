# cities-shops-map

Observed shop density across five cities and seven shop types, counted from official business
registers instead of OpenStreetMap, with a data-quality grade and its own caveats attached to
every city and shop-type combination.

**Nothing in this pipeline is modelled.** An earlier version scored each 250 m cell on where to
open a new shop, using the reference project's demand-versus-competition blend. That was the only
predictive element in the project and it has been removed, along with the two inputs that existed
solely to feed it. What is published is one ratio from two observed counts:

```
shops_per_1k_residents = 1000 * shops_within_400m / residents_within_400m
```

`PLAN.md` holds the dataset appraisal that chose the cities and the original build plan. This
file documents what was actually built, including where it departs from the plan and why.

## The point of the project

The reference project, [AzeemPatil/kadikoy-cafe-site-selection](https://github.com/AzeemPatil/kadikoy-cafe-site-selection),
ranks candidate cafe sites in one Istanbul district from 744 OpenStreetMap cafes. The method is
sound; the data cannot be audited, because OpenStreetMap completeness is unmeasured. A cell may
score well because it is genuinely underserved or because nobody mapped the bars on that street.

This pipeline replaces the data and then measures what the swap was worth. It also drops the
prediction: the reference project's output is a recommendation, and this one is a measurement.
Against Madrid's register, `report.osm_register_gap` finds that **46.6% of registered bars and
67.9% of bakeries have no OpenStreetMap object within 75 m**, while OpenStreetMap holds 2.1x as
many fast-food objects as the register has licences and only 36.6% of those sit near a
registered fast-food premise. The gap is not a simple undercount; the categories do not mean the
same thing.

## Cities and shop types

| City | Source | Licence |
|---|---|---|
| Madrid | [Censo de locales y actividades](https://datos.madrid.es/dataset/200085-0-censo-locales), Ayuntamiento de Madrid | CC BY 4.0 |
| Paris | [APUR BDCOM 2023](https://opendata.apur.org/datasets/bdcom-2023) ground-floor commercial census | ODbL |
| Mexico City | [INEGI DENUE](https://www.inegi.org.mx/app/mapa/denue/) national statistical business directory | INEGI open terms |
| London | [FSA Food Hygiene Rating Scheme API](https://api.ratings.food.gov.uk/help) | OGL v3.0 |
| Chicago | [Business Licenses - Current Active](https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses-Current-Active/uupf-x98q), BACP | City of Chicago open data |

Seven canonical shop types: `bar_pub`, `nightclub`, `cafe`, `restaurant`, `fast_food`, `bakery`,
`bookstore`. 32 of the 35 combinations are mappable. The three that are not are London bakery
and London bookstore (FHRS has no class for either) and Chicago bookstore (24 mappable premises,
below the 50-premise floor).

Data-quality score out of 100, from `report.coverage_matrix`:

| Shop type | Madrid | Mexico City | Paris | Chicago | London |
|---|---:|---:|---:|---:|---:|
| Bar / pub | **95** | 81 | 77 | 90 | 79 |
| Nightclub | **93** | 79 | 87 | 82 | 74 |
| Cafe | **93** | 81 | 80 | 77 | 74 |
| Restaurant | 90 | 91 | **92** | 85 | 76 |
| Fast food / takeaway | **93** | 88 | 92 | 85 | 88 |
| Bakery / pastry | **95** | 91 | 90 | 65 | 54 |
| Bookstore | **93** | 91 | 90 | 58 | 50 |

Rubric weights: source authority 20, spatial coverage 20, **category granularity 25**, freshness
15, geocoding 10, access and licence 10. Granularity carries the most weight because it is the
criterion that most often silently invalidates this kind of study.

## Assets

### `assets/raw/` - ingestion

| Asset | Rows | Notes |
|---|---:|---|
| `raw.madrid_premises` | 203,560 | 89 MB CSV. `(0, 0)` is the missing-coordinate sentinel and is nulled on ingest |
| `raw.madrid_activities` | 225,461 | 454 distinct `desc_epigrafe` classes; joins to premises on `id_local` |
| `raw.madrid_terraces` | 6,588 | Licensed outdoor seating area and chair counts; annotation only |
| `raw.paris_bdcom_premises` | 83,154 | ArcGIS REST, paged at 1,000, `outSR=4326`. Includes 9,060 vacant units |
| `raw.paris_bdcom_nomenclature` | 220 | The `codact` lookup plus its 47-post and 18-post aggregations |
| `raw.cdmx_denue_units` | 462,732 | 45 MB zip, latin-1, 100% geocoded, 931 SCIAN classes |
| `raw.london_fhrs_establishments` | 81,169 | 33 London authorities; matches the publisher's own count exactly |
| `raw.chicago_business_licenses` | 54,961 | Public SoQL endpoint, paged at 20,000 |
| `raw.ghsl_population_100m` | 478,350 | GHS-POP R2023A epoch 2025, six Mollweide tiles |
| `raw.osm_madrid_amenities` | 12,873 | The OpenStreetMap comparison set for Madrid |

### `assets/staging/` - transformation

- `shop_type_crosswalk` - every source code mapped to a canonical shop type, written as a
  literal `VALUES` list so each classification decision is reviewable in a diff. Grain is
  `(city, native_code, shop_type)` and the mapping is deliberately many-to-many.
- `shops_unified` - one row per `(city, establishment_id, shop_type)`.
- `data_quality_scores` - the rubric as data, one row per city and shop type.
- `analysis_grid` - the 250 m cells, 20,474 across the five cities.
- `grid_population` - residents and total shops within 400 m of each cell centre.

### `assets/report/` - map layers

- `coverage_matrix` - 35 rows, drives the map's caveat panel.
- `shop_points` - the establishment point layer.
- `shop_density` - 127,599 rows of `(city, shop_type, cell)` with shops per 1,000 residents.
- `density_bins` - the real value range of every legend class, so the legend states numbers.
- `paris_vacant_candidates` - surveyed vacant addresses with churn history, Paris only.
- `osm_register_gap` - the OpenStreetMap comparison.

## Method

```
shops_per_1k_residents = 1000 * shops_within_400m / residents_within_400m
```

Both counts are taken within 400 m of the cell centre, roughly a five-minute walk and the radius
at which retail catchment is normally measured. Using the same radius for numerator and
denominator means the ratio describes one neighbourhood; the consequence is that neighbouring
cells overlap, so the surface is smoothed rather than independent per cell.

Three choices decide what the ratio says:

1. **The ratio is withheld below 500 residents within 400 m.** Without a floor, a City of London
   cell with two residents and two cafes reports 937 cafes per 1,000 residents, which is
   arithmetic rather than information. Those cells are published with a null ratio and their own
   map class, because a workplace district with almost no residents is a real category rather
   than missing data. The floor removes 1.1% of Madrid cells and 4.5% of London's.
2. **Zero is its own class, not the bottom of a scale.** A cell with none of a shop type within
   400 m is qualitatively different from one with few.
3. **Bins are value-based, not rank-based.** Two cells with the same ratio always land in the
   same class, which `NTILE` cannot guarantee. `report.density_bins` publishes the real value
   range of every class so the legend states numbers rather than "quartile 3".

Bins are computed within `(city, shop_type)`. The five registers differ in completeness, so a
shared scale across cities would compare data quality as much as retail provision.

### What was removed

| Asset | Why |
|---|---|
| `report.site_scores` | The modelled recommendation. Replaced by `report.shop_density` |
| `staging.demand_grid` | Blended three z-scored demand inputs. Replaced by `staging.grid_population`, which keeps only the population and total-shop counts |
| `raw.osm_transit_stations` | Rail and metro proximity was a demand term with nothing left to feed |

The distance-decay competition term (`SUM(exp(-d / 200 m))` within 800 m) and the
complementary-premise term went with them.

### Spatial methodology

One 250 m grid for all five cities, per the `AGENTS.md` consistent-spatial-methodology rule:

```
cell_x = FLOOR(lon * 111320 * COS(lat_ref_rad) / 250)
cell_y = FLOOR(lat * 110540 / 250)
```

| City | `lat_ref` | Bounding box (min_lon, min_lat, max_lon, max_lat) | Cells | Area |
|---|---:|---|---:|---:|
| Madrid | 40.4780 | -3.889, 40.312, -3.518, 40.644 | 2,176 | 136.0 km² |
| Paris | 48.8585 | 2.224, 48.815, 2.470, 48.902 | 1,161 | 72.6 km² |
| Mexico City | 19.3205 | -99.365, 19.048, -98.940, 19.593 | 8,209 | 513.1 km² |
| London | 51.4894 | -0.5104, 51.2868, 0.3340, 51.6919 | 6,791 | 424.4 km² |
| Chicago | 41.8338 | -87.9401, 41.6445, -87.5241, 42.0230 | 2,137 | 133.6 km² |

`lat_ref` is the centre of each city's bounding box. The grid holds only cells containing at
least one geocoded establishment of any shop type, which is the analogue of the reference project
scoring street-network nodes: a cell with no commercial premises has no evidence of being usable
retail space. Areas therefore describe commercial extent, not municipal area.

## Departures from PLAN.md

Each of these changes a documented limitation, so they are recorded rather than folded in
silently.

1. **One population source, not five.** The plan called for Madrid padrón, INSEE Filosofi, INEGI
   Censo 2020, ONS LSOA and ACS estimates, and flagged as a known limitation that their vintages
   differ by up to six years. GHS-POP 2025 at 100 m removes the limitation instead of documenting
   it: one estimator, one epoch, one resolution, one projection. The cost is that GHS-POP is a
   modelled disaggregation of census counts onto built-up area, so it is more reliable about where
   people are than exactly how many.
2. **One transit source, not five.** Station proximity comes from OpenStreetMap for all five
   cities rather than CRTM, IDFM, Metro, TfL and CTA, because five feeds would make the z-score
   mean something different in each city. Defensible here and not for shops: a metro network is a
   few hundred large, heavily-edited objects whose completeness is checkable. The asset logs the
   check, and the five cities land at 91-113% of their published rapid-transit network size.
3. **Chicago via SoQL, not `ingestr`.** The repo's `socrata-chicago-open-data` connection returns
   HTTP 403 "Invalid app_token specified". The asset pages the public SoQL endpoint directly and
   reads no credential.
4. **Coverage scored per shop type, not per city.** The plan varied only granularity by shop type.
   That would have scored the Mexico City bar map at 93 while saying nothing about the fact that
   1,043 registered bars for 9.2M residents is a 17-fold per-capita gap against Madrid. Mexico
   City bar coverage is scored 3.
5. **No DAC dashboard.** DAC has no map widget, so the deliverable is a standalone MapLibre page.

## Corrections found while building

| Finding | Effect |
|---|---|
| Paris `SA404 Discotheque et club prive` exists (114 premises) | The plan concluded Paris had no nightclub class, having searched only the CH restauration block. Paris nightclub rises to 87 |
| Paris `CE101 Librairie` exists (560 premises) | Also outside the CH block |
| Chicago activity token `Preparation and Sale of Coffee and/or Drinks` (520 licences) | Cafes are partly separable; the plan looked only at `license_description` |
| Chicago Late Hour 4am liquor permit | A tighter nightclub proxy than Public Place of Amusement, which also covers bowling alleys and art studios |
| London has no bakery class | The plan scored London bakery 74 for a universe that does not exist. Corrected to 54 and excluded from the map |
| Madrid `(0, 0)` coordinate sentinel on 50,762 of 203,560 premises | Left unhandled these reproject into the Atlantic. The gap is structured: 96.6% of street-front premises are geocoded against 30% of premises inside markets and galleries |
| Paris `surfexacte` present on 1.9% of premises | The plan credited Paris with per-premise floor area; only the banded `surf` column is broadly populated |
| Mexico City DENUE holds 20,586 `Semifijo` units | Not the fixed-premises-only register the plan described |
| Paris wave columns run to `c20_libact` | Churn history spans 2000-2020, eight waves, one more than the plan recorded |
| 5 Paris `codact` values (368 premises) absent from the 2017 lookup | All in non-food families, so no shop type is affected |

## Notable caveats

- **Madrid `BAR RESTAURANTE`** (4,548 open) is a genuine hybrid and larger than any pure bar
  class. It is assigned to `restaurant` because epigrafe 561004 sits in CNAE division 5610 food
  service rather than 5630 drink service. Assigning it to bars instead would move the Madrid bar
  count from 6,444 to 10,992, a 71% swing.
- **Mexico City bars and nightclubs are under-registered.** 1,043 bars and 71 nightclubs for 9.2M
  residents. The register undercounts them, so the true density is higher than the map shows.
- **London cannot separate cafes from restaurants.** `BusinessTypeId 1` is a single
  "Restaurant/Cafe/Canteen" bucket, and `7843` merges pubs, bars and nightclubs.
- **Paris cannot separate bars from cafes.** `CH403` is literally "Bar ou Cafe sans tabac", so it
  is mapped to both types and counted as a competitor for both.
- **Population denominators.** Per-capita figures use GHS-POP within a bounding box, not the
  municipal boundary, so they describe the mapped envelope rather than the administrative city.
  They are context only and never enter the score.

## Running it

```bash
# Validate every asset definition
bruin validate cities-shops-map/

# Raw assets, individually (the register files are 45-125 MB each)
bruin run cities-shops-map/assets/raw/madrid_premises.py
bruin run cities-shops-map/assets/raw/madrid_activities.py
bruin run cities-shops-map/assets/raw/madrid_terraces.py
bruin run cities-shops-map/assets/raw/paris_bdcom_premises.py
bruin run cities-shops-map/assets/raw/paris_bdcom_nomenclature.py
bruin run cities-shops-map/assets/raw/cdmx_denue_units.py
bruin run cities-shops-map/assets/raw/london_fhrs_establishments.py
bruin run cities-shops-map/assets/raw/chicago_business_licenses.py
bruin run cities-shops-map/assets/raw/ghsl_population_100m.py
bruin run cities-shops-map/assets/raw/osm_madrid_amenities.py

# Staging then report, in order
bruin run cities-shops-map/assets/staging/shop_type_crosswalk.sql
bruin run cities-shops-map/assets/staging/shops_unified.sql
bruin run cities-shops-map/assets/staging/data_quality_scores.sql
bruin run cities-shops-map/assets/staging/analysis_grid.sql
bruin run cities-shops-map/assets/staging/grid_population.sql     # ~2 min
bruin run cities-shops-map/assets/report/coverage_matrix.sql
bruin run cities-shops-map/assets/report/shop_points.sql
bruin run cities-shops-map/assets/report/shop_density.sql          # ~4 min
bruin run cities-shops-map/assets/report/density_bins.sql
bruin run cities-shops-map/assets/report/paris_vacant_candidates.sql
bruin run cities-shops-map/assets/report/osm_register_gap.sql
```

`raw.osm_madrid_amenities` hits Overpass, which rate-limits. It retries with exponential
backoff; a 429 on the first attempt is normal.

## The map

```bash
# Export the report tables to GeoJSON (~70 MB, ~8 min)
python3 cities-shops-map/map/export_geojson.py

# Serve
python3 -m http.server 8080 --directory cities-shops-map/map
```

Then open **http://localhost:8080**.

Three tabs: the map, the data-quality matrix, and the method plus the OpenStreetMap comparison.
The map has a city selector crossed with a shop-type selector, driving three toggleable layers:
the site-score choropleth over the 250 m grid, existing premises, and Paris vacant units.

`map/data/` is gitignored via `map/.gitignore` because the full export is roughly 78 MB. It needs
its own ignore file: the repo-root `data/*` rule contains a slash, so git anchors it to the
repository root and it does not reach this directory. Re-run `export_geojson.py` to rebuild.

Encoding, per `VISUALIZATIONS.md` §11:

- Choropleth uses a **single-hue sequential blue ramp**, safe under every form of colour vision
  deficiency because it varies lightness rather than hue, and every bin carries an explicit
  numeric label in the legend.
- Premise points encode classification certainty as **filled versus hollow**, so the
  data-quality warning does not depend on colour.
- Paris vacant units are **diamonds** with churn risk in the stroke weight as well as the colour.
- Title, insight, legend and caveat panel are static, so the map is readable without hovering.

## Verification performed

| Check | Result |
|---|---|
| `bruin validate cities-shops-map/` | 21 assets, no issues |
| Raw row counts against PLAN.md | All within 0.03%. Madrid drifts -52 rows because the register reloads daily |
| Per-type regression counts | 11 of 11 exact: Madrid `561005` 3,946, `561004` 4,548, `561006` 3,199, `476101` 449; CDMX `722412` 1,043, `722411` 71, `722515` 9,860; Paris `CH403` 1,394, `CH401` 405; London `7843` 3,863; Chicago Tavern 773 |
| Geocoding rate per city | Mexico City 100%, Paris 100%, Chicago 99.6%, Madrid 93.2%, London 91.2% |
| Administrative coverage | Complete: 21 Madrid districts, 20 Paris arrondissements, 16 alcaldías, 33 London boroughs, 77 Chicago community areas |
| Grid resolution | 250 m in all five cities |
| OpenStreetMap versus register | Quantified in `report.osm_register_gap` |
| Every city and shop-type combination screenshotted | Reviewed with Playwright, screenshots deleted afterwards per `AGENTS.md` |

## Known limitations

- Densities are binned within a city and shop type, so the same colour means a different number
  in another city. The legend always states the real range.
- The map measures provision, not opportunity. A dense cell may be dense because the location
  works or because it is saturated, and this data cannot distinguish the two.
- Population counts residents only, so districts serving commuters or tourists read as
  over-provided. Central London reaches 306 eat-in premises per 1,000 residents for that reason.
- The grid is bounded by a rectangular envelope, not the municipal boundary, so a small number of
  cells near the edges sit just outside the administrative city. Establishments are all inside it,
  because each register only publishes its own jurisdiction.
- Registers miss businesses trading without a licence. Madrid's own portal warns its data is "an
  administrative registry without statistical processing" that "may contain inconsistencies".
- Madrid is the only city with an OpenStreetMap comparison. The gap elsewhere is unmeasured.
- There is no spending, footfall, tourism or commuter-flow input, and no transit input since the
  station asset was removed with the score.
- Paris data is the June 2023 survey wave. The other four are current, so Paris competition
  reflects a market three years stale.
