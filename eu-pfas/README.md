# eu-pfas

PFAS ("forever chemicals") drinking-water contamination atlas for the EU-27, at
NUTS3 resolution.

## What it does

Aggregates known PFAS contamination at every measurable site in the EU-27 from
overlapping open and national-monitoring sources:

1. **Forever Pollution Project** (Le Monde consortium, 2023) — 17,000+ documented
   sites, GeoJSON bulk export.
2. **EEA Waterbase** — pan-EU surface- and ground-water quality reporting.
3. **National monitoring** where bulk-accessible: DE UBA / LANUV, NL RIVM, DK GEUS,
   SE SLV, IT ISS, FR ARS regional. Other member states show data-sparse, not
   contamination-free — limitation acknowledged in every chart footnote.
4. **OSM Overpass** — military installations (`military=*` polygons).
5. **Manufacturer facility seed** — hand-curated 3M / Solvay / Daikin / Chemours /
   Arkema EU plants.

Cross-references each site against the **EU Drinking Water Directive 2020/2184**
threshold of 0.5 µg/L total PFAS (in force from 12 January 2026).

## Datasets

| Layer | BigQuery dataset | Purpose |
|---|---|---|
| Raw | `eu_pfas_raw` | Untransformed pulls from each source. |
| Staging | `eu_pfas_staging` | Harmonised site dim, NUTS3 exposure index, source attribution. |
| Reports | `eu_pfas_report` | Marts for the DAC dashboard. |

## Geographic scope

EU-27. Sites are spatially joined to the shared NUTS3 dimension built in the
`eu-mortality` pipeline.

## Run

```bash
bruin run eu-pfas
```

## Dashboard

Shared with `eu-mortality` at `montreal-v1/eu-env-dashboard/`.
