# eu-mortality

Heat-attributable excess mortality at NUTS3 resolution for the EU-27 (2015 – 2025).

## What it does

Joins three independent open sources to estimate excess deaths attributable to
heat in every NUTS3 region (1,166 regions × 27 member states):

1. **Eurostat `demo_r_mwk3_05`** — weekly all-cause deaths × NUTS3 × 5-year age band.
2. **Open-Meteo Archive (ERA5 reanalysis)** — hourly 2m temperature at NUTS3 centroids,
   aggregated to ISO-week mean / max / heat-index.
3. **Eurostat `demo_r_pjangrp3`** — NUTS3 population denominator by age band.

Plus vulnerability dimensions (regional GDP per capita, DEGURBA urban/rural class,
hospital bed density, urban-fabric fraction from JRC GHSL).

The excess mortality model uses a 2015–2019 climatological baseline (ISGlobal /
Ballester 2023 framework, adapted for week-level resolution). Heat attribution
restricts to weeks with temperature anomaly > +2 σ above the 1991–2020 climatology.

## Datasets

| Layer | BigQuery dataset | Purpose |
|---|---|---|
| Raw | `eu_mortality_raw` | Untransformed pulls from Eurostat / Open-Meteo / JRC. |
| Staging | `eu_mortality_staging` | Harmonised NUTS3-week panels, dimension tables. |
| Reports | `eu_mortality_report` | Marts for the DAC dashboard. |

## Geographic scope

EU-27 only (NUTS3 codes starting with the ISO-2 country code of every member state).
The shared NUTS3 dimension is built here and re-used by the `eu-pfas` pipeline.

## Run

```bash
bruin run eu-mortality                        # full pipeline
bruin run eu-mortality/assets/eu_mortality_raw/eurostat_mortality.py   # single asset
bruin validate eu-mortality                   # schema + reference checks
```

## Dashboard

Built in `montreal-v1/eu-env-dashboard/`. Serves on `http://localhost:8321`:

```bash
cd montreal-v1/eu-env-dashboard && dac serve --dir . --port 8321
```
