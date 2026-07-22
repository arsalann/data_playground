# eu-env-dashboard

DAC dashboard combining the `eu-mortality` (heat-attributable excess deaths) and
`eu-pfas` (forever-chemical drinking-water contamination) pipelines at NUTS3 in
the EU-27.

## Sections

1. **Hero** — composite environmental-burden index map (NUTS3).
2. **Heat mortality** — annual excess deaths, top-20 deadliest NUTS3, vulnerability
   decomposition.
3. **PFAS contamination** — site map, drinking-water exceedance ranking, source
   attribution (military / manufacturer / unattributed).
4. **Compound burden** — NUTS3 in the worst quintile of both indices.
5. **Methodology and limits** — explicit footnote section.

## Serve

```bash
cd montreal-v1/eu-env-dashboard
dac serve --dir . --port 8321
```

URL: `http://localhost:8321`
