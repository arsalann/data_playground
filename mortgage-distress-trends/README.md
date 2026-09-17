# Mortgage Distress Search Trends

This pipeline tracks monthly US Google Trends interest for search terms related to mortgage distress and compares current search behavior with the June 2007–December 2010 financial-crisis window.

## Data source

- [Google Trends](https://trends.google.com/trends), United States, monthly web-search interest, retrieved with [pytrends](https://github.com/GeneralMills/pytrends).
- Google Trends values are relative 0–100 indices, not search counts. Each term is queried separately and normalized to its own historical maximum.

## Assets

- `assets/raw/mortgage_distress_google_trends.py` — fetches five core mortgage-distress terms plus three context terms. The current partial month is excluded.
- `dashboard-dac/dashboards/queries/*.sql` — composite index, term time series, crisis comparison, and detail-table queries.
- `dashboard-dac/dashboards/mortgage-distress.yml` — Bruin DAC dashboard.

## Run commands

```bash
bruin validate mortgage-distress-trends/
bruin run --start-date 2004-01-01 --end-date 2026-08-31 mortgage-distress-trends/assets/raw/mortgage_distress_google_trends.py
dac validate --dir mortgage-distress-trends/dashboard-dac
dac check --dir mortgage-distress-trends/dashboard-dac
dac serve --dir mortgage-distress-trends/dashboard-dac --port 8323
```

Dashboard: http://localhost:8323/

## Limitations

The composite is a custom equal-weight standardized indicator, not aggregate search volume. Search interest can reflect borrowers, advisors, journalists, researchers, investors, or policy attention, and cannot estimate defaults, foreclosures, household counts, or assistance-program participation. Low-volume terms can be noisy, and multi-word queries use Google Trends' default term matching.
