# US Public Transit — COVID Recovery & Spending Efficiency

Analysis of US public transit ridership recovery after COVID-19, comparing systems by mode, metro area, and spending efficiency.

## Data Sources

### 1. National Transit Database (NTD) — Federal Transit Administration
- **Monthly Module** (Socrata `8bui-9xvu`): Monthly ridership (UPT), vehicle revenue miles/hours, VOMS by agency and mode. January 2002 – present. ~365K rows.
- **Annual Database — Metrics** (Socrata `ekg5-frzt`): Annual ridership, fare revenues, operating expenses, derived efficiency metrics by agency and mode. 2022–2024.
- Source: [data.transportation.gov](https://data.transportation.gov)
- License: Public domain (US Government), no authentication required.

### 2. US Census American Community Survey (ACS)
- **Table B08301**: Commuting mode share by Metropolitan Statistical Area (MSA). Total workers, transit commuters, walkers, work-from-home.
- Years: 2010–2023 (1-year ACS estimates).
- Source: [Census API](https://api.census.gov/data/)
- License: Public domain, no API key required for basic access.

## Assets

### Raw Layer (`assets/raw/`)
| Asset | Description |
|-------|-------------|
| `transit_ntd_monthly.py` | Monthly ridership by agency/mode from NTD Socrata API |
| `transit_ntd_annual.py` | Annual metrics (service + financials) from NTD Socrata API |
| `transit_census_acs.py` | ACS commuting mode share by MSA |

### Staging Layer (`assets/staging/`)
| Asset | Description |
|-------|-------------|
| `transit_ridership_recovery.sql` | Monthly ridership indexed to 2019 baseline by agency and mode |
| `transit_agency_efficiency.sql` | Annual agency-level efficiency metrics: cost/trip, fare recovery, trips/capita |
| `transit_metro_comparison.sql` | Metro-area aggregation with Census WFH rates for cross-metro comparison |

## Run Commands

```bash
# Validate pipeline
bruin validate public-transit-analysis/

# Run individual raw assets
bruin run public-transit-analysis/assets/raw/transit_ntd_monthly.py
bruin run public-transit-analysis/assets/raw/transit_ntd_annual.py
bruin run public-transit-analysis/assets/raw/transit_census_acs.py

# Run staging (after raw data exists)
bruin run public-transit-analysis/assets/staging/transit_ridership_recovery.sql
bruin run public-transit-analysis/assets/staging/transit_agency_efficiency.sql
bruin run public-transit-analysis/assets/staging/transit_metro_comparison.sql

# Run full pipeline
bruin run public-transit-analysis/
```

## Key Metrics

- **UPT (Unlinked Passenger Trips)**: Each boarding = 1 trip. A ride with a transfer = 2 UPTs.
- **VRM (Vehicle Revenue Miles)**: Miles traveled in revenue service.
- **Recovery %**: (Current month UPT / Same month 2019 UPT) * 100.
- **Fare Recovery Ratio**: Fare revenue / operating expenses.
- **Cost per Trip**: Operating expenses / UPT.

## Known Limitations

- Annual financial data (fare revenues, operating expenses) only available for 2022–2024. Long-term financial trends cannot be analyzed.
- NTD uses "UZA" (Urbanized Area), Census uses "MSA" — these don't map 1:1. Metro-level joins use fuzzy name matching, some metros may not match.
- Monthly data goes back to 2002 but some agencies start reporting later.
- Census ACS 1-year estimates are only available for areas with population 65,000+.
