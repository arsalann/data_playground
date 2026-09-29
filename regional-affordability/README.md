# Regional Affordability

This pipeline builds an apples-to-apples OECD TL2 regional affordability panel for Europe and North America.

## Data Sources

- [OECD Regional Economy: Income - Regions](https://data-explorer.oecd.org/vis?df%5Bds%5D=dsDisseminateFinalDMZ&df%5Bid%5D=DSD_REG_ECO%40DF_INC), accessed through the DBnomics API mirror. Used for TL2 net primary income (`B5N`) and net disposable income (`B6N`) in USD PPP per person.
- [OECD Regional Social: Housing - Regions](https://data-explorer.oecd.org/vis?df%5Bds%5D=dsDisseminateFinalDMZ&df%5Bid%5D=DSD_REG_SOC%40DF_HOUSING), accessed through the DBnomics API mirror. Used for TL2 housing cost as a percent of household net disposable income (`HOUSE_COST`, `PT_B6N_S14`).
- [Eurostat GISCO NUTS boundaries](https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/territorial-units-statistics) and [Natural Earth Admin 1 boundaries](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/) for the static map image.

## Assets

- `raw.oecd_tl2_income`: annual OECD TL2 primary and disposable income observations.
- `raw.oecd_tl2_housing`: annual OECD TL2 housing-cost burden observations.
- `staging.regional_affordability`: strict region-year join of income and housing metrics, restricted to Europe and North America.
- `report.regional_affordability_snapshot`: 2022 snapshot used by the DAC dashboard and map.

## Methodology

City-level data was excluded because no single public source provides pre-tax income, post-tax income, and cost categories at a consistent city grain across Europe and North America. Country-level cost categories were also excluded because they would violate the same-level comparison requirement.

The dashboard uses OECD TL2 regions. A row is kept only when the same `region_code` and `year` has all three required metrics:

- primary income per person (`B5N`, USD PPP/person)
- disposable income per person (`B6N`, USD PPP/person)
- housing cost as a percent of net disposable household income (`HOUSE_COST`, `PT_B6N_S14`)

Disposable after-housing income is calculated as:

```text
disposable_income_ppp_pc * (1 - housing_cost_pct_disposable_income / 100)
```

The affordability score indexes that value to the same-year median. A score of 120 means the region is 20% above the comparable-region median.

## Run Commands

```bash
bruin validate regional-affordability/
bruin run regional-affordability/assets/raw/oecd_tl2_income.py
bruin run regional-affordability/assets/raw/oecd_tl2_housing.py
bruin run regional-affordability/assets/staging/regional_affordability.sql
bruin run regional-affordability/assets/report/regional_affordability_snapshot.sql

python3 regional-affordability/dashboard-dac/scripts/build_map.py
python3 -m http.server 8330 --directory regional-affordability/dashboard-dac/dashboards/assets
dac validate --dir regional-affordability/dashboard-dac
dac check --dir regional-affordability/dashboard-dac
dac serve --dir regional-affordability/dashboard-dac --port 8321
```

Dashboard URL: <http://localhost:8321>

## Known Limitations

- The United States is excluded from the 2022 snapshot because the OECD regional housing-cost burden series does not provide matching TL2 observations for the same year and grain.
- Transportation, groceries, utilities, and broader cost-of-living categories are excluded. Mixing country-level CPI weights, commercial city indexes, or crowdsourced city prices would break the apples-to-apples rule.
- The static choropleth uses public NUTS/Admin-1 geometry joins for display; it is not an OECD-official map boundary product.
