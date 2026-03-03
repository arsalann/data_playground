Build a new staging and reporting layer for the existing @ga_sample  pipeline (Google Analytics 360 sample data).

**Context:** utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md  strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

Raw data already exists in BigQuery as `ga_sample.raw_ga` (~900K rows). It was imported from BigQuery's public dataset `bigquery-public-data.google_analytics_sample`. The schema is defined in `@ga_sample/assets/ga_sample/raw_ga.asset.yml`.

### What to transform

The raw table has nested RECORD columns that need to be flattened, and the date is stored as a string. Create two SQL assets:

1. **`staging_ga`** — clean and flatten the raw data: parse the date string into a proper DATE, unnest device/geo/traffic/totals RECORDs into flat columns, deduplicate sessions, add temporal dimensions
2. **`report_ga_daily`** — daily aggregate report summarizing sessions, users, pageviews, bounce rate, and avg session duration broken down by channel, device category, country, and other relevant dimensions

### Naming

All asset/table names should have prefix **`ga_`**
Destination: BigQuery
Dataset: ga_sample

### Build order

1. First, query `ga_sample.raw_ga` directly in BigQuery to understand the data — profile the nested RECORD fields (`device`, `geoNetwork`, `totals`, `trafficSource`, `hits`) to see what sub-fields are available and what the data looks like
2. Create `staging_ga.sql` that cleans, flattens, and deduplicates the raw data. Test it.
3. Create `report_ga_daily.sql` that aggregates the staged data into a daily summary by key dimensions. Test it.
4. Test the full pipeline end-to-end

### Constraints

- No Python extraction needed — raw data already exists as a `bq.source` asset
- The pipeline directory and `pipeline.yml` already exist — do not recreate them
- Place new SQL assets under `ga_sample/assets/` in appropriate subdirectories