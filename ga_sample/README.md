# Google Analytics 360 Sample Pipeline

Web analytics pipeline built on Google's obfuscated [Google Analytics 360 sample dataset](https://support.google.com/analytics/answer/7586738). Covers the Google Merchandise Store from August 2016 to August 2017 (~903K sessions, ~714K unique visitors).

## Data Source

- **BigQuery public dataset**: `bigquery-public-data.google_analytics_sample.ga_sessions_*`
- **Imported to**: `bruin-playground-arsalan.ga_sample.raw_ga`
- **License**: [Google BigQuery public datasets terms](https://cloud.google.com/bigquery/public-data)

## Assets

### Raw

| Asset | File | Description |
|-------|------|-------------|
| `ga_sample.raw_ga` | `assets/ga_sample/raw_ga.asset.yml` | Source reference to the imported GA360 sessions table (~903K rows). Contains nested RECORD fields for device, geoNetwork, totals, trafficSource, and hits. |

### Staging

| Asset | File | Description |
|-------|------|-------------|
| `ga_sample.staging_ga` | `assets/ga_sample/staging_ga.sql` | Flattened and deduplicated sessions. Parses date strings to DATE, unnests device/geo/traffic/totals RECORDs, deduplicates by fullVisitorId + visitId, adds temporal dimensions (year, month, quarter, day of week, weekend flag). |

### Reports

| Asset | File | Description |
|-------|------|-------------|
| `ga_sample.report_ga_daily` | `assets/ga_sample/report_ga_daily.sql` | Daily aggregate report: sessions, users, pageviews, bounce rate, avg session duration, transactions, revenue, and conversion rate — broken down by date, channel, device category, and country. |

## Lineage

```
raw_ga → staging_ga → report_ga_daily
```

## Run Commands

```bash
# Validate the pipeline
bruin validate ga_sample/

# Run individual assets
bruin run ga_sample/assets/ga_sample/staging_ga.sql
bruin run ga_sample/assets/ga_sample/report_ga_daily.sql

# Run full pipeline (source checks + staging + report)
bruin run ga_sample/

# Query the output tables
bruin query -c bruin-playground-arsalan -q "SELECT * FROM ga_sample.staging_ga LIMIT 10"
bruin query -c bruin-playground-arsalan -q "SELECT * FROM ga_sample.report_ga_daily LIMIT 10"
```

## Known Limitations

- The dataset is obfuscated: `clientId`, `visitorId`, `userId`, and most device sub-fields (browserVersion, mobileDeviceBranding, etc.) contain "not available in demo dataset" or null values.
- Geographic data is partially available — `continent`, `subContinent`, and `country` are populated; `region`, `city`, and lat/long are mostly "not available in demo dataset".
- Revenue values are stored as integers in micros (divided by 1e6 in staging to get USD).
- The `hits` RECORD is not unnested in staging (would require `UNNEST` producing a hit-level table); the pipeline uses session-level `totals` instead.
