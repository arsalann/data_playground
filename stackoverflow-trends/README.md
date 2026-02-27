# stackoverflow-trends

Analyzes Stack Overflow activity from 2008 to present — question volume, answer rates, and per-tag community trends across the growth, plateau, and post-ChatGPT eras.

## Data Sources

- **BigQuery public dataset** (`bigquery-public-data.stackoverflow`) — Full post history with rich per-question metrics (score, views, answers, acceptance). Frozen at Sep 2022.
- **Stack Exchange API** (`api.stackexchange.com/2.3`) — Live aggregate counts to supplement BigQuery from Oct 2022 onward. Anonymous limit: 300 requests/day.

## Assets

### Raw

| Asset | Type | Description |
|---|---|---|
| `stackoverflow_questions_monthly` | SQL | Monthly question aggregates from BigQuery public dataset |
| `stackoverflow_tags_monthly` | SQL | Monthly per-tag question counts from BigQuery public dataset |
| `stackoverflow_api_monthly` | Python | Monthly question + answer counts from SE API (Oct 2022+) |
| `stackoverflow_tags_api_monthly` | Python | Monthly per-tag counts from SE API (Oct 2022+) |

### Staging

| Asset | Description |
|---|---|
| `stackoverflow_monthly` | Unions BQ + API data, deduplicates, adds era labels and YoY metrics |
| `stackoverflow_tag_trends` | Unions BQ + API tag data, deduplicates, computes peak-normalized trends |

### Reports

| Asset | Description |
|---|---|
| `streamlit_app` | Interactive dashboard with Altair charts |

## Running

```bash
# Full pipeline
bruin run stackoverflow-trends

# Ingest new API data for a specific interval
bruin run --start-date 2024-07-01 --end-date 2026-02-28 stackoverflow-trends/assets/raw/stackoverflow_tags_api_monthly.py

# Launch dashboard
streamlit run stackoverflow-trends/assets/reports/streamlit_app.py
```
