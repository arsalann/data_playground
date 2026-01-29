## Footnotes — processing, cleaning, reporting

### Scope
- **Data source**: NYC TLC yellow taxi trips
- **Date range**: January 2020 – November 2025
- **Raw records ingested**: ~433M rows
- **Records after cleaning**: ~365M rows (~84% retained)

### Tech stack
- **Ingestion/processing**: Bruin pipelines materializing into MotherDuck (DuckDB)
- **Visualization**: Streamlit + Altair + PyDeck

### Two-stage cleaning process

**Stage 1: Staging layer** (`staging.trips_summary`)
- Remove records with NULL pickup/dropoff times or location IDs
- Filter out zero or negative trip durations
- Filter out durations > 8 hours (28,800 seconds)
- Filter out negative total amounts or trip distances
- Only include actual payments: credit card, cash, flex fare (excludes no-charge, disputes, voided)
- Deduplicate records using latest extraction timestamp

**Stage 2: Report layer** (applied in dashboard queries)
- Borough validity: exclude `Unknown`, `N/A`, `Outside of NYC`
- Distance: 0.05–100 miles
- Duration: 60–7,200 seconds (1–120 minutes)
- Speed: 1–80 mph
- Total amount: $0–$500
- Tips: tip_amount ≥ 0; tip_rate ≤ 100%

### Dashboard metrics
- Tip % heatmap by day-of-week × month
- Tip % heatmap by day-of-week × hour
- Tip % by borough pickup → dropoff flow
- Payment type distribution by zone (cash vs credit card)
- Low-tip rate map for credit card payments
- Monthly totals: trips, fare sum, tip sum, total sum + tip rate trend

### Notes
- Cash payments typically show $0 tip in the data (tips are manually entered, often not recorded)
- Credit card tips are automatically captured by the payment terminal
- Cleaning filters are applied consistently across all dashboard visualizations