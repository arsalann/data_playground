## NYC Taxi Insights Dashboard – Run & Screenshot Guide

1) **Prep MotherDuck token**
   - Easiest: keep `.bruin.yml` in repo root; the app will auto-read the `motherduck-prod` token.
   - Or export manually (overrides):
     - macOS/Linux: `export MOTHERDUCK_TOKEN="<token>"`
     - Windows (PowerShell): `$Env:MOTHERDUCK_TOKEN="<token>"`
2) **Install deps** (in repo root or a venv):
   ```bash
   pip install streamlit duckdb motherduck altair pandas pyyaml
   ```
3) **(Optional) Quick data spot-checks via Bruin**
   ```bash
   bruin query --connection motherduck-prod --query "select * from staging.trips_summary limit 5"
   ```
4) **Run the dashboard**
   ```bash
   streamlit run nyc-taxi/assets/reports/streamlit_app.py
   ```
5) **What’s inside (charts)**
   - Tip % by day-of-week × month (`seasonality_dow.sql`)
   - Tip % by day-of-week × hour (`dow_hour_tip.sql`)
   - Tip % by borough pickup→dropoff flow (`socio_trends.sql`)
   - Monthly totals: trips, fare, tip, total + tip-rate trend (`monthly_totals.sql`)
   - Cleaning applied: distance 0.05–100 mi; duration 1–120 min; speed 1–80 mph; fares $0–$500; non-negative tips; fare_amount > 0; tip_rate <= 100%; exclude unknown/N/A/outside boroughs.
6) **Refresh data**
   - Re-run the pipeline for the latest interval, then restart Streamlit; queries hit the cleaned tables directly.
