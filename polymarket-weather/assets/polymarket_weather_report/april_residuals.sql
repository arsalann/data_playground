/* @bruin

name: polymarket_weather_report.april_residuals
type: bq.sql
description: |
  Forensic analysis dataset containing hourly temperature residuals for Paris-Charles
  de Gaulle airport (station 07157) throughout April 2026. Each row represents one hour
  of the month, creating a 30 days × 24 hours = 720 cell grid used to power the calendar-style
  heatmap visualization in the dashboard.

  Core purpose is to identify temperature anomalies beyond the two days (April 6 and 15)
  reported by the press in connection with alleged sensor tampering. Verified from the
  warehouse during Phase 5: 8 CDG hourly anomalies in April 2026 across 7 distinct days.
  The peer_residual column shows how much CDG deviated from the median of the other five
  Paris-region stations at each hour. Apr 6 17:00 UTC shows the upward spike (+4.0 °C);
  Apr 27 18:00 UTC shows the strongest negative residual (-5.7 °C) — outside the
  press-reported window.

  The dataset isolates CDG readings against peer baselines, filtering the broader
  cross-station anomaly analysis to focus specifically on the suspect sensor. Expected
  to show minimal residuals (<1°C typical) under normal conditions, with significant
  positive residuals (≥3°C) flagged as potential tampering incidents. Negative residuals
  may indicate sensor malfunction or environmental obstruction.

  Feeds directly into Streamlit dashboard charts using Wong-2011 colorblind palette
  with temperature deviation encoded via color intensity and anomaly flags via visual markers.
connection: bruin-playground-arsalan
tags:
  - forensic_analysis
  - temperature_anomaly
  - polymarket_investigation
  - cdg_airport
  - april_2026
  - dashboard_feed
  - calendar_heatmap
  - peer_comparison
  - fact_table
  - hourly_data

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.anomaly_residuals

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: local_date
    type: DATE
    description: Calendar date in Europe/Paris local time. Composite primary key component for the hour×day grid. Covers April 1-30, 2026 (the month containing the alleged tampering incidents).
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: local_hour
    type: INTEGER
    description: Hour of the day in Europe/Paris local time (0-23). Composite primary key component enabling hourly granularity analysis. The alleged tampering incidents occurred around hour 18 (18:30 local).
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: peer_residual
    type: DOUBLE
    description: CDG temperature minus peer median in degrees Celsius. Core forensic metric - positive values indicate CDG was warmer than peer stations. Residuals ≥3°C trigger anomaly investigation. Expected range -5°C to +10°C under normal conditions.
  - name: peer_z
    type: DOUBLE
    description: Robust z-score (peer_residual / 1.4826*IQR) measuring how many standard deviations CDG deviated from peer stations. Values ≥2.0 combined with large residuals indicate potential sensor tampering. Unitless metric immune to outliers.
  - name: is_anomaly
    type: BOOLEAN
    description: Anomaly flag when ABS(peer_residual) ≥3°C AND ABS(peer_z) ≥2.0. Identifies hours where CDG significantly deviated from all peer stations simultaneously - the signature of potential tampering events. Expected to be FALSE for >99% of normal hours.
    nullable: false
  - name: temp_cdg
    type: DOUBLE
    description: Raw CDG airport temperature reading in degrees Celsius from Meteostat station 07157. The suspect sensor at the center of the investigation. Typical April range 5-25°C with alleged spikes reaching 21-22°C during market resolution periods.
  - name: peer_median
    type: DOUBLE
    description: Median temperature across the five other Paris-region stations (07145, 07149, 07150, 07156, 07147) at the same UTC hour, excluding CDG. Robust baseline uncontaminated by the suspect sensor, used as ground truth for detecting single-station anomalies.
  - name: peer_count
    type: INTEGER
    description: Number of peer stations with valid temperature readings contributing to the peer_median calculation. Typically 5 (all other Paris stations) but may be lower due to missing data. Lower counts reduce confidence in anomaly detection.
    checks:
      - name: not_null

@bruin */

SELECT
    local_date,
    local_hour,
    peer_residual,
    peer_z,
    COALESCE(is_anomaly, FALSE) AS is_anomaly,
    temp_c AS temp_cdg,
    peer_median,
    peer_count
FROM `bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals`
WHERE source_id = '07157'
  AND local_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
