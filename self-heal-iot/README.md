# self-heal-iot

BigQuery-backed fixture pipeline for testing self-healing pipeline skills against IoT sensor data. It generates deterministic hourly readings for 12 sensors and intentionally injects bad values, late-arriving writes, and a type-shape change.

This pipeline is safe for local `bruin run` testing. It is not a production-pattern pipeline and does not exercise Bruin Cloud actions directly.

For Bruin Cloud testing, schedule this pipeline daily and run a one-time daily backfill from `2026-03-01` through `2026-05-15` to create clean healthy history. Injected failures start on `2026-05-16`.

## Critical Agent Warning

**DO NOT TARGET THE PYTHON DATA GENERATOR.** `assets/raw/sensor_readings.py` exists only to create deterministic fixture data and inject known failures. It is not part of the self-healing scenario surface. Do not diagnose, repair, open PRs against, backfill because of, or classify the Python generator logic. Use it only to load fixture data, then run the self-healing skills against the generated BigQuery tables, SQL staging asset, Bruin checks, Cloud run/check state, and `.context/` findings.

## Assets

- `self_heal_test_raw.sensor_readings` (`assets/raw/sensor_readings.py`) generates hourly sensor readings with temperature, humidity, battery, reading time, and ingest time.
- `self_heal_test_staging.hourly_sensor_stats` (`assets/staging/hourly_sensor_stats.sql`) computes one row per sensor-hour with ingest lag and filters physically impossible temperature values from downstream aggregates.

## Skill Scenarios

| Scenario | Date/window | Trigger asset/check | Expected skill path | Expected classification |
|---|---:|---|---|---|
| Impossible sensor values | `2026-05-16` | BigQuery table `self_heal_test_raw.sensor_readings`, check `temperature_in_physical_range` | `pipeline-triage` -> `data-quality-investigate` -> `pipeline-report` | `quality-fail`, mode `source-bug` |
| Type-shape narrowing | Starts `2026-05-18` | BigQuery column `self_heal_test_raw.sensor_readings.temperature_c` distribution changes from decimal-like floats to whole-number floats | `pipeline-diagnose` or `schema-drift-check` -> `pipeline-report` | `observed-type-drift` / `type-narrowed`, escalation |
| Late-arriving data | `2026-05-22` | BigQuery table `self_heal_test_raw.sensor_readings`, check `readings_arrive_within_one_hour`; staging check `ingest_lag_under_60_min` | `pipeline-triage` -> `data-quality-investigate` or `freshness-sla-check` -> `pipeline-report` | `late-arriving-data` / `table-frozen` style freshness signal |
| Backfill risk review | Any historical rerun over an already-loaded range | Warehouse asset `self_heal_test_raw.sensor_readings` has append materialization | `pipeline-backfill` dry run -> `pipeline-report` | Requires approval for append rerun where data already exists |

## What This Pipeline Covers

- `pipeline-triage`: quality-fail and stale/late-data routing.
- `pipeline-diagnose`: error-pattern and repo-context gathering for suspicious sensor data.
- `data-quality-investigate`: source-bug and late-arriving-data modes.
- `schema-drift-check`: observed type drift / type narrowing escalation.
- `freshness-sla-check`: table-state freshness through `created_at` vs `reading_time`.
- `pipeline-backfill`: dry-run risk assessment for append materialization.
- `pipeline-report`: final human-readable incident or digest summary from the generated findings.

It does not directly test Slack posting, GitHub PR creation, Bruin Cloud rerun execution, capacity failures, code-regression attribution, or transient Cloud failures. Those require Cloud/Slack/GitHub context or synthetic Cloud run metadata outside this data fixture.

## Useful Commands

```bash
bruin validate self-heal-iot --output json
bruin run self-heal-iot --start-date 2026-03-01 --end-date 2026-05-15
bruin run self-heal-iot/assets/raw/sensor_readings.py --start-date 2026-05-16 --end-date 2026-05-17
bruin run --only checks self-heal-iot/assets/raw/sensor_readings.py
bruin run self-heal-iot/assets/raw/sensor_readings.py --start-date 2026-05-22 --end-date 2026-05-23
bruin run self-heal-iot/assets/staging/hourly_sensor_stats.sql
bruin run --only checks self-heal-iot/assets/staging/hourly_sensor_stats.sql
bruin lineage self-heal-iot/assets/staging/hourly_sensor_stats.sql --output json --full
```

## Expected Notes for Agents

- Treat local `bruin run` as allowed only because this is a self-heal test pipeline.
- Exclude `assets/raw/sensor_readings.py` from self-healing task scope. It is fixture setup, not the thing to fix.
- For production pipelines, the skills must use Bruin Cloud MCP or `bruin cloud ... --output json`.
- A whole-pipeline run can leave failing checks by design. Use narrow date windows when exercising one scenario at a time.
- Type narrowing here is represented as an observed value-shape drift, not a hard physical column-type failure, because the warehouse column is declared as `DOUBLE`.
