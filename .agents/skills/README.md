# Agent Skills

This directory contains flat skill folders discovered by the agent runtime. Keep each skill at `.agents/skills/<skill-name>/SKILL.md` unless the runtime is confirmed to support nested discovery.

## Skill Collections

- [Self-healing pipeline skills](self-healing-pipelines.md) - Bruin Cloud operational skills for triage, diagnosis, freshness, quality, schema drift, backfills, anomaly investigation, and maintenance PR proposals.

## Individual Skills

- [create-dashboard](create-dashboard/SKILL.md) - Create, modify, review, or understand Bruin DAC dashboards, widgets, filters, SQL queries, semantic models, and validation workflows.
- [pipeline-triage](pipeline-triage/SKILL.md) - Entry point for self-healing pipeline state scans and issue routing.
- [pipeline-diagnose](pipeline-diagnose/SKILL.md) - Single-asset Bruin Cloud failure forensics.
- [pipeline-backfill](pipeline-backfill/SKILL.md) - Safe Bruin Cloud reruns or backfills after preflight.
- [schema-drift-check](schema-drift-check/SKILL.md) - Source/live schema drift detection and response planning.
- [data-quality-investigate](data-quality-investigate/SKILL.md) - Failed Bruin quality check investigation.
- [freshness-sla-check](freshness-sla-check/SKILL.md) - Freshness and stale asset classification.
- [anomaly-investigate](anomaly-investigate/SKILL.md) - Metric spike/drop attribution through dimension slicing.
- [maintenance-pr](maintenance-pr/SKILL.md) - Finding-gated routine maintenance PR creation.
