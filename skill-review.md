# Skill Review Summary

This file replaces the original TODO checklist. The self-healing pipeline skill review has been applied to the relevant skills, and the collection documentation has been split out of the root skills README.

## Sources Used

- Bruin docs: https://getbruin.com/docs/bruin/
- Bruin command docs checked: `run`, `validate`, `query`, `cloud`
- Local Bruin source/docs under `/Users/bear/Github/bruin`, especially:
  - `cmd/cloud.go`
  - `cmd/run.go`
  - `cmd/fetch.go`
  - `cmd/lint.go`
  - `cmd/lineage.go`
  - `docs/commands/*.md`
  - `docs/pipelines/variants.md`
  - `docs/assets/definition-schema.md`
  - `docs/quality/overview.md`
  - `docs/quality/custom.md`
  - `pkg/pipeline/pipeline.go`
  - `pkg/scheduler/scheduler.go`

## Applied

- Replaced abbreviated Bruin Cloud command references with exact `bruin cloud ... --output json` forms and required flags.
- Documented that `bruin cloud pipelines errors --output json` has no `--project-id` flag and must be filtered client-side.
- Standardized Cloud run statuses to `success`, `failed`, and `running`; retained `checks_failed` for asset instance state.
- Clarified Bruin Cloud API token resolution: the CLI reads `--api-key`, then `BRUIN_CLOUD_API_KEY`, then the first configured `.bruin.yml` `bruin` connection. `bruin-cloud` is a repo convention, not a CLI-selectable connection name.
- Added a canonical Cloud CLI command matrix to the self-healing collection guide.
- Documented that `runs trigger` and `runs rerun` return a success envelope, so skills must poll `runs list` and verify with `runs get` to obtain the run ID.
- Recast `meta.freshness` as a project convention, not an official Bruin field.
- Added schedule-language guidance: docs prefer `@daily`, `@hourly`, or cron; source also accepts simple schedule names.
- Added `bruin query` as the approved read-only warehouse diagnostic command, with `--description`, `--limit`, `--timeout`, `--dry-run`, and JSON output guidance.
- Added a guardrail that `bruin query --dangerously-bypass-soft-limits` requires human approval.
- Added local quality-check execution syntax:
  - `bruin run --only checks <asset-file>`
  - `bruin run --only checks --single-check <check-id> <asset-file>`
- Clarified that `--single-check` requires a Bruin check ID and a single asset path.
- Added variant-aware static command guidance for `bruin validate` and `bruin lineage`.
- Resolved the custom-check `blocking` default contradiction by documenting runtime behavior from quality docs/source: omitted `blocking` behaves as `true`.

## Skill Updates

- `pipeline-triage`: exact Cloud state commands, Cloud status wording, client-side pipeline-error filtering, and read-only `bruin query` probes.
- `pipeline-diagnose`: exact diagnose/run/asset/instance/log commands, static validation option, machine-readable lineage, variant handling, and `pipeline-backfill` routing for transient retries.
- `pipeline-backfill`: Cloud-only trigger/rerun flow, run-ID polling, interval semantics, `success` status, and expanded materialization risk classification.
- `schema-drift-check`: exact Cloud commands, `bruin query` source sampling, `columns:` metadata distinction, asset definition fields in scope, and full lineage guidance.
- `data-quality-investigate`: corrected custom-check wording, docs-backed custom/column check behavior, read-only failing-row query examples, targeted local check syntax, and Cloud-first verification.
- `freshness-sla-check`: project-convention freshness metadata, `pipeline: all` enumeration, exact Cloud commands, schedule language, table metadata/freshness/growth probes.
- `anomaly-investigate`: concrete `bruin query` examples, Cloud run/check confirmation commands, lineage, and query audit/cost controls.
- `maintenance-pr`: exact validation commands and scope decisions, client-side Cloud error filtering, write-behavior guardrails, quality-check verification wording, and variant-aware validation.
- `pipeline-report`: exact Cloud context commands, Slack-vs-Bruin boundary, and secret redaction rules.

## Documentation Reorganization

- `.agents/skills/README.md` is now a short global skills index.
- `.agents/skills/self-healing-pipelines.md` now contains the collection-level self-healing pipeline guide.
- Skill directories remain flat under `.agents/skills/<skill-name>/SKILL.md` so runtime discovery remains safe.

## Not Applied

- `create-dashboard/SKILL.md` updates from the review were reverted at the user's request. The standalone skill remains in its original pre-review form.

## Verification

- `git diff --check -- .agents/skills` passed after the skill updates and documentation split.
