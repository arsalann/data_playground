# Skill Review TODOs

Reviewed skills in `.agents/skills` against Bruin official docs and local Bruin source at `/Users/bear/Github/bruin`. No skill files were changed.

## Sources Used

- Official docs: https://getbruin.com/docs/bruin/
- Local docs source:
  - `/Users/bear/Github/bruin/docs/commands/cloud.md`
  - `/Users/bear/Github/bruin/docs/commands/run.md`
  - `/Users/bear/Github/bruin/docs/commands/validate.md`
  - `/Users/bear/Github/bruin/docs/commands/lineage.md`
  - `/Users/bear/Github/bruin/docs/commands/query.md`
  - `/Users/bear/Github/bruin/docs/commands/connections.md`
  - `/Users/bear/Github/bruin/docs/pipelines/definition.md`
  - `/Users/bear/Github/bruin/docs/assets/definition-schema.md`
  - `/Users/bear/Github/bruin/docs/assets/columns.md`
  - `/Users/bear/Github/bruin/docs/assets/materialization.md`
  - `/Users/bear/Github/bruin/docs/quality/overview.md`
  - `/Users/bear/Github/bruin/docs/quality/custom.md`
- CLI/source of truth:
  - `/Users/bear/Github/bruin/cmd/cloud.go`
  - `/Users/bear/Github/bruin/cmd/run.go`
  - `/Users/bear/Github/bruin/cmd/lint.go`
  - `/Users/bear/Github/bruin/cmd/lineage.go`
  - `/Users/bear/Github/bruin/cmd/connections.go`
  - `/Users/bear/Github/bruin/pkg/pipeline/yaml.go`
  - `/Users/bear/Github/bruin/pkg/pipeline/pipeline.go`
  - `/Users/bear/Github/bruin/pkg/config/manager.go`
  - `/Users/bear/Github/bruin/pkg/config/connections.go`
  - `/Users/bear/Github/bruin/pkg/bruincloud/api.go`
  - `/Users/bear/Github/bruin/pkg/bruincloud/types.go`

## Cross-Cutting TODOs

- [ ] Replace all abbreviated Bruin Cloud command mentions with exact CLI forms and required flags. Source `cmd/cloud.go` requires `--project-id` and `--pipeline` for most run/instance commands, plus either `--run-id` or `--latest` where applicable. Skills should not say only `bruin cloud instances get`, `bruin cloud instances logs`, `bruin cloud instances failed-logs`, or `bruin cloud assets list` without showing the required flags.

- [ ] Standardize Cloud run status wording to the values used by the CLI/API. `cmd/cloud.go` and `docs/cloud/runs.md` use `success`, `failed`, and `running` for runs; asset instances can also surface `checks_failed`. Replace `succeeded` where it is meant as an API status, or clarify it as prose.

- [ ] Clarify Bruin Cloud API token resolution. The skills repeatedly require a `.bruin.yml` `bruin` connection named `bruin-cloud`, but `cmd/cloud.go` reads `--api-key`, then `BRUIN_CLOUD_API_KEY`, then the first configured `bruin` connection in `.bruin.yml`; it does not select by connection name. Keep `bruin-cloud` as a repo convention only, and tell agents to export `BRUIN_CLOUD_API_KEY` when they need a specific named connection.

- [ ] Add a short "verified Cloud CLI command matrix" to each Cloud skill or link to one canonical matrix in `.agents/skills/README.md`. The matrix should include:
  - `bruin cloud projects list --output json`
  - `bruin cloud pipelines list --project-id <project-id> --output json`
  - `bruin cloud pipelines get --project-id <project-id> --name <pipeline> --output json`
  - `bruin cloud pipelines errors --output json`
  - `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json`
  - `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
  - `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
  - `bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline> --start-date <start> --end-date <end> --output json`
  - `bruin cloud runs rerun --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --only-failed --output json`
  - `bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json`
  - `bruin cloud assets get --project-id <project-id> --pipeline <pipeline> --asset <asset> --output json`
  - `bruin cloud instances list --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
  - `bruin cloud instances get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --asset <asset> --output json`
  - `bruin cloud instances logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) (--asset <asset> [--step-name <step>] | --step-id <step-id>) --output json`
  - `bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`

- [ ] Note that `bruin cloud pipelines errors` has no `--project-id` flag in source. Any project/pipeline filtering must happen client-side after `bruin cloud pipelines errors --output json`.

- [ ] Note that `bruin cloud runs trigger` and `bruin cloud runs rerun` return a success envelope in JSON, not a run ID. `cmd/cloud.go` calls `printSuccessForOutput`, and `pkg/bruincloud/api.go` discards the API response. Skills that need a resulting run ID must poll `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 1 --output json` after triggering/rerunning.

- [ ] Distinguish Bruin-defined asset fields from local conventions. Bruin supports arbitrary `meta` on assets, but `meta.freshness` is not documented as an official freshness SLA field in the checked docs/source. If skills keep `meta.freshness`, call it a project convention and document expected format.

- [ ] Use actual Bruin pipeline schedule references. Official docs prefer `@daily`, `@hourly`, or cron; source validation also accepts `daily`, `hourly`, `weekly`, and `monthly`. Skills can mention both, but should avoid implying only `daily` is canonical.

- [ ] Add `bruin query` as the approved read-only warehouse diagnostic command where skills say "run diagnostic SELECTs". Official docs define `bruin query --connection <name> --query <sql> --output json`, asset-anchored `bruin query --asset <asset-file> --query <sql> --output json`, and semantic query modes.

- [ ] Add `bruin run --only checks <asset-file>` where a skill needs to verify quality checks locally in a non-operational/static context. Official quality docs state checks can run on their own with `bruin run --only checks assets/my_asset.sql`; operational Cloud skills should still avoid local runs unless explicitly in fake-data test pipelines.

## Per-Skill TODOs

### `.agents/skills/README.md`

- [ ] Add source citations to the "Useful Cloud CLI commands" block. The command list is mostly source-correct, but readers should know it was verified against `cmd/cloud.go`, not only docs.
- [ ] Clarify that `bruin cloud pipelines errors --output json` is global to the token/account from the CLI perspective and accepts no `--project-id` in source.
- [ ] Update the credential section to say the CLI cannot select the `bruin` connection by name; if multiple `bruin` connections exist, export `BRUIN_CLOUD_API_KEY` from the intended one.
- [ ] In "Typical Run Shapes", avoid pseudo-CLI like `pipeline-triage --pipeline all --since 24h` unless it is clearly skill invocation syntax rather than a Bruin CLI command.

### `pipeline-triage/SKILL.md`

- [ ] Expand context step 7 to exact commands with required flags:
  - `bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json`
  - `bruin cloud instances list --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
  - `bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
- [ ] Clarify `bruin cloud pipelines errors --output json` has no project flag and must be filtered after retrieval.
- [ ] Use `success` / `failed` / `running` for Cloud run statuses. Keep `checks_failed` for asset instance status when relevant.
- [ ] Add `bruin query` as the read-only method for row counts or freshness checks when Cloud state is not enough.

### `pipeline-diagnose/SKILL.md`

- [ ] Replace shorthand "`--run-id <run-id>`" with the full command: `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json`.
- [ ] Expand instance/log commands to exact forms, including `--asset` or `--step-id` for logs.
- [ ] Add `bruin lineage <asset-file-path> --output json` as the preferred machine-readable lineage form. Source supports `--output json` and `--full`.
- [ ] Add `bruin validate <asset-file-path> --output json` as a static validation option if the diagnostic depends on parsing the local asset definition.
- [ ] Replace recommendation text "retry once" with "route to `pipeline-backfill`, which may call `bruin cloud runs rerun ... --only-failed` or `bruin cloud runs trigger ...` after preflight".

### `pipeline-backfill/SKILL.md`

- [ ] Change verification text from Cloud run status `succeeded` to `success`.
- [ ] Remove or rewrite `result.cloud_run_id`; `runs trigger` and `runs rerun` do not return a run ID in source. Poll `runs list --limit 1` after the action and match by timestamp/interval.
- [ ] Clarify `start` and `end` semantics. The skill says inclusive start and inclusive end, but Bruin interval semantics are generally a run window; avoid promising inclusivity unless Bruin Cloud API docs explicitly define it.
- [ ] Add `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json` after trigger/rerun polling for final verification.
- [ ] Include all source-supported materialization strategies in risk classification or explicitly mark some as unsupported/escalate: `truncate+insert`, `ddl`, `scd2_by_time`, `scd2_by_column`, `datavault_hub`, `datavault_link`, `datavault_satellite`.

### `schema-drift-check/SKILL.md`

- [ ] Expand `bruin cloud assets get`, `bruin cloud runs diagnose`, and instance logs to exact CLI forms with required flags.
- [ ] Use `bruin lineage <asset-file-path> --output json` and `--full` when complete upstream/downstream evidence is needed.
- [ ] Change "A `bruin validate` warning flagged drift" to a more precise statement. Official `validate` performs config and platform dry-run validation; it can catch query/schema issues, but not every source schema drift.
- [ ] Avoid saying the declared schema can be inferred from SQL projection as equivalent to asset `columns:` metadata. Bruin asset definition docs define `columns` metadata separately; query-column matching is a validation policy, not a substitute for declared metadata.
- [ ] Document the exact asset definition fields in scope: `name`, `type`, `connection`, `depends`, `materialization`, `columns`, `custom_checks`, `secrets`, `meta`.

### `data-quality-investigate/SKILL.md`

- [ ] Fix frontmatter description from `custom_check` to Bruin's documented top-level `custom_checks` or "custom check".
- [ ] Expand Cloud instance/log command references to exact forms with required flags.
- [ ] Add docs-backed details for custom checks: `query`, optional `value`, optional `count`, optional `blocking`, default matching behavior when `value` is omitted.
- [ ] Add docs-backed details for column checks: `columns[].checks[].name`, optional `value`, optional `blocking`, default `blocking: true`.
- [ ] When saying "rerun only read-only diagnostic SELECTs", point to `bruin query --connection <connection> --query <sql> --output json` or `bruin query --asset <asset-file> --query <sql> --output json`.
- [ ] For verification, distinguish Cloud verification from local fake-data tests. Operational verification should be Cloud state; local `bruin run --only checks` is appropriate only for static validation or test pipelines.

### `freshness-sla-check/SKILL.md`

- [ ] Recast `meta.freshness` as a project convention, not a Bruin-defined field, unless a Bruin Cloud API/source field is added.
- [ ] Add exact commands for obtaining asset/run/instance state:
  - `bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json`
  - `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json`
  - `bruin cloud instances list --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
- [ ] Replace `bruin cloud runs trigger` shorthand with full required flags, including `--start-date` and `--end-date`.
- [ ] Clarify how to handle `pipeline: all`: enumerate projects with `projects list`, enumerate pipelines with `pipelines list`, then run per-pipeline commands. There is no single `--pipeline all` Bruin Cloud CLI flag.
- [ ] Use official schedule language: `@daily` / `@hourly` / cron are documented; source also accepts `daily`, `hourly`, `weekly`, `monthly`.

### `anomaly-investigate/SKILL.md`

- [ ] Add concrete Bruin query commands for metric history and dimension slicing, for example `bruin query --asset <asset-file> --query <sql> --output json` or `bruin query --connection <connection> --query <sql> --output json`.
- [ ] Add exact Cloud commands used to confirm runs and checks passed, especially `runs list`, `runs get`, `runs diagnose`, and `instances list`.
- [ ] Use `bruin lineage <asset-file-path> --output json --full` when downstream impact or full upstream provenance is needed.
- [ ] Clarify that "checks passed" maps to Cloud run status `success` or asset instance status not equal to `failed` / `checks_failed`, depending on the API response being inspected.

### `maintenance-pr/SKILL.md`

- [ ] Make the local validation command exact: `bruin validate <pipeline-dir> --output json` or `bruin validate <asset-file-path> --output json`. Source supports both pipeline-directory and asset-path validation.
- [ ] Add a required `bruin validate` scope decision: asset-only validation is faster, but pipeline validation is needed when dependency definitions, downstream SQL, or pipeline defaults changed.
- [ ] Replace generic "Bruin Cloud validation-error inspection" with `bruin cloud pipelines errors --output json` and note that CLI filtering is client-side.
- [ ] Add guardrails for generated PRs that change materialization strategy, `primary_key`, `update_on_merge`, `merge_sql`, or `incremental_key`; these directly affect how Bruin writes data.
- [ ] Confirm PR verification should not claim quality checks passed unless Cloud or `bruin run --only checks <asset>` was actually executed.

### `pipeline-report/SKILL.md`

- [ ] If the report pulls fresh Cloud context, add exact command examples rather than `bruin cloud ...` shorthand.
- [ ] Add a rule to redact `.bruin.yml` connection values, `BRUIN_CLOUD_API_KEY`, and any command output containing `api_token`.
- [ ] Clarify that Slack is outside Bruin CLI; Bruin Cloud docs cover Slack notifications in `pipeline.yml`, but this skill appears to post through a separate Slack integration.
- [ ] When linking to Cloud runs/assets, source the run ID from `runs get/list` and asset names from `assets list/get` instead of from free-form finding files only.

### `create-dashboard/SKILL.md`

- [ ] Separate DAC-specific commands from Bruin CLI commands. `dac validate`, `dac check`, `dac serve`, and `dac query` are not Bruin CLI commands in `/Users/bear/Github/bruin/cmd`; the skill should cite `DAC.md` or the DAC source/schema for those.
- [ ] Align project layout with this repo's rule: DAC discovers the repo-root `.bruin.yml` by walking upward; do not imply every DAC project should contain its own `.bruin.yml`.
- [ ] Add explicit Bruin query validation guidance for dashboard SQL where applicable: `bruin query --connection <connection> --query <sql> --output json` or `dac check --dir <pipeline>/dashboard-dac` when DAC is the intended executor.
- [ ] Keep the `VISUALIZATIONS.md` requirement, but add concrete dashboard structure expectations from the repo rule: header text widget, chart widget with `hideName: true`, footnote text widget, and final Methodology text widget.
- [ ] Add a warning that DAC commands are separate from the official Bruin CLI docs reviewed here; future reviewers should validate DAC fields against `DAC.md` and the local DAC implementation.

## Official Docs vs Source Notes

- [ ] The checked `docs/commands/cloud.md` examples sometimes omit flags that `cmd/cloud.go` requires, especially `--pipeline` for `runs get`, `runs rerun`, `instances list`, `instances get`, `instances logs`, and `instances failed-logs`. Skills should follow `cmd/cloud.go` for executable commands.
- [ ] `docs/commands/cloud.md` says all cloud subcommands support `--output json`, and source confirms a shared output flag. For mutating commands, source returns only a generic success JSON envelope, not the created/affected object.
- [ ] Pipeline definition docs show `@daily` and cron; source validation additionally accepts `daily`, `hourly`, `weekly`, `monthly`, and `continuous` / `@continuous`.
- [ ] Asset definition docs state SQL/Python assets embed Bruin YAML in the same file and standalone YAML assets must be named `<name>.asset.yml` or `<name>.asset.yaml`; skills that discuss edits should preserve this distinction.
