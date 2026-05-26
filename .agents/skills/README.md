# Self-Healing Pipeline Skills

A set of composable skills for Bruin Cloud scheduled agents to perform routine data-engineering and analytics-engineering work: triaging failures, diagnosing issues, backfilling, investigating data quality, handling schema drift, explaining anomalies, opening maintenance PRs, and reporting to Slack.

These skills are for **Bruin Cloud only**. They must use Bruin Cloud MCP when available and `bruin cloud ... --output json` commands as the CLI fallback. They must not create operational runs with local `bruin run`; local repo commands are allowed only for static evidence such as `bruin lineage`, file inspection, validation before a maintenance PR, and git/PR history.

## Bruin Cloud Access

Use the Bruin Cloud API token from the `.bruin.yml` `bruin` connection named `bruin-cloud`. The connection shape is:

```yaml
environments:
  default:
    connections:
      bruin:
        - name: "bruin-cloud"
          api_token: "..."
```

The Cloud CLI resolves credentials from `--api-key`, `BRUIN_CLOUD_API_KEY`, or a `.bruin.yml` `bruin` connection. If multiple `bruin` connections exist and the CLI cannot select by name, export `BRUIN_CLOUD_API_KEY` from the `bruin-cloud` connection for the duration of the command. Never print or persist the token in `.context/`, Slack, PRs, or logs.

When a skill needs Bruin Cloud state or action, use this preference order:

1. Bruin Cloud MCP tools, if connected and capable of the operation.
2. `bruin cloud ... --output json` commands, with exact flags checked against current Bruin docs/source.
3. Human escalation if neither MCP nor Cloud CLI can perform the operation safely.

Useful Cloud CLI commands verified against the Bruin docs/source:

```shell
bruin cloud projects list --output json
bruin cloud pipelines list --project-id <project-id> --output json
bruin cloud pipelines get --project-id <project-id> --name <pipeline> --output json
bruin cloud pipelines errors --output json
bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json
bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --latest --output json
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --latest --output json
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline> --start-date <ISO-or-date> --end-date <ISO-or-date> --output json
bruin cloud runs rerun --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --only-failed --output json
bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json
bruin cloud assets get --project-id <project-id> --pipeline <pipeline> --asset <asset> --output json
bruin cloud instances list --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
bruin cloud instances get --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --asset <asset> --output json
bruin cloud instances logs --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --asset <asset> --output json
bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
```

Bruin Cloud `runs trigger` triggers a pipeline run for an interval. It is not an asset-level local backfill command. If an operation requires asset-scoped execution, confirm that Bruin Cloud MCP supports it; otherwise escalate to a human instead of inventing a local workaround.

## Evidence Rules

Every investigative skill must use Bruin docs/source behavior for Cloud commands, Bruin Cloud MCP when available, and repo evidence when relevant. The repo is part of the evidence surface: inspect asset files, `bruin lineage`, git history, and recent PRs/code changes before assigning cause to source drift, code regression, data quality, or anomaly patterns.

Each skill is a single `SKILL.md` file in its own directory. The skills are atomic on purpose: one skill, one job. Composition happens at the scheduled-agent layer, not inside any individual skill.

## The 7-Section Contract

Every skill follows the same shape so the scheduler can pick the right one and the human can audit the run:

1. **When to use** — and explicitly when NOT to.
2. **Inputs** — required vs. optional, with examples.
3. **Context to gather** — the read-only calls run before any decision.
4. **Decision tree** — the classification logic, in pseudo-code.
5. **Actions & guardrails** — auto-allowed / requires-approval / never-allowed.
6. **Verification** — how the skill knows the action stuck.
7. **Reporting** — what gets written to `.context/` and to Slack.

`.context/` is the shared filesystem between skills. Triage drops a snapshot; diagnose reads it; maintenance-pr requires a finding file as input; report annotates them all with the resulting permalink. The chain is auditable end-to-end.

## Skill Map

| Skill | One-line job | Writes to repo? | Posts to Slack? | Uses Bruin Cloud/MCP? |
|---|---|---|---|---|
| [`pipeline-triage`](pipeline-triage/SKILL.md) | Classify what's wrong and route | no | no (hands to report) | yes (read) |
| [`pipeline-diagnose`](pipeline-diagnose/SKILL.md) | Single-asset forensics | no | no | yes (read) |
| [`pipeline-backfill`](pipeline-backfill/SKILL.md) | Rerun a range safely | no | no (hands to report) | yes (Cloud action) |
| [`schema-drift-check`](schema-drift-check/SKILL.md) | Detect & classify source drift | no | no | yes (read) |
| [`data-quality-investigate`](data-quality-investigate/SKILL.md) | Walk failed checks to root cause | no | no | yes (read) |
| [`freshness-sla-check`](freshness-sla-check/SKILL.md) | Find stale data, classify cause | no | no | yes (read + limited Cloud retrigger) |
| [`anomaly-investigate`](anomaly-investigate/SKILL.md) | Explain a metric spike/dip | no | no | yes (read) |
| [`maintenance-pr`](maintenance-pr/SKILL.md) | Open a PR for an allow-listed fix | **yes** | no (hands to report) | yes (read/validate) |
| [`pipeline-report`](pipeline-report/SKILL.md) | Slack status / incident / digest | annotates findings | **yes** | no |

Only `maintenance-pr` writes to the repo. Only `pipeline-report` posts to Slack. Every other skill is read-only against the world.

## Typical Run Shapes

### Alert-driven (most common)

```
alert webhook
  → pipeline-triage             (classify each failing asset)
      → schema-drift-check         (if any class = schema-drift)
          → maintenance-pr
      → data-quality-investigate   (if any class = quality-fail)
          → maintenance-pr            → pipeline-backfill
      → anomaly-investigate        (if any class = anomaly)
      → freshness-sla-check        (if any class = stale)
      → pipeline-diagnose          (if class = unknown)
  → pipeline-report             (always last; one consolidated message)
```

### Scheduled tick (no alert)

```
cron tick (every 15-30 min)
  → freshness-sla-check         (catch missed runs even when nothing errored)
  → pipeline-triage             (full state scan)
  → pipeline-report             (only if there's something to say)
```

### Human-asked ("why did this fail?")

```
human question
  → pipeline-diagnose <asset>   (gather context, produce hypothesis)
  → pipeline-report             (post the hypothesis to a thread)
```

### Daily digest

```
daily cron
  → pipeline-triage --pipeline all --since 24h
  → pipeline-report             (severity: info, digest format)
```

## Per-Skill Reference

### `pipeline-triage`

**Purpose** — Entry point and dispatcher. Pulls full pipeline state, classifies every issue into exactly one of 9 classes, hands each one to the right specialist.

**Use when** — an alert fires, a scheduled tick runs, a human asks "what's broken", or another skill needs a fresh state snapshot.

**Don't use for** — single-asset deep dives (use `pipeline-diagnose`), running fixes (use specialists), or composing the human-facing message (use `pipeline-report`).

**Key inputs** — `pipeline` (name or `all`), `since` (lookback), `severity_floor`.

**Key guardrail** — never modifies state; every issue must receive a class, even if `unknown`; partial state never reported as health.

**Hands off to** — every other specialist skill, and always ends with `pipeline-report`.

---

### `pipeline-diagnose`

**Purpose** — Forensics on one asset. Pulls logs, error fingerprint, upstream state, recent commits, and matches against a library of common error patterns to produce a root-cause hypothesis.

**Use when** — triage classified an asset as `unknown`, a retry already failed, or a human asks "why".

**Don't use for** — scanning a whole pipeline (use `pipeline-triage`), or executing a fix.

**Key inputs** — `asset`, optional `run_id`.

**Key guardrail** — speculation without a matched signal is forbidden. "I don't know" with evidence is a valid output; "I don't know" with no work shown is not.

**Hands off to** — whichever specialist the matched pattern recommends, or `pipeline-report` if `unknown`.

---

### `pipeline-backfill`

**Purpose** — Safely trigger or rerun Bruin Cloud pipeline runs for a specific time range, interval-by-interval where intervals are meaningful, with Cloud run verification at each step.

**Use when** — a fix has merged, a transient failure needs a retry, or an upstream republished historical data.

**Don't use for** — first-time runs (those happen on schedule), running a pipeline with no prior success, source/raw table full refreshes, or "just rerun everything" requests with no scoped range.

**Key inputs** — `project_id`, `pipeline`, optional motivating `asset`, `start`, `end`, `reason` (required, logged for audit), `mode` (`trigger`/`rerun`), `dry_run` (default true).

**Key guardrail** — pre-flight checks must pass; Cloud reruns are planned by Bruin run interval where intervals are meaningful; destructive or irreversible source/raw table refreshes are prohibited; ranges > 7 days, large tables, and uncertain consequences require human approval or full human handoff.

**Hands off to** — `pipeline-report` with full row-count delta.

---

### `schema-drift-check`

**Purpose** — Compare a Bruin asset's declared schema to the live source schema and observed values, classify each diff into drift types (`column-added`, `column-renamed`, `type-narrowed`, `observed-type-drift`, `enum-value-added`, etc.), and propose the minimum-impact fix.

**Use when** — `pipeline-diagnose` hypothesizes schema drift, `bruin validate` warns about drift, or a vendor announces a schema change.

**Don't use for** — wrong values with a correct schema (use `data-quality-investigate`), or proposing new columns nobody asked for.

**Key inputs** — `asset`, optional `suspected_column`, `project_id`, `pipeline`, and lineage scope.

**Key guardrail** — `type-narrowed` and `observed-type-drift` are never auto-actioned without clear downstream impact; `column-removed` requires approval when downstream count > 0; column-level lineage must be listed.

**Hands off to** — `maintenance-pr` for most drift types; escalates to `pipeline-report` for narrowings.

---

### `data-quality-investigate`

**Purpose** — When a Bruin custom check or column check fails, pull the offending rows, profile them by interval/dimension, bisect Cloud history to find the first-failure interval, and classify the failure into one of 9 modes (`source-bug`, `transform-bug`, `late-arriving-data`, `seasonality-miss`, etc.).

**Use when** — a quality check failed on an otherwise successful run, or a downstream consumer reported wrong numbers.

**Don't use for** — schema problems (use `schema-drift-check`), missing data (use `freshness-sla-check`), or designing new checks (that's a human task).

**Key inputs** — `asset`, `check_name`, optional `run_id`, `project_id`, and `pipeline`.

**Key guardrail** — never deletes failing rows to make a check pass; never silences a check; threshold changes require approval.

**Hands off to** — `maintenance-pr` for transform fixes, `pipeline-backfill` once a fix is merged.

---

### `freshness-sla-check`

**Purpose** — Find assets past their inferred freshness expectation and classify why: upstream stale, source down, scheduler missed a tick, run is stuck, table is frozen, or growth regressed. Infers freshness from asset descriptions, pipeline schedule, warehouse table metadata, max freshness columns, table growth history, and Cloud run cadence.

**Use when** — scheduled tick (every 15-30 min), triage flagged staleness, after a maintenance window, or a human asks "is the data fresh".

**Don't use for** — failed runs that produced errors (use `pipeline-diagnose`) or assets where no expectation can be inferred from description, schedule, table metadata, or history (those are listed as `unmonitored`).

**Key inputs** — `pipeline` (or `all`), `grace_minutes`.

**Key guardrail** — never marks an asset fresh without Cloud and table-level evidence; never fabricates Bruin metadata; can retrigger at most one missed Cloud interval per asset automatically, more requires approval. A PR to document freshness/growth expectations or add custom checks requires approval.

**Hands off to** — `pipeline-diagnose` for `attempted-failed` cases, `pipeline-report` for everything else.

---

### `anomaly-investigate`

**Purpose** — Explain a metric spike or dip via dimension slicing and upstream attribution. Distinguishes pipeline causes (double-count, undercount, definitional change) from real-world causes (new segment, seasonality, distribution shift).

**Use when** — a tracked metric is out of expected range but no error or quality check fired, or a human asks "why did X spike on day Y".

**Don't use for** — failed runs, schema problems, freshness issues, or claiming real-world causation (we surface signals, not interpret).

**Key inputs** — `metric` (or `asset.column`), `window`, optional `dimensions`, `baseline` method.

**Key guardrail** — read-only; attribution > 100% is forbidden (a common bug with overlapping dimensions); coverage < 50% must be reported as `anomaly-unexplained`, not stretched.

**Hands off to** — `data-quality-investigate` if pipeline cause attributed, `pipeline-report` otherwise.

---

### `maintenance-pr`

**Purpose** — The only skill with repo write access. Opens a PR for routine, allow-listed maintenance: column rename, type widening, dependency patch bump, dedup-window adjust, asset/column description updates, custom check create/update, dead-code removal. Every PR is traceable to a finding file produced by another skill.

**Use when** — another skill produced a finding with `action: maintenance-pr`, a scheduled tick found an allow-listed task, or a human explicitly requested a routine PR.

**Don't use for** — feature work, refactors, anything that changes pipeline behavior in a user-visible way, or any change without a finding file.

**Key inputs** — `finding_file` (must exist and validate), optional `branch_name`, `draft` (default true).

**Key guardrail** — only allow-listed change types; never merges (humans only); branch must start with `self-healing/`; branch scoped to finding's declared files; secrets scan on the diff; CI must run before the PR is considered "open"; end-to-end tests run only in safe non-prod environments, otherwise the PR must clearly say it was not tested.

**Hands off to** — `pipeline-report` with the PR URL and CI status.

---

### `pipeline-report`

**Purpose** — Post structured Slack messages for status, incidents, escalations, and digests. Consistent shape (subject → what happened → what was done → what needs attention → evidence → suggested follow-up) so a human can pick up cold.

**Use when** — end of every self-healing run (even when nothing was done), a specialist produced an escalation, or a scheduled digest is due.

**Don't use for** — ad-hoc conversation, posting raw query results, or replacing PR review comments.

**Key inputs** — `channel`, `severity` (info/warn/error/critical), `subject`, optional `source_files`, `thread_ts`, `mentions`.

**Key guardrail** — severity is set by the caller, never silently changed; dedups against last hour of channel history (replies in thread instead of double-posting); never includes secrets or full row dumps; never pages someone not on current on-call.

**Hands off to** — nothing; this is always the terminal skill.

## Composition Rules

A few invariants that hold across the whole system:

1. **Every run ends with `pipeline-report`.** Silent runs are forbidden — if nothing was wrong, post an `info` digest line; if nothing was done because approval is required, post a `warn` with the plan.
2. **`pipeline-triage` is the only entry point.** Other skills can be called directly by humans, but scheduled agents should always start from triage so classification is consistent.
3. **Findings flow through `.context/`.** Skills don't pass structured state to each other through memory — they write a file and pass the path. This is what makes the whole chain auditable.
4. **Read-only by default.** A skill that touches state (`pipeline-backfill`, `maintenance-pr`) is the exception, not the rule, and must declare every write in its guardrails section.
5. **One specialist per class.** Triage groups issues by class so each specialist sees a batch of similar problems, not a mixed bag.

## What These Skills Deliberately Don't Cover

- **Feature work** — building new assets, new dashboards, new metrics. That's a human task.
- **Designing new quality checks** — the skills act on checks that exist, they don't author new ones.
- **Capacity / billing decisions** — the agent can detect capacity exhaustion but always escalates.
- **Source-system changes** — we're consumers; we don't push schema changes back to vendors.
- **Merge decisions** — `maintenance-pr` opens PRs as drafts. A human reviews and merges.
- **Real-world causation** — `anomaly-investigate` attributes deltas to dimensions; it doesn't claim to know why the world changed.

## Testing Locally

Three fake-data pipelines exist for exercising the skills end-to-end without touching production:

- `fake-shop/` — orders + products. Injects schema drift, duplicate keys, country-concentration anomaly, freshness gap.
- `fake-iot/` — sensor readings. Injects impossible values, type narrowing, late-arriving data.
- `fake-webevents/` — pageviews. Injects single-dimension anomaly, new browser segment, multi-day freshness gap.

Each pipeline's raw asset file documents the injected issues, their dates, and which skill should detect each one. Local `bruin run` is acceptable only for these fake-data test pipelines; it is not the operational path for the Bruin Cloud skills.
