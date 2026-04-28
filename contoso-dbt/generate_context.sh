#!/usr/bin/env bash
# Generate an AI-agent context layer for the dbt-materialized datasets.
#
# Produces `contoso-dbt/context/` — a Bruin pipeline whose assets describe the
# three datasets (contoso_dbt_raw / contoso_dbt_staging / contoso_dbt_reports)
# as SQL + YAML with AI-enhanced descriptions, tags, and column docs. This
# context layer is *documentation only* — not a runnable transform pipeline;
# AI agents can read it to reason about the shape and semantics of the tables.
#
# A local `.bruin.yml` is written inside context/ so this script does not
# depend on (or modify) the repo-root bruin config. By default the connection
# uses Google Application Default Credentials. If GOOGLE_APPLICATION_CREDENTIALS
# is set, that keyfile is used instead.
#
# Steps:
#   1. Write an isolated .bruin.yml + pipeline.yml into context/.
#   2. `bruin import database` to generate a starter asset per table.
#   3. `bruin ai enhance` to fill in descriptions / metadata / quality checks.
#   4. `bruin validate` to catch any YAML corruption from the enhance step.
#
# Usage:
#   ./generate_context.sh                     # full run (uses claude)
#   ./generate_context.sh --model opus        # pass a specific model
#   ./generate_context.sh --skip-enhance      # import only, no AI enhance
#   ./generate_context.sh --skip-import       # enhance existing assets only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTEXT_DIR="${SCRIPT_DIR}/context"
CONFIG_FILE="${CONTEXT_DIR}/.bruin.yml"
CONNECTION_NAME="${BRUIN_CONNECTION:-contoso_dbt_bq}"
PROJECT_ID="${BIGQUERY_PROJECT:-bruin-playground-arsalan}"
LOCATION="${BIGQUERY_LOCATION:-EU}"

SKIP_IMPORT=0
SKIP_ENHANCE=0
ENHANCE_MODEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-import)  SKIP_IMPORT=1; shift ;;
    --skip-enhance) SKIP_ENHANCE=1; shift ;;
    --model)        ENHANCE_MODEL="$2"; shift 2 ;;
    --model=*)      ENHANCE_MODEL="${1#--model=}"; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

command -v bruin >/dev/null 2>&1 || { echo "bruin CLI not found on PATH" >&2; exit 1; }

mkdir -p "${CONTEXT_DIR}/assets"

# 1. Write an isolated .bruin.yml (idempotent — only write if missing).
if [[ ! -f "${CONFIG_FILE}" ]]; then
  if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    CRED_BLOCK="service_account_file: ${GOOGLE_APPLICATION_CREDENTIALS}"
  else
    CRED_BLOCK="use_application_default_credentials: true"
  fi
  cat > "${CONFIG_FILE}" <<YAML
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: ${CONNECTION_NAME}
          project_id: ${PROJECT_ID}
          location: ${LOCATION}
          ${CRED_BLOCK}
YAML
  echo "==> Wrote isolated bruin config at ${CONFIG_FILE}"
fi

# 2. Pipeline descriptor.
if [[ ! -f "${CONTEXT_DIR}/pipeline.yml" ]]; then
  cat > "${CONTEXT_DIR}/pipeline.yml" <<YAML
name: contoso_dbt_context
schedule: daily
start_date: "2016-01-01"

default_connections:
  google_cloud_platform: "${CONNECTION_NAME}"
YAML
  echo "==> Wrote ${CONTEXT_DIR}/pipeline.yml"
fi

export BRUIN_CONFIG_FILE="${CONFIG_FILE}"

# 3. Import each dbt schema as bruin assets.
if [[ "${SKIP_IMPORT}" -eq 0 ]]; then
  echo "==> bruin import database (contoso_dbt_raw, _staging, _reports)"
  bruin import database \
    --config-file "${CONFIG_FILE}" \
    --connection "${CONNECTION_NAME}" \
    --schemas contoso_dbt_raw \
    --schemas contoso_dbt_staging \
    --schemas contoso_dbt_reports \
    "${CONTEXT_DIR}"

  # Drop dlt internal bookkeeping tables — they aren't useful as context.
  find "${CONTEXT_DIR}/assets" -name "_dlt_*.asset.yml" -delete
else
  echo "==> Skipping import (--skip-import)"
fi

# 4. AI-enhance.
if [[ "${SKIP_ENHANCE}" -eq 0 ]]; then
  echo "==> bruin ai enhance (this may take several minutes)"
  enhance_args=(--claude)
  if [[ -n "${ENHANCE_MODEL}" ]]; then
    enhance_args+=(--model "${ENHANCE_MODEL}")
  fi
  bruin ai enhance "${enhance_args[@]}" "${CONTEXT_DIR}/assets"
else
  echo "==> Skipping enhance (--skip-enhance)"
fi

# 5. Validate — AI enhance has been known to corrupt YAML column blocks,
#    so this post-enhance validate is mandatory.
echo "==> bruin validate ${CONTEXT_DIR}"
bruin validate --config-file "${CONFIG_FILE}" "${CONTEXT_DIR}"

echo
echo "Context layer ready at: ${CONTEXT_DIR}"
echo "  pipeline.yml + assets/*.{sql,yml} describe the dbt-materialized tables."
echo "  Point AI agents at this directory for analysis / docs / query generation."
