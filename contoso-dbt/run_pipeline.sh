#!/usr/bin/env bash
# End-to-end runner: dlt ingest -> dbt deps + build.
#
# Usage: ./run_pipeline.sh [--skip-ingest] [--skip-build]
#
# Uses gcloud ADC for auth. Set GOOGLE_APPLICATION_CREDENTIALS if you prefer
# a keyfile. Venv is created on first run at contoso-dbt/.venv.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${SCRIPT_DIR}/.venv"
PY="${VENV}/bin/python"

SKIP_INGEST=0
SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --skip-ingest) SKIP_INGEST=1 ;;
    --skip-build)  SKIP_BUILD=1 ;;
    *) echo "Unknown flag: $arg" >&2; exit 1 ;;
  esac
done

if [[ ! -x "${PY}" ]]; then
  echo "==> Creating venv at ${VENV}"
  python3 -m venv "${VENV}"
  "${VENV}/bin/pip" install --quiet --upgrade pip
  "${VENV}/bin/pip" install --quiet -r "${SCRIPT_DIR}/ingest/requirements.txt"
fi

export DESTINATION__BIGQUERY__LOCATION="${DESTINATION__BIGQUERY__LOCATION:-EU}"
export DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID="${DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID:-bruin-playground-arsalan}"
export GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT:-bruin-playground-arsalan}"

if [[ "${SKIP_INGEST}" -eq 0 ]]; then
  echo "==> Running dlt ingest -> contoso_dbt_raw"
  "${PY}" "${SCRIPT_DIR}/ingest/pipeline.py"
else
  echo "==> Skipping dlt ingest (--skip-ingest)"
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "==> dbt deps"
  "${VENV}/bin/dbt" deps --project-dir "${SCRIPT_DIR}" --profiles-dir "${SCRIPT_DIR}"
  echo "==> dbt build"
  "${VENV}/bin/dbt" build --project-dir "${SCRIPT_DIR}" --profiles-dir "${SCRIPT_DIR}"
else
  echo "==> Skipping dbt build (--skip-build)"
fi

echo "==> Done."
