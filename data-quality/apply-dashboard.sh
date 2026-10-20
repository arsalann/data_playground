#!/usr/bin/env bash
set -euo pipefail

# Run after this branch is merged and Bruin Cloud has synced the semantic catalog.
# Update creates a draft of the existing dashboard; publication is separate.
data_quality_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
bruin cloud dashboards update \
  --dashboard-id 1135 \
  --state-file "$data_quality_dir/dashboard-dac/dashboards/data-quality.yml"
