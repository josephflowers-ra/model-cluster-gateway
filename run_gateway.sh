#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Load optional local env file first.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

# Keep admin auth usable in local/dev runs even if env is missing.
export GATEWAY_ADMIN_USER="${GATEWAY_ADMIN_USER:-admin}"
export GATEWAY_ADMIN_PASS="${GATEWAY_ADMIN_PASS:-adminpass}"

python3 scripts/model_cluster_gateway.py --config "$ROOT_DIR/config/gateway_config.json" --host 0.0.0.0 --port 4010
