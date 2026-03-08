#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

python3 scripts/model_cluster_gateway.py --config "$ROOT_DIR/config/gateway_config.json" --host 0.0.0.0 --port 4010
