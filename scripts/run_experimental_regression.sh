#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

bazel test //:experimental_regression
bazel run //:actor_rpc_smoke
./scripts/run_actor_rpc_e2e.sh --payload "experimental regression payload"

echo "[summary] experimental regression ok"
