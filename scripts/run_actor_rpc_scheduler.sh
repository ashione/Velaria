#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run_actor_rpc_scheduler.sh [--args ...]

Env:
  DO_BUILD          default 0 (set to 1 to run bazel build first)

Examples:
  scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler
EOF
}

if [[ "${1-}" == "-h" || "${1-}" == "--help" ]]; then
  usage
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

DO_BUILD="${DO_BUILD:-0}"

if [[ "${DO_BUILD}" == "1" ]]; then
  bazel build //:actor_rpc_scheduler
fi

exec bazel run //:actor_rpc_scheduler -- "$@"
