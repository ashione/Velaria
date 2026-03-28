#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run_actor_rpc_e2e.sh [--sql "SELECT ..."]

Env:
  ADDRESS           default 127.0.0.1:61000
  WORKER_ID         default worker-1
  PAYLOAD           default "demo payload"
  TIMEOUT_SECONDS   default 20
  DO_BUILD          default 0 (set to 1 to run build first)
  BUILD_DASHBOARD   default 1 (set to 0 to skip dashboard frontend build)

Examples:
  scripts/run_actor_rpc_e2e.sh
  scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
  scripts/run_actor_rpc_e2e.sh --payload "hello world"
EOF
}

ADDRESS="${ADDRESS:-127.0.0.1:61000}"
WORKER_ID="${WORKER_ID:-worker-1}"
PAYLOAD="${PAYLOAD:-demo payload}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-20}"
DO_BUILD="${DO_BUILD:-0}"
BUILD_DASHBOARD="${BUILD_DASHBOARD:-1}"
MODE_SQL=""
MODE_PAYLOAD=""

while (($# > 0)); do
  case "$1" in
    --sql)
      MODE_SQL="$2"
      shift 2
      ;;
    --payload)
      MODE_PAYLOAD="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -n "${MODE_SQL}" && -n "${MODE_PAYLOAD}" ]]; then
  echo "cannot pass both --sql and --payload" >&2
  exit 1
fi

if [[ -z "${MODE_SQL}" && -z "${MODE_PAYLOAD}" ]]; then
  MODE_PAYLOAD="${PAYLOAD}"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [[ "${BUILD_DASHBOARD}" != "0" ]]; then
  ./scripts/build_dashboard_frontend.sh
fi

if [[ "${DO_BUILD}" == "1" ]]; then
  bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

SCHED_LOG="${tmp_dir}/scheduler.log"
WORKER_LOG="${tmp_dir}/worker.log"
HOST="${ADDRESS%%:*}"
PORT="${ADDRESS##*:}"

wait_tcp_open() {
  local host="$1"
  local port="$2"
  local timeout="${3:-${TIMEOUT_SECONDS}}"
  local start
  start="$(date +%s)"
  while true; do
    if python3 - "${host}" "${port}" <<'PY' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

try:
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.settimeout(0.3)
  s.connect((host, port))
  s.close()
  print("ok")
except Exception:
  raise SystemExit(1)
PY
    then
      return 0
    fi
    if (( $(date +%s) - start >= timeout )); then
      echo "[e2e] wait timeout: ${host}:${port} open" >&2
      return 1
    fi
    sleep 0.2
  done
}

wait_with_log() {
  local log_file="$1"
  local message="$2"
  local timeout="${3:-${TIMEOUT_SECONDS}}"
  local start
  start="$(date +%s)"
  while true; do
    if [[ -f "${log_file}" ]] && grep -qF "${message}" "${log_file}"; then
      return 0
    fi
    if (( $(date +%s) - start >= timeout )); then
      echo "[e2e] wait timeout: ${message}" >&2
      echo "[e2e] log preview:" >&2
      sed -n '1,50p' "${log_file}" >&2 || true
      return 1
    fi
    sleep 0.2
  done
}

bazel run //:actor_rpc_scheduler -- --listen "${ADDRESS}" --node-id scheduler >"${SCHED_LOG}" 2>&1 &
SCHED_PID=$!
trap 'kill ${SCHED_PID} "${WORKER_PID:-}" "${CLIENT_PID:-}" 2>/dev/null || true; exit 1' INT TERM
wait_tcp_open "${HOST}" "${PORT}"

bazel run //:actor_rpc_worker -- --connect "${ADDRESS}" --node-id "${WORKER_ID}" >"${WORKER_LOG}" 2>&1 &
WORKER_PID=$!
wait_with_log "${WORKER_LOG}" "[worker] connected"

if [[ -n "${MODE_SQL}" ]]; then
  bazel run //:actor_rpc_client -- --connect "${ADDRESS}" --sql "${MODE_SQL}"
else
  bazel run //:actor_rpc_client -- --connect "${ADDRESS}" --payload "${MODE_PAYLOAD}"
fi
CLIENT_EXIT=$?

sleep 0.5
kill "${WORKER_PID}" "${SCHED_PID}" 2>/dev/null || true

echo "----- scheduler.log -----"
sed -n '1,120p' "${SCHED_LOG}"
echo "----- worker.log -----"
sed -n '1,120p' "${WORKER_LOG}"

exit ${CLIENT_EXIT}
