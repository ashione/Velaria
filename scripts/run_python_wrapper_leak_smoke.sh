#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${VELARIA_PYTHON_BIN:-}" ]]; then
  echo "VELARIA_PYTHON_BIN is required for wrapper leak smoke" >&2
  exit 1
fi

if ! command -v "${VELARIA_PYTHON_BIN}" >/dev/null 2>&1; then
  echo "VELARIA_PYTHON_BIN does not resolve to an executable: ${VELARIA_PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for wrapper leak smoke" >&2
  exit 1
fi

bazel build //:velaria_pyext //python_api:bench_realtime_wrapper_stress
bazel run //python_api:sync_native_extension

uv sync --project python_api --python "${VELARIA_PYTHON_BIN}"

STRESS_ITERATIONS="${VELARIA_WRAPPER_STRESS_ITERATIONS:-1}"
MODES="${VELARIA_WRAPPER_MODES:-rows arrow}"
PYTHON_BIN="${ROOT}/python_api/.venv/bin/python"

for mode in ${MODES}; do
  PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
    "${PYTHON_BIN}" python_api/benchmarks/bench_realtime_wrapper_stress.py "${STRESS_ITERATIONS}" "${mode}"
done

if command -v leaks >/dev/null 2>&1; then
  LEAK_ITERATIONS="${VELARIA_WRAPPER_LEAK_ITERATIONS:-1}"
  for mode in ${MODES}; do
    leaks -atExit -- "${PYTHON_BIN}" python_api/benchmarks/bench_realtime_wrapper_stress.py "${LEAK_ITERATIONS}" "${mode}"
  done
else
  echo "[summary] leaks not available; skipped process leak smoke"
fi

echo "[summary] python wrapper leak smoke ok"
