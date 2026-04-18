#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "python wrapper sanitizer smoke is intended for Linux ASan/LSan." >&2
  echo "On macOS, LeakSanitizer is unsupported for this path and AddressSanitizer interceptors are not reliably installed for Python-loaded extensions." >&2
  echo "Use ./scripts/run_python_wrapper_leak_smoke.sh on macOS instead." >&2
  exit 0
fi

if [[ -z "${VELARIA_PYTHON_BIN:-}" ]]; then
  echo "VELARIA_PYTHON_BIN is required for wrapper sanitizer run" >&2
  exit 1
fi

if ! command -v "${VELARIA_PYTHON_BIN}" >/dev/null 2>&1; then
  echo "VELARIA_PYTHON_BIN does not resolve to an executable: ${VELARIA_PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for wrapper sanitizer run" >&2
  exit 1
fi

STRESS_ITERATIONS="${VELARIA_WRAPPER_STRESS_ITERATIONS:-1}"
MODES="${VELARIA_WRAPPER_MODES:-rows arrow}"

bazel build --config=asan //:velaria_pyext //python_api:bench_realtime_wrapper_stress
bazel run --config=asan //python_api:sync_native_extension

uv sync --project python_api --python "${VELARIA_PYTHON_BIN}"

PYTHON_BIN="${ROOT}/python_api/.venv/bin/python"

export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:abort_on_error=1:strict_init_order=1:check_initialization_order=1:fast_unwind_on_malloc=0:detect_stack_use_after_return=1}"
export LSAN_OPTIONS="${LSAN_OPTIONS:-report_objects=1:print_suppressions=1:suppressions=${ROOT}/scripts/lsan_pyarrow.supp}"

if command -v gcc >/dev/null 2>&1; then
  ASAN_LIB="$(gcc -print-file-name=libasan.so)"
  if [[ -n "${ASAN_LIB}" && "${ASAN_LIB}" != "libasan.so" && -f "${ASAN_LIB}" ]]; then
    export LD_PRELOAD="${ASAN_LIB}${LD_PRELOAD:+:${LD_PRELOAD}}"
  fi
fi

for mode in ${MODES}; do
  PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
    "${PYTHON_BIN}" python_api/benchmarks/bench_realtime_wrapper_stress.py "${STRESS_ITERATIONS}" "${mode}"
done

echo "[summary] python wrapper sanitizer smoke ok"
