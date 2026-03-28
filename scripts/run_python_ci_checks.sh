#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${VELARIA_PYTHON_BIN:-}" ]]; then
  echo "VELARIA_PYTHON_BIN is required" >&2
  exit 1
fi

if ! command -v "${VELARIA_PYTHON_BIN}" >/dev/null 2>&1; then
  echo "VELARIA_PYTHON_BIN does not resolve to an executable: ${VELARIA_PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for Python CI checks" >&2
  exit 1
fi

bazel build //:velaria_pyext

uv sync --project python_api --python "${VELARIA_PYTHON_BIN}"
PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/demo_batch_sql_arrow.py
PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/demo_stream_sql.py
