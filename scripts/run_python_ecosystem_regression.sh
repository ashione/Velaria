#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${VELARIA_PYTHON_BIN:-}" ]]; then
  echo "VELARIA_PYTHON_BIN is required for python ecosystem regression" >&2
  exit 1
fi

if ! command -v "${VELARIA_PYTHON_BIN}" >/dev/null 2>&1; then
  echo "VELARIA_PYTHON_BIN does not resolve to an executable: ${VELARIA_PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for python ecosystem regression" >&2
  exit 1
fi

bazel build //:velaria_pyext //python_api:velaria_whl //python_api:velaria_native_whl //python_api:velaria_cli
bazel test //:python_ecosystem_regression

uv sync --project python_api --python "${VELARIA_PYTHON_BIN}"
PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
  uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
  uv run --project python_api python python_api/examples/demo_stream_sql.py

tmp_csv="$(mktemp "${TMPDIR:-/tmp}/velaria-cli-XXXXXX.csv")"
tmp_vec_csv="$(mktemp "${TMPDIR:-/tmp}/velaria-cli-vector-XXXXXX.csv")"
trap 'rm -f "$tmp_csv" "$tmp_vec_csv"' EXIT
printf 'id,name\n1,alice\n2,bob\n' >"$tmp_csv"
printf 'id,embedding\n1,[1 0 0]\n2,[0.9 0.1 0]\n3,[0 1 0]\n' >"$tmp_vec_csv"

PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
  uv run --project python_api python python_api/velaria_cli.py \
    csv-sql \
    --csv "$tmp_csv" \
    --query "SELECT * FROM input_table LIMIT 1"

PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
  uv run --project python_api python python_api/velaria_cli.py \
    vector-search \
    --csv "$tmp_vec_csv" \
    --vector-column embedding \
    --query-vector "1.0,0.0,0.0" \
    --metric cosine \
    --top-k 2

echo "[summary] python ecosystem regression ok"
