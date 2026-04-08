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
bazel run //python_api:sync_native_extension

uv sync --project python_api --python "${VELARIA_PYTHON_BIN}"
bazel test //:python_ecosystem_regression

tmp_csv="$(mktemp "${TMPDIR:-/tmp}/velaria-python-ci-XXXXXX.csv")"
tmp_vec_csv="$(mktemp "${TMPDIR:-/tmp}/velaria-python-ci-vector-XXXXXX.csv")"
trap 'rm -f "$tmp_csv" "$tmp_vec_csv"' EXIT
printf 'id,name\n1,alice\n2,bob\n' >"$tmp_csv"
printf 'id,embedding\n1,[1 0 0]\n2,[0.9 0.1 0]\n3,[0 1 0]\n' >"$tmp_vec_csv"

PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/examples/demo_stream_sql.py
PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/velaria_cli.py \
  file-sql \
    --csv "$tmp_csv" \
    --query "SELECT * FROM input_table LIMIT 1"
PYTHONPATH="${PYTHONPATH:-$(pwd)/python_api}" \
  uv run --project python_api python python_api/velaria_cli.py \
    vector-search \
    --csv "$tmp_vec_csv" \
    --vector-column embedding \
    --query-vector "1.0,0.0,0.0" \
    --metric cosine \
    --top-k 2
