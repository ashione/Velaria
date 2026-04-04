#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${VELARIA_PYTHON_BIN:-}" ]]; then
  echo "VELARIA_PYTHON_BIN is required for python stage benchmark" >&2
  exit 1
fi

if ! command -v "${VELARIA_PYTHON_BIN}" >/dev/null 2>&1; then
  echo "VELARIA_PYTHON_BIN does not resolve to an executable: ${VELARIA_PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for python stage benchmark" >&2
  exit 1
fi

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/velaria-stage-bench-XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT
report_dir="$tmp_root/report"
fixture_csv="${ROOT}/python_api/benchmarks/data/stage_input_100k_anonymized.csv"
stage_csv="${VELARIA_STAGE_BENCH_CSV:-}"
if [[ -z "${stage_csv}" && -f "${fixture_csv}" ]]; then
  stage_csv="${fixture_csv}"
fi
default_rounds="3"
if [[ -n "${stage_csv}" ]]; then
  default_rounds="1"
fi
PYTHONPATH="${PYTHONPATH:-${ROOT}/python_api}" \
  uv run --project python_api python python_api/benchmarks/bench_stage_paths.py \
  --outdir "$report_dir" \
  --rows "${VELARIA_STAGE_BENCH_ROWS:-20000}" \
  --rounds "${VELARIA_STAGE_BENCH_ROUNDS:-${default_rounds}}" \
  ${stage_csv:+--csv "$stage_csv"}

uv run --project python_api python - "$report_dir/summary.json" <<'PY'
import json
import pathlib
import sys

summary = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
hardcode = summary["hardcode"]
full = summary["velaria_full"]
reuse = summary["velaria_reuse"]

if len(set(hardcode["row_counts"])) != 1:
    raise SystemExit("hardcode benchmark row counts drifted")
if len(set(full["row_counts"])) != 1:
    raise SystemExit("velaria full benchmark row counts drifted")
if len(set(reuse["row_counts"])) != 1:
    raise SystemExit("velaria reuse benchmark row counts drifted")
if full["row_counts"][0] != reuse["row_counts"][0]:
    raise SystemExit("velaria full/reuse benchmark row counts diverged")
if reuse["total"]["avg"] >= full["total"]["avg"]:
    raise SystemExit("reuse path should remain faster than full path")
if full["sql"]["avg"] >= full["total"]["avg"] * 0.2:
    raise SystemExit("sql stage unexpectedly dominates full-path benchmark")
if full["to_arrow"]["avg"] <= full["sql"]["avg"]:
    raise SystemExit("to_arrow stage should remain heavier than sql in stage benchmark")
if full["to_arrow_pylist"]["avg"] <= full["to_arrow"]["avg"]:
    raise SystemExit("combined export stage should remain heavier than to_arrow alone")

print(
    "[summary] python stage benchmark ok "
    f"hardcode_avg={hardcode['elapsed']['avg']:.6f}s "
    f"full_avg={full['total']['avg']:.6f}s "
    f"reuse_avg={reuse['total']['avg']:.6f}s"
)
PY
