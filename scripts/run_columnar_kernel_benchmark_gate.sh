#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

batch_rows="${VELARIA_COLUMNAR_GATE_BATCH_ROWS:-1048576}"
batch_rounds="${VELARIA_COLUMNAR_GATE_BATCH_ROUNDS:-3}"
file_rows="${VELARIA_COLUMNAR_GATE_FILE_ROWS:-200000}"
file_rounds="${VELARIA_COLUMNAR_GATE_FILE_ROUNDS:-3}"
string_rows="${VELARIA_COLUMNAR_GATE_STRING_ROWS:-100000}"
string_rounds="${VELARIA_COLUMNAR_GATE_STRING_ROUNDS:-3}"

batch_output="$(mktemp)"
file_output="$(mktemp)"
string_output="$(mktemp)"
trap 'rm -f "$batch_output" "$file_output" "$string_output"' EXIT

bazel run //:batch_aggregate_benchmark -- "$batch_rows" "$batch_rounds" >"$batch_output"
bazel run //:file_source_benchmark -- "$file_rows" "$file_rounds" >"$file_output"
bazel run //:string_builtin_benchmark -- "$string_rows" "$string_rounds" >"$string_output"

uv run --project python python - "$batch_output" "$file_output" "$string_output" <<'PY'
import json
import pathlib
import re
import sys

batch_path = pathlib.Path(sys.argv[1])
file_path = pathlib.Path(sys.argv[2])
string_path = pathlib.Path(sys.argv[3])

batch_lines = batch_path.read_text().splitlines()
file_lines = file_path.read_text().splitlines()
string_lines = string_path.read_text().splitlines()

batch_pattern = re.compile(
    r"^\[batch-aggregate-bench\] "
    r"scenario=(?P<scenario>\S+) rows=(?P<rows>\d+) "
    r"selected_impl=(?P<impl>\S+) partial_layout=(?P<layout>\S+) "
    r"runtime_shape=(?P<shape>\S+) ordered_input=(?P<ordered>\S+) "
    r"elapsed_ms=(?P<elapsed>\d+)"
)
batch = {}
for line in batch_lines:
    match = batch_pattern.match(line)
    if not match:
        continue
    item = match.groupdict()
    item["elapsed_ms"] = int(item["elapsed"])
    batch[item["scenario"]] = item

required_batch = {
    "single-int64-low-domain": ("state-columnar", "sum-single-int64-key"),
    "single-int64-low-domain-count": ("state-columnar", "count-single-int64-key"),
    "single-int64-low-domain-avg": ("state-columnar", "avg-single-int64-key"),
    "double-int64": ("state-columnar", "sum-double-int64-key"),
    "double-int64-count": ("state-columnar", "count-double-int64-key"),
    "double-int64-avg": ("state-columnar", "avg-double-int64-key"),
}
missing_batch = sorted(set(required_batch) - set(batch))
if missing_batch:
    raise SystemExit(f"missing batch aggregate scenarios: {', '.join(missing_batch)}")
for scenario, (layout, shape) in required_batch.items():
    item = batch[scenario]
    if item["layout"] != layout or item["shape"] != shape:
        raise SystemExit(
            f"{scenario} expected {layout}/{shape}, got {item['layout']}/{item['shape']}"
        )
    if item["elapsed_ms"] <= 0:
        raise SystemExit(f"{scenario} reported non-positive elapsed_ms")

file_rows = [json.loads(line) for line in file_lines if line.startswith("{")]
compare = {
    row["case"]: row
    for row in file_rows
    if row.get("bench") == "file-input-compare"
}
required_pushdown = {
    "sql_csv_predicate_and_group_count",
    "sql_csv_predicate_or_group_count",
    "sql_csv_predicate_mixed_group_count",
    "sql_line_predicate_or_group_count",
    "sql_json_predicate_or_group_count",
}
missing_pushdown = sorted(required_pushdown - set(compare))
if missing_pushdown:
    raise SystemExit(f"missing file-source pushdown comparisons: {', '.join(missing_pushdown)}")
for case in sorted(required_pushdown):
    ratio = compare[case].get("ratio")
    if ratio is None:
        raise SystemExit(f"{case} missing pushdown ratio")
    if ratio >= 0.40:
        raise SystemExit(f"{case} pushdown ratio too high: {ratio}")

string_rows = [json.loads(line) for line in string_lines if line.startswith('{"bench":"string-builtins"')]
string_cases = {row["case"]: row for row in string_rows}
required_string = {
    "copy-column",
    "single-arg-functions",
    "multi-arg-functions",
    "dependent-chain",
    "sql-plan-and-execute",
    "sql-reused-plan",
}
missing_string = sorted(required_string - set(string_cases))
if missing_string:
    raise SystemExit(f"missing string benchmark cases: {', '.join(missing_string)}")
for case, row in string_cases.items():
    if row.get("rows_per_s", 0) <= 0:
        raise SystemExit(f"{case} reported non-positive throughput")
if string_cases["sql-reused-plan"]["avg_us"] >= string_cases["sql-plan-and-execute"]["avg_us"]:
    raise SystemExit("sql-reused-plan should remain faster than sql-plan-and-execute")

print("[summary] columnar kernel benchmark gate ok")
PY
