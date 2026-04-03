#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

stream_output="$(mktemp)"
actor_output="$(mktemp)"
smoke_output="$(mktemp)"
string_output="$(mktemp)"
trap 'rm -f "$stream_output" "$actor_output" "$smoke_output" "$string_output"' EXIT

bazel run //:stream_benchmark -- 4 512 2 >"$stream_output"
bazel run //:stream_actor_benchmark -- 4 1024 2 2 0 0 all >"$actor_output"
bazel run //:actor_rpc_smoke >"$smoke_output"
bazel run //:string_builtin_benchmark -- 10000 1 >"$string_output"

uv run python - "$stream_output" "$actor_output" "$smoke_output" "$string_output" <<'PY'
import json
import pathlib
import sys

stream_path = pathlib.Path(sys.argv[1])
actor_path = pathlib.Path(sys.argv[2])
smoke_path = pathlib.Path(sys.argv[3])
string_path = pathlib.Path(sys.argv[4])

stream_lines = stream_path.read_text().splitlines()
actor_lines = actor_path.read_text().splitlines()
smoke_lines = smoke_path.read_text().splitlines()
string_lines = string_path.read_text().splitlines()

bench_json = [
    json.loads(line.split(" ", 1)[1])
    for line in stream_lines
    if line.startswith("[bench-json] ")
]
if not bench_json:
    raise SystemExit("missing [bench-json] output from stream_benchmark")
for item in bench_json:
    for key in [
        "name",
        "processed",
        "mode",
        "reason",
        "transport_mode",
        "blocked_count",
        "actor_eligible",
        "used_actor_runtime",
        "used_shared_memory",
    ]:
        if key not in item:
            raise SystemExit(f"stream_benchmark JSON missing key: {key}")

actor_profiles = [
    json.loads(line.split(" ", 1)[1])
    for line in actor_lines
    if line.startswith("[actor-stream-json] ")
]
if not actor_profiles:
    raise SystemExit("missing [actor-stream-json] output from stream_actor_benchmark")
for item in actor_profiles:
    for key in [
        "label",
        "control_plane",
        "data_plane_copy",
        "data_plane_shared_memory",
        "coordinator_merge",
        "raw_metrics",
    ]:
        if key not in item:
            raise SystemExit(f"stream_actor_benchmark JSON missing key: {key}")

decision_lines = [
    json.loads(line.split(" ", 1)[1])
    for line in actor_lines
    if line.startswith("[actor-stream-decision-json] ")
]
if not decision_lines:
    raise SystemExit("missing [actor-stream-decision-json] output from stream_actor_benchmark")
for key in [
    "chosen_mode",
    "sampled_batches",
    "rows_per_batch",
    "average_projected_payload_bytes",
    "actor_speedup",
    "compute_to_overhead_ratio",
    "thresholds_met",
    "reason",
]:
    if key not in decision_lines[0]:
        raise SystemExit(f"actor auto decision JSON missing key: {key}")

if not any("[smoke] actor rpc codec roundtrip ok" in line for line in smoke_lines):
    raise SystemExit("actor_rpc_smoke did not report the expected success marker")

string_bench = [
    json.loads(line)
    for line in string_lines
    if line.startswith('{"bench":"string-builtins"')
]
if not string_bench:
    raise SystemExit("missing string_builtin_benchmark JSON output")

required_cases = {
    "copy-column",
    "single-arg-functions",
    "multi-arg-functions",
    "dependent-chain",
    "sql-plan-and-execute",
    "sql-reused-plan",
}
seen_cases = {item.get("case") for item in string_bench}
missing_cases = sorted(required_cases - seen_cases)
if missing_cases:
    raise SystemExit(f"string_builtin_benchmark missing cases: {', '.join(missing_cases)}")

for item in string_bench:
    for key in [
        "bench",
        "case",
        "rows",
        "rounds",
        "avg_us",
        "rows_per_s",
        "checksum",
        "result_columns",
    ]:
        if key not in item:
            raise SystemExit(f"string_builtin_benchmark JSON missing key: {key}")
    if item["rows_per_s"] <= 0:
        raise SystemExit(f"string_builtin_benchmark non-positive throughput for case: {item['case']}")

print("[summary] stream observability regression ok")
PY
