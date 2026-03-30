#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

vector_output="$(mktemp)"
trap 'rm -f "$vector_output"' EXIT

bazel run //:vector_search_benchmark -- --quick >"$vector_output"

uv run python - "$vector_output" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = path.read_text().splitlines()

query_profiles = []
transport_profiles = []
for line in lines:
    if not line.startswith("{"):
        continue
    payload = json.loads(line)
    bench = payload.get("bench")
    if bench == "vector-query":
        query_profiles.append(payload)
    elif bench == "vector-transport":
        transport_profiles.append(payload)

if not query_profiles:
    raise SystemExit("missing vector-query benchmark output")
if not transport_profiles:
    raise SystemExit("missing vector-transport benchmark output")

for item in query_profiles:
    for key in [
        "bench",
        "rows",
        "dimension",
        "top_k",
        "metric",
        "cold_query_us",
        "warm_query_avg_us",
        "warm_explain_avg_us",
        "result_rows",
    ]:
        if key not in item:
            raise SystemExit(f"vector-query benchmark JSON missing key: {key}")
    if item["bench"] != "vector-query":
        raise SystemExit("unexpected bench kind in vector-query payload")
    if item["top_k"] != 10:
        raise SystemExit("vector-query benchmark top_k drifted from v0.1 baseline")
    if item["metric"] not in {"cosine", "dot", "l2"}:
        raise SystemExit(f"unexpected vector metric in benchmark output: {item['metric']}")
    if item["result_rows"] <= 0:
        raise SystemExit("vector-query benchmark returned no rows")

for item in transport_profiles:
    for key in [
        "bench",
        "rows",
        "dimension",
        "proto_serialize_us",
        "proto_deserialize_us",
        "proto_payload_bytes",
        "binary_serialize_us",
        "binary_deserialize_us",
        "binary_payload_bytes",
        "actor_rpc_encode_us",
        "actor_rpc_decode_us",
        "actor_rpc_control_bytes",
    ]:
        if key not in item:
            raise SystemExit(f"vector-transport benchmark JSON missing key: {key}")
    if item["bench"] != "vector-transport":
        raise SystemExit("unexpected bench kind in vector-transport payload")

print("[summary] vector search benchmark baseline ok")
PY
