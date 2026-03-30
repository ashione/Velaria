#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

bazel test //:core_regression
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo

echo "[summary] core regression ok"
