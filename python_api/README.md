# Velaria Python Ecosystem

This document is the entrypoint for Velaria's supported Python ecosystem layer.

Python is a supported ingress, interop, and packaging surface. It is not the execution core. Core semantics still come from the native kernel and the runtime contract in [docs/runtime-contract.md](../docs/runtime-contract.md).

The Python layer must assume the core kernel is column-first.

Practical implications:

- Python interop should preserve columnar data as deep as possible
- `rows` are a compatibility/export surface, not the preferred internal execution form
- Arrow import/export work should reduce copy and rematerialization rather than normalizing row-first behavior
- performance-sensitive changes should prefer native kernel improvements and lower-copy boundaries over Python-side workarounds

## Scope

### Supported

The supported Python ecosystem includes:

- the `velaria/` package and `Session` API
- Arrow ingestion and Arrow output
- `uv`-based local workflow
- native extension build
- wheel / native wheel packaging
- the supported CLI entrypoint `velaria_cli.py`
- Excel ingestion via `read_excel(...)`
- Bitable adapters and stream source integration
- custom source / custom sink adapters
- vector search and vector explain APIs

### Examples

Examples and helper assets include:

- `examples/demo_batch_sql_arrow.py`
- `examples/demo_stream_sql.py`
- `examples/demo_bitable_group_by_owner.py`
- `examples/demo_vector_search.py`
- `benchmarks/bench_arrow_ingestion.py`
- local ecosystem scripts and skills

### Experimental

The Python experimental area is currently reserved under `experimental/`.

Anything placed there is explicitly outside the supported ecosystem surface until it is promoted into `velaria/`, `velaria_cli.py`, or a supported adapter module.

### Not In Scope

Python does not define:

- execution hot-path semantics
- a separate progress schema
- a separate checkpoint contract
- a separate vector scoring implementation for supported APIs
- Python UDFs in the hot path
- a row-first fallback policy for the native kernel

## API Surface

Main `Session` API:

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.create_dataframe_from_arrow(...)`
- `Session.create_stream_from_arrow(...)`
- `Session.create_temp_view(...)`
- `Session.read_stream_csv_dir(...)`
- `Session.stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `Session.start_stream_sql(...)`
- `Session.vector_search(...)`
- `Session.explain_vector_search(...)`

Additional ecosystem helpers:

- `read_excel(...)`
- `CustomArrowStreamSource`
- `CustomArrowStreamSink`
- `create_stream_from_custom_source(...)`
- `consume_arrow_batches_with_custom_sink(...)`

Mapping rule:

- Python names may be ecosystem-friendly
- behavior must map back to the same native kernel contract exposed by C++
- Python wrappers should not force row materialization earlier than required by the user-facing boundary

Current SQL mapping carried by Python:

- `Session.sql(...)` maps to core SQL v1 batch semantics:
  - `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, and the current minimal `JOIN`
- batch builtins currently exposed through the same core path:
  - `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`
  - `LENGTH`, `LEN`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `REVERSE`
  - `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR` / `SUBSTRING`, `POSITION`, `REPLACE`
  - `ABS`, `CEIL`, `FLOOR`, `ROUND`, `YEAR`, `MONTH`, `DAY`
- `Session.stream_sql(...)`, `Session.explain_stream_sql(...)`, and `Session.start_stream_sql(...)` share the same stream SQL front-door checks:
  - source must be a source table / stream source
  - sink target must be a sink table
  - only the current stream-stable projection, filter, window, stateful aggregate, and bounded-source `ORDER BY` shapes are accepted
- current SQL v1 keeps `ORDER BY` scoped to columns present in the `SELECT` output
- unsupported SQL shapes are expected to surface as explicit parse / semantic / unsupported SQL v1 / table-kind errors from the core path, not Python-only behavior

## Repository Layout

Stable Python layout in this repo:

- supported library:
  - `python_api/velaria/`
- supported CLI tool:
  - `python_api/velaria_cli.py`
- examples:
  - `python_api/examples/`
- benchmarks:
  - `python_api/benchmarks/`
- reserved experimental area:
  - `python_api/experimental/`
- regression tests:
  - `python_api/tests/`

## Toolchain and Environment

Repository Python commands use `uv`.

Recommended local baseline:

- CPython `3.12` or `3.13`
- `uv`
- local CPython headers (`Python.h`)

Bazel Python detection currently probes local CPython interpreters in the `3.9` to `3.13` range. If auto-discovery fails, set:

```bash
export VELARIA_PYTHON_BIN=/path/to/python3.13
```

That interpreter must expose `Python.h`; otherwise Bazel cannot build the native extension.

## Development Workflow

Bootstrap:

```bash
bazel build //:velaria_pyext
bazel run //python_api:sync_native_extension
uv sync --project python_api --python python3.13
```

If you run `python_api/velaria_cli.py` or other source-checkout Python entrypoints directly,
keep `python_api/velaria/_velaria.so` in sync with:

```bash
bazel run //python_api:sync_native_extension
```

Run demos:

```bash
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
uv run --project python_api python python_api/examples/demo_stream_sql.py
uv run --project python_api python python_api/examples/demo_vector_search.py
```

Recommended regression entrypoint:

```bash
./scripts/run_python_ecosystem_regression.sh
```

That script covers:

- native extension build
- wheel and native wheel build
- Bazel Python regression targets
- demo smoke
- CLI smoke

## Packaging

Build targets:

- native extension:
  - `//:velaria_pyext`
- sync built native extension into the source checkout:
  - `//python_api:sync_native_extension`
- pure-Python wheel wrapper:
  - `//python_api:velaria_whl`
- native wheel:
  - `//python_api:velaria_native_whl`
- Python CLI:
  - `//python_api:velaria_cli`

Single-file CLI packaging:

```bash
./scripts/build_py_cli_executable.sh
./dist/velaria-cli csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

The CLI is part of the ecosystem layer. For supported paths, it should delegate to the same native session contract as Python and C++.

Repo-visible CLI entrypoints are:

- source checkout:
  - `uv run --project python_api python python_api/velaria_cli.py ...`
- installed wheel or local package install:
  - `velaria-cli ...`
  - `velaria_cli ...`
- packaged binary:
  - `./dist/velaria-cli ...`

The global commands are expected only after installing the wheel or package into your environment.

Every top-level command and subcommand supports `--help`.
Use `velaria-cli -i` to enter interactive mode.

### Workspace + Artifacts

The CLI also supports a local workspace layout for tracked runs and artifact indexing.

Default paths:

- runs: `~/.velaria/runs/<run_id>/`
- index: `~/.velaria/index/artifacts.sqlite`

You can override the root with:

```bash
export VELARIA_HOME=/tmp/velaria-home
```

Tracked run commands:

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --run-name "cn_slow_query_24h_2026-04-03" \
  --description "score filter result for demo input" \
  --tag cn \
  --tag "slow-query,demo" \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli run start -- csv-sql \
  --run-name "cn_slow_query_24h_2026-04-03" \
  --description "score filter result for demo input" \
  --tag cn \
  --tag "slow-query,demo" \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py run list --tag cn --query "slow query" --limit 20
uv run --project python_api python python_api/velaria_cli.py run result --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python_api python python_api/velaria_cli.py run show --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run status --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
uv run --project python_api python python_api/velaria_cli.py run cleanup --keep-last 10
```

The tracked workspace contract is:

- stdout returns JSON only
- logs go to `stdout.log` / `stderr.log`
- `run.json` can carry `run_name`, `description`, and `tags` for human-readable context and filtering
- `run list` returns summary-friendly fields such as `artifact_count` and `duration_ms`
- failures return structured JSON with `error_type`, `phase`, optional `run_id`, and `details`
- stream progress appends native `snapshotJson()` output to `progress.jsonl`
- stream explain keeps the native `logical` / `physical` / `strategy` structure
- large results stay in files under `artifacts/`; SQLite stores only index rows and small previews
- deleting run directories requires the explicit `--delete-files` switch

End-to-end examples:

CSV SQL to parquet plus preview:

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --run-name "high_score_rows" \
  --description "high score rows for local inspection" \
  --tag local-demo \
  --tag scores \
  --csv /path/to/input.csv \
  --query "SELECT name, score FROM input_table WHERE score > 10"

uv run --project python_api python python_api/velaria_cli.py run list --tag scores --query "high score"
uv run --project python_api python python_api/velaria_cli.py run result --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
```

Stream SQL once plus status:

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- stream-sql-once \
  --source-csv-dir /path/to/source_dir \
  --sink-schema "key STRING, value_sum INT" \
  --query "INSERT INTO output_sink SELECT key, SUM(value) AS value_sum FROM input_stream GROUP BY key"

uv run --project python_api python python_api/velaria_cli.py run status --run-id <run_id>
```

For this action, the query still follows the core stream SQL boundary:

- `--query` must be `INSERT INTO <sink> SELECT ...`
- the source side must resolve to the stream source table created by the command
- the sink side must resolve to the sink table created from `--sink-schema`
- explain output remains `logical / physical / strategy`, and progress stays native `snapshotJson()`

Vector search plus explain artifact:

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --top-k 5

uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
```

Python ecosystem source groups:

- supported:
  - `//python_api:velaria_python_supported_sources`
- examples and benchmarks:
  - `//python_api:velaria_python_example_sources`
- experimental placeholder:
  - `//python_api:velaria_python_experimental_sources`

## Arrow Contract

Arrow is the preferred interop form for high-volume results.

Guidance:

- prefer Arrow/native columnar paths over `to_rows()` when benchmarking or integrating large results
- treat `to_rows()` as a convenience/debugging surface
- when profiling Python API performance, separate `sql()` cost from `to_arrow()` / `to_rows()` export cost

Supported Arrow ingestion inputs:

- `pyarrow.Table`
- `pyarrow.RecordBatch`
- `pyarrow.RecordBatchReader`
- objects implementing `__arrow_c_stream__`
- Python sequences of Arrow batches

Vector-preferred Arrow shape:

- `FixedSizeList<float32>`

Preferred local CSV vector text shape:

- `[1 2 3]`
- `[1,2,3]`

Current vector search scope:

- local exact scan only
- metrics: `cosine`, `dot`, `l2`
- no ANN / distributed execution / standalone vector DB behavior

## Excel, Bitable, and Custom Streams

### Excel

`read_excel(...)` reads `.xlsx` through:

1. `pandas.read_excel`
2. `pyarrow.Table` conversion
3. `Session.create_dataframe_from_arrow(...)`

Example:

```python
from velaria import Session, read_excel

session = Session()
df = read_excel(session, "/path/to/file.xlsx", sheet_name="Sheet1")
session.create_temp_view("staff", df)
print(session.sql("SELECT * FROM staff LIMIT 5").to_rows())
```

### Bitable and Custom Streams

Supported ecosystem integrations include:

- Bitable-backed stream source flows
- custom Arrow stream sources
- custom Arrow stream sinks

These are supported as ecosystem integrations, not as alternate execution cores.

## Regression Matrix

Python ecosystem regression targets:

- `//python_api:streaming_v05_test`
- `//python_api:arrow_stream_ingestion_test`
- `//python_api:vector_search_test`
- `//python_api:read_excel_test`
- `//python_api:custom_stream_source_test`
- `//python_api:bitable_stream_source_test`
- `//python_api:bitable_group_by_owner_integration_test`

Python-layer grouped suite:

- `//python_api:velaria_python_supported_regression`

Root-level grouped suite:

- `//:python_ecosystem_regression`

## Relation to Core

Python may:

- wrap
- package
- automate
- project ecosystem-friendly names

Python may not:

- redefine progress/checkpoint/explain semantics
- become the source of truth for runtime decisions
- introduce a second vector-search implementation for supported interfaces
- pull the native kernel back toward a row-first design for ecosystem convenience

For core boundaries, see [docs/core-boundary.md](../docs/core-boundary.md). For stable runtime semantics, see [docs/runtime-contract.md](../docs/runtime-contract.md).
