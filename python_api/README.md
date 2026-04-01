# Velaria Python Ecosystem

This document is the entrypoint for Velaria's supported Python ecosystem layer.

Python is a supported ingress, interop, and packaging surface. It is not the execution core. Core semantics still come from the native kernel and the runtime contract in [docs/runtime-contract.md](../docs/runtime-contract.md).

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

- CPython `3.12`
- `uv`
- local CPython headers (`Python.h`)

Bazel Python detection currently probes local CPython interpreters in the `3.9` to `3.13` range. If auto-discovery fails, set:

```bash
export VELARIA_PYTHON_BIN=/path/to/python3.12
```

That interpreter must expose `Python.h`; otherwise Bazel cannot build the native extension.

## Development Workflow

Bootstrap:

```bash
bazel build //:velaria_pyext
bazel run //python_api:sync_native_extension
uv sync --project python_api --python python3.12
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
- packaged binary:
  - `./dist/velaria-cli ...`

Do not assume a global `velaria-cli` command exists unless you have separately installed and exposed one in your environment.

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
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli run start -- csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py run show --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run status --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
uv run --project python_api python python_api/velaria_cli.py run cleanup --keep-last 10
```

The tracked workspace contract is:

- stdout returns JSON only
- logs go to `stdout.log` / `stderr.log`
- stream progress appends native `snapshotJson()` output to `progress.jsonl`
- stream explain keeps the native `logical` / `physical` / `strategy` structure
- large results stay in files under `artifacts/`; SQLite stores only index rows and small previews
- deleting run directories requires the explicit `--delete-files` switch

End-to-end examples:

CSV SQL to parquet plus preview:

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT name, score FROM input_table WHERE score > 10"

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

For core boundaries, see [docs/core-boundary.md](../docs/core-boundary.md). For stable runtime semantics, see [docs/runtime-contract.md](../docs/runtime-contract.md).
