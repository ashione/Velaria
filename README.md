# Velaria: A Pure C++20 Local Dataflow Kernel

`README.md` is the English source of truth. The Chinese mirror lives in [README-zh.md](./README-zh.md). Keep both files aligned.

Velaria is a performance-first, unified batch/stream, columnar-native C++20 dataflow kernel with built-in vector capability.

## 1. What This Project Is

Velaria is a local-first C++20 dataflow engine research project.

Its current goal is narrow:

- keep one native kernel as the execution source of truth
- pursue extreme performance on the native path
- keep batch and stream on one unified kernel
- keep the runtime column-oriented end to end as deep as possible
- keep vector capability inside the core kernel instead of as a detached subsystem
- keep the single-node path stable
- expose that kernel through a supported Python ecosystem layer
- use the same-host actor/rpc path as an experiment lane, not as a second kernel

This repository is not trying to present a completed distributed runtime.
The current working direction is to stabilize the local kernel first, then extend execution carefully from in-proc operator chaining to same-host multi-process coordination.

## 2. Key Characteristics And Architecture

Velaria is organized around one kernel and two non-kernel layers.

### Core Kernel

Owns:

- local batch and streaming execution
- column-first runtime direction
- logical planning and minimal SQL mapping
- source/sink ABI
- explain / progress / checkpoint contract
- local vector search

### Python Ecosystem

Owns:

- native binding in `python_api`
- Arrow ingress/output
- CLI, packaging, and `uv` workflow
- Excel / Bitable / custom stream adapters
- local workspace and run tracking

Does not own:

- execution hot-path semantics
- a separate explain / progress / checkpoint contract
- a second execution engine

### Experimental Runtime

Owns:

- same-host `actor/rpc/jobmaster` experiments
- transport / codec / scheduler observation

Does not imply:

- completed distributed scheduling
- production distributed execution

### Core Design Traits

- one native kernel remains the only execution source of truth
- performance is a first-order design goal, not a secondary optimization pass
- batch and stream share one kernel instead of diverging into separate engines
- the runtime direction is explicitly column-first
- columnar ownership should survive as deep into execution as possible
- vector search is a built-in kernel capability
- `DataflowSession` remains the public session entry
- rows are compatibility boundaries, not the preferred hot-path form
- SQL is an ingress surface, not the driver of runtime architecture

The golden path is:

```text
Arrow / CSV / Python ingress
  -> DataflowSession / DataFrame / StreamingDataFrame
  -> local runtime kernel
  -> sink
  -> explain / progress / checkpoint
```

## 3. Current Status

Available today:

- one native kernel for batch + streaming
- a performance-first execution direction on the native path
- column-first execution direction with lazy row materialization at compatibility boundaries
- batch SQL v1:
  - `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, current minimal `JOIN`
- current batch builtins:
  - string: `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `LEN`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `REVERSE`, `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR` / `SUBSTRING`, `POSITION`, `REPLACE`
  - numeric/date: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `YEAR`, `MONTH`, `DAY`
- stream path:
  - `readStream(...)`, `readStreamCsvDir(...)`
  - `single-process` and `local-workers`
  - query-local backpressure, progress snapshots, checkpoint path
  - basic stream operators and stateful grouped aggregates
- stream SQL subset:
  - `streamSql(...)` accepts `SELECT`
  - `explainStreamSql(...)` accepts `SELECT` or `INSERT INTO <sink> SELECT ...`
  - `startStreamSql(...)` accepts `INSERT INTO <sink> SELECT ...`
  - stream source must be a source table and stream target must be a sink table
  - unbounded-source `ORDER BY` is rejected explicitly
- local exact vector search on fixed-dimension `float32`
- Python Arrow ingress/output and workspace-backed run tracking
- AI / agent / skill integration through the supported Python ecosystem layer, with workspace and artifact management for result reuse and local data management
- same-host actor/rpc/jobmaster smoke path

Current constraints:

- `CREATE SOURCE TABLE` is read-only and rejects `INSERT`
- `CREATE SINK TABLE` accepts writes but cannot be used as query input
- SQL v1 does not expand to `CTE`, subquery, `UNION`, or richer join semantics
- Python callbacks / Python UDFs are not part of the hot path
- the repository does not claim a completed distributed runtime

Stable public surfaces:

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`
- `StreamingQueryProgress`
- `snapshotJson()`
- `explainStreamSql(...)` returning `logical / physical / strategy`

For more detail, use:

- boundaries and ownership: [docs/core-boundary.md](./docs/core-boundary.md)
- runtime contract: [docs/runtime-contract.md](./docs/runtime-contract.md)
- streaming runtime shape: [docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- Python ecosystem details: [python_api/README.md](./python_api/README.md)
- current maintained plan: [plans/core-runtime-columnar-plan.md](./plans/core-runtime-columnar-plan.md)
- plan directory guide: [plans/README.md](./plans/README.md)

## 4. Development

Repository baseline:

- language baseline: `C++20`
- build system: `Bazel`
- keep `DataflowSession` as the public session entry
- do not break `sql_demo / df_demo / stream_demo`
- keep example source files as `.cc`
- use `uv` for Python commands in this repository

Real entry points:

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke
uv run --project python_api python python_api/velaria_cli.py --help
./dist/velaria-cli --help
```

Minimal validation:

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke

bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:actor_rpc_smoke

./scripts/run_python_ecosystem_regression.sh
```

Development docs:

- English: [docs/development.md](./docs/development.md)
- Chinese: [docs/development-zh.md](./docs/development-zh.md)
