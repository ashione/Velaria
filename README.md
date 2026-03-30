# Velaria: A Pure C++17 Streaming-First Dataflow Kernel

`README.md` is the English source of truth. The Chinese mirror lives in [README-zh.md](./README-zh.md). Keep both files aligned.

Velaria is a local-first C++17 dataflow engine research project. The current goal is narrow on purpose: stabilize the single-machine streaming path, keep batch and stream inside one execution model, and extend carefully toward same-host multi-process execution without claiming a finished distributed runtime.

## What It Is

Today the main path is:

`micro-batch in-proc + query-local backpressure + local worker scale-up`

The repository also keeps `actor + rpc + jobmaster` as a same-host multi-process experiment. That path is for observability and execution research, not for claiming distributed scheduling, fault recovery, state migration, or resource governance.

Core public objects:

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`

Core local flow:

```text
source -> StreamingDataFrame/operator chain -> sink
```

Same-host experimental flow:

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

## Current Capability Boundary

Available today:

- batch + streaming execution through one local runtime
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local backpressure, bounded backlog, progress snapshots, checkpoint path
- execution modes: `single-process`, `local-workers`, `actor-credit`, `auto`
- local file source/sink support
- basic stream operators: `select / filter / withColumn / drop / limit / window`
- stateful `sum` and `count`
- stream SQL subset on top of existing streaming operators
- Python Arrow ingestion and Arrow output
- same-host actor/rpc/jobmaster smoke path

Out of scope in the current repo state:

- completed distributed runtime claims
- Python callback execution in the hot path
- Python UDFs
- Python sink callbacks into the native sink ABI
- generic plan parallelization from `actor-credit`
- broad SQL expansion such as full `JOIN / CTE / subquery / UNION / streaming AVG/MIN/MAX`

## Streaming Runtime Contracts

Main streaming entry points:

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

### Progress and Strategy

`StreamingQueryProgress` and `snapshotJson()` expose:

- execution choice: `execution_mode`, `execution_reason`, `transport_mode`
- workload estimates: `estimated_state_size_bytes`, `estimated_batch_cost`
- source/sink state: `source_is_bounded`, `sink_is_blocking`
- flow-control counters: backlog, inflight, blocked, watermark fields
- checkpoint fields: `checkpoint_delivery_mode`, `last_source_offset`

`explainStreamSql(...)` returns three sections:

- `logical`
- `physical`
- `strategy`

The strategy section explains selected mode, fallback reason, actor hot-path eligibility, transport, batch/state estimates, and the actor/shared-memory knobs used for the decision.

### Backpressure

Velaria currently defines backpressure as query-local and bounded:

- `backlog` is the queued batch count after pull and before drain
- `blocked_count` counts producer wait events, not loop iterations
- `max_backlog_batches` is the largest observed backlog immediately after enqueue
- `inflight_batches` and `inflight_partitions` describe queued work not yet consumed
- slow sink, slow state finalize, or local partition pressure feed back into the same query-local backlog counters

Latency fields:

- `last_batch_latency_ms`: batch start to sink flush completion
- `last_sink_latency_ms`: sink write + flush
- `last_state_latency_ms`: state/window finalize time, `0` for stateless batches

### Checkpoint and Resume

Checkpoint files are local and persisted atomically.

Current delivery semantics:

- default `at-least-once`: source offset is not restored; replay and duplicate sink output are allowed
- `best-effort`: restore source offset only when the source implements `restoreOffsetToken(...)`; still not exactly-once sink delivery

Current built-in source behavior:

- `MemoryStreamSource`: can resume by batch offset under `best-effort`
- `DirectoryCsvStreamSource`: can resume by last completed file under `best-effort`

### Actor-Credit and Auto

`actor-credit` and `auto` only target one narrow hot path:

- all upstream transforms must be partition-local
- the final barrier must be grouped by `window_start + key`
- the aggregate must be `sum(value)`

Queries outside that shape must fall back to `single-process`, and the reason is recorded in `execution_reason`.

## Streaming SQL

Velaria keeps stream SQL intentionally small and maps it back to existing streaming operators.

### Entry Points

- `session.streamSql("SELECT ...") -> StreamingDataFrame`
- `session.startStreamSql("INSERT INTO sink_table SELECT ...", options) -> StreamingQuery`
- `session.explainStreamSql(...) -> string`

### Supported Subset

- single-table `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `LIMIT`
- `SUM(col)`
- `COUNT(*)`
- minimal window SQL: `WINDOW BY <time_col> EVERY <window_ms> AS <output_col>`

Supported DDL/DML:

- `CREATE SOURCE TABLE ... USING csv`
- `CREATE SINK TABLE ... USING csv`
- `INSERT INTO sink_table SELECT ...`

Not supported in stream SQL:

- `JOIN`
- `AVG / MIN / MAX`
- `INSERT INTO ... VALUES`
- broad ANSI window SQL
- CTE / subquery / `UNION`

Example:

```sql
CREATE SOURCE TABLE stream_events (ts STRING, key STRING, value INT)
USING csv OPTIONS(path '/tmp/stream-input', delimiter ',');

CREATE SINK TABLE stream_summary (window_start STRING, key STRING, value_sum INT)
USING csv OPTIONS(path '/tmp/stream-output.csv', delimiter ',');

INSERT INTO stream_summary
SELECT window_start, key, SUM(value) AS value_sum
FROM stream_events
WINDOW BY ts EVERY 60000 AS window_start
GROUP BY window_start, key;
```

## Python API

Python remains an exchange/front-end layer. It does not become the execution hot path.

Main API:

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.create_dataframe_from_arrow(...)`
- `Session.create_stream_from_arrow(...)`
- `Session.create_temp_view(...)`
- `Session.read_stream_csv_dir(...)`
- `Session.stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `Session.start_stream_sql(...)`
- `Session.vectorQuery(table, vector_column, query_vector, top_k, metric)` (`metric`: cosine/dot/l2)
- `Session.explainVectorQuery(table, vector_column, query_vector, top_k, metric)`

Arrow ingestion accepts:

- `pyarrow.Table`
- `pyarrow.RecordBatch`
- `RecordBatchReader`
- objects implementing `__arrow_c_stream__`
- Python sequences of Arrow batches

Python commands in this repo should use `uv`:

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
```

Build a single-file CLI executable (bundles Python runtime deps + native `_velaria.so`):

```bash
./scripts/build_py_cli_executable.sh
./dist/velaria-cli csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

Build a native CLI binary (no Python runtime dependency required at runtime):

```bash
bazel build //:velaria_cli
./bazel-bin/velaria_cli \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

Vector query (fixed-length vector, cosine/dot/l2) via native CLI:

```bash
./bazel-bin/velaria_cli \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

Runtime-level vector transport now preserves `FixedVector` through proto-like and binary row batch codecs, so cross-process payloads keep vector type and dimensions.
FixedVector serialization now uses raw float bit payload encoding in internal codecs to avoid text round-trip precision loss.
Current vector search scope is local-only exact scan (`mode=exact-scan`) with fixed-dimension float vectors; no ANN/distributed path in v0.1.
Arrow ingestion now includes a direct `FixedSizeList<float32>` fast path in the native bridge, reducing Python object conversion overhead on vector columns.

## Same-Host Multi-Process Experiment

The same-host path is intentionally minimal:

- scheduler accepts submission and keeps snapshots
- worker runs the local operator chain
- dashboard and client both submit through the worker execution path

Build:

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

Smoke:

```bash
bazel run //:actor_rpc_smoke
```

Three-process local run:

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

Dashboard:

- URL: `http://127.0.0.1:8080`
- source: `src/dataflow/runner/dashboard/app.ts`
- build target: `//:dashboard_app_js`

## Benchmarks and Observability

Useful local targets:

- `//:stream_benchmark`
- `//:stream_actor_benchmark`
- `//:tpch_q1_style_benchmark`
- `//:vector_search_benchmark`

Same-host observability regression:

```bash
./scripts/run_stream_observability_regression.sh
```

The benchmark path exposes structured profile output for same-host execution diagnosis. This is for regression tracking and explainability, not for machine-stable absolute throughput gates.

## Build and Verification

Single-node baseline:

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

Core regression set:

```bash
bazel test //:sql_regression_test //:planner_v03_test //:stream_runtime_test //:stream_actor_credit_test //:source_sink_abi_test //:stream_strategy_explain_test
bazel test //python_api:custom_stream_source_test //python_api:streaming_v05_test //python_api:arrow_stream_ingestion_test
```

One-line build/smoke summary:

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```

## Repository Rules

- language baseline: `C++17`
- build system: `Bazel`
- keep `DataflowSession` as the public session entry
- do not break `sql_demo / df_demo / stream_demo` while extending same-host experiments
- keep example source files as `.cc`
- use `uv` for Python commands in this repository

## CI and Packaging

CI stays intentionally narrow:

- PR CI covers native build, regression tests, and Python smoke
- wheel jobs build Linux and macOS native wheels
- releases are tag-driven and validated against `velaria.__version__`

The goal is to keep routine development cheap while still validating the public surfaces that currently matter.
