# Velaria: A Pure C++17 Streaming-First Dataflow Kernel

`README.md` is the English source of truth. The Chinese counterpart lives in [`README-zh.md`](./README-zh.md). Any future README update must keep both files aligned.

Velaria is a lightweight dataflow engine research project focused on an evolvable runtime. The current goal is intentionally narrow: local-first execution, a streaming-first programming model, and stable stateful throughput on a single machine before expanding the runtime toward local multi-process execution and later distributed experiments.

The repository does not claim to be a completed distributed system. Today it centers on `DataflowSession`, `StreamingDataFrame`, local state management, flow control and backpressure, and local multi-process execution paths. Batch remains supported, but is modeled as bounded streaming inside the same execution framework.

## Milestone Status

### v0.5 snapshot (2026-03-29)

The current execution route is:

`micro-batch in-proc + query-local backpressure + local worker scale-up`

The repository also keeps `actor + rpc + jobmaster` as the local multi-process experiment path.

Current progress:

- Single-node streaming path: `readStream -> StreamingDataFrame -> writeStream` supports query-level options, checkpoint/progress reporting, local file source/sink, window columns, and basic stateful aggregation.
- Stability path: `StreamingQuery` provides query-local flow control and end-to-end backpressure with bounded queues, so slow sink or state processing blocks upstream pull instead of allowing unbounded accumulation.
- Query-level execution modes: `single-process`, `local-workers`, `actor-credit`, and `auto`, with the final decision and reason recorded in progress snapshots.
- Logical and physical planning: unified logical nodes such as `WindowAssign` and `Sink`, plus structured progress fields for execution mode, transport mode, watermarking, and batch cost estimates.
- Source and sink ABI: `RuntimeSource` and `RuntimeSink` define explicit `query_id / checkpoint / backpressure` context as a stable boundary for later cross-process source/sink work.
- Source and sink adapters: `RuntimeSourceAdapter` and `RuntimeSinkAdapter` bridge runtime ABI objects into `readStream(...)` and `writeStream(...)` without changing query-local backpressure or state semantics.
- Single-process demos: `sql_demo`, `df_demo`, and `stream_demo` remain the baseline local validation path.
- Local multi-process experiments: `actor + rpc + jobmaster` stays as the minimal same-host task dispatch loop and should not be described as a finished distributed runtime.

### v0.4 / v0.5 additions

This round extends the project in the order of semantic boundaries first, entry points second:

- `v0.4`: completed source/sink ABI integration into the streaming main path through `makeRuntimeSourceAdapter` and `makeRuntimeSinkAdapter`.
- `v0.5`: completed test coverage for query progress contracts, checkpoint and ack passthrough, and Python Arrow plus stream SQL end-to-end paths.

Repository boundaries remain unchanged:

- The local multi-process path is still an experiment, not a completed distributed scheduler.
- Python continues to use Arrow for data exchange. Custom Python stream sources are supported through Arrow micro-batches. Python callback/UDF support is still out of scope. Custom sinks are exposed as Arrow batch callbacks rather than native sink ABI callbacks.

## Current Local Multi-Process Architecture

```text
client
  -> scheduler(jobmaster)
      -> worker
          -> in-proc operator chain
      -> result back to client
```

Role summary:

- `in-proc operator chain`: a worker still runs a local operator chain instead of a cross-node execution graph.
- `cross-process RPC`: client, scheduler, and worker communicate through RPC and codec messages.
- `simple jobmaster`: `actor_rpc_scheduler` handles submission, dispatch, forwarding, and status observation. It does not implement advanced fault tolerance, retry policy, or resource isolation.
- `single-node fallback`: the `--single-node` path remains available so local validation does not depend on the multi-process experiment.

## Terminology

To avoid external framework naming leaking into repository interfaces, use these project terms consistently:

- `Session` style naming: `DataflowSession`
- SQL entry point: `session.sql(...)`
- Data reading entry point: `session.read(...)`
- Instead of `compatibility`: use "semantic alignment", "interface mapping", or "external behavior reference"

Avoid introducing external framework product names into main interfaces, comments, or documentation.

## Current Capability Boundary

Available today:

- `DataflowSession`, `DataFrame`, and `StreamingDataFrame`
- `read_csv`, `readStream(...)`, and `readStreamCsvDir(...)`
- Streaming query options: trigger interval, max in-flight batches, max queued partitions, checkpoint path, and execution modes such as `single-process / local-workers / actor-credit / auto`
- Flow control and backpressure: query-local backlog watermarks, blocked counters, bounded backlog, and progress snapshots
- Streaming sources and sinks: memory source, directory CSV incremental source, console sink, and append file sink
- Python exchange paths: CSV input, in-process Arrow batch/stream input, `RecordBatchReader`, `__arrow_c_stream__`, and Arrow output
- Basic operators: `select / filter / withColumn / drop / limit / window`
- Aggregation: `groupBy + sum/count`, plus batch `SUM / COUNT / AVG / MIN / MAX`
- Basic stateful aggregation: stateful `sum/count`
- Local SQL main path: `SqlParser -> SqlPlanner -> DataFrame`
- Minimum streaming SQL loop: `streamSql(SELECT ...)` and `startStreamSql(INSERT INTO ... SELECT ...)`
- Streaming SQL CSV tables: `CREATE SOURCE TABLE ... USING csv` and `CREATE SINK TABLE ... USING csv`
- Python wrapper v1: `Session / DataFrame / StreamingDataFrame / StreamingQuery`
- Temporary views: `createTempView / resolve`
- Minimal actor/rpc loop: `scheduler / worker / client / smoke`

Current limits:

- Execution is `micro-batch only`; no continuous mode.
- Window support is limited to fixed tumbling-style window columns and basic stateful aggregation.
- No CTE support.
- No subquery expansion in this round.
- No `UNION` support in this round.
- `JOIN` remains intentionally minimal.
- Streaming SQL does not support `JOIN / AVG / MIN / MAX / INSERT ... VALUES`.
- Python bindings auto-detect local CPython headers through repository rules and support custom stream sources through Arrow conversion, but still do not support Python callback/UDF execution or direct Python sink callbacks into the native sink ABI.
- Backpressure is query-local; global fairness across queries is out of scope.
- The multi-process path remains a same-host experiment and does not include true distributed scheduling, fault recovery, state migration, or resource governance.

## Streaming Runtime

Current streaming entry points:

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`
- `StreamingQuery.start() / awaitTermination() / stop()`

`StreamingQueryOptions` currently carries:

- `trigger_interval_ms`
- `max_inflight_batches`
- `max_queued_partitions`
- `backpressure_high_watermark / backpressure_low_watermark`
- `checkpoint_path`
- `execution_mode = single-process / local-workers / actor-credit / auto`
- `local_workers`
- `actor_workers / actor_max_inflight_partitions`
- `actor_shared_memory_transport / actor_shared_memory_min_payload_bytes`
- `actor_auto_options`
- `max_retained_windows`

`StreamingQueryProgress` and `snapshotJson()` additionally expose:

- `execution_mode`
- `execution_reason`
- `estimated_state_size_bytes / estimated_batch_cost`
- `backpressure_max_queue_batches / backpressure_high_watermark / backpressure_low_watermark`

These fields explain which local execution path was chosen and why it was selected or downgraded.

`explainStreamSql(...)` renders three sections:

- `logical`
- `physical`
- `strategy`

The strategy section snapshots the selected mode, fallback reason, actor eligibility, transport, state/batch estimates, backpressure thresholds, and actor/shared-memory knobs before execution starts.

### Query-level actor and auto selection

- `actor-credit` and `auto` only activate for explicitly supported hot paths: all upstream transforms must be partition-local and the final barrier must be `sum(value)` grouped by `window_start + key`.
- That hot path is pushed into the local actor-stream runtime, which performs partition-local partial aggregation on `window_start / key / value` and merges the result back into `StreamingQuery`.
- Queries that do not match the supported shape fall back to `single-process`, and `execution_reason` records the reason.
- This means actor/auto support is not generic plan parallelization. It is a targeted acceleration path for a recognized windowed group-by-sum workload.

### Backpressure semantics

- Backpressure is controlled at `batch / partition` watermarks.
- `backlog` is defined as the queued batch count after pull and before consumer drain.
- `blocked_count` counts producer-side wait events caused by bounded backlog or partition pressure. It does not count loop iterations.
- `max_backlog_batches` records the largest observed queued backlog immediately after enqueue.
- `inflight_batches / inflight_partitions` report the queued backlog that still has not been consumed.
- When backlog exceeds the threshold, source pull blocks instead of continuing to accumulate work.
- Slow sinks, slow state updates, or too much local parallel partition work all feed back into the query-local backlog counters.
- `last_batch_latency_ms` covers one batch from execution start through sink flush completion; `last_sink_latency_ms` is the sink write/flush portion; `last_state_latency_ms` is state/window finalize time and is `0` for stateless batches.
- Minimum observable counters include `blocked_count / max_backlog_batches / inflight_batches / inflight_partitions / last_batch_latency_ms / last_sink_latency_ms / last_state_latency_ms`.

### Checkpoint and progress

- After each batch, the runtime atomically persists `batches_pulled / batches_processed / blocked_count / max_backlog_batches / last_source_offset` into the local checkpoint file.
- Default checkpoint delivery mode is `at-least-once`: source offset is not restored, replay is allowed, and sinks may observe duplicate output after restart.
- `best-effort` restores the source offset only when the source implements `restoreOffsetToken(...)`; it still does not claim exactly-once sink delivery.
- `MemoryStreamSource` resumes by batch offset when `best-effort` restore is enabled.
- `DirectoryCsvStreamSource` resumes by the last completed file name when `best-effort` restore is enabled.
- `snapshotJson()` also carries `execution_mode / execution_reason` for actor-credit and auto fallback diagnostics.

### State indexing and window eviction

- Stateful aggregation indexes state by `group key -> aggregate state`.
- If grouping keys include window columns such as `window_start`, the runtime also maintains a window membership index so old state can be evicted by window.
- When `max_retained_windows > 0`, only the latest N windows remain in memory to avoid unbounded state growth.

## Streaming SQL

Velaria exposes a minimal streaming SQL subset on top of the existing `StreamingDataFrame` API.

### API entry points

- `session.streamSql("SELECT ...") -> StreamingDataFrame`
- `session.startStreamSql("INSERT INTO sink_table SELECT ...", options) -> StreamingQuery`

### Supported query subset

- Single-table `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `LIMIT`
- `SUM(col)`
- `COUNT(*)`

The current implementation maps SQL back into existing streaming operators:

- `FROM source` -> registered streaming view
- `WHERE` -> `filter`
- `GROUP BY + SUM/COUNT(*)` -> `groupBy + sum/count`
- `HAVING` -> post-aggregation `filter`
- `LIMIT` -> existing `StreamingDataFrame::limit`

### Supported DDL and DML

- `CREATE SOURCE TABLE ... USING csv OPTIONS(path 'dir', delimiter ',')`
- `CREATE SINK TABLE ... USING csv OPTIONS(path '/tmp/out.csv', delimiter ',')`
- `INSERT INTO sink_table SELECT ...`

Binding details:

- `SOURCE TABLE USING csv` maps to `DirectoryCsvStreamSource`
- `SINK TABLE USING csv` maps to `FileAppendStreamSink`
- `INSERT INTO ... SELECT ...` must be started through `session.startStreamSql(...)`

Example:

```sql
CREATE SOURCE TABLE stream_events (key STRING, value INT)
USING csv OPTIONS(path '/tmp/stream-input', delimiter ',');

CREATE SINK TABLE stream_summary (key STRING, value_sum INT)
USING csv OPTIONS(path '/tmp/stream-output.csv', delimiter ',');

INSERT INTO stream_summary
SELECT key, SUM(value) AS value_sum
FROM stream_events
WHERE value > 6
GROUP BY key
HAVING value_sum > 15
LIMIT 10;
```

### Current SQL limits

- Only `USING csv`
- Only single-table queries
- No `JOIN`
- No `AVG / MIN / MAX`
- Minimal window SQL only: `WINDOW BY <time_col> EVERY <window_ms> AS <output_col>`
- No streaming `INSERT INTO ... VALUES`
- `LIMIT` follows current streaming API semantics and should not be read as a full infinite-stream global SQL limit

## SQL Planning and Execution

`session.sql()` currently follows `Parser -> Planner -> DataFrame`:

- `SqlParser` converts SQL into the `SqlQuery` AST. It supports `ON (...)`, `HAVING`, `LIMIT`, and simple parenthesized predicates such as `WHERE a.score > (5)`.
- `SqlPlanner::buildLogicalPlan(...)` produces logical steps in the form `Scan -> (Join?) -> (Filter?) -> (Aggregate?) -> (Having?) -> Project -> (Limit?)`.
- `SqlPlanner::buildPhysicalPlan(...)` maps logical steps into physical steps:
  - `Join` and `Aggregate` are barrier stages.
  - Consecutive `Filter/Project/Limit/Having` may fuse into `FusedUnary`.
  - Consecutive `Project` stages are deduplicated and consecutive `Limit` stages keep the minimum bound.
- `materializeFromPhysical(...)` replays the physical plan as the existing `DataFrame` operator chain and reuses the local executor.

## SQL DDL Semantics

Velaria currently supports these table categories inside the local in-memory catalog:

- `CREATE TABLE`: normal table, readable and writable
- `CREATE SOURCE TABLE`: source-oriented table, currently read-only
- `CREATE SINK TABLE`: sink-oriented table, currently write-only and not queryable

`SOURCE` and `SINK` describe table semantics and enforce execution constraints. They do not yet introduce external connectors.

## SQL DDL / DML Support

### DDL

- `CREATE TABLE`: creates a normal in-memory table for reads and writes
- `CREATE SOURCE TABLE`: creates a source table that can only be read
- `CREATE SINK TABLE`: creates a sink/result table that can only be written

Example:

```sql
CREATE TABLE users (id INT, name STRING, score INT);
CREATE SOURCE TABLE source_users (name STRING, score INT);
CREATE SINK TABLE summary_by_region (region STRING, score_sum INT);
```

### DML

- `INSERT INTO ... VALUES`: supports full-column insertion and explicit column lists
- `INSERT INTO ... SELECT`: writes query output into a target table

Example:

```sql
INSERT INTO users (id, name, score) VALUES (1, 'alice', 10), (2, 'bob', 20);

INSERT INTO summary_by_region
SELECT name AS region, SUM(score) AS score_sum
FROM users
GROUP BY name;
```

### Semantic constraints

- Any `INSERT` into a `SOURCE` table is a semantic error.
- `SELECT` against a `SINK` table, including use as a `JOIN` input, is a semantic error.
- `SELECT` still must satisfy current-version planner rules such as `HAVING` on aggregate semantics and `GROUP BY` projection consistency.

### Scheduler model

- The scheduler only orchestrates and dispatches. It does not execute SQL locally.
- All submission entry points, including the dashboard, must go through the worker execution path.
- If auto-started workers are disabled with `--no-auto-worker` and no worker is available, submission returns `no worker available` instead of silently executing inside the scheduler.

### Scheduling policy

- Submission path: dashboard and client build the plan and call `submitRemote`, which creates `job -> chain -> task`.
- Queueing semantics: remotely runnable tasks enter the `JobMaster` `pending_remote_tasks_` queue. A task is dispatched only after its chain becomes runnable and the scheduler sees an idle worker.
- Dispatch rule: the dispatcher takes tasks from the queue head and assigns them to idle workers in arrival order.
- Worker binding: each task binds to a worker connection. Success updates status to `TASK_SUBMITTED / CHAIN_SUBMITTED`; failure feeds back `TASK_FAILED / JOB_FAILED`.
- Concurrency and parallelism: chain parallelism is limited by `chain_parallelism` and the number of idle workers. Each task supports retries and heartbeat observation.
- Result confirmation: workers return `Result`, and the scheduler updates `job / chain / task` snapshots before pushing them back to the submitter.
- Operational advice: observe log events and `/api/jobs` snapshots to track `JOB_* / CHAIN_* / TASK_*` state transitions.
- Default worker policy: the scheduler auto-starts local workers through `--local-workers` with a default of `2`. Use `--no-auto-worker` for explicit manual mode.

## Python Wrapper

The repository includes a minimal Python binding layer so Python code can call `DataflowSession`, stream APIs, and streaming SQL directly.

### Binding strategy

- No extra third-party binding framework
- Handwritten CPython extension
- Four public Python objects:
  - `Session`
  - `DataFrame`
  - `StreamingDataFrame`
  - `StreamingQuery`

### GIL strategy

These operations explicitly release the GIL during native execution:

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.stream_sql(...)`
- `Session.start_stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `DataFrame.count()`
- `DataFrame.to_rows()`
- `DataFrame.show()`
- `StreamingQuery.start()`
- `StreamingQuery.await_termination(...)`
- `StreamingQuery.stop()`

Out of scope in this first version:

- Python callback execution
- Python UDF
- Direct Python sink callbacks into the native sink ABI

### Current Python API

- `Session.read_csv(path, delimiter=',')`
- `Session.sql(sql_text)`
- `Session.create_dataframe_from_arrow(pyarrow_table)`
- `Session.create_stream_from_arrow(pyarrow_table_or_batches)`
- `Session.create_temp_view(name, df_or_stream_df)`
- `Session.read_stream_csv_dir(path, delimiter=',')`
- `Session.stream_sql(sql_text)`
- `Session.explain_stream_sql(sql_text, trigger_interval_ms=1000, checkpoint_path='')`
- `Session.start_stream_sql(sql_text, trigger_interval_ms=1000, checkpoint_path='')`
- `DataFrame.to_rows() / to_arrow() / count() / show()`
- `StreamingDataFrame.select(...) / filter(...) / with_column(...) / drop(...) / limit(...)`
- `StreamingDataFrame.window(...) / group_sum(...) / group_count(...)`
- `StreamingDataFrame.write_stream_csv(...) / write_stream_console(...)`
- `StreamingQuery.start() / await_termination(...) / stop() / progress()`

Notes:

- `create_dataframe_from_arrow(...)` accepts `pyarrow.Table` and objects that can be normalized by `pyarrow.table(...)`
- `create_stream_from_arrow(...)` accepts a single `pyarrow.Table` or `RecordBatch`, and also Python sequences of multiple `Table` or `RecordBatch` batches
- `explain_stream_sql(...)` exposes the same logical / physical / strategy breakdown as `DataflowSession::explainStreamSql(...)`
- Python may still use CSV, but it can also skip file materialization entirely and feed Arrow data directly into batch or stream execution

### Build and run

The Python wrapper auto-detects a usable CPython interpreter and development headers when the Bazel workspace initializes.

To override the detected interpreter for repository setup, use:

```bash
export VELARIA_PYTHON_BIN=/path/to/pythonX.Y
```

Python dependency management and demo execution use `uv`.

The `python_api/` directory contains:

- `python_api/pyproject.toml`
- `python_api/requirements.lock`
- `python_api/BUILD.bazel`

Key packaging targets:

- `//python_api:velaria_py_pkg`: Bazel runtime package that places Python sources and `_velaria.so` in the same package tree for tests and binaries
- `//python_api:velaria_whl`: pure Python wheel
- `//python_api:velaria_native_whl`: native wheel with `_velaria.so` injected
- `python_api/pyproject.toml` declares `velaria/_velaria.so` as package data so setuptools/uv packaging can include the native module when it exists in the package directory
- Installed native wheel: `import velaria` loads the packaged `_velaria.so`
- Source checkout: if `//:velaria_pyext` has been built, `velaria` auto-discovers `bazel-bin/_velaria.so` without extra environment variables or `PYTHONPATH`
- Python package versioning is centralized through `python_api/version.bzl` and `python_api/velaria/_version.py`. Use `./scripts/bump_velaria_version.sh <version>` to bump the package version and refresh `uv.lock`.

Build extension artifacts:

```bash
bazel build //:velaria_pyext
bazel build //python_api:velaria_whl
bazel build //python_api:velaria_native_whl
```

Install Python runtime dependencies:

```bash
uv sync --project python_api --python /opt/homebrew/bin/python3.13
```

Run demos:

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python /opt/homebrew/bin/python3.13
uv run --project python_api python python_api/demo_stream_sql.py
uv run --project python_api python python_api/demo_batch_sql_arrow.py
```

Run Python tests:

```bash
bazel test //python_api:custom_stream_source_test //python_api:streaming_v05_test
uv run --project python_api python -m unittest discover -s python_api/tests
```

These demos cover:

- Creating a Python `Session`
- Building batch and stream inputs directly from `pyarrow.Table`
- Starting `INSERT INTO ... SELECT ...` through streaming SQL
- Reading sink CSV output and printing query progress
- Round-tripping `pyarrow.Table -> DataFrame`
- Running batch SQL directly over Arrow-backed temporary views

## Build and Run Commands

### Single-node examples

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:stream_sql_demo
bazel run //:stream_stateful_demo
bazel test //:stream_runtime_test
bazel run //:stream_benchmark
bazel run //:stream_actor_benchmark
bazel test //:stream_actor_credit_test
./scripts/run_stream_observability_regression.sh
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
uv run --project python_api python python_api/bench_arrow_ingestion.py
```

### Local actor-stream experiments

- `stream_actor_benchmark`: validates the local worker process path with partition credits and actor RPC for window + key partial aggregation.
- `stream_benchmark`: validates `single-process / local-workers / actor-credit / auto` execution modes from the `StreamingQuery` entry point.
- Both benchmarks now emit machine-readable JSON lines in addition to the existing human-readable summaries.
- `stream_actor_credit_test`: verifies that:
  - credit limits are respected
  - blocked counters are triggered
  - multi-process output matches the single-process baseline

This path is still an isolated experiment runtime:

- It validates the minimum loop of `partition credit + local worker process + result merge`.
- It focuses on semantics and flow control, not on guaranteed performance wins.
- Workers currently perform partition-level partial aggregation, while final state merge still happens on the coordinator.
- Large local payloads use `shared memory + mmap` on the same host. Both worker and coordinator can decode from buffer views without first copying into `std::vector<uint8_t>`.
- For lightweight aggregation, actor-stream can still be slower than single-process execution. For CPU-heavy aggregation, use:

```bash
bazel run //:stream_actor_benchmark -- 16 65536 4 4 0 400
```

Parameter order:

- `batch_count`
- `rows_per_batch`
- `worker_count`
- `max_inflight_partitions`
- `worker_delay_ms`
- `cpu_spin_per_row`
- optional seventh parameter: `mode = single / actor / auto / all`

For same-host observability regression, use:

```bash
./scripts/run_stream_observability_regression.sh
```

Reference result for the query-level `stream_benchmark`:

- `stateful-single`: `89k rows/s`
- `stateful-local-workers`: `88k rows/s`
- `stateful-actor-credit`: `108k rows/s`
- `stateful-auto`: may still conservatively fall back to `single-process` under the default threshold

This shows that query-level actor pushdown is already connected to `StreamingQuery`, but `auto` thresholds still need real workload tuning.

### Build actor/rpc/jobmaster targets

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

## CI and Packaging

GitHub Actions CI is intentionally limited to `pull_request` events. Regular branch pushes do not trigger CI.

Current CI jobs:

- `native-and-python`: native build, C++ regression tests, and Python smoke checks
- `wheel-manylinux`: builds `//python_api:velaria_native_whl`, repairs it with `auditwheel`, and uploads a manylinux wheel artifact
- `wheel-macos`: builds `//python_api:velaria_native_whl` on macOS and uploads a macOS wheel artifact

This keeps routine development cheaper while still validating PRs and producing native wheel artifacts for review.

Release publishing is separate from PR CI:

- Create a Git tag in the form `vX.Y.Z`
- The release workflow validates that the tag matches `velaria.__version__`
- It then builds a manylinux wheel and a macOS wheel and publishes both assets to the GitHub release

### Smoke check

```bash
bazel run //:actor_rpc_smoke
```

Expected output:

```text
[smoke] actor rpc codec roundtrip ok
```

### Three-process local verification

Terminal A:

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

The scheduler auto-starts one local worker by default. To force manual worker startup, keep `--no-auto-worker` and start:

Terminal B:

```bash
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
```

Terminal C:

```bash
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

Expected logs:

```text
[scheduler] listen 127.0.0.1:61000
[worker] connected 127.0.0.1:61000
[client] job accepted: job_1
[client] job result: rows=..., cols=..., schema=..., first_row=...
```

### Dashboard (TypeScript + React)

- URL: `http://127.0.0.1:8080`
- Frontend source lives in `src/dataflow/runner/dashboard/app.ts`
- Bazel target `//:dashboard_app_js` compiles and writes `src/dataflow/runner/dashboard/app.js`
- Page capabilities:
  - submit tasks through raw payload or SQL
  - inspect `/api/jobs` and `/api/jobs/{id}`
  - refresh automatically every second or manually on demand

Start example:

```bash
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

Skip TypeScript rebuild if assets are already compiled:

```bash
BUILD_DASHBOARD=0 ./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

### One-command end-to-end flow

```bash
./scripts/run_actor_rpc_e2e.sh
```

Examples:

```bash
./scripts/run_actor_rpc_e2e.sh --payload "hello world"
./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
DO_BUILD=1 ./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
```

Script environment variables:

- `ADDRESS`: RPC address, default `127.0.0.1:61000`
- `WORKER_ID`: worker name, default `worker-1`
- `PAYLOAD`: default payload, default `demo payload`
- `TIMEOUT_SECONDS`: timeout, default `20`
- `DO_BUILD`: whether to build first, `1` or `0`, default `0`
- `BUILD_DASHBOARD`: whether to build dashboard first, `1` or `0`, default `1`

If no worker is available at submit time, the dashboard API returns `503 Service Unavailable` with `message: "scheduler reject: no worker available"`, which means the task has not entered the runnable scheduling queue.

### Single-node fallback path

```bash
bazel run //:actor_rpc_client -- --single-node --payload "local-only payload"
```

## Debugging and Acceptance

### 1. Build

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### 2. Smoke

```bash
bazel run //:actor_rpc_smoke
```

Acceptance criteria:

- process exits with code `0`
- output contains `[smoke] actor rpc codec roundtrip ok`
- output must not contain `codec roundtrip failed` or `codec mismatch`

### 3. Multi-process verification

Start in order: `scheduler -> worker -> client`.

Acceptance criteria:

- scheduler prints `[scheduler] listen 127.0.0.1:61000`
- worker prints `[worker] connected 127.0.0.1:61000`
- client receives `job accepted` before `job result`
- logs must not contain `no-worker-available`
- logs must not contain `cannot connect`
- logs must not contain `scheduler closed connection`

### 4. Common failure patterns

- `scheduler reject: no-worker-available`: worker failed to connect, or the client started too early
- `[worker] cannot connect ...` or `[client] cannot connect ...`: scheduler is not running, the address is wrong, or the port does not match
- `scheduler failed to listen on ...`: port conflict or bind failure
- `Invalid --listen endpoint` or `Invalid --connect endpoint`: parameters must use `host:port`

## Example Source File Extension Check

Example sources remain `.cc` files only:

- `src/dataflow/examples/wordcount.cc`
- `src/dataflow/examples/dataframe_demo.cc`
- `src/dataflow/examples/stream_demo.cc`
- `src/dataflow/examples/stream_stateful_demo.cc`
- `src/dataflow/examples/stream_state_container_demo.cc`
- `src/dataflow/examples/sql_demo.cc`
- `src/dataflow/examples/actor_rpc_scheduler.cc`
- `src/dataflow/examples/actor_rpc_worker.cc`
- `src/dataflow/examples/actor_rpc_client.cc`
- `src/dataflow/examples/actor_rpc_smoke.cc`

Verification command:

```bash
find src/dataflow/examples -maxdepth 1 -type f ! -name '*.cc'
```

If the command prints nothing, the example directory still contains only `.cc` sources.

## One-Line Build and Smoke Summary

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```
