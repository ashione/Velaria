# Velaria: A Pure C++17 Local Dataflow Kernel

`README.md` is the English source of truth. The Chinese mirror lives in [README-zh.md](./README-zh.md). Keep both files aligned.

Velaria is a local-first C++17 dataflow engine research project. The current goal is narrow and explicit:

- keep one native kernel as the execution source of truth
- keep the single-node path stable
- expose that kernel through a supported Python ecosystem layer
- use the same-host actor/rpc path as an experiment lane, not as a second kernel

## Layer Model

### Core Kernel

Owns:

- local batch and streaming execution
- logical planning and minimal SQL mapping
- source/sink ABI
- explain / progress / checkpoint contract
- local vector search

Repository entrypoints:

- docs:
  - [docs/core-boundary.md](./docs/core-boundary.md)
  - [docs/runtime-contract.md](./docs/runtime-contract.md)
  - [docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- source groups:
  - `//:velaria_core_logical_sources`
  - `//:velaria_core_execution_sources`
  - `//:velaria_core_contract_sources`
- regression:
  - `//:core_regression`

### Python Ecosystem

Owns:

- native binding in `python_api`
- Arrow ingress and output
- `uv` workflow
- wheel / native wheel / CLI packaging
- Excel / Bitable / custom stream adapters
- local workspace tracking for runs and artifacts

Does not own:

- execution hot-path semantics
- independent explain / progress / checkpoint semantics
- replacement checkpoint storage
- SQLite as a large-result engine

Repository entrypoints:

- docs:
  - [python_api/README.md](./python_api/README.md)
- source groups:
  - `//:velaria_python_ecosystem_sources`
  - `//python_api:velaria_python_supported_sources`
  - `//python_api:velaria_python_example_sources`
  - `//python_api:velaria_python_experimental_sources`
- regression:
  - `//:python_ecosystem_regression`
  - `//python_api:velaria_python_supported_regression`
  - `./scripts/run_python_ecosystem_regression.sh`

### Experimental Runtime

Owns:

- same-host `actor/rpc/jobmaster` experiments
- transport / codec / scheduler observation
- same-host smoke and benchmark tooling

Does not imply:

- distributed scheduling
- distributed fault recovery
- cluster resource governance
- production distributed execution

Repository entrypoints:

- source group:
  - `//:velaria_experimental_sources`
- regression:
  - `//:experimental_regression`
  - `./scripts/run_experimental_regression.sh`

## Golden Path

```text
Arrow / CSV / Python ingress
  -> DataflowSession / DataFrame / StreamingDataFrame
  -> local runtime kernel
  -> sink
  -> explain / progress / checkpoint
```

Public session entry:

- `DataflowSession`

Core user-facing objects:

- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`

## Stable Runtime Contract

Main stream entry points:

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

Stable contract surfaces:

- `StreamingQueryProgress`
- `snapshotJson()`
- `explainStreamSql(...)`
- `execution_mode / execution_reason / transport_mode`
- `checkpoint_delivery_mode`
- source/sink lifecycle: `open -> nextBatch -> checkpoint -> ack -> close`

`explainStreamSql(...)` always returns:

- `logical`
- `physical`
- `strategy`

`strategy` is the single outlet for mode selection, fallback reason, transport, backpressure, and checkpoint delivery mode.

When a stream SQL statement writes to a sink, `logical` and `physical` also keep the source/sink binding visible so explain output stays aligned with the runtime path.

Workspace persistence keeps the kernel contract unchanged:

- `explain.json` stores `logical / physical / strategy`
- `progress.jsonl` appends native `snapshotJson()` output line by line
- large results stay in files; SQLite stores only index rows and small previews

## Current Scope

Available today:

- one native kernel for batch + streaming
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local backpressure, bounded backlog, progress snapshots, checkpoint path
- execution modes: `single-process`, `local-workers`
- file source/sink support
- core SQL v1 batch path:
  - `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE`, `GROUP BY`, `LIMIT`, current minimal `JOIN`
- batch SQL string builtins:
  - `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`
  - `LENGTH`, `LEN`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `REVERSE`
  - `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR` / `SUBSTRING`, `POSITION`, `REPLACE`
- basic stream operators: `select / filter / withColumn / drop / limit / window`
- stateful stream aggregates: `sum / count / min / max / avg`
- stream SQL subset aligned to the current kernel:
  - `session.streamSql(...)` accepts `SELECT`
  - `session.explainStreamSql(...)` accepts `SELECT` or `INSERT INTO <sink> SELECT ...`
  - `session.startStreamSql(...)` accepts `INSERT INTO <sink> SELECT ...`
  - stream sources must come from source tables and stream targets must be sink tables
  - window and stateful aggregate explain remains `logical / physical / strategy`
- local vector search on fixed-dimension float vectors
- Python Arrow ingress/output
- tracked local runs with run directory persistence and artifact indexing
- same-host actor/rpc/jobmaster smoke path

Out of scope:

- completed distributed runtime claims
- Python callbacks or Python UDFs in the hot path
- broader SQL expansion such as richer `JOIN`, `CTE`, `subquery`, or `UNION`
- ANN / standalone vector DB / distributed vector execution

Current SQL v1 constraints:

- `CREATE SOURCE TABLE` is read-only and rejects `INSERT`
- `CREATE SINK TABLE` accepts writes but cannot be used as query input
- stream SQL rejects batch-only shapes with explicit `not supported in SQL v1` or table-kind errors instead of falling through to ambiguous runtime failures

## Python Ecosystem

Main supported Python surfaces:

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
- `read_excel(...)`
- custom source / custom sink adapters

### Workspace Model

- `Workspace`
  - root under `VELARIA_HOME` or `~/.velaria`
- `RunStore`
  - one run directory per execution
  - persists `run.json`, `inputs.json`, `explain.json`, `progress.jsonl`, logs, and `artifacts/`
- `ArtifactIndex`
  - SQLite-first metadata index
  - JSONL fallback when SQLite is unavailable
  - preview cache for small result slices only

This layer is for agent/skill invocation, local traceability, and machine-readable CLI integration. It is not a second execution engine.

### CLI Entry Points

Repo-visible CLI entrypoints are:

- source checkout:
  - `uv run --project python_api python python_api/velaria_cli.py ...`
- installed wheel or local package install:
  - `velaria-cli ...`
  - `velaria_cli ...`
- packaged binary:
  - `./dist/velaria-cli ...`

The global commands are expected only after installing the wheel or package.

### Python Workflow

Bootstrap:

```bash
bazel build //:velaria_pyext
bazel run //python_api:sync_native_extension
uv sync --project python_api --python python3.13
```

Run examples:

```bash
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
uv run --project python_api python python_api/examples/demo_stream_sql.py
uv run --project python_api python python_api/examples/demo_vector_search.py
```

Tracked run examples:

```bash
uv run --project python_api python python_api/velaria_cli.py -i

uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --run-name "score_demo" \
  --description "score filter result for demo input" \
  --tag demo \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py run list --tag demo --query "score"
uv run --project python_api python python_api/velaria_cli.py run result --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python_api python python_api/velaria_cli.py run show --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
```

## Local Vector Search

Vector search is a local kernel capability, not a separate subsystem.

Current scope:

- fixed-dimension `float32`
- metrics: `cosine`, `dot`, `l2`
- `top-k`
- exact scan only
- Python `Session.vector_search(...)`
- Arrow `FixedSizeList<float32>`
- explain output

Preferred local CSV vector text shape:

- `[1 2 3]`
- `[1,2,3]`

Design doc:

- [docs/local_vector_search_v01.md](./docs/local_vector_search_v01.md)

CLI examples:

```bash
uv run --project python_api python python_api/velaria_cli.py csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

Vector explain is part of the stable contract. Current fields include:

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+heap-topk`

Benchmark baseline:

```bash
./scripts/run_vector_search_benchmark.sh
```

## Experimental Runtime

Same-host flow:

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

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
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

## Build and Verification

Single-node baseline:

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

Layered regression entrypoints:

```bash
./scripts/run_core_regression.sh
./scripts/run_python_ecosystem_regression.sh
./scripts/run_experimental_regression.sh
./scripts/run_stream_observability_regression.sh
```

Direct Bazel suites:

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

## Repository Rules

- language baseline: `C++17`
- build system: `Bazel`
- keep `DataflowSession` as the public session entry
- do not break `sql_demo / df_demo / stream_demo`
- keep example source files as `.cc`
- use `uv` for Python commands in this repository
- keep `README.md` and `README-zh.md` aligned
