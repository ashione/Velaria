# Velaria: A Pure C++17 Local Dataflow Kernel

`README.md` is the English source of truth. The Chinese mirror lives in [README-zh.md](./README-zh.md). Keep both files aligned.

Velaria is a local-first C++17 dataflow engine research project. The repository is now organized around one kernel plus two explicit non-kernel layers:

- `Core Kernel`
  - local execution semantics
  - batch + stream in one model
  - stable explain / progress / checkpoint contract
- `Python Ecosystem`
  - supported Arrow / wheel / CLI / `uv` / Excel / Bitable / custom stream adapters
  - projects the kernel outward without becoming the hot path
- `Experimental Runtime`
  - same-host `actor/rpc/jobmaster`
  - execution and observability research lane, not a second kernel

## Golden Path

The only golden path is:

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

The same-host actor/rpc path stays in the repo, but it is not the main product story.

## Repository Layers

### Core Kernel

Core owns:

- logical planning and minimal SQL mapping
- table/value execution model
- local batch and streaming runtime
- source/sink ABI
- runtime contract surfaces
- local vector search capability

Repository entrypoints:

- docs:
  - [docs/core-boundary.md](./docs/core-boundary.md)
  - [docs/runtime-contract.md](./docs/runtime-contract.md)
  - [docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- Bazel source groups:
  - `//:velaria_core_logical_sources`
  - `//:velaria_core_execution_sources`
  - `//:velaria_core_contract_sources`
- regression suite:
  - `//:core_regression`

### Python Ecosystem

Python is a supported ecosystem layer, not a convenience-only wrapper.

It includes:

- native binding in `python_api`
- Arrow ingestion and output
- `uv` workflow
- wheel / native wheel / CLI packaging
- Excel and Bitable adapters
- custom source / custom sink adapters
- supported CLI tooling in `python_api/velaria_cli.py`
- Python ecosystem demos in `python_api/examples`
- Python benchmarks in `python_api/benchmarks`

It does not define:

- execution hot-path behavior
- independent progress/checkpoint semantics
- independent vector-search semantics

Repository entrypoints:

- docs:
  - [python_api/README.md](./python_api/README.md)
- Bazel source group:
  - `//:velaria_python_ecosystem_sources`
- Python-layer source groups:
  - `//python_api:velaria_python_supported_sources`
  - `//python_api:velaria_python_example_sources`
  - `//python_api:velaria_python_experimental_sources`
- regression suite:
  - `//:python_ecosystem_regression`
- Python-layer regression suite:
  - `//python_api:velaria_python_supported_regression`
- shell entrypoint:
  - `./scripts/run_python_ecosystem_regression.sh`

### Experimental Runtime

Experimental runtime includes:

- actor runtime
- rpc codec / transport experiments
- scheduler / worker / client flow
- same-host smoke and benchmark tools

Repository entrypoints:

- Bazel source group:
  - `//:velaria_experimental_sources`
- regression suite:
  - `//:experimental_regression`
- shell entrypoint:
  - `./scripts/run_experimental_regression.sh`

### Examples

Examples and helper scripts illustrate layers; they do not define them.

- Bazel source group:
  - `//:velaria_examples_sources`

## Runtime Contract

The stable runtime-facing contract is documented in [docs/runtime-contract.md](./docs/runtime-contract.md).

Main stream entry points:

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

Stable stream contract surfaces:

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

`strategy` is the single explanation outlet for mode selection, fallback reason, transport, backpressure thresholds, and checkpoint delivery mode.

## Current Capability Boundary

Available today:

- local batch + streaming execution through one kernel
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local backpressure, bounded backlog, progress snapshots, checkpoint path
- execution modes: `single-process`, `local-workers`
- file source/sink support
- basic stream operators: `select / filter / withColumn / drop / limit / window`
- stateful `sum` and `count`
- minimal stream SQL subset
- local vector search on fixed-dimension float vectors
- Python Arrow ingestion and output
- same-host actor/rpc/jobmaster smoke path

Out of scope in the current repo state:

- completed distributed runtime claims
- Python callback execution in the hot path
- Python UDFs
- generic actor parallelization for arbitrary plans
- broad SQL expansion such as full `JOIN / CTE / subquery / UNION`
- ANN / standalone vector DB / distributed vector execution

## Python Ecosystem

Python remains a supported ingress and packaging layer. It does not become the execution core.

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

Python ecosystem commands in this repo use `uv`:

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
uv run --project python_api python python_api/examples/demo_stream_sql.py
uv run --project python_api python python_api/examples/demo_vector_search.py
```

Python ecosystem build/test prerequisites:

- `uv`
- a local CPython interpreter with `Python.h`
- `VELARIA_PYTHON_BIN` when Bazel cannot auto-discover a usable interpreter

Recommended regression entrypoint:

```bash
./scripts/run_python_ecosystem_regression.sh
```

## Local Vector Search

Vector search is a local kernel capability, not a new subsystem.

Scope in `v0.1`:

- fixed-dimension `float32`
- metrics: `cosine`, `dot`, `l2`
- `top-k`
- exact scan only
- `DataFrame` / `DataflowSession`
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
bazel build //:velaria_cli
./bazel-bin/velaria_cli \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

Vector explain is part of the stable contract. Current required fields include:

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

The script runs a quick exact-scan baseline. Use `bazel run //:vector_search_benchmark` for the full sweep.

## Experimental Runtime

The same-host path stays intentionally narrow:

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

It exists for:

- same-host execution experiments
- transport and codec observation
- benchmark and observability development

It does not imply:

- distributed scheduling
- distributed fault recovery
- cluster resource governance
- production distributed vector execution

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
```

Direct Bazel suites:

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

Same-host observability regression:

```bash
./scripts/run_stream_observability_regression.sh
```

## Repository Rules

- language baseline: `C++17`
- build system: `Bazel`
- keep `DataflowSession` as the public session entry
- do not break `sql_demo / df_demo / stream_demo`
- keep example source files as `.cc`
- use `uv` for Python commands in this repository
- keep `README.md` and `README-zh.md` aligned
