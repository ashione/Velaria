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
- local app-side service and Electron prototype support
- offline embedding generation helpers and versioned embedding asset management
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
- batch file ingress through explicit and probed readers:
  - `session.read_csv(...)`, `session.read_line_file(...)`, `session.read_json(...)`
  - `session.probe(...)` and `session.read(...)` for generic file input
- batch SQL file registration:
  - `CREATE TABLE ... USING csv|line|json OPTIONS(...)`
  - `CREATE TABLE ... OPTIONS(path: '...')` with source probing
- batch SQL v1:
  - `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE` (including `AND` / `OR`), `GROUP BY`, `ORDER BY`, `LIMIT`, current minimal `JOIN`
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
- local desktop app prototype under `app/`, backed by `velaria-service`
- macOS desktop packaging prototype producing `.dmg`
- AI / agent / skill integration through the supported Python ecosystem layer, with workspace and artifact management for result reuse and local data management
- same-host actor/rpc/jobmaster smoke path

Current constraints:

- `CREATE SOURCE TABLE` is read-only and rejects `INSERT`
- `CREATE SINK TABLE` accepts writes but cannot be used as query input
- SQL v1 does not expand to `CTE`, subquery, `UNION`, or richer join semantics
- Python callbacks / Python UDFs are not part of the hot path
- the Electron desktop app is still a local prototype, not a stable product surface
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

## 4. Usage Examples

Batch SQL on an auto-probed file source:

```sql
CREATE TABLE rpc_input OPTIONS(path: '/tmp/input.jsonl');
SELECT * FROM rpc_input LIMIT 5;
```

Batch SQL on an explicit CSV source:

```sql
CREATE TABLE csv_input USING csv OPTIONS(path: '/tmp/input.csv', delimiter: ',');
SELECT * FROM csv_input LIMIT 5;
```

Python batch file input:

```python
import velaria

session = velaria.Session()
probe = session.probe("/tmp/input.jsonl")
df = session.read("/tmp/input.jsonl")
```

Explicit CSV reader:

```python
csv_df = session.read_csv("/tmp/input.csv")
```

Regex line input:

```python
regex_df = session.read_line_file(
    "/tmp/events.log",
    mappings=[("uid", 1), ("action", 2), ("latency", 3), ("ok", 4), ("note", 5)],
    mode="regex",
    regex_pattern=r'^uid=(\d+) action="([^"]+)" latency=(\d+) ok=(true|false) note=(.+)$',
)
```

CLI examples:

```bash
uv run --project python_api python python_api/velaria_cli.py file-sql \
  --csv /tmp/input.csv \
  --input-type csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py file-sql \
  --input-path /tmp/input.jsonl \
  --input-type auto \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py file-sql \
  --input-path /tmp/events.log \
  --input-type line \
  --line-mode regex \
  --regex-pattern '^uid=(\\d+) action=\"([^\"]+)\" latency=(\\d+) ok=(true|false) note=(.+)$' \
  --mappings 'uid:1,action:2,latency:3,ok:4,note:5' \
  --query "SELECT * FROM input_table LIMIT 5"
```

Real entry points:

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:file_source_benchmark -- 200000 3
uv run --project python_api python python_api/velaria_cli.py --help
./dist/velaria-cli --help
```

Desktop app prototype entry points:

```bash
cd app
npm install
npm start

bash app/scripts/build-sidecar-macos.sh
bash app/scripts/package-macos.sh
```

## 5. Development Docs

- English: [docs/development.md](./docs/development.md)
- Chinese: [docs/development-zh.md](./docs/development-zh.md)

## 6. Packaging Notes

Current repository-visible release packaging includes:

- Linux native wheels for:
  - `manylinux x86_64`
  - `manylinux aarch64`
- macOS native wheels for:
  - `universal2`
- macOS desktop prototype packaging for:
  - `.dmg`

Linux release packaging keeps one wheel per OS/arch and relies on runtime SIMD dispatch inside that wheel rather than publishing separate wheels for each SIMD instruction set.
