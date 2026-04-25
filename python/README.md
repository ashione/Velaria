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
- offline embedding pipeline helpers for versioned vector assets
- offline keyword-index build helpers and reusable BM25 keyword-search assets
- agent runtime wrapper for Claude Code / Claude Agent SDK and Codex App Server integration
- interactive agent CLI via `velaria_cli.py -i`

### Examples

Examples and helper assets include:

- `examples/demo_batch_sql_arrow.py`
- `examples/demo_stream_sql.py`
- `examples/demo_bitable_group_by_owner.py`
- `examples/demo_vector_search.py`
- `benchmarks/bench_arrow_ingestion.py`
- `examples/demo_embedding_pipeline.py`
- `benchmarks/bench_embedding_pipeline.py`
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

- `Session.probe(...)`
- `Session.read(...)`
- `Session.read_csv(...)`
- `Session.read_line_file(...)`
- `Session.read_json(...)`
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
- `build_embedding_rows(...)`
- `materialize_embeddings(...)`
- `load_embedding_dataframe(...)`
- `embed_query_text(...)`
- `SentenceTransformerEmbeddingProvider(...)`
- `build_keyword_index(...)`
- `search_keyword_index(...)`

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

File reader mapping:

- `Session.probe(path)` returns inferred source kind, schema, normalized options, the final selected format, scored candidates, confidence, and warnings
- `Session.read(path, ...)` is the preferred batch file front door and probes the source automatically
- `Session.read_csv(...)` remains the explicit CSV override
- `Session.read_line_file(path, mappings=[...], ...)` maps to the native line/regex source connector
- `Session.read_json(path, columns=[...], ...)` maps to the native JSON lines / JSON array source connector
- all four file readers share the same source materialization knobs:
  `materialization`, `materialization_dir`, and `materialization_format`
- all four file readers also support `cache_in_memory=True` to retain the projected source snapshot
  inside the current session for repeated same-session queries; it is a reuse-oriented tradeoff,
  not a pure free cache hint, because it can bypass source pushdown on the first query
- `velaria-cli file-sql` defaults to `--input-type auto` and registers batch sources through `CREATE TABLE ... OPTIONS(path: '...')`
- versioned embedding datasets written as Parquet / Arrow should be loaded through `pyarrow` plus `Session.create_dataframe_from_arrow(...)` or `load_embedding_dataframe(...)`, not `Session.read(...)`
- reusable keyword indexes are directory artifacts built from Arrow / Parquet or batch file inputs and should be queried through the service / helper APIs, not treated as plain table files

Regex line usage:

- regex is an explicit line-reader mode; it is not auto-probed
- `mappings` source indexes follow regex capture-group numbering:
  - `0` is the full match
  - `1..n` are capture groups
- unmatched lines are skipped

Regex example:

```python
regex_df = session.read_line_file(
    "events.log",
    mappings=[("uid", 1), ("action", 2), ("latency", 3), ("ok", 4), ("note", 5)],
    mode="regex",
    regex_pattern=r'^uid=(\d+) action="([^"]+)" latency=(\d+) ok=(true|false) note=(.+)$',
)
```

Minimal examples:

```python
import velaria

session = velaria.Session()

csv_df = session.read_csv("input.csv")

probe = session.probe("events.jsonl")
auto_df = session.read("events.jsonl")

# probe includes:
# kind / final_format / score / confidence / schema / candidates / warnings

json_df = session.read_json(
    "events.jsonl",
    columns=["user_id", "action", "latency"],
    format="json_lines",
)

json_array_df = session.read_json(
    "events.json",
    columns=["event", "cost"],
    format="json_array",
)

nested_json_df = session.read_json(
    "events_nested.json",
    columns=["a", "b"],
    format="json_array",
)

# JSON reader notes:
# - top-level rows must still be JSON objects
# - nested object fields are preserved as raw JSON strings
# - numeric JSON arrays are still parsed as vector values when read directly as a field
```

JSON source examples:

```json
{"user_id":1,"action":"open","latency":12.5}
{"user_id":2,"action":"close","latency":9.0}
```

```json
[
  {"event":"open","cost":1.5},
  {"event":"close","cost":2}
]
```

```json
[
  {"a":1,"b":{"b1":1}},
  {"a":2,"b":{"b1":2,"b2":["x",3,null]}}
]
```

Nested-object result shape with `columns=["a", "b"]`:

```text
[1, "{\"b1\":1}"]
[2, "{\"b1\":2,\"b2\":[\"x\",3,null]}"]
```

Current JSON limits:

- `json_lines` and `json_array` are supported
- each top-level row must be a JSON object
- `columns=[...]` is required for explicit JSON reads
- nested object values are returned as JSON text, not flattened columns
- a top-level scalar array such as `["a", "b"]` is not supported as a table source
- nested arrays inside an object string are preserved inside that JSON text
- direct field values that are numeric JSON arrays still map to vector values

Embedding pipeline example:

```python
from velaria import (
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_WARMUP_TEXT,
    HashEmbeddingProvider,
    Session,
    SentenceTransformerEmbeddingProvider,
    build_mixed_text_embedding_rows,
    download_embedding_model,
    materialize_mixed_text_embeddings,
    run_mixed_text_hybrid_search,
)

records = [
    {
        "doc_id": "doc-1",
        "title": "Alpha",
        "summary": "Payment page timeout",
        "tags": ["billing", "checkout"],
        "bucket": 1,
        "source_updated_at": 1,
    },
    {
        "doc_id": "doc-2",
        "title": "Beta",
        "summary": "Refund delay in worker queue",
        "tags": ["refund", "queue"],
        "bucket": 2,
        "source_updated_at": 2,
    },
]
provider = HashEmbeddingProvider(dimension=8)
session = Session()
materialize_mixed_text_embeddings(
    records,
    provider=provider,
    model="hash-demo",
    template_version="text-v1",
    text_fields=("title", "summary", "tags"),
    output_path="docs_embeddings.parquet",
)

result = run_mixed_text_hybrid_search(
    session,
    "docs_embeddings.parquet",
    provider=provider,
    model="hash-demo",
    query_text="payment page hangs during checkout",
    where_sql="bucket = 1 AND doc_id = 'doc-1'",
    top_k=2,
    metric="cosine",
)
```

For a local semantic baseline with `all-MiniLM-L6-v2`, install the optional provider dependency first:

```bash
uv sync --project python --extra embedding
```

Then swap the provider:

```python
provider = SentenceTransformerEmbeddingProvider(
    model_name=DEFAULT_LOCAL_EMBEDDING_MODEL,
)
```

If you want to avoid remote Hub resolution on every machine, put the model files in a local directory and point the provider at that directory. Supported lookup order for the default MiniLM model is:

1. `VELARIA_EMBEDDING_MODEL_DIR`
2. `python/models/all-MiniLM-L6-v2`
3. fallback to the Hugging Face model id

Example:

```bash
export VELARIA_EMBEDDING_MODEL_DIR=/absolute/path/to/all-MiniLM-L6-v2
export VELARIA_EMBEDDING_CACHE_DIR=/absolute/path/to/hf-cache
```

Then `SentenceTransformerEmbeddingProvider(model_name=DEFAULT_LOCAL_EMBEDDING_MODEL)` will load from the local directory instead of the Hub.

You can also explicitly pre-download and warm up the model before serving queries:

```python
from velaria import (
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    SentenceTransformerEmbeddingProvider,
    download_embedding_model,
)

local_dir = download_embedding_model(DEFAULT_LOCAL_EMBEDDING_MODEL)
provider = SentenceTransformerEmbeddingProvider(model_name=DEFAULT_LOCAL_EMBEDDING_MODEL)
provider.warmup(
    download_if_missing=False,
    warmup_text="warmup embedding text",
)
```

Recommended startup flow:

1. `download_embedding_model(...)` during environment/bootstrap time
2. `provider.warmup(...)` once during process start
3. run batch embedding or online query embedding after the model is already resident

CLI examples:

```bash
uv run --project python python python/velaria_cli.py file-sql \
  --csv /tmp/input.csv \
  --input-type csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py file-sql \
  --input-path /tmp/input.jsonl \
  --input-type auto \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py file-sql \
  --input-path /tmp/events.log \
  --input-type line \
  --line-mode regex \
  --regex-pattern '^uid=(\\d+) action=\"([^\"]+)\" latency=(\\d+) ok=(true|false) note=(.+)$' \
  --mappings 'uid:1,action:2,latency:3,ok:4,note:5' \
  --query "SELECT * FROM input_table LIMIT 5"
```

Mixed-text embedding pipeline through the CLI:

```bash
uv run --project python python python/velaria_cli.py embedding-build \
  --input-path /tmp/docs.csv \
  --input-type csv \
  --text-columns title,summary,tags \
  --provider minilm \
  --output-path /tmp/docs_embeddings.parquet

uv run --project python python python/velaria_cli.py embedding-query \
  --dataset-path /tmp/docs_embeddings.parquet \
  --provider minilm \
  --query-text "payment page hangs during checkout" \
  --where-sql "bucket = 1 AND region = 'apac'" \
  --top-k 5

# direct query from the raw file without a prebuilt embedding dataset
uv run --project python python python/velaria_cli.py embedding-query \
  --input-path /tmp/docs.csv \
  --input-type csv \
  --text-columns title,summary,tags \
  --provider minilm \
  --query-text "payment page hangs during checkout" \
  --where-sql "bucket = 1 AND region = 'apac'" \
  --top-k 5
```

Reusable keyword-index build and BM25 keyword search through the service:

```bash
curl -sS http://127.0.0.1:37491/api/v1/runs/keyword-index-build \
  -H 'Content-Type: application/json' \
  -d '{
    "input_path": "/tmp/docs.csv",
    "input_type": "csv",
    "text_columns": ["title", "body"],
    "analyzer": "jieba"
  }'

curl -sS http://127.0.0.1:37491/api/v1/runs/keyword-search \
  -H 'Content-Type: application/json' \
  -d '{
    "index_path": "/tmp/keyword_index",
    "query_text": "payment timeout",
    "where_sql": "bucket = 1",
    "top_k": 10
  }'
```

### Agent Runtime (Optional)

Codex runtime dependencies are installed with the default Python package. Install
Claude runtime support only when using Claude Code:

```bash
uv sync --project python --extra ai-claude    # Claude Agent SDK
```

The agent runtime provides:

- Codex/Claude-backed interactive agent runtime via `velaria_cli.py -i`
- Thread persistence under `agentRuntimeWorkspace`
- Automatic injection of `skills/velaria_python_local/SKILL.md`
- Velaria local functions exposed through the runtime bridge / MCP server:
  `velaria_read`, `velaria_schema`, `velaria_sql`, `velaria_explain`,
  `velaria_dataset_download`, `velaria_dataset_import`,
  `velaria_dataset_normalize`, `velaria_dataset_process`,
  `velaria_cli_run`, `velaria_artifact_preview`
- Natural language to SQL generation via `velaria_cli.py ai generate-sql`
- Session-based compatibility commands via `velaria_cli.py ai session` and
  `velaria_cli.py ai analyze`

Minimal Codex runtime config:

```json
{
  "agentRuntime": "codex",
  "agentAuthMode": "oauth",
  "agentProvider": "openai",
  "agentModel": "gpt-5.4-mini",
  "agentRuntimeWorkspace": "~/.velaria/ai-runtime",
  "agentCodexNetworkAccess": true
}
```

Codex uses the local `codex app-server` command and defaults to `gpt-5.4-mini`
when `agentModel` is omitted. `agentRuntimeWorkspace` is the runtime working directory
used to save and resume agent threads. If omitted, Velaria creates a
project-scoped directory under `~/.velaria/ai-runtime/`. `agentAuthMode: "oauth"`
reuses the local Codex or Claude login. Use `agentAuthMode: "api_key"` together
with `agentApiKey` and `agentBaseUrl` when the provider should be driven by
explicit API credentials. Use `agentRuntimePath` / `agentCodexRuntimePath` only
when overriding the local Codex executable. Use `agentClaudeRuntimePath` for
Claude Code runtime. Codex workspace-write network access is enabled by default;
set `agentCodexNetworkAccess` to `false` only for offline runtime sessions.
The runtime inherits standard proxy environment variables such as `http_proxy`,
`https_proxy`, and `all_proxy`.

AI CLI examples:

```bash
uv run --project python python python/velaria_cli.py -i

# Inside interactive mode, plain text goes to the active agent thread.
velaria> 读取 data/sales.csv，按 region 汇总 amount，并保存 run
velaria> /status
velaria> :run list --limit 5

uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "top 5 by score" --schema "name,score,region"
```

Current SQL mapping carried by Python:

- `Session.sql(...)` maps to core SQL v1 batch semantics:
  - `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE`, `GROUP BY` columns/scalar expressions, `ORDER BY`, `LIMIT`, `UNION` / `UNION ALL`, and the current minimal `JOIN`
  - batch `WHERE` supports single predicates, column-to-column predicates, plus `AND` / `OR` expressions
  - batch `KEYWORD SEARCH(title, body) QUERY '...' TOP_K ...` on single-table non-aggregate queries
  - batch `HYBRID SEARCH ... QUERY ...` on single-table non-aggregate queries
  - current Python service can combine reusable keyword-index recall with vector rerank by passing both `index_path` and `dataset_path` to `hybrid-search`
- batch builtins currently exposed through the same core path:
  - `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`
  - `LENGTH`, `LEN`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `REVERSE`
  - `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR` / `SUBSTRING`, `POSITION`, `REPLACE`, `CAST`
  - `ABS`, `CEIL`, `FLOOR`, `ROUND`, `YEAR`, `MONTH`, `DAY`, `ISO_YEAR`, `ISO_WEEK`, `WEEK`, `YEARWEEK`
  - supported scalar functions can be nested in projection expressions
- `Session.stream_sql(...)`, `Session.explain_stream_sql(...)`, and `Session.start_stream_sql(...)` share the same stream SQL front-door checks:
  - source must be a source table / stream source
  - sink target must be a sink table
  - only the current stream-stable projection, filter, window, stateful aggregate, and bounded-source `ORDER BY` shapes are accepted
- current SQL v1 keeps `ORDER BY` scoped to columns present in the `SELECT` output
- unsupported SQL shapes are expected to surface as explicit parse / semantic / unsupported SQL v1 / table-kind errors from the core path, not Python-only behavior

Desktop / service import behavior:

- local file import preview still only inspects schema + preview rows
- when saving a dataset from the desktop app, embedding build and keyword-index build can both be configured and will run asynchronously in the background
- bitable import can build:
  - a reusable embedding dataset from selected text columns
  - a reusable keyword index from selected keyword columns
  - both in parallel within the same background import run
- packaged sidecar builds copy jieba dictionaries from the resolved Bazel `cppjieba` dependency at build time; the source repo does not need to carry those dictionary files

## Repository Layout

Stable Python layout in this repo:

- supported library:
  - `python/velaria/`
- supported CLI tool:
  - `python/velaria_cli.py`
- examples:
  - `python/examples/`
- benchmarks:
  - `python/benchmarks/`
- reserved experimental area:
  - `python/experimental/`
- regression tests:
  - `python/tests/`

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
bazel run //python:sync_native_extension
uv sync --project python --python python3.13
```

If you run `python/velaria_cli.py` or other source-checkout Python entrypoints directly,
keep `python/velaria/_velaria.so` in sync with:

```bash
bazel run //python:sync_native_extension
```

Run demos:

```bash
uv run --project python python python/examples/demo_batch_sql_arrow.py
uv run --project python python python/examples/demo_stream_sql.py
uv run --project python python python/examples/demo_vector_search.py
uv run --project python python python/examples/demo_embedding_pipeline.py
```

Recommended regression entrypoint:

```bash
./scripts/run_python_ecosystem_regression.sh
```

Repository benchmark fixture generation:

- the stage benchmark can generate synthetic data at runtime
- if you want a locally realistic benchmark input, generate an anonymized CSV from a private raw export with:

```bash
uv run --project python python scripts/generate_stage_benchmark_fixture.py \
  --input /path/to/raw_rows_100k.csv \
  --output python/benchmarks/data/stage_input_100k_anonymized.csv
```

- keep that generated CSV local and untracked; it is ignored by `.gitignore`

That script covers:

- native extension build
- wheel and native wheel build
- Bazel Python regression targets
- demo smoke
- CLI smoke

Benchmark regression entrypoint:

```bash
./scripts/run_python_stage_benchmark.sh
```

Embedding pipeline benchmark:

```bash
uv run --project python python python/benchmarks/bench_embedding_pipeline.py
```

For the local MiniLM provider:

```bash
uv sync --project python --extra embedding
uv run --project python python python/benchmarks/bench_embedding_pipeline.py \
  --provider minilm \
  --model sentence-transformers/all-MiniLM-L6-v2
```

The benchmark reports both:

- batch embedding/materialization throughput
- online query embedding latency
- online hybrid search latency on the resulting embedding dataset

Core file-input benchmark entrypoint:

```bash
bazel run //:file_source_benchmark -- 200000 3
```

That benchmark currently reports:

- CSV hardcode / explicit / auto-probed paths
- CSV scan-only / full-columnar / full-row-materialize / projected / filter-pushdown / aggregate-pushdown sub-cases
- line split hardcode / explicit / auto-probed paths and direct filter-pushdown / aggregate-pushdown cases
- line regex parse and grouped-aggregate paths
- JSON lines hardcode / explicit / auto-probed paths and direct filter-pushdown / aggregate-pushdown cases
- SQL `CREATE TABLE ... OPTIONS(path: '...')` registration costs plus CSV / line / JSON predicate-pushdown comparisons

Current pushdown lowering also classifies source work into:

- `ConjunctiveFilterOnly`
- `SingleKeyCount`
- `SingleKeyNumericAggregate`
- `Generic`

Representative clean-`main` vs current snapshot on `200000 / 3`:

- `read_line_regex_explicit_group_sum`: `5679936 us -> 641735 us`
- `sql_csv_predicate_and_group_count`: `133011 us -> 109146 us`
- `sql_csv_predicate_or_group_count`: `307313 us -> 171556 us`
- `sql_csv_predicate_mixed_group_count`: `462000 us -> 275583 us`
- `sql_line_predicate_or_group_count`: `314852 us -> 174627 us`
- `sql_json_predicate_or_group_count`: `604404 us -> 420423 us`

For Linux perf sampling on the native CSV path:

```bash
perf record --call-graph=dwarf bazel-bin/file_source_benchmark -- 200000 3
perf report
```

By default that script generates benchmark input at runtime.
To use a local anonymized CSV instead, set `VELARIA_STAGE_BENCH_CSV=/path/to/file.csv`.
The default scenario is `groupby_count_max`.

Benchmark scenario controls:

- set `VELARIA_STAGE_BENCH_SCENARIO=groupby_count_max` for the `caller_psm / count / max(latency)` path
- set `VELARIA_STAGE_BENCH_SCENARIO=filter_lower_limit` for the `LOWER(method) + filter + LIMIT` path
- set `VELARIA_STAGE_BENCH_QUERY="..."` only when you intentionally want a custom Velaria query
- pass `--cache-in-memory` to `python/benchmarks/bench_stage_paths.py` when you want the
  reuse path to retain projected source columns in the current session
- when `VELARIA_STAGE_BENCH_QUERY` does not match the selected scenario query, also set
  `VELARIA_STAGE_BENCH_SKIP_HARDCODE=1`; otherwise the benchmark rejects the run

`hardcode` is only reported when it is semantically aligned with the selected scenario.
The benchmark now enforces row-count parity between the hardcode baseline and Velaria result
before it prints ratios.

Interpretation guardrails for the stage benchmark:

- `Session.read_csv(...)` and `Session.sql(...)` are setup/planning calls in this harness; they do not
  represent file scan or query execution time by themselves
- `DataFrame.to_arrow()` is the first materialization point in this harness, so its stage includes
  execution plus Arrow export
- `to_pylist()` only measures the Python-side conversion from the already materialized Arrow table
- the `hardcode` baseline in `python/benchmarks/bench_stage_paths.py` is a scenario-specific
  Python stdlib baseline built with `csv.DictReader` and manual logic; it is useful for relative
  comparison inside this harness, but it is not a native C/C++ kernel upper bound
- packaged CLI startup is a separate measurement surface from the Python API stage benchmark

## Packaging

Build targets:

- native extension:
  - `//:velaria_pyext`
- sync built native extension into the source checkout:
  - `//python:sync_native_extension`
- pure-Python wheel wrapper:
  - `//python:velaria_whl`
- native wheel:
  - `//python:velaria_native_whl`
- Python CLI:
  - `//python:velaria_cli`

Single-file CLI packaging:

```bash
./scripts/build_py_cli_executable.sh
./dist/velaria-cli file-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

That single-file CLI is packaged with PyInstaller `--onefile`, so cold-start measurements include
Python/bootstrap overhead in addition to engine work.

The CLI is part of the ecosystem layer. For supported paths, it should delegate to the same native session contract as Python and C++.

Repo-visible CLI entrypoints are:

- source checkout:
  - `uv run --project python python python/velaria_cli.py ...`
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
uv run --project python python python/velaria_cli.py run start -- file-sql \
  --run-name "cn_slow_query_24h_2026-04-03" \
  --description "score filter result for demo input" \
  --tag cn \
  --tag "slow-query,demo" \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli run start -- file-sql \
  --run-name "cn_slow_query_24h_2026-04-03" \
  --description "score filter result for demo input" \
  --tag cn \
  --tag "slow-query,demo" \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py run list --tag cn --query "slow query" --limit 20
uv run --project python python python/velaria_cli.py run result --run-id <run_id>
uv run --project python python python/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python python python/velaria_cli.py run show --run-id <run_id>
uv run --project python python python/velaria_cli.py run status --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts preview --artifact-id <artifact_id>
uv run --project python python python/velaria_cli.py run cleanup --keep-last 10
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
uv run --project python python python/velaria_cli.py run start -- file-sql \
  --run-name "high_score_rows" \
  --description "high score rows for local inspection" \
  --tag local-demo \
  --tag scores \
  --csv /path/to/input.csv \
  --query "SELECT name, score FROM input_table WHERE score > 10"

uv run --project python python python/velaria_cli.py run list --tag scores --query "high score"
uv run --project python python python/velaria_cli.py run result --run-id <run_id>
uv run --project python python python/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python python python/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts preview --artifact-id <artifact_id>
```

Stream SQL once plus status:

```bash
uv run --project python python python/velaria_cli.py run start -- stream-sql-once \
  --source-csv-dir /path/to/source_dir \
  --sink-schema "key STRING, value_sum INT" \
  --query "INSERT INTO output_sink SELECT key, SUM(value) AS value_sum FROM input_stream GROUP BY key"

uv run --project python python python/velaria_cli.py run status --run-id <run_id>
```

For this action, the query still follows the core stream SQL boundary:

- `--query` must be `INSERT INTO <sink> SELECT ...`
- the source side must resolve to the stream source table created by the command
- the sink side must resolve to the sink table created from `--sink-schema`
- explain output remains `logical / physical / strategy`, and progress stays native `snapshotJson()`

Vector search plus explain artifact:

```bash
uv run --project python python python/velaria_cli.py run start -- vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --top-k 5

uv run --project python python python/velaria_cli.py artifacts list --run-id <run_id>
```

Hybrid search through the CLI keeps the same command and adds optional filter / threshold controls:

```bash
uv run --project python python python/velaria_cli.py vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5 \
  --where-column bucket \
  --where-op = \
  --where-value 1 \
  --score-threshold 0.02
```

Batch SQL also supports a minimal hybrid search clause through `file-sql`:

```bash
uv run --project python python python/velaria_cli.py file-sql \
  --csv /path/to/vectors.csv \
  --query "SELECT id, bucket, vector_score FROM input_table WHERE bucket = 1 HYBRID SEARCH embedding QUERY '[0.1 0.2 0.3]' METRIC cosine TOP_K 5 SCORE_THRESHOLD 0.02"
```

Python ecosystem source groups:

- supported:
  - `//python:velaria_python_supported_sources`
- examples and benchmarks:
  - `//python:velaria_python_example_sources`
- experimental placeholder:
  - `//python:velaria_python_experimental_sources`

## Arrow Contract

Arrow is the preferred interop form for high-volume results.

Guidance:

- prefer Arrow/native columnar paths over `to_rows()` when benchmarking or integrating large results
- treat `to_rows()` as a convenience/debugging surface
- `Session.sql(...)` returns a lazy batch `DataFrame` handle
- `to_arrow()` / `to_rows()` trigger materialization; the first materialization stage includes execution
  plus conversion to the requested result form
- if you need pandas, use `session.sql(...).to_arrow().to_pandas()`; there is no direct
  `DataFrame.to_pandas()` helper in the current API

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
- batch SQL supports a minimal `HYBRID SEARCH ... QUERY ...` clause
- CLI `vector-search` supports optional `--where-column/--where-op/--where-value` and `--score-threshold`
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

- `//python:streaming_v05_test`
- `//python:arrow_stream_ingestion_test`
- `//python:vector_search_test`
- `//python:read_excel_test`
- `//python:custom_stream_source_test`
- `//python:bitable_stream_source_test`
- `//python:bitable_group_by_owner_integration_test`

Python-layer grouped suite:

- `//python:velaria_python_supported_regression`

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
