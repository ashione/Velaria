# Velaria Runtime Contract

## Summary

This document defines the stable runtime-facing contract for Velaria's local kernel.

It is the source of truth for:

- progress fields
- checkpoint fields and delivery modes
- explain structure
- execution-mode reporting
- source/sink lifecycle semantics
- Python ecosystem mapping rules
- vector explain surface

This document complements:

- [core-boundary.md](./core-boundary.md)
- [agentic-service-api.md](./agentic-service-api.md)
- [streaming_runtime_design.md](./streaming_runtime_design.md)
- [local_vector_search_v01.md](./local_vector_search_v01.md)

## Public Contract Surfaces

Core C++ surfaces:

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQueryProgress`
- source/sink ABI in `stream/source_sink_abi.h`

Stable batch file-input surfaces:

- `DataflowSession::probe(...)`
- `DataflowSession::read(...)`
- `DataflowSession::read_csv(...)`
- `DataflowSession::read_line_file(...)`
- `DataflowSession::read_json(...)`
- `CREATE TABLE ... USING csv|line|json OPTIONS(...)`
- `CREATE TABLE ... OPTIONS(path: '...')` with source probing

Python ecosystem projections:

- `Session.probe(...)`
- `Session.read(...)`
- `Session.stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `Session.start_stream_sql(...)`
- `Session.vector_search(...)`
- `Session.explain_vector_search(...)`
- `Session.create_dataframe_from_arrow(...)`
- `Session.create_stream_from_arrow(...)`
- `Session.create_realtime_stream_source(...)`
- `Session.read_realtime_stream_source(...)`
- `Session.create_realtime_stream_sink(...)`
- `read_excel(...)`

Python-facing APIs may keep Pythonic naming, but their behavior must project the same semantics as the C++ kernel.

The local agentic service and monitor/focus-event API are documented separately in
`docs/agentic-service-api.md`; they build on this runtime contract but are not
themselves kernel contract surfaces.

## Batch File Input Contract

Stable file-input behavior:

- explicit file readers and probed file readers must project the same schema and row semantics for the same source
- `probe(...)` may expose scoring, confidence, warnings, and candidate formats, but those fields are advisory and must not silently change the selected format semantics
- explicit `USING csv|line|json` overrides source-kind probing
- `CREATE TABLE ... OPTIONS(path: '...')` resolves the source kind through probing when `USING ...` is omitted
- `OPTIONS(...)` uses `key: value` syntax

Current probed result shape exposed through Python:

- `kind`
- `final_format`
- `score`
- `confidence`
- `schema`
- `suggested_table_name`
- `candidates`
- `warnings`

## StreamingQueryProgress

The following fields are treated as stable contract output:

- identity and status
  - `query_id`
  - `status`
- execution choice
  - `requested_execution_mode`
  - `execution_mode`
  - `execution_reason`
  - `transport_mode`
- throughput and queueing
  - `batches_pulled`
  - `batches_processed`
  - `blocked_count`
  - `max_backlog_batches`
  - `inflight_batches`
  - `inflight_partitions`
- latency
  - `last_batch_latency_ms`
  - `last_sink_latency_ms`
  - `last_state_latency_ms`
- source and strategy shape
  - `last_source_offset`
  - `backpressure_active`
  - `actor_eligible`
  - `used_actor_runtime`
  - `used_shared_memory`
  - `has_stateful_ops`
  - `has_window`
  - `sink_is_blocking`
  - `source_is_bounded`
- estimates and thresholds
  - `estimated_partitions`
  - `projected_payload_bytes`
  - `sampled_batches`
  - `sampled_rows_per_batch`
  - `average_projected_payload_bytes`
  - `actor_speedup`
  - `compute_to_overhead_ratio`
  - `estimated_state_size_bytes`
  - `estimated_batch_cost`
  - `backpressure_max_queue_batches`
  - `backpressure_high_watermark`
  - `backpressure_low_watermark`
- checkpoint
  - `checkpoint_delivery_mode`

Field names must remain stable in both `StreamingQueryProgress` and Python dictionary projections unless there is an intentional versioned migration.

## snapshotJson()

`snapshotJson()` is a serialized projection of the same progress contract, not a separate contract family.

Rules:

- it must expose the same stable field names where available
- it must not rename execution, checkpoint, or vector-related fields independently
- Python and docs must treat it as a serialized view of `StreamingQueryProgress`

## Explain Contract

### Stream SQL Explain

`explainStreamSql(...)` must return exactly three top-level sections:

- `logical`
- `physical`
- `strategy`

The `strategy` section must be the single explanation outlet for:

- selected mode
- fallback / downgrade reason
- actor hot-path eligibility
- transport mode
- state and batch estimates
- shared-memory knobs
- checkpoint delivery mode
- backpressure threshold snapshot

If runtime behavior changes, `strategy` text and `StreamingQueryProgress` must change together.

### Stream Window Contract

Current stable stream window behavior is:

- fixed tumbling windows through the native `window(..., window_ms, ...)` path
- fixed sliding windows through `window(..., window_ms, ..., slide_ms)` when `slide_ms <= window_ms`
- stream SQL supports `WINDOW BY <time_column> EVERY <window_ms> [SLIDE <slide_ms>] AS <output_column>`
- `SLIDE` defaults to `EVERY` when omitted
- unbounded global windows are not part of the stable contract
- event-time / watermark semantics are not yet a stable contract family

The planner and runtime must agree on:

- window size
- slide size
- output column name
- row-expansion behavior for overlapping sliding windows

### Vector Explain

Vector explain output is a stable local capability contract.

Current required fields:

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `backend=<simd-backend>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+simd-topk`

Python `Session.explain_vector_search(...)` and CLI output must project this same core explain behavior.

Preferred vector ingestion contract:

- Arrow uses `FixedSizeList<float32>` as the first-class shape
- local CSV uses bracketed vector text such as `[1 2 3]` or `[1,2,3]`
- supported v0.1 execution remains local exact scan only

## Execution Modes

Stable execution-mode contract fields:

- `requested_execution_mode`
- `execution_mode`
- `execution_reason`
- `transport_mode`

Semantics:

- requested mode records what the caller asked for
- execution mode records what the kernel actually used
- execution reason records fallback, downgrade, or final selection reason
- transport mode records the data-plane transport used by the kernel

The planner/explain path and runtime path must not describe different decisions.

## Checkpoint Delivery

Stable checkpoint contract:

- `at-least-once`
  - default mode
  - allows replay and duplicate sink output
  - source offset is not restored by default
- `best-effort`
  - restores source offset only when the source implements restore support
  - still does not claim exactly-once sink semantics

Stable checkpoint-related outputs:

- `checkpoint_delivery_mode`
- `last_source_offset`

Checkpoint files remain local and atomically replaced.

## Source/Sink Lifecycle

The stable lifecycle is:

```text
open -> nextBatch -> checkpoint -> ack -> close
```

Rules:

- source and sink open with query-scoped context
- checkpoint markers carry source offset and batch progress
- source receives `ack(token)` after checkpointed progress is recorded
- lifecycle semantics are query-local and do not claim distributed coordination

## Python Ecosystem Mapping Rules

Python is allowed to:

- wrap core APIs
- package and distribute bindings
- offer ecosystem-friendly names
- expose explicit and probed batch file readers
- provide Arrow/Excel/Bitable/custom source entrypoints
- compose demos and helper scripts

Python is not allowed to:

- invent a separate progress schema
- invent a separate checkpoint contract
- implement a separate vector-scoring semantic for supported CLI/API paths
- treat experimental runtime behavior as a required dependency

## Build and Toolchain Contract

Python ecosystem build rules require:

- `uv` for repo-level Python commands
- a local CPython interpreter with `Python.h`
- `VELARIA_PYTHON_BIN` when Bazel cannot discover a usable interpreter automatically

Repository entrypoints:

- `./scripts/run_core_regression.sh`
- `./scripts/run_python_ecosystem_regression.sh`
- `./scripts/run_experimental_regression.sh`
- `./scripts/run_python_ci_checks.sh`

## Regression Anchors

The following targets anchor this contract:

- core
  - `//:sql_regression_test`
  - `//:file_source_test`
  - `//:file_source_probe_test`
  - `//:stream_runtime_test`
  - `//:source_sink_abi_test`
  - `//:stream_strategy_explain_test`
  - `//:vector_runtime_test`
- python ecosystem
  - `//python:streaming_v05_test`
  - `//python:arrow_stream_ingestion_test`
  - `//python:file_source_api_test`
  - `//python:python_cli_contract_test`
  - `//python:workspace_runs_test`
  - `//python:vector_search_test`
  - `//python:read_excel_test`
  - `//python:custom_stream_source_test`
  - `//python:bitable_stream_source_test`
  - `//python:bitable_group_by_owner_integration_test`
