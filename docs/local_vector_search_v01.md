# Local Vector Search v0.1 (Velaria)

This document describes the minimal local vector search design. For the stable runtime-facing explain and ecosystem contract, see [runtime-contract.md](./runtime-contract.md). For repository positioning, see [core-boundary.md](./core-boundary.md).

## Scope

This document defines a minimal local-first vector search path for Velaria.

### Goals

- Fixed-dimension `float32` vector column support.
- Exact scan backend only.
- Metrics: `cosine`, `dot`, `l2`.
- `top-k` query support.
- C++ API via `DataFrame` / `DataflowSession`.
- Python front-end API for invoking vector search.
- Explain text that mirrors actual runtime behavior.
- Keep ingestion/query path zero-copy-oriented where possible.

### Non-goals (v0.1)

- No ANN index (HNSW/IVF/PQ).
- No distributed vector execution.
- No standalone vector database subsystem.
- No ANN-oriented SQL grammar or standalone vector database SQL surface in this phase.

## Minimal abstractions

- `Value::DataType::FixedVector` stores fixed-dimension float vectors.
- `VectorIndex` runtime interface with an `ExactScanVectorIndex` implementation.
- `ExactScanVectorIndex` uses flat contiguous buffers and heap top-k selection for scan acceleration.
- Internal vector transport codecs use raw float bit payloads to avoid text precision loss.
- `VectorSearchMetric`: cosine/dot/l2.
- `VectorSearchResult`: `{row_id, score}`.

## Public API draft

### C++

- `DataFrame::vectorQuery(vector_column, query_vector, top_k, metric)`
- `DataFrame::explainVectorQuery(vector_column, query_vector, top_k, metric)`
- `DataFrame::hybridSearch(vector_column, query_vector, options)`
- `DataFrame::explainHybridSearch(vector_column, query_vector, options)`
- `DataflowSession::vectorQuery(table, vector_column, query_vector, top_k, metric)`
- `DataflowSession::explainVectorQuery(table, vector_column, query_vector, top_k, metric)`
- `DataflowSession::hybridSearch(table, vector_column, query_vector, options)`
- `DataflowSession::explainHybridSearch(table, vector_column, query_vector, options)`

### Python

- `Session.vector_search(table, vector_column, query_vector, top_k=10, metric="cosine")`
- `Session.explain_vector_search(table, vector_column, query_vector, top_k=10, metric="cosine")`
- `Session.hybrid_search(table, vector_column, query_vector, top_k=10, metric="cosine", score_threshold=None)`
- `Session.explain_hybrid_search(table, vector_column, query_vector, top_k=10, metric="cosine", score_threshold=None)`

### SQL

Batch SQL supports a minimal clause:

```sql
SELECT id, bucket, vector_score
FROM input_table
WHERE bucket = 1
HYBRID SEARCH embedding QUERY '[1 0 0]' METRIC cosine TOP_K 10 SCORE_THRESHOLD 0.02
```

Current boundary:

- batch only
- single-table only
- no `JOIN`
- no aggregate query

### Ingestion shapes

- preferred Arrow shape: `FixedSizeList<float32>`
- supported Python Arrow entrypoints: `Table`, `RecordBatch`, `RecordBatchReader`, and `__arrow_c_stream__`
- supported local CSV text shape: bracketed vectors such as `[1 2 3]` or `[1,2,3]`
- current CSV parser is still minimal; whitespace-separated bracketed vectors are the safest local format

## Explain fields

Current explain output contains:

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+simd-topk`
- `backend=<simd-backend>`

Hybrid explain output adds:

- `mode=exact-scan-hybrid-search`
- `score_threshold=<value|none>`
- `score_threshold_compare=(>=|<=)`
- `input_rows=<N>`
- `returned_rows=<K>`
- `column_filter_stage=before-vector`
- `column_filter_execution=<source-pushdown|post-load-filter|none>`

## Test matrix

- Vector value roundtrip in proto-like serializer.
- Vector value roundtrip in binary row batch codec.
- Runtime query correctness for cosine/l2/dot top-k.
- Hybrid query correctness for:
  - filtered input row reconstruction
  - score-threshold semantics
  - `vector_score` output column
  - single-source restriction
  - explain filter execution reporting
- Dimension mismatch rejection.
- Python API shape and argument validation.
- Arrow `FixedSizeList<float32>` ingestion fast path coverage.
- CSV bracketed vector ingestion coverage.

## Benchmark baseline

Repository entrypoints:

- C++ vector benchmark:
  - `bazel run //:vector_search_benchmark`
- stable benchmark wrapper:
  - `./scripts/run_vector_search_benchmark.sh`

The script uses the benchmark binary's `--quick` preset so repository verification stays lightweight. Use the raw Bazel target for the full baseline sweep.

The benchmark baseline is still intentionally narrow:

- local exact-scan only
- query metrics: `cosine`, `dot`, `l2`
- query comparison cases:
  - pure vector query
  - hybrid query on filtered candidates with `none` / `medium` / `high` selectivity
- transport roundtrip coverage for proto-like, binary row batch, and actor-rpc control payloads
- no ANN comparisons
- no distributed claims
