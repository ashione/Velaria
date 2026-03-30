# Local Vector Search v0.1 (Velaria)

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
- No new SQL grammar for vector search in this phase.

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
- `DataflowSession::vectorQuery(table, vector_column, query_vector, top_k, metric)`
- `DataflowSession::explainVectorQuery(table, vector_column, query_vector, top_k, metric)`

### Python

- `Session.vector_search(table, vector_column, query_vector, top_k=10, metric="cosine")`
- `Session.explain_vector_search(table, vector_column, query_vector, top_k=10, metric="cosine")`

## Explain fields

Current explain output contains:

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+heap-topk`

## Test matrix

- Vector value roundtrip in proto-like serializer.
- Vector value roundtrip in binary row batch codec.
- Runtime query correctness for cosine/l2/dot top-k.
- Dimension mismatch rejection.
- Python API shape and argument validation.
- Arrow `FixedSizeList<float32>` ingestion fast path coverage.
