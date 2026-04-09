# Benchmarks

This document tracks reproducible benchmark entrypoints and recent performance snapshots for the current `simd` branch.

## Repro Commands

Vector benchmark:

```bash
./scripts/run_vector_search_benchmark.sh
```

The wrapper now validates both:

- `vector-query` exact-scan baselines
- `hybrid-search` filtered-candidate baselines with `none` / `medium` / `high` selectivity

Batch benchmark:

```bash
bazel build //:tpch_q1_style_benchmark
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single string-keys q18
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single numeric-keys q1
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 all string-keys q6
```

## Measurement Notes

- Snapshot date: April 6, 2026
- Branch under test: `simd`
- Batch rows: `500 * 4096 = 2,048,000`
- `main` baselines were measured with the same benchmark harness in a separate `main` worktree
- Benchmarks were run serially to avoid cross-process CPU contention
- Latest follow-up numbers below include the actor execution optimizer split, C++20 hot-path updates (`std::span` / `<bit>`), enum-bound filter comparisons, and the numeric window-key-sum fast path
- For rerun-heavy batch cases, the table uses the median of three serial runs

## TPCH-Like Batch Suite

Current `simd` snapshot:

| Benchmark | Rows | Mode | Elapsed | Rows/s | Notes |
|---|---:|---|---:|---:|---|
| `q1` (`string-keys`) | 2,048,000 | `single-process` | `2907 ms` | `704,506` | median of 3 reruns, `result_rows=6` |
| `q1` (`string-keys`) | 2,048,000 | `actor-credit` | `2786 ms` | `735,104` | median of 3 reruns, `coord_serialize_ms≈2.0s` |
| `q1` (`string-keys`) | 2,048,000 | `auto-selected` | `1334 ms` | `1,535,230` | median of 3 reruns, chose `single-process` |
| `q1` (`numeric-keys`) | 2,048,000 | `single-process` | `3228 ms` | `634,449` | median of 3 reruns, `result_rows=32,896` |
| `q1` (`numeric-keys`) | 2,048,000 | `actor-credit` | `5405 ms` | `378,908` | median of 3 reruns, actor transport still dominates |
| `q1` (`numeric-keys`) | 2,048,000 | `auto-selected` | `1496 ms` | `1,368,980` | median of 3 reruns, chose `single-process` |
| `q6-like-scan-filter-sum` | 2,048,000 | `batch` | `747 ms` | `2,741,630` | median of 3 reruns, `result_rows=1` |
| `q3-like-join-group-order` | 2,304,000 | `batch` | `5346 ms` | `430,976` | previous snapshot, not rerun in this follow-up |
| `q18-like-high-card-group-order` | 2,048,000 | `batch` | `1050 ms` | `1,950,480` | median of 3 reruns, `result_rows=100` |

Measured `main` comparison snapshot:

| Benchmark | Rows | `simd` | `main` baseline | Delta |
|---|---:|---:|---:|---:|
| `q18-like-high-card-group-order` | 2,048,000 | `1050 ms` | `2421 ms` | `2.31x` faster |
| `q1` single-process (`numeric-keys`) | 2,048,000 | `3228 ms` | `7936 ms` | `2.46x` faster |
| `q6-like-scan-filter-sum` | 2,048,000 | `747 ms` | `5168 ms` | `6.92x` faster |

## String Builtins

Current `simd` snapshot, `100,000` rows, `5` rounds:

| Case | Avg time | Rows/s |
|---|---:|---:|
| `copy-column` | `237,197 us` | `421,591` |
| `single-arg-functions` | `524,354 us` | `190,711` |
| `multi-arg-functions` | `816,910 us` | `122,412` |
| `dependent-chain` | `1,234,700 us` | `80,992` |
| `sql-plan-and-execute` | `1,485,770 us` | `67,305` |
| `sql-reused-plan` | `374,353 us` | `267,127` |

## Vector Exact Scan

Current `simd` snapshot:

| Rows | Dim | Metric | Warm query avg | Cold query |
|---|---:|---|---:|---:|
| `10,000` | `128` | `cosine` | `2,938 us` | `57,800 us` |
| `10,000` | `128` | `dot` | `2,728 us` | `55,215 us` |
| `10,000` | `128` | `l2` | `2,722 us` | `54,046 us` |
| `10,000` | `768` | `cosine` | `19,527 us` | `336,414 us` |
| `10,000` | `768` | `dot` | `14,533 us` | `253,381 us` |
| `10,000` | `768` | `l2` | `14,673 us` | `235,871 us` |
| `100,000` | `128` | `cosine` | `28,136 us` | `564,258 us` |
| `100,000` | `128` | `dot` | `26,256 us` | `544,686 us` |
| `100,000` | `128` | `l2` | `27,889 us` | `545,915 us` |
| `100,000` | `768` | `cosine` | `143,918 us` | `2,504,985 us` |
| `100,000` | `768` | `dot` | `141,099 us` | `2,410,975 us` |
| `100,000` | `768` | `l2` | `142,327 us` | `2,400,458 us` |

## Vector + Column Hybrid Query

Current benchmark harness also emits `hybrid-search` JSON rows for:

- no filter / near-full candidate set
- medium selectivity filter
- high selectivity filter

Use the same repro entrypoint above. `--quick` remains the repository verification preset.

Current `--quick` snapshot (`10,000` rows, `128` dim, `top_k=10`):

| Metric | Filter case | Selectivity | Candidate rows | Warm query avg | Cold query | Warm explain avg |
|---|---|---:|---:|---:|---:|---:|
| `cosine` | `none` | `1.00` | `10,000` | `3,346 us` | `38,855 us` | `3,100 us` |
| `cosine` | `medium` | `0.20` | `2,000` | `676 us` | `9,113 us` | `586 us` |
| `cosine` | `high` | `0.01` | `100` | `98 us` | `956 us` | `43 us` |
| `dot` | `none` | `1.00` | `10,000` | `2,979 us` | `37,938 us` | `2,734 us` |
| `dot` | `medium` | `0.20` | `2,000` | `637 us` | `9,224 us` | `568 us` |
| `dot` | `high` | `0.01` | `100` | `116 us` | `1,065 us` | `47 us` |
| `l2` | `none` | `1.00` | `10,000` | `2,932 us` | `38,922 us` | `2,844 us` |
| `l2` | `medium` | `0.20` | `2,000` | `670 us` | `12,136 us` | `610 us` |
| `l2` | `high` | `0.01` | `100` | `189 us` | `1,184 us` | `45 us` |

This quick snapshot shows the expected shape:

- `none` stays near the pure vector-query baseline
- `medium` reduces warm query latency to roughly one quarter of the full-candidate scan
- `high` pushes warm query into sub-`0.2 ms` territory
- `explainHybridSearch(...)` currently runs the search path to report `returned_rows`, so explain cost scales with candidate count

## Vector Transport

Current `simd` snapshot:

| Rows | Dim | Codec | Serialize | Deserialize | Payload bytes |
|---|---:|---|---:|---:|---:|
| `10,000` | `128` | `proto` | `208,568 us` | `719,269 us` | `14,295,898` |
| `10,000` | `128` | `binary` | `50,719 us` | `30,375 us` | `5,171,773` |
| `10,000` | `128` | `arrow-ipc` | `110,836 us` | `2,978 us` | `5,200,512` |
| `10,000` | `768` | `proto` | `1,145,527 us` | `4,232,640 us` | `84,681,100` |
| `10,000` | `768` | `binary` | `276,843 us` | `131,815 us` | `30,771,773` |
| `10,000` | `768` | `arrow-ipc` | `575,440 us` | `38,952 us` | `30,800,512` |
| `100,000` | `128` | `proto` | `2,103,876 us` | `7,087,232 us` | `143,058,371` |
| `100,000` | `128` | `binary` | `492,848 us` | `304,959 us` | `51,791,773` |
| `100,000` | `128` | `arrow-ipc` | `1,114,975 us` | `38,956 us` | `52,000,512` |
| `100,000` | `768` | `proto` | `11,880,117 us` | `42,873,547 us` | `846,907,112` |
| `100,000` | `768` | `binary` | `2,886,000 us` | `1,320,479 us` | `307,791,773` |
| `100,000` | `768` | `arrow-ipc` | `5,841,326 us` | `244,186 us` | `308,000,512` |

## What These Results Reflect

These numbers reflect the current column-first runtime work, including:

- execution optimizer lowering
- selection-vector filtering
- zero-copy Arrow prefix slicing
- aggregate fast-path routing through optimizer-selected shapes
