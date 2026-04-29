# Benchmarks

This document tracks reproducible benchmark entrypoints and recent performance snapshots for the current development branch.

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
bazel run //:batch_aggregate_benchmark
bazel build //:tpch_q1_style_benchmark
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single string-keys q18
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 single numeric-keys q1
./bazel-bin/tpch_q1_style_benchmark 500 4096 4 4 0 0 all string-keys q6
```

String builtin benchmark:

```bash
bazel run //:string_builtin_benchmark
```

File-input benchmark:

```bash
bazel run //:file_source_benchmark -- 200000 3
perf record --call-graph=dwarf bazel-bin/file_source_benchmark -- 200000 3
perf report
```

The file-input benchmark emits JSON rows for:

- CSV hardcode / explicit / auto-probed paths
- CSV scan-only / full-columnar / full-row-materialize / projected / filter-pushdown / aggregate-pushdown sub-cases
- line split / regex explicit paths plus direct filter-pushdown / aggregate-pushdown cases
- JSON lines explicit / auto-probed paths plus direct filter-pushdown / aggregate-pushdown cases
- SQL file-registration and CSV / line / JSON predicate-pushdown paths

Current file-source optimizer/executor layering:

- executor lowering classifies source pushdown into `ConjunctiveFilterOnly`, `SingleKeyCount`, `SingleKeyNumericAggregate`, or `Generic`
- file sources use those shapes to select lighter fast paths where semantics allow
- current fast paths are most effective for line split, line regex, JSON selected-field pushdown, and simple CSV single-key aggregate cases

## Latest Local Regression Snapshot

- Snapshot date: April 29, 2026
- Branch under test: `auto/sql-optimization-a`
- Run mode: local serial Bazel benchmarks on the developer machine
- Measurement shape: average of `3` outer serial benchmark runs unless a table states a different rerun count; each benchmark keeps its own internal `rounds`
- Scope: native execution benchmarks only; Python API overhead, packaged CLI startup, and agent runtime startup are excluded
- This snapshot includes the columnar hot-path tuning that keeps full cache validation out of repeated execution loops and avoids per-row string copies in packed aggregate keys.
- The batch aggregate table was rerun with `5` outer serial runs after removing benchmark-specific sorted-key probing; it uses only optimizer-selected dense, packed-hash, fixed-hash, and sort-streaming execution paths.
- Repeated legacy SQL text now reuses a bounded parse cache after `kBeforeSqlParse`; plugin hooks after parse and plan build still run for each call.

String builtin snapshot, `100,000` rows, `5` internal rounds:

| Case | Avg time | Rows/s |
|---|---:|---:|
| `copy-column` | `99,416 us` | `1,005,872` |
| `single-arg-functions` | `226,192 us` | `442,103` |
| `multi-arg-functions` | `329,301 us` | `303,674` |
| `dependent-chain` | `489,359 us` | `204,349` |
| `sql-plan-and-execute` | `549,990 us` | `181,821` |
| `sql-reused-plan` | `171,188 us` | `584,153` |

Batch aggregate snapshot, `1,048,576` rows, `5` outer runs:

| Scenario | Selected impl | Elapsed | Rows/s | Output groups |
|---|---|---:|---:|---:|
| `single-int64-low-domain` | `dense` | `57.4 ms` | `18,267,875` | `1,024` |
| `single-int64-high-domain` | `hash-fixed` | `561.6 ms` | `1,867,123` | `1,048,576` |
| `double-int64` | `hash-packed` | `201.4 ms` | `5,206,435` | `4,096` |
| `mixed-string-int64` | `hash-packed` | `335.0 ms` | `3,130,078` | `16,384` |
| `mixed-string-int64-nullable` | `hash-packed` | `324.0 ms` | `3,236,346` | `17,921` |
| `int64-two-string` | `hash-packed` | `500.4 ms` | `2,095,476` | `32,768` |
| `int64-string-bool` | `hash-packed` | `427.4 ms` | `2,453,383` | `32,768` |
| `ordered-string` | `sort-streaming` | `311.4 ms` | `3,367,296` | `32,768` |

Stream runtime snapshot:

| Case | Elapsed | Rows/s | Last batch latency |
|---|---:|---:|---:|
| `stateless-single` | `1.5 s` | `170,694` | `45.0 ms` |
| `stateless-local-workers` | `1.9 s` | `138,088` | `55.3 ms` |
| `stateful-single` | `2.9 s` | `91,697` | `91.7 ms` |
| `stateful-local-workers` | `5.4 s` | `48,460` | `167.7 ms` |
| `stateful-single-sliding` | `5.5 s` | `47,496` | `170.3 ms` |
| `stateful-local-workers-sliding` | `9.9 s` | `26,394` | `317.0 ms` |

Stream actor runtime snapshot:

| Case | Elapsed | Rows/s | Result rows |
|---|---:|---:|---:|
| `single-process` | `157.7 ms` | `835,750` | `1,280` |
| `actor-credit` | `247.3 ms` | `530,352` | `1,280` |
| `auto-selected` | `110.7 ms` | `1,186,292` | `1,280` |

File-source benchmark snapshot, `200,000` rows, `3` internal rounds:

| Case | Best time | Result rows |
|---|---:|---:|
| `probe_csv` | `95 us` | `3` |
| `probe_line` | `96 us` | `3` |
| `probe_json_lines` | `97 us` | `3` |
| `read_csv_hardcode_group_sum` | `57,103 us` | `16` |
| `read_csv_explicit_group_sum` | `149,302 us` | `16` |
| `read_csv_auto_group_sum` | `151,156 us` | `16` |
| `read_csv_scan_only` | `63,231 us` | `200,000` |
| `read_csv_full_columnar_only` | `184,786 us` | `200,000` |
| `read_csv_full_materialize_rows` | `336,807 us` | `200,000` |
| `read_csv_projected_group_sum` | `148,139 us` | `16` |
| `read_csv_filter_only` | `260,682 us` | `99,800` |
| `read_csv_aggregate_pushdown` | `179,868 us` | `16` |
| `read_line_hardcode_group_sum` | `54,728 us` | `16` |
| `read_line_explicit_group_sum` | `286,047 us` | `16` |
| `read_line_auto_group_sum` | `285,538 us` | `16` |
| `read_line_filter_only` | `292,154 us` | `99,800` |
| `read_line_aggregate_pushdown` | `293,716 us` | `16` |
| `read_line_regex_parse` | `111 us` | `1` |
| `read_line_regex_hardcode_group_sum` | `244,894 us` | `16` |
| `read_line_regex_explicit_group_sum` | `783,038 us` | `16` |
| `read_json_hardcode_group_sum` | `165,309 us` | `16` |
| `read_json_explicit_group_sum` | `641,127 us` | `16` |
| `read_json_auto_group_sum` | `635,095 us` | `16` |
| `read_json_filter_only` | `635,698 us` | `99,800` |
| `read_json_aggregate_pushdown` | `638,904 us` | `16` |
| `sql_create_table_probe_only_json` | `1,808,254 us` | `1` |
| `sql_create_table_explicit_json` | `1,766,208 us` | `1` |
| `sql_csv_predicate_and_group_count` | `131,533 us` | `1` |
| `sql_csv_predicate_or_group_count` | `180,255 us` | `2` |
| `sql_csv_predicate_mixed_group_count` | `294,823 us` | `2` |
| `sql_line_predicate_or_group_count` | `208,696 us` | `2` |
| `sql_json_predicate_or_group_count` | `505,876 us` | `2` |

File-source SQL predicate pushdown snapshot, `200,000` rows, `3` internal rounds:

| Case | No pushdown | Pushdown | Ratio |
|---|---:|---:|---:|
| `sql_csv_predicate_and_group_count` | `760,868 us` | `131,533 us` | `0.173` |
| `sql_csv_predicate_or_group_count` | `736,269 us` | `180,255 us` | `0.245` |
| `sql_csv_predicate_mixed_group_count` | `791,505 us` | `294,823 us` | `0.372` |
| `sql_line_predicate_or_group_count` | `1,110,699 us` | `208,696 us` | `0.188` |
| `sql_json_predicate_or_group_count` | `1,641,476 us` | `505,876 us` | `0.308` |

Baseline comparison against the April 26, 2026 local snapshot:

| Area | Case | April 26 baseline | Current | Delta |
|---|---|---:|---:|---:|
| batch aggregate | `single-int64-low-domain` | `64.0 ms` | `57.4 ms` | `-10.3%` |
| batch aggregate | `single-int64-high-domain` | `551.0 ms` | `561.6 ms` | `+1.9%` |
| batch aggregate | `double-int64` | `304.0 ms` | `201.4 ms` | `-33.8%` |
| batch aggregate | `mixed-string-int64` | `499.0 ms` | `335.0 ms` | `-32.9%` |
| batch aggregate | `mixed-string-int64-nullable` | `452.0 ms` | `324.0 ms` | `-28.3%` |
| batch aggregate | `int64-two-string` | `690.0 ms` | `500.4 ms` | `-27.5%` |
| batch aggregate | `int64-string-bool` | `584.0 ms` | `427.4 ms` | `-26.8%` |
| batch aggregate | `ordered-string` | `337.0 ms` | `311.4 ms` | `-7.6%` |
| string builtin | `dependent-chain` | `488,326 us` | `489,359 us` | `+0.2%` |
| string builtin | `sql-plan-and-execute` | `520,191 us` | `549,990 us` | `+5.7%` |
| string builtin | `sql-reused-plan` | `170,364 us` | `171,188 us` | `+0.5%` |
| file-source SQL pushdown | `sql_csv_predicate_and_group_count` | `103,135 us` | `131,533 us` | `+27.5%` |
| file-source SQL pushdown | `sql_csv_predicate_or_group_count` | `149,105 us` | `180,255 us` | `+20.9%` |
| file-source SQL pushdown | `sql_csv_predicate_mixed_group_count` | `255,453 us` | `294,823 us` | `+15.4%` |
| file-source SQL pushdown | `sql_line_predicate_or_group_count` | `170,632 us` | `208,696 us` | `+22.3%` |
| file-source SQL pushdown | `sql_json_predicate_or_group_count` | `420,300 us` | `505,876 us` | `+20.4%` |

Negative delta means faster than the baseline. The remaining regressions are concentrated in repeated SQL planning and file-source SQL pushdown absolute time; pushdown ratios still preserve the expected reduction versus no-pushdown execution.

Interpret this snapshot as a local regression guardrail. It is useful for catching order-of-magnitude regressions and preserving the expected pushdown shape, but it is not a release-grade cross-machine benchmark.

The remaining sections preserve the older April 6, 2026 `simd` measurements for historical comparison.

## Historical File-Source Comparison

Current clean-`main` comparison snapshot for representative file-source cases:

| Case | clean `main` | current | delta |
|---|---:|---:|---:|
| `read_line_regex_explicit_group_sum` | `5679936 us` | `641735 us` | `-88.7%` |
| `sql_csv_predicate_and_group_count` | `133011 us` | `109146 us` | `-17.9%` |
| `sql_csv_predicate_or_group_count` | `307313 us` | `171556 us` | `-44.2%` |
| `sql_csv_predicate_mixed_group_count` | `462000 us` | `275583 us` | `-40.3%` |
| `sql_line_predicate_or_group_count` | `314852 us` | `174627 us` | `-44.5%` |
| `sql_json_predicate_or_group_count` | `604404 us` | `420423 us` | `-30.4%` |

## Historical Measurement Notes

- Snapshot date: April 6, 2026
- Branch under test: `simd`
- Batch rows: `500 * 4096 = 2,048,000`
- `main` baselines were measured with the same benchmark harness in a separate `main` worktree
- Benchmarks were run serially to avoid cross-process CPU contention
- Latest follow-up numbers below include the actor execution optimizer split, C++20 hot-path updates (`std::span` / `<bit>`), enum-bound filter comparisons, and the numeric window-key-sum fast path
- For rerun-heavy batch cases, the table uses the median of three serial runs

Interpretation boundaries:

- `file_source_benchmark` is a native C++ benchmark. It does not include Python API overhead or packaged CLI startup.
- `python/benchmarks/bench_stage_paths.py` is a separate Python API harness. In that harness, `Session.sql(...)` is planning-only, and the first `to_arrow()` call includes execution plus Arrow export.
- The `hardcode` row in the Python stage benchmark is a scenario-specific Python stdlib baseline built with `csv.DictReader`; it is not a native-kernel upper bound.
- Packaged `./dist/velaria-cli` startup is measured separately again. The current single-file CLI is built with PyInstaller `--onefile`, so cold start includes bootstrap overhead outside the native engine.

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
