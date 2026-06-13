# Columnar-first Kernel Performance Delivery Report

## Summary

Implemented the first columnar-first aggregate proof points: dense single-`INT64` group-key numeric `SUM`, `COUNT`, and `AVG`, plus two-`INT64` group-key numeric `SUM`, `COUNT`, and `AVG`, now use typed reducer state and report `state-columnar` partial layout instead of remaining on the generic accumulator shapes. Also extended typed source pushdown shape selection to single-key predicate aggregate `COUNT` and numeric `SUM` / `AVG`, with CSV predicate aggregate execution using the typed reducer path.

## Changes

- `execution_optimizer.cc`: dense single-int64 SUM/COUNT/AVG and two-int64 SUM/COUNT/AVG now select typed runtime shapes and `state-columnar` partial layout.
- `execution_optimizer.h`: exposes source pushdown shape classification for focused tests and shared planner/runtime use.
- `executor.cc`: added direct dense typed sum/count/avg slots, typed single-key hash fallback, typed two-key sum/count/avg reducers, and now delegates source pushdown shape classification to the optimizer.
- `executor.cc`: dense int64 domain/slot math now uses ordered unsigned int64 keys to avoid signed overflow when a forced dense path sees extreme int64 values.
- `csv.cc`: single-key predicate aggregate COUNT and numeric SUM/AVG can use the typed source pushdown reducer path while evaluating `predicate_expr`.
- `batch_aggregate_benchmark.cc`: added `single-int64-low-domain-count`, `single-int64-low-domain-avg`, `double-int64-count`, and `double-int64-avg`.
- `planner_v03_test.cc`: added failing-first coverage for optimizer selection, partial layout, aggregate result semantics, columnar cache validation, and source pushdown predicate aggregate shape classification.
- `file_source_test.cc`: added CSV predicate aggregate COUNT/SUM coverage using typed source pushdown shape selection.
- `core-runtime-columnar-plan.md`: updated the current status board for typed state-columnar aggregate paths.
- `plans/columnar-first-kernel-design.md`: recorded the implementation result and measured proof point.

## Verification

- `bazel test //:planner_v03_test --test_output=errors`: failed before SUM implementation, before COUNT implementation, before StateColumnar/AVG implementation, before two-int64 SUM optimizer selection, before two-int64 COUNT implementation, and before two-int64 AVG implementation; passed after each implementation.
- `bazel test //:core_regression`: passed.
- `bazel test //:experimental_regression --test_output=errors`: passed after the source-pushdown change.
- `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- `bazel run //:actor_rpc_smoke`: passed.
- Final `git diff --check`: passed.
- `scripts/validate-delivery-run.sh`: not present in this repository, so the delivery package validator is unavailable.
- Final refined targeted secret-pattern scan over changed files: no matches, including after source predicate aggregate changes.
- `bazel run //:batch_aggregate_benchmark -- 1048576 5`: single-int64 SUM/COUNT/AVG and two-int64 SUM/COUNT/AVG baselines/current branch measured.
- Final post-rebase `bazel run //:batch_aggregate_benchmark -- 1048576 5`: passed; target typed shapes still selected (`single-int64` SUM/COUNT/AVG at `39/29/39 ms`, two-int64 SUM/COUNT/AVG at `112/103/117 ms`).
- Final post-rebase `bazel run //:string_builtin_benchmark -- 100000 5`: passed; `sql-reused-plan` averaged `130,845 us`.
- `bazel run //:file_source_benchmark -- 200000 3`: source predicate aggregate before/current branch measured.
- Final post-rebase `bazel run //:file_source_benchmark -- 200000 3`: passed; CSV predicate aggregate ratios remained below `0.40`.
- A mixed string/int64 state-only attempt and hash-packed reserve-cap attempt were measured, rejected, and removed from the production diff before final validation.
- After PR CI exposed an already-merged `float_simd_benchmark` duplicate on the GitHub merge ref, the branch was rebased onto `origin/main`; post-rebase CI-equivalent local validation passed:
  `bazel build //:sql_demo //:stream_demo //:velaria_pyext && bazel test //:core_regression //:experimental_regression --test_output=errors`, plus the Python wrapper leak smoke with one stress/leak iteration.

## PR and CI Status

- PR: https://github.com/ashione/Velaria/pull/57
- Head branch: `auto/columnar-first-kernel-performance`
- Base branch: `main`
- Initial CI run `27472635527` failed before compilation because the GitHub merge ref contained duplicate `float_simd_benchmark` Bazel rules from already-merged base work plus the branch's older copy.
- Branch was rebased onto `origin/main` to remove the duplicate from the PR merge result.
- Local CI-equivalent status after rebase:
  - `native-and-python` equivalent: passed locally.
  - `python-wrapper-leak-smoke` equivalent: passed locally.
  - `wheel-macos`: skipping
  - `wheel-manylinux`: skipping
- The rebased branch was force-pushed after local validation; fresh GitHub Actions should be read from the latest PR checks because any report-only amend creates a new commit SHA.

## Performance Result

Target scenario: `single-int64-low-domain`.

| Version | Runtime shape | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---:|---:|---:|
| baseline `d02b11c` | `generic-single-int64-key` | `51 ms`, `53 ms` | `52.0 ms` | `20.17M` |
| current branch | `sum-single-int64-key` | `39 ms`, `38 ms` | `38.5 ms` | `27.24M` |

Result: about `1.35x` speedup for the targeted dense int64 SUM scenario.

Target scenario: `single-int64-low-domain-count`.

| Version | Runtime shape | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `41 ms`, `41 ms` | `41.0 ms` | `25.58M` |
| current branch | `count-single-int64-key` | `28 ms`, `28 ms` | `28.0 ms` | `37.45M` |

Result: about `1.46x` speedup for the targeted dense int64 COUNT scenario.

Target scenario: `single-int64-low-domain-avg`.

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `generic-table` | `55 ms`, `52 ms` | `53.5 ms` | `19.61M` |
| current branch | `avg-single-int64-key` | `state-columnar` | `38 ms`, `38 ms` | `38.0 ms` | `27.59M` |

Result: about `1.41x` speedup for the targeted dense int64 AVG scenario.

Target scenario: `double-int64`.

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| pre-change current branch | `generic-packed-keys-2` | `key-columnar` | `163 ms` | `163.0 ms` | `6.43M` |
| current branch | `sum-double-int64-key` | `state-columnar` | `122 ms`, `121 ms` | `121.5 ms` | `8.63M` |

Result: about `1.34x` speedup for the targeted two-int64 SUM scenario.

Target scenario: `double-int64-count`.

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| pre-change current branch + benchmark scenario | `generic-packed-keys-2` | `key-columnar` | `159 ms` | `159.0 ms` | `6.59M` |
| current branch | `count-double-int64-key` | `state-columnar` | `107 ms`, `109 ms` | `108.0 ms` | `9.71M` |

Result: about `1.47x` speedup for the targeted two-int64 COUNT scenario.

Target scenario: `double-int64-avg`.

| Version | Runtime shape | Partial layout | Best elapsed runs | Avg best elapsed | Avg rows/s |
|---|---|---|---:|---:|---:|
| pre-change current branch + benchmark scenario | `generic-packed-keys-2` | `key-columnar` | `166 ms` | `166.0 ms` | `6.32M` |
| current branch | `avg-double-int64-key` | `state-columnar` | `122 ms`, `123 ms` | `122.5 ms` | `8.56M` |

Result: about `1.36x` speedup for the targeted two-int64 AVG scenario.

Source predicate aggregate target scenarios:

| Scenario | Pre-change best | Current best | Current pushdown ratio | Result |
|---|---:|---:|---:|---:|
| `sql_csv_predicate_and_group_count` | `109,077 us` | `100,439 us` | `0.159` | `1.09x` |
| `sql_csv_predicate_or_group_count` | `156,629 us` | `140,559 us` | `0.229` | `1.11x` |
| `sql_csv_predicate_mixed_group_count` | `267,744 us` | `234,300 us` | `0.353` | `1.14x` |

The line/json predicate aggregate cases remained within the planned gate; line improved from `177,848 us` to `167,840 us`, and JSON improved from `452,206 us` to `410,307 us`.

## Risks and Follow-up

- Scope is intentionally narrow; this does not prove broad operator migration yet.
- State-columnar is now explicit for single-int64 SUM/COUNT/AVG and two-int64 SUM/COUNT/AVG, but string/mixed-key aggregate state still need the same treatment.
- A mixed string/int64 state-only reducer attempt was measured and rejected because it did not produce reliable speedup and a bucket-reserve cap regressed the benchmark; this path was removed from production diff.
- Source pushdown typed reducer is now started for single-key predicate aggregates, but multi-key/multi-aggregate source reducers and `ColumnarExecBatch`/`ColumnarExecView` remain the next generalization steps.
- A per-scenario benchmark filter would make future performance gates less noisy.

## Gate Ledger

- Requirements/design/pre-change validation/implementation/post-change validation passed.
- Final whitespace and secrets checks passed.

## Review Ledger

- Public contract unchanged.
- Regression coverage added and passed.
- Performance evidence is paired against a detached baseline worktree at `d02b11c`.
- Local diff review found and fixed a dense int64 overflow edge case before PR preparation.
