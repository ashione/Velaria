# Columnar-first Kernel Performance Verification

## Environment

- Worktree: `/Users/wjf/.codex/worktrees/c1ea/cpp-dataflow-distributed-engine-research`
- Branch: `auto/columnar-first-kernel-performance`
- Start commit: `d02b11c`
- Rebased base: `origin/main` at `e6568c8` after PR CI exposed an already-merged `float_simd_benchmark` duplicate on the GitHub merge ref.
- Date: 2026-06-13

## Baseline Before Code Changes

- `bazel test //:core_regression`: passed, 11/11 cached.
- `bazel run //:batch_aggregate_benchmark -- 1048576 3`: passed.
- Additional baseline worktree: detached `d02b11c` at `/tmp/velaria-baseline-columnar`, removed after measurement.

Target baseline:

| Scenario | Selected impl | Partial layout | Runtime shape | Best elapsed | Rows/s |
|---|---|---|---|---:|---:|
| `single-int64-low-domain` | `dense` | `generic-table` | `generic-single-int64-key` | `51 ms` | `20.5603M` |

Paired baseline runs using `bazel run //:batch_aggregate_benchmark -- 1048576 5`:

| Run | Runtime shape | Best elapsed | Rows/s |
|---|---|---:|---:|
| baseline 1 | `generic-single-int64-key` | `51 ms` | `20.5603M` |
| baseline 2 | `generic-single-int64-key` | `53 ms` | `19.7845M` |

## Failing-first Test

- Added a focused assertion in `//:planner_v03_test` for compact `INT64` key + single numeric `SUM`.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected.
- Failure message: `dense single int64 sum should use typed sum runtime shape`.
- Added a second focused assertion for compact `INT64` key + `COUNT`.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected before COUNT implementation.
- Failure message: `dense single int64 count should use typed count runtime shape`.
- Added a third focused assertion for compact `INT64` key + `AVG`, and upgraded SUM/COUNT assertions to require `state-columnar` partial layout.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected before StateColumnar/AVG implementation.
- Failure message: `dense single int64 sum should expose state-columnar partial layout`.
- Added a fourth focused assertion for two `INT64` group keys + single numeric `SUM`, requiring the existing typed double-int64 sum reducer and `state-columnar` partial layout.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected before the two-key optimizer selection change.
- Failure message: `two int64 key sum should use typed double-int64 sum runtime shape`.
- Added focused assertions and benchmark scenarios for two `INT64` group keys + `COUNT` / `AVG`, requiring typed double-int64 reducers and `state-columnar` partial layout.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected before the two-key COUNT implementation.
- Failure message: `two int64 key count should use typed double-int64 count runtime shape`.
- After COUNT implementation, `bazel test //:planner_v03_test --test_output=errors` failed as expected before the two-key AVG implementation.
- Failure message: `two int64 key avg should use typed double-int64 avg runtime shape`.

COUNT-specific baseline:

- Temporary baseline worktree: detached `d02b11c` at `/tmp/velaria-count-baseline`, patched only with the benchmark COUNT scenario, removed after measurement.
- Command: `bazel run //:batch_aggregate_benchmark -- 1048576 5`.

| Run | Runtime shape | Best elapsed | Rows/s |
|---|---|---:|---:|
| count baseline 1 | `generic-single-int64-key` | `41 ms` | `25.5750M` |
| count baseline 2 | `generic-single-int64-key` | `41 ms` | `25.5750M` |

AVG-specific baseline:

- Temporary baseline worktree: detached `d02b11c` at `/tmp/velaria-avg-baseline`, patched only with the benchmark COUNT/AVG scenarios, removed after measurement.
- Command: `bazel run //:batch_aggregate_benchmark -- 1048576 5`.

| Run | Runtime shape | Partial layout | Best elapsed | Rows/s |
|---|---|---|---:|---:|
| avg baseline 1 | `generic-single-int64-key` | `generic-table` | `55 ms` | `19.0650M` |
| avg baseline 2 | `generic-single-int64-key` | `generic-table` | `52 ms` | `20.1649M` |

Two-key COUNT/AVG baseline after adding benchmark scenarios, before typed COUNT/AVG implementation:

- Command: `bazel run //:batch_aggregate_benchmark -- 1048576 5`.

| Scenario | Runtime shape | Partial layout | Best elapsed | Rows/s |
|---|---|---|---:|---:|
| `double-int64-count` | `generic-packed-keys-2` | `key-columnar` | `159 ms` | `6.5948M` |
| `double-int64-avg` | `generic-packed-keys-2` | `key-columnar` | `166 ms` | `6.3167M` |

## Post-change Validation

- `bazel test //:planner_v03_test --test_output=errors`: passed.
- `bazel test //:core_regression`: passed, 11/11.
- Final `bazel test //:core_regression`: passed, 11/11 cached after doc/include updates.
- Post-COUNT `bazel test //:planner_v03_test --test_output=errors`: passed.
- Post-COUNT `bazel test //:core_regression`: passed, 11/11.
- Post-StateColumnar/AVG `bazel test //:planner_v03_test --test_output=errors`: passed.
- Post-StateColumnar/AVG `bazel test //:core_regression`: passed, 11/11.
- Post-two-int64-SUM `bazel test //:planner_v03_test --test_output=errors`: passed.
- Post-two-int64-SUM `bazel test //:core_regression`: passed, 11/11.
- Post-two-int64-COUNT implementation `bazel test //:planner_v03_test --test_output=errors`: failed as expected on AVG shape selection.
- Post-two-int64-COUNT/AVG implementation `bazel test //:planner_v03_test --test_output=errors`: passed.
- Added focused source-pushdown assertions requiring single-key predicate `COUNT` and `SUM` pushdown specs to classify as typed source shapes.
- `bazel test //:planner_v03_test --test_output=errors`: failed as expected before moving source pushdown shape classification into `execution_optimizer`.
- Failure mode: ambiguous/private `classifySourcePushdownShape` implementation in `executor.cc`.
- Added CSV predicate aggregate pushdown coverage for typed single-key `COUNT` and `SUM`.
- Post-source-predicate-aggregate `bazel test //:planner_v03_test --test_output=errors`: passed.
- Post-source-predicate-aggregate `bazel test //:file_source_test --test_output=errors`: passed.
- Review-found dense domain overflow risk fixed by replacing signed `max - min` slot math with ordered unsigned int64 slot/domain helpers.
- Added `LLONG_MIN` / `LLONG_MAX` forced-dense fallback regression coverage in `planner_v03_test`.
- Post-overflow-fix `bazel test //:planner_v03_test --test_output=errors`: passed.
- Post-two-int64-COUNT/AVG `bazel test //:core_regression`: passed, 11/11.
- Final post-source-predicate-aggregate `bazel test //:core_regression --test_output=errors`: passed, 11/11.
- Final post-source-predicate-aggregate `bazel test //:experimental_regression --test_output=errors`: passed.
- Final `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- Final post-overflow-fix `bazel test //:core_regression --test_output=errors`: passed, 11/11.
- Final post-overflow-fix `bazel test //:experimental_regression --test_output=errors`: passed.
- Final post-overflow-fix `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final post-overflow-fix `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- Final post-COUNT `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final post-COUNT `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- Final post-StateColumnar/AVG `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final post-StateColumnar/AVG `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- Final post-two-int64-SUM/COUNT/AVG `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final post-two-int64-SUM/COUNT/AVG `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- Final post-rejected-mixed-key-attempt `bazel test //:core_regression --test_output=errors`: passed, 11/11 cached.
- Final post-rejected-mixed-key-attempt `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- Final post-rejected-mixed-key-attempt `bazel run //:actor_rpc_smoke`: passed with `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- `git diff --check`: passed with no output.
- Final post-source-predicate-aggregate `git diff --check`: passed with no output.
- Final post-overflow-fix `git diff --check`: passed with no output.
- `scripts/validate-delivery-run.sh`: unavailable in this repository.
- Phase 1 substrate follow-up added `ColumnarExecBatch`, `ColumnarExecColumn`, and `ColumnarExecView` under `src/dataflow/core/execution/columnar_exec.*`.
- Post-substrate `bazel test //:columnar_batch_test --test_output=errors`: passed.
- Post-substrate `bazel test //:planner_v03_test --test_output=errors`: passed after the second mixed-key dictionary-id reducer attempt was rejected and removed.
- Post-substrate small gate smoke passed:
  `VELARIA_COLUMNAR_GATE_BATCH_ROWS=65536 VELARIA_COLUMNAR_GATE_BATCH_ROUNDS=1 VELARIA_COLUMNAR_GATE_FILE_ROWS=20000 VELARIA_COLUMNAR_GATE_FILE_ROUNDS=1 VELARIA_COLUMNAR_GATE_STRING_ROWS=10000 VELARIA_COLUMNAR_GATE_STRING_ROUNDS=1 ./scripts/run_columnar_kernel_benchmark_gate.sh`.
- Post-substrate `ColumnarExecView` empty-selection fix added explicit `has_selection` state so an empty selected result is no longer confused with an unfiltered view.

## Performance Evidence

Post-change paired runs using `bazel run //:batch_aggregate_benchmark -- 1048576 5`:

| Run | Runtime shape | Best elapsed | Rows/s |
|---|---|---:|---:|
| current 1 | `sum-single-int64-key` | `39 ms` | `26.8866M` |
| current 2 | `sum-single-int64-key` | `38 ms` | `27.5941M` |

Target comparison:

| Version | Avg best elapsed | Avg rows/s | Speedup |
|---|---:|---:|---:|
| baseline `d02b11c` | `52.0 ms` | `20.1724M` | `1.00x` |
| current branch | `38.5 ms` | `27.2404M` | `1.35x` |

The speedup is local to `single-int64-low-domain`: dense single-`INT64` group key with one numeric `SUM`.

COUNT target comparison:

| Version | Runtime shape | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `41.0 ms` | `25.5750M` | `1.00x` |
| current branch | `count-single-int64-key` | `28.0 ms` | `37.4491M` | `1.46x` |

The COUNT speedup is local to `single-int64-low-domain-count`: dense single-`INT64` group key with `COUNT`.

AVG target comparison:

| Version | Runtime shape | Partial layout | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---|---:|---:|---:|
| baseline `d02b11c` + benchmark scenario | `generic-single-int64-key` | `generic-table` | `53.5 ms` | `19.6150M` | `1.00x` |
| current branch | `avg-single-int64-key` | `state-columnar` | `38.0 ms` | `27.5941M` | `1.41x` |

The AVG speedup is local to `single-int64-low-domain-avg`: dense single-`INT64` group key with numeric `AVG`.

Two-key SUM target comparison:

Pre-change current-branch baseline using `bazel run //:batch_aggregate_benchmark -- 1048576 5`:

| Run | Runtime shape | Partial layout | Best elapsed | Rows/s |
|---|---|---|---:|---:|
| two-key baseline 1 | `generic-packed-keys-2` | `key-columnar` | `163 ms` | `6.4330M` |

Post-change paired runs using the same command:

| Run | Runtime shape | Partial layout | Best elapsed | Rows/s |
|---|---|---|---:|---:|
| two-key current 1 | `sum-double-int64-key` | `state-columnar` | `122 ms` | `8.5949M` |
| two-key current 2 | `sum-double-int64-key` | `state-columnar` | `121 ms` | `8.6659M` |

| Version | Runtime shape | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---:|---:|---:|
| pre-change current branch | `generic-packed-keys-2` | `163.0 ms` | `6.4330M` | `1.00x` |
| current branch | `sum-double-int64-key` | `121.5 ms` | `8.6304M` | `1.34x` |

The two-key SUM speedup is local to `double-int64`: two `INT64` group keys with one numeric `SUM`.

Two-key COUNT target comparison:

| Version | Runtime shape | Partial layout | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---|---:|---:|---:|
| pre-change current branch + benchmark scenario | `generic-packed-keys-2` | `key-columnar` | `159.0 ms` | `6.5948M` | `1.00x` |
| current branch | `count-double-int64-key` | `state-columnar` | `108.0 ms` | `9.7099M` | `1.47x` |

Two-key AVG target comparison:

| Version | Runtime shape | Partial layout | Avg best elapsed | Avg rows/s | Speedup |
|---|---|---|---:|---:|---:|
| pre-change current branch + benchmark scenario | `generic-packed-keys-2` | `key-columnar` | `166.0 ms` | `6.3167M` | `1.00x` |
| current branch | `avg-double-int64-key` | `state-columnar` | `122.5 ms` | `8.5600M` | `1.36x` |

Rejected mixed-key attempt:

- Added a failing-first assertion requiring mixed string/`INT64` SUM to use a typed
  `sum-packed-keys-2` state-columnar path; the assertion failed as expected before implementation.
- Baseline before the attempt: `mixed-string-int64` was `304 ms` and
  `mixed-string-int64-nullable` was `296 ms`, both `generic-packed-keys-2` / `key-columnar`.
- Typed state-only attempt results: `315 ms` / `298 ms` for `mixed-string-int64`, and
  `329 ms` / `284 ms` for nullable.
- Additional hash-packed reserve-cap attempt worsened the target scenarios to `355 ms` and
  `348 ms` respectively.
- The production `sum-packed-keys-2` shape and reserve-cap heuristic were removed after measurement.
- Reverted benchmark check: `mixed-string-int64` returned to `generic-packed-keys-2` /
  `key-columnar` at `308 ms`; nullable returned to `generic-packed-keys-2` /
  `key-columnar` at `299 ms`.

Rejected mixed-key dictionary-id attempt:

- A second attempt changed the mixed string/`INT64` reducer path toward dictionary-id style
  typed state and added temporary planner expectations for `sum-dictionary-string-int64-key`
  and `count-dictionary-string-int64-key`.
- `bazel test //:planner_v03_test --test_output=errors` passed during the attempt, but the
  performance result was not acceptable.
- `bazel run //:batch_aggregate_benchmark -- 1048576 5` reported
  `mixed-string-int64` at `634 ms` and `mixed-string-int64-nullable` at `301 ms`.
- The non-null mixed scenario regressed too severely, so the dictionary-id reducer production
  path and test expectations were removed.
- The retained conclusion is unchanged: mixed string/int key work needs a stronger
  dictionary/key-id view and bucket policy design before typed reducer state is useful.

Source predicate aggregate pushdown comparison:

- Baseline command before the source predicate aggregate change:
  `bazel run //:file_source_benchmark -- 200000 3`.
- Current command after the source predicate aggregate change:
  `bazel run //:file_source_benchmark -- 200000 3`.

| Scenario | Pre-change best | Current best | Ratio/current pushdown gate | Result |
|---|---:|---:|---:|---:|
| `sql_csv_predicate_and_group_count` | `109,077 us` | `100,924 us` | `0.156` | `1.08x` |
| `sql_csv_predicate_or_group_count` | `156,629 us` | `143,279 us` | `0.232` | `1.09x` |
| `sql_csv_predicate_mixed_group_count` | `267,744 us` | `235,904 us` | `0.353` | `1.13x` |
| `sql_line_predicate_or_group_count` | `177,848 us` | `170,314 us` | `0.192` | `1.04x` |
| `sql_json_predicate_or_group_count` | `452,206 us` | `415,799 us` | `0.314` | `1.09x` |

The source predicate aggregate slice is narrower than the aggregate-state proof points. Its
main value is making typed source pushdown shape selection explicit for predicate expressions
and moving CSV predicate aggregate execution onto the existing typed single-key reducer path.

Final benchmark smoke after all source-predicate changes:

- `bazel run //:batch_aggregate_benchmark -- 1048576 5`: target shapes still selected.
  - `single-int64-low-domain`: `state-columnar` / `sum-single-int64-key`, `38 ms`.
  - `single-int64-low-domain-count`: `state-columnar` / `count-single-int64-key`, `28 ms`.
  - `single-int64-low-domain-avg`: `state-columnar` / `avg-single-int64-key`, `38 ms`.
  - `double-int64`: `state-columnar` / `sum-double-int64-key`, `112 ms`.
  - `double-int64-count`: `state-columnar` / `count-double-int64-key`, `104 ms`.
  - `double-int64-avg`: `state-columnar` / `avg-double-int64-key`, `119 ms`.
- `bazel run //:string_builtin_benchmark -- 100000 5`: passed; `sql-reused-plan`
  averaged `132,732 us` versus `402,253 us` for `sql-plan-and-execute`.
- Final `bazel run //:file_source_benchmark -- 200000 3`: passed; source predicate
  aggregate numbers are recorded in the table above.
- Post-substrate benchmark gate script:
  - Small smoke with reduced row counts passed and printed
    `[summary] columnar kernel benchmark gate ok`.
  - Full default gate passed and printed `[summary] columnar kernel benchmark gate ok`.

## Sensitivity and Limits

- The benchmark reports the best elapsed time across the requested in-process rounds.
- A first post-change run with `rounds=3` reported `65 ms`, so paired evidence uses `rounds=5` for both baseline and current branch to reduce best-of-round noise.
- Non-target scenarios were printed by the same benchmark, but the retained production changes only intentionally affect single-int64 SUM/COUNT/AVG and two-int64 SUM/COUNT/AVG selection and execution. Other timings should be treated as incidental system noise unless isolated by a per-scenario harness.
- The source predicate aggregate benchmark is measured through the file-source end-to-end benchmark, so non-CSV changes in that table should be treated as gate checks rather than claimed direct speedups.
- This is not a full columnar-first kernel migration. It is a compact set of typed aggregate-state proof points behind the existing public `Table` contract.

## Sensitive Data / Secrets Scan

- Targeted `rg` scan over changed source, plan, and delivery files for common API key, private key, password, secret, and bearer-token patterns: no matches.
- Final targeted `rg` scan after COUNT changes: no matches.
- Final targeted `rg` scan after StateColumnar/AVG changes: no matches.
- First final scan attempt used an overly broad `token|secret` style pattern and produced false positives on documentation text and `tokenizer` identifiers.
- Final refined targeted `rg` scan for assignment-shaped credentials, bearer values, and private-key headers: no matches.
- Final refined targeted `rg` scan after rejected mixed-key attempt was documented: no matches.

## Post-rebase CI-equivalent Validation

- Initial PR run `27472635527` failed before compilation because the GitHub merge ref contained two `float_simd_benchmark` `cc_binary` rules. Local `HEAD` had one rule, and `origin/main` had independently gained the same rule; rebasing this branch onto `origin/main` removed the duplicate from the merge result.
- Post-rebase `bazel build //:sql_demo //:stream_demo //:velaria_pyext && bazel test //:core_regression //:experimental_regression --test_output=errors`: passed.
- Post-rebase wrapper smoke, using the repository `uv` environment to set `VELARIA_PYTHON_BIN`:
  `VELARIA_PYTHON_BIN="$(uv run --project python python -c 'import sys; print(sys.executable)')" VELARIA_WRAPPER_STRESS_ITERATIONS=1 VELARIA_WRAPPER_LEAK_ITERATIONS=1 bash scripts/run_python_wrapper_leak_smoke.sh`: passed with `[summary] python wrapper leak smoke ok` and `0 leaks`.
- Post-rebase `bazel run //:batch_aggregate_benchmark -- 1048576 5`: passed.
  - `single-int64-low-domain`: `state-columnar` / `sum-single-int64-key`, `39 ms`.
  - `single-int64-low-domain-count`: `state-columnar` / `count-single-int64-key`, `29 ms`.
  - `single-int64-low-domain-avg`: `state-columnar` / `avg-single-int64-key`, `39 ms`.
  - `double-int64`: `state-columnar` / `sum-double-int64-key`, `112 ms`.
  - `double-int64-count`: `state-columnar` / `count-double-int64-key`, `103 ms`.
  - `double-int64-avg`: `state-columnar` / `avg-double-int64-key`, `117 ms`.
- Post-rebase `bazel run //:file_source_benchmark -- 200000 3`: passed.
  - `sql_csv_predicate_and_group_count`: `100,439 us`, ratio `0.158719`.
  - `sql_csv_predicate_or_group_count`: `140,559 us`, ratio `0.229195`.
  - `sql_csv_predicate_mixed_group_count`: `234,300 us`, ratio `0.352712`.
  - `sql_line_predicate_or_group_count`: `167,840 us`, ratio `0.190617`.
  - `sql_json_predicate_or_group_count`: `410,307 us`, ratio `0.312001`.
- Post-rebase `bazel run //:string_builtin_benchmark -- 100000 5`: passed.
  - `sql-plan-and-execute`: `403,522 us`.
  - `sql-reused-plan`: `130,845 us`.
- Final refined targeted `rg` scan after source predicate aggregate changes: no matches.
- Final refined targeted `rg` scan after overflow fix and delivery-record updates: no matches.

## 2026-06-14 Phase 1 Substrate Validation

- `bazel test //:columnar_batch_test --test_output=errors`: passed.
- `bazel test //:core_regression --test_output=errors`: passed, 11/11.
- `bazel build //:sql_demo //:df_demo //:stream_demo`: passed.
- `./scripts/run_columnar_kernel_benchmark_gate.sh`: passed with
  `[summary] columnar kernel benchmark gate ok`.
- `bazel test //:experimental_regression --test_output=errors`: passed.
- `bazel run //:actor_rpc_smoke`: passed with
  `[smoke] actor rpc codec roundtrip ok (control/data-batch)`.
- `git diff --check`: passed with no output.
- Final refined targeted secret-pattern scan over changed files: no matches.

## PR / CI

- Created PR: https://github.com/ashione/Velaria/pull/57.
- Initial `gh pr checks 57 --watch=false` immediately after PR creation:
  - `native-and-python`: pending
  - `python-wrapper-leak-smoke`: pending
  - `wheel-macos`: skipping
  - `wheel-manylinux`: skipping
- `gh pr view` and `gh api repos/ashione/Velaria/pulls/57` both returned GitHub EOF errors when fetching metadata, after `gh pr create` had already returned the PR URL.
- The first completed CI run failed before compilation on duplicate `float_simd_benchmark` Bazel targets in the GitHub merge ref.
- The branch was rebased onto `origin/main` to remove the already-merged duplicate from the PR merge result.
- Post-rebase local equivalents for `native-and-python` and `python-wrapper-leak-smoke` passed; the rebased branch was force-pushed, and latest GitHub Actions status should be read from the PR checks for the current head SHA.
