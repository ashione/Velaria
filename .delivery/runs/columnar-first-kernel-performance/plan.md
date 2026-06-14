# Columnar-first Kernel Performance Plan

## Design Interpretation

`plans/columnar-first-kernel-design.md` recommends a columnar-first internal substrate while keeping rows as a public boundary. For this implementation run, the first proof point should be narrow enough to validate safely but broad enough to demonstrate the direction.

Selected implementation target:

- Improve dense single-`INT64` group-key numeric `SUM` aggregation so it uses a typed state path instead of the generic row/table partial shape.

Scope expansion accepted during the same delivery:

- Extend the same typed state path to dense single-`INT64` `COUNT` and `AVG`.
- Extend fixed-width two-`INT64` group-key `SUM`, `COUNT`, and `AVG` to typed reducer state while preserving the existing packed hash grouping decision.
- Measure and reject a mixed string/`INT64` state-only attempt when benchmark evidence showed no reliable speedup.
- Start the typed source pushdown generalization by classifying single-key predicate aggregate `COUNT` and numeric `SUM` / `AVG` as typed source shapes, and by moving CSV predicate aggregate execution onto the typed single-key reducer path.
- Add the Phase 1 internal execution substrate skeleton (`ColumnarExecBatch`, `ColumnarExecColumn`, `ColumnarExecView`) with `Table` / retained-cache adapters, explicit materialization boundaries, Arrow-backed preservation tests, and empty-selection view support.
- Add a reproducible columnar kernel benchmark gate script that verifies aggregate typed-shape selection, file-source pushdown ratios, and string builtin plan-reuse behavior.
- Measure and reject a second mixed string/`INT64` dictionary-id reducer attempt when benchmark evidence showed a severe non-null regression.

Why this target:

- It is directly visible in the current benchmark baseline as `single-int64-low-domain`.
- It is a real execution shape, not a benchmark-only shortcut: compact integer group keys with numeric reducers are common in analytical workloads.
- It aligns with the design goal of moving aggregate state toward typed key/state columns before changing the public `Table` contract.
- It can be tested through optimizer selection and result equivalence.

## Planned Steps

1. Baseline current behavior:
   - `bazel test //:core_regression`
   - `bazel run //:batch_aggregate_benchmark -- 1048576 3`
2. Inspect current optimizer/executor dispatch for aggregate runtime shapes.
3. Add a failing-first focused test that expects the dense single-int64 `SUM` shape to select a typed sum runtime path.
4. Implement the typed dense single-int64 sum path.
5. Run the focused failing test and core regression.
6. Re-run the same aggregate benchmark and compare the selected scenario against baseline.
7. Run sensitivity checks if timing is noisy.
8. Update design/report docs with the actual result and limitations.
9. For accepted scope expansions, repeat the failing-first test, focused implementation, benchmark, and delivery-record loop before merging the expanded scope into the final report.
10. For Phase 1 substrate work, validate adapters and Arrow-backed lifetime behavior before any broad operator migration.
11. For benchmark gating, keep the script shape-based and ratio-based so it catches optimizer fallback and pushdown regressions without baking in machine-local absolute timings.

## Guardrails

- Keep public `DataflowSession` and `DataFrame` behavior unchanged.
- Preserve generic fallback for unsupported aggregate shapes.
- Do not alter SQL semantics.
- Avoid unrelated refactors in `file_source.cc`, streaming runtime, actor/rpc, or Python; source-pushdown edits are limited to the accepted predicate aggregate typed-shape slice.
- Prefer existing optimizer/executor naming and Bazel patterns.
- Keep `columnar_exec.*` internal and adjacent to `columnar_batch.*`; do not change public `Table`, `DataFrame`, or session APIs in this run.
- Reject measured fast paths that do not produce reliable improvement, even if tests can be made to pass.

## Rollback Plan

Rollback is limited to:

- The focused optimizer/executor aggregate changes.
- The focused CSV source predicate aggregate changes.
- The focused `columnar_exec.*` substrate and adapter tests.
- The benchmark gate script.
- The added test.
- The run-local delivery artifacts.

Existing design documentation changes from the prior research phase should be preserved unless explicitly superseded.
