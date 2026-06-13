# Columnar-first Kernel Performance Requirements

## Request

User approved moving from research into implementation and asked:

- Implement based on `plans/columnar-first-kernel-design.md`.
- Validate the implementation.
- Provide reliable evidence of performance improvement.

## Scope

This run is Mobius full-mode because it touches core execution behavior and performance-critical runtime code.

In scope:

- C++20 core kernel implementation under `src/dataflow/core/`.
- Focused regression tests under `src/dataflow/tests/`.
- Benchmark evidence using existing Bazel benchmark targets.
- Delivery evidence under `.delivery/runs/columnar-first-kernel-performance/`.

Out of scope:

- Public API changes to `DataflowSession`, `DataFrame`, or streaming contract.
- Python hot-path execution changes.
- Distributed/actor runtime changes.
- Storage-format or persistent index changes.

## Acceptance Criteria

1. Preserve current stable public behavior.
2. Add at least one failing-first regression test for the selected optimization shape.
3. Implement a columnar-first kernel improvement that is general enough to be useful beyond one benchmark fixture.
4. Run pre-change and post-change validation with the same relevant commands.
5. Report performance using before/after numbers and speedup, with enough context to avoid overstating the result.
6. Record any skipped gates or residual risks explicitly.

## Gate Ledger

| Gate | Status | Evidence |
|---|---|---|
| G1 requirements | passed | This file records request, scope, and acceptance criteria. |
| G2 design | passed | `plan.md` maps the design to dense typed single-int64 SUM aggregation. |
| G3 pre-change validation | passed | Baseline tests and benchmarks are recorded in `verification.md`. |
| G4 implementation | passed | Optimizer and executor now use typed dense sum slots for the selected shape. |
| G5 post-change validation | passed | Post-change tests, smoke, and benchmark evidence are recorded in `verification.md`. |
| G6 delivery | in-progress | Final report summarizes scope, evidence, and risks. |

## Delegation Ledger

No subagents are used in this run. The work is sequential because the benchmark target, optimizer behavior, executor dispatch, and tests share state.

## Hook Ledger

| Hook | Status | Notes |
|---|---|---|
| repo instructions | passed | User supplied `AGENTS.md`; implementation follows C++20/Bazel and baseline-before-after rules. |
| branch isolation | passed | Work is on `auto/columnar-first-kernel-performance`. |
| delivery artifact bootstrap | passed | Repository helper was unavailable, so artifacts are created manually. |
| secrets/sensitive scan | passed | Targeted secret pattern scan over changed files produced no matches. |

## Review Ledger

| Review Item | Status | Notes |
|---|---|---|
| Public contract review | passed | No public API, Python, stream, source/sink, or actor/rpc contract changes. |
| Regression review | passed | `planner_v03_test` covers optimizer selection, result semantics, and columnar cache validation. |
| Performance review | passed | Two baseline and two current benchmark runs show target speedup; limitations are recorded. |
