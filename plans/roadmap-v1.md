# Velaria Roadmap v1

Status: planning document, not a status board.

This document sequences the next milestones across the repository.
It complements, but does not replace:

- [../README.md](../README.md) — current scope and supported surfaces
- [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md) — current status board for the core-runtime columnar line
- [columnar-first-kernel-design.md](./columnar-first-kernel-design.md) — research design, risks, and validation gates

When this roadmap and the status board disagree, the status board wins for what is implemented;
this document owns only sequencing and exit criteria.

## 1. Starting Point

- `main` is `v0.3.1`; recent merged work includes typed columnar aggregate reducers (`#57`),
  the Velaria Agent TUI (`#58`), multi-key source pushdown hardening (`#59`), and CSV
  aggregate group-key canonicalization (`#60`, `4de8347`).
- `release/v0.3.2` currently carries only a version bump and is not merged.
- The kernel is single-node first: one native batch/stream kernel, column-first runtime
  direction, local vector search, Python as an Arrow/interop layer, same-host actor/rpc as an
  experiment lane only.
- Measured gaps carried into this roadmap:
  - file-source SQL pushdown is still `15.4%–27.5%` slower in absolute time than the
    April 26, 2026 local baseline, while remaining `2.7–5.8x` faster than no-pushdown
    execution on the same run;
  - repeated SQL planning is tracked separately from execution;
  - large `Int64` / `Double` comparison loses precision above `2^53`, unchecked
    `Double -> Int64` conversion truncates silently, and packed aggregate keys distinguish
    `1.0` from `Int64 1` while `Value::compare` treats them as equal.

## 2. Principles

1. One kernel. No second execution engine, no second explain/progress contract.
2. Public contracts stay stable: `DataflowSession`, `DataFrame`, `StreamingDataFrame`,
   streaming progress, `explainStreamSql`.
3. Every performance claim is an A/B on the same commit, machine, harness, fixture, and
   scheduling settings. Cross-date snapshot deltas are guardrails, not proof.
4. Generic fallbacks stay alive. A typed path is selected explicitly by the optimizer and can
   be disabled.
5. No benchmark-shape shortcuts, no per-format control-flow forks without a measured reason.
6. Small reversible slices: each milestone lands behind existing contracts and can be rolled
   back without a public API change.

## 3. Milestones

### M0 — Release hygiene (`v0.3.2`)

Goal: ship the already-verified work before opening new fronts.

Deliverables:

- merge or drop the `release/v0.3.2` version bump;
- record the release snapshot in `docs/benchmarks.md` / `docs/benchmarks-zh.md`.

Exit criteria:

- `core_regression`, `experimental_regression`, `python_ecosystem_regression` green;
- demo targets (`sql_demo`, `df_demo`, `stream_demo`) and `actor_rpc_smoke` green;
- no open correctness question from review.

### M1 — Numeric and key semantics (`v0.4.0`)

Goal: remove semantic traps before pushing more typed fast paths.

Deliverables:

- exact `Int64` / `Double` comparison: handle NaN/Inf separately, compare as `Int64` when the
  double is integral and in range, otherwise use range checks plus truncated compare;
- checked `Double -> Int64` conversion with structured errors (`error_type`, `message`,
  `hint`), no silent truncation;
- one key-identity rule shared by `Value` comparison and packed/generic aggregate keys:
  canonicalize integral doubles to `Int64`, normalize `±0.0`, canonicalize NaN;
- regression coverage: `2^53±1`, NaN/Inf, `±0.0`, `1` vs `1.0` vs `1e3`, mixed-type
  `GROUP BY` and `JOIN`, nullable keys.

Exit criteria:

- new regression cases pass in `core_regression` / `file_source_test`;
- packed and generic aggregate paths return identical groups for the same input;
- no benchmark scenario regresses more than `5%` without a documented semantic reason.

### M2 — Recover source pushdown absolute time (`v0.4.0`)

Goal: bring file-source SQL pushdown back to the April 26 baseline in absolute time, while
keeping the pushdown ratio below `0.40`.

Deliverables:

- typed source pushdown: scanners emit only the typed slots needed by filter / key / aggregate;
- reducer specialization beyond single/two `Int64` keys: double keys, string key ownership or
  dictionary/key-id views before touching CSV/line multi-key reducers again;
- per-format gates for CSV, line, and JSON so one format cannot regress behind another;
- empty-predicate short-circuit and predicate-shape classification kept explicit in the
  optimizer, not in scanners.

Exit criteria:

- same-machine A/B versus the April 26 baseline: every covered case at parity or faster;
- pushdown ratio guardrail still `< 0.40` on all covered SQL cases;
- high-cardinality unique-key cases stay within `5%` of the current best;
- `scripts/run_columnar_kernel_benchmark_gate.sh` passes.

### M3 — End-to-end columnar operators (`v0.5.0`)

Goal: stop rebuilding rows in the middle of a pipeline.

Priority order:

1. `filter` / `select` / `withColumn`
2. `aggregate`
3. `window`
4. `join`

Deliverables:

- operators consume and produce `ColumnarExecView` where a generic fallback exists;
- rows materialize only at compatibility boundaries (console/file sinks, Python rows,
  actor/rpc wire formats);
- explain / provenance can name the selected path and the fallback reason without changing the
  stable top-level explain contract.

Exit criteria:

- batch benchmarks at parity or faster than M2 for the covered shapes;
- `core_regression` and `experimental_regression` green;
- public row/Arrow behavior unchanged, verified by existing tests.

### M4 — Arrow retention and bound plans (`v0.5.0`)

Goal: let Arrow-aligned data survive longer and make reused plans cheap.

Deliverables:

- Arrow-backed batches stay Arrow-backed until an explicit materialization boundary;
- schema fingerprint sufficient to invalidate bound-plan column access;
- Arrow import/export benchmarks that separate ingress, execution, and export.

Exit criteria:

- Arrow lifetime and lifetime-stress tests green;
- no copy-count or latency regression on Arrow import/export;
- stale-plan invalidation covered by tests.

### M5 — Ecosystem consolidation (parallel, after M1)

Goal: keep the supported surface honest while the kernel moves.

Deliverables:

- Agent CLI/TUI session recovery, streaming, and error surfaces;
- desktop prototype kept on the same `velaria-service` contract, no second execution path;
- finance public-data pack freshness metadata and provider failure diagnostics;
- `skills/*.md` and `python/README.md` kept aligned with real entrypoints.

Non-goals: new execution semantics in Python, Python UDFs in the hot path, a second progress
schema.

### M6 — Experimental runtime (frozen)

- keep `actor_rpc_smoke` and the local multi-process e2e green;
- do not expand scheduling while the kernel substrate is still moving;
- no core dependency on actor/rpc.

## 4. Sequencing

```text
M0 -> M1 -> M2 -> M3 -> M4
       \-> M5 (parallel, docs/product surface)
M6 stays frozen
```

Dependencies:

- M2 depends on M1: typed reducers must not encode keys with two different equality rules.
- M3 depends on M2: columnar operator work should reuse the typed slot/key views.
- M4 can start after M2 and is independent of M3 ordering.
- M5 must not block kernel milestones.

## 5. Gates For Every Milestone

1. Record a baseline before the change (same machine, same harness, same fixture).
2. Land the change on a branch with the generic fallback intact.
3. Run the A/B and the regression suite listed in
   [columnar-first-kernel-design.md](./columnar-first-kernel-design.md) (validation section).
4. Run `scripts/run_columnar_kernel_benchmark_gate.sh`.
5. Update the status board and `docs/benchmarks*.md` with the measured result.
6. Only then report the milestone ready.

## 6. Non-goals

- a completed distributed runtime: no distributed scheduling, cross-machine shuffle, or
  multi-node scheduling plan;
- `CTE`, subqueries, richer `JOIN` semantics, aggregate `KEYWORD SEARCH` / `HYBRID SEARCH`;
- ANSI window syntax expansion beyond the current `WINDOW BY ... EVERY ... [SLIDE ...]`;
- Python callbacks / UDFs in the execution hot path;
- benchmark-specific branches or shape shortcuts to move numbers.

## 7. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Semantic fixes change aggregate/group results | High | land M1 before M2; regression cases for key identity |
| Typed key ownership is under-designed | High | solve key views before replacing CSV/line reducers |
| Benchmark gains are environment-specific | Medium | same-commit A/B, taskset pinning, record machine and fixture |
| Scope creep into product surface | Medium | M5 is parallel and non-blocking |
| Verification toolchain unavailable in a workspace | Medium | run gates where Bazel exists; never claim a gate without output |

## 8. Release Mapping

| Release | Milestones |
|---|---|
| `v0.3.2` | M0 |
| `v0.4.0` | M1, M2 |
| `v0.5.0` | M3, M4 |
| unscheduled | M5, M6 |

## 9. North Star (beyond v0.5)

The milestone list above is deliberately small. The repository's own boundary says the local
kernel must be stable before anything is claimed about a product platform, so each milestone is a
release-gated step rather than the whole ambition. The arc has two stages.

### Stage 1 — Kernel (now → v0.5)

One local native batch/stream kernel, column-first, measurable at every step, with stable public
contracts. Success: every covered operator path has a same-machine baseline, a generic fallback,
and a documented selection rule.

### Stage 2 — Agent-native local data platform (after v0.5)

Turn the kernel into the execution substrate for the documented platform direction:

- agentic event service: `external_event` sources, monitors, signals, focus events
  ([agentic-event-data-platform-v1.md](./agentic-event-data-platform-v1.md))
- search and grounding over local datasets and reusable embedding / keyword assets
- public-data finance packs feeding monitor workflows
- desktop app on the same `velaria-service` contract, plus agent CLI/TUI and MCP skills
  ([visual-data-app-v1.md](./visual-data-app-v1.md))

Success: a user can ingest, monitor, and query local data through one kernel with explainable
contracts, without a second execution engine.

### Out of scope — scale-out

Scale-out is not part of this roadmap. There is no multi-node scheduling, shuffle, or distributed
runtime milestone, and the experimental actor/rpc lane stays frozen. Local multi-process
coordination remains an experiment lane only; it is not on the release path.

