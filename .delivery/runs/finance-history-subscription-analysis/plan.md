# Finance History Subscription Analysis Plan

Status: active

## Repo Findings

- Finance logic lives in `python/velaria/finance_pack/` and is exposed through `python/velaria/cli/finance.py`.
- Generic service routes already support external-event sources, ingestion, monitors, focus-event polling, and realtime monitor runners.
- Repository rules prohibit finance-specific logic in agent/runtime core or generic service code.

## Selected Design

Implement the full product chain in CLI:

1. Add `provider=yahoo` for historical OHLCV through Yahoo chart JSON.
2. Add `finance pipeline` as the complete chain:
   - fetch historical OHLCV,
   - persist it as Parquet or JSONL,
   - create/update the external-event source and monitor,
   - poll quote provider for one or more iterations,
   - emit focus events and analysis,
   - include service compatibility metadata.
3. Keep `watch` as the subscription primitive and reuse its monitor/tick implementation.
4. Keep Velaria service domain-neutral; the CLI writes into the same `AgenticStore`, so service users can inspect the generated source/monitor/focus-event state with existing API routes.
5. Keep provider selection behind a registry/adapter contract so CLI choices,
   `finance sources`, operation support errors, and provider metadata all come
   from one runtime provider catalog instead of duplicated hardcoded branches.

## Dependency Decision

`existing-toolchain`: use Python standard library `urllib`, `json`, `datetime`, existing `pandas`, `pyarrow`, `AgenticStore`, and monitor runtime. No new dependency.

## Implementation Tasks

1. Add failing tests for Yahoo historical parsing and `finance pipeline`.
2. Implement Yahoo symbol normalization and chart payload parsing in `finance_pack.__init__`.
3. Add `finance pipeline` to `finance_pack.cli` and top-level `velaria.cli.finance`.
4. Add `finance_pack.providers` registry/adapter abstractions and route fetch dispatch plus provider catalog output through the registry.
5. Update user-facing README and skill help.
6. Run focused unit tests, real public data smoke, Bazel Python ecosystem regression, diff review, and secret scan.
7. Commit, push, and track PR CI.

## Acceptance Criteria

- `finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --iterations 1 --interval-sec 0 --format json` completes with real public data.
- JSON output includes `history.row_count`, `history.artifact`, `subscription.ticks`, `focus_events`, `analysis`, and `service_integration`.
- Text output is readable and does not dump raw JSON by default.
- Tests pass under `uv run --project python --extra finance python -m unittest ...`.
- `bazel test --cache_test_results=no //:python_ecosystem_regression` passes.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G2 Plan | plan | Implementation path, affected areas, dependency decision, validation strategy explicit | pass | This file | |
| Writing plans | plan | Superpowers writing-plans decision | exception | Mobius artifact plan is used directly; user asked for immediate implementation | Avoided a second plan artifact to keep scope focused |
