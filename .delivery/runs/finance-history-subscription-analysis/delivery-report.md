# Finance History Subscription Analysis Delivery Report

Status: active

## Executive Summary

Implemented a complete CLI-owned finance chain for historical data retrieval, live quote subscription, monitor execution, and analysis output. The implementation intentionally keeps the local service domain-neutral: the CLI writes into Velaria's shared `AgenticStore`, and the existing service generic routes can inspect the generated sources, monitors, and focus events when launched with the same `VELARIA_HOME`.

Provider selection is now routed through a finance provider registry and adapter
contract. CLI choices, `finance sources`, operation support errors, and provider
metadata all come from that registry instead of duplicated hardcoded provider
lists.

## Implementation Summary

- Added `provider=yahoo` for historical OHLCV through public Yahoo chart JSON.
- Added `finance_pack.providers` with provider specs, adapters, and operation
  capability lookup for history and quote workflows.
- Added `finance pipeline` for the full chain:
  - fetch historical rows,
  - write a history artifact,
  - create/update an `external_event` source,
  - poll quote rows as subscription ticks,
  - run a monitor and emit FocusEvents,
  - return readable text or JSON with `history`, `subscription`, `quote`, `focus_events`, `analysis`, `analysis_prompt`, and `service_integration`.
- Updated `finance sources`, CLI help, Python README, local skill, and public data smoke defaults.
- Verified service compatibility through existing service routes without adding finance-specific service endpoints.

## Changed Files

- `python/velaria/finance_pack/__init__.py`
- `python/velaria/finance_pack/cli.py`
- `python/velaria/finance_pack/providers.py`
- `python/velaria/cli/finance.py`
- `python/BUILD.bazel`
- `python/tests/test_finance_pack.py`
- `python/examples/finance_public_data_smoke.py`
- `python/README.md`
- `skills/velaria_python_local/SKILL.md`
- `.delivery/runs/finance-history-subscription-analysis/*.md`

## Validation Summary

Local validation passed:

- Focused finance and service tests: 26 tests OK.
- Syntax compilation: passed.
- Real Yahoo historical fetch: `cn:000001` returned 18 rows.
- Real Yahoo historical fetch: `us:AAPL` returned 12 rows for
  `20260501`-`20260518`.
- Real Tencent quote fetch: `us:AAPL` returned one quote row with
  `freshness=delayed`.
- Real U.S. full pipeline: `us:AAPL` returned history row_count 12,
  subscription tick_count 1, one FocusEvent, and service-visible source/monitor/event state.
- Real full pipeline: history + Tencent live quote + monitor + FocusEvent passed.
- Service compatibility: service saw the pipeline-created source, monitor, and focus event.
- Public data smoke: CN/US history and quote checks passed.
- Bazel Python ecosystem regression: 14/14 passed.
- Diff check and focused secret scan passed.

## PR and CI

PR URL: existing PR #52.

CI status is pending for the new commit until pushed and observed.

## Risks and Follow-Ups

- Yahoo chart JSON and Tencent quote endpoints are public providers without project-owned SLA.
- AkShare/Eastmoney remains available but failed in this environment due proxy/upstream behavior.
- Long-running subscription management is still CLI-based (`watch --iterations 0 --jsonl`); no daemon lifecycle manager was added.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G6 PR/MR | submit | PR URL or reason | active | existing PR #52, pending push of this commit | |
| G7 CI/CD | submit | terminal CI state | active | pending push/CI | |
| G8 Report | report | delivery report complete | active | this file, pending final CI update | |
