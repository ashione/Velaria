# Finance History Subscription Analysis Requirements

Status: active

## Goal

Deliver a user-ready finance CLI chain that obtains historical market data, subscribes to live quote updates, ingests observations into Velaria's agentic store, runs monitors, and produces an analysis payload/report without requiring finance-specific service routes.

## Background

The existing finance pack can fetch quotes, run one-symbol watches, and produce analysis context. It is still incomplete as a product workflow because historical data is separate from live subscription and AkShare/Eastmoney history is not reachable in the current environment. Existing Velaria service already exposes generic external-event, monitor, and focus-event APIs; hardcoding finance behavior in service would violate the domain-neutral service/core boundary.

## Success Criteria

- A single CLI command runs the full chain: history fetch, history artifact persistence, quote subscription ticks, monitor execution, focus-event output, and analysis context.
- The historical path uses a public provider that is verified in this environment; AkShare remains available but is not the only usable path.
- The output is user-readable by default and machine-readable with `--format json`.
- The chain writes to Velaria's shared agentic store so the existing service can observe sources, monitors, and focus events when using the same `VELARIA_HOME`.
- Failure paths return structured finance provider errors, not mock data or tracebacks.
- Tests cover the new provider parser and the full CLI chain with mocked provider calls.

## Scope

- Add a public historical provider for Yahoo chart JSON because it currently works for A-share Yahoo symbols such as `000001.SZ` and U.S. symbols such as `AAPL`.
- Add a product command, tentatively `finance pipeline`, for the full history + subscription + analysis chain.
- Update top-level CLI forwarding, help text, Python README, and the Velaria Python local skill.
- Keep service domain-neutral; document service compatibility through shared store and generic routes.

## Non-Goals

- No trading, order placement, strategy backtesting, portfolio management, or investment recommendation.
- No finance-specific service API routes.
- No mock market data.
- No long-running daemon manager beyond existing CLI `watch --iterations 0 --jsonl`.

## Risks

- Public providers can change behavior or throttle requests.
- Yahoo chart is a public JSON endpoint rather than a contracted market-data SLA.
- AkShare/Eastmoney may remain blocked behind local proxy/network policy.

## Open Questions

None blocking. The user requested implementation now, and repository constraints answer the service-vs-CLI choice: CLI should own finance orchestration while service remains generic.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G1 Requirements | requirements | Goal, criteria, scope, non-goals, risks explicit | pass | This file | |
| Brainstorming | requirements | Superpowers brainstorming decision | exception | User already selected direction and asked to implement; design is constrained by repo rules and prior productization feedback | Skipped interactive approval to preserve momentum |
