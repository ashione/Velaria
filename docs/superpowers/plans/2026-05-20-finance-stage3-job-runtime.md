# Finance Productization Stage 3: Durable Job Runtime

## Goal

Turn the current finance watch-session/intelligence CLI workflow into a durable job surface that an agent can inspect and control without reconstructing state from logs.

## Scope

- Add a Python ecosystem `finance_pack.jobs` module backed by Velaria `external_event` records.
- Persist job events for watch-session async runs and intelligence artifacts.
- Add model-readable CLI commands:
  - `finance intelligence jobs --session-id ... --format json`
  - `finance intelligence status --session-id ... --format json`
  - `finance intelligence stop --session-id ... --format json`
  - `finance intelligence resume --session-id ... --format json`
- Keep this in the Python ecosystem layer; do not introduce finance-specific behavior in the C++ core.

## Non-goals

- No long-running daemon in this stage.
- No new provider credentials or private APIs.
- No synthetic market data.

## Validation

1. Red tests for missing job module and CLI commands.
2. Focused unittest for finance job helpers and CLI job controls.
3. Full finance/CLI unittest suite.
4. Bazel Python regression targets.
5. Live public-data smoke against an existing US watch/intelligence session when available.

## Acceptance Criteria

- Job records are persisted as `finance_intelligence_jobs` external events.
- `jobs/status/stop/resume` return JSON payloads with `ok`, `action`, `watch_session_id`, and agentic-friendly failure fields where appropriate.
- `resume` reuses the original durable watch-session argv when it is safe and reports `resume_unavailable` otherwise.
- Existing watch-session commands keep their behavior.
