# Finance Productization Stage 4: Replay Evaluation

## Goal

Add a replay evaluation layer that reads persisted watch-session/intelligence rows only, produces quality metrics, and stores those metrics as durable Velaria events.

## Scope

- Add `finance_pack.evaluation` for persisted-row metrics.
- Add CLI commands:
  - `finance intelligence evaluate --session-id ... --format json`
  - `finance intelligence eval-report --session-id ... --format json`
- Persist evaluation rows in `finance_intelligence_evaluations`.
- Include job records for evaluation/report actions.

## Non-goals

- No refetching provider data during evaluation.
- No investment recommendation output.
- No model-generated evaluation content.

## Metrics

- Signal outcomes: signal count, signal type distribution, forward outcome availability, and unavailable reason when future rows are absent.
- Provider quality: row counts, unavailable events, freshness distribution, provider distribution, source URL coverage, and error categories.
- Retrieval quality: search/index counts, top evidence feed distribution, semantic status, index status, and no-hash invariant.
- Runtime quality: durable job counts, statuses, restart count, and artifact availability.

## Validation

1. Red tests for missing evaluation module and CLI commands.
2. Focused unittest for direct evaluation and CLI persistence.
3. Full finance/CLI unittest suite.
4. Bazel Python regression targets.
5. Live persisted-session smoke using an existing US watch session.
