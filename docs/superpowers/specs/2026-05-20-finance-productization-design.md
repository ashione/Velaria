# Finance Productization Design

Status: pending user review
Date: 2026-05-20
Scope: Python finance ecosystem, CLI/service runtime, evidence retrieval, replay evaluation

## Goal

Make Velaria finance intelligence a productizable public-data workflow instead of a large CLI demo. The target system continuously watches public market data and news, persists realtime rows as replayable history, builds explainable evidence indexes, runs durable review/report jobs, and evaluates signal quality after the fact.

Outputs remain research evidence only. The system must not produce brokerage actions, exchange-grade realtime claims, or hidden LLM-only scores.

## Success Criteria

- Users and agents can run one finance intelligence session through CLI commands and inspect status, logs, signals, evidence, indexes, reports, and evaluations.
- Quote/history/news/fundamental/feature/signal rows are persisted with provider, freshness, event time, ingestion time, source identity, and structured unavailable/error rows when providers fail.
- Batch backfill, realtime watch, replay, evidence search, and reports share explicit contracts instead of duplicating business logic inside the CLI parser.
- Evidence retrieval uses production-safe components by default: BM25 keyword search, structured finance signals, recency, and RRF. Semantic retrieval stays disabled until an explicitly configured production embedding/reranker provider is present.
- Jobs are durable enough to start, stop, resume, and diagnose without external driver scripts.
- Replay evaluation measures signal, provider, retrieval, and runtime quality from persisted rows without refetching provider data.

## Non-Goals

- No trading execution, brokerage integration, portfolio management, or order routing.
- No paid or credentialed data provider as the required default path.
- No finance-specific logic in the C++ core or generic agent runtime.
- No hash embeddings in production finance retrieval or ranking paths.
- No LLM-generated recommendation that cannot be traced to persisted evidence.

## Architecture

The implementation is split into four stages that can be delivered sequentially in the same PR line.

### Stage 1: Finance Modularization

`python/velaria/finance_pack/cli.py` should become command parsing and dispatch only. Business logic moves into focused modules:

- `watch_session.py`: watch-session lifecycle, event source lookup, event reads, summaries, review/supervise helpers.
- `intelligence.py`: intelligence start/review/replay/report/search/index orchestration.
- `evidence_index.py`: evidence docs, keyword index build/load/search, metadata fingerprints, stale detection.
- `signal_policy.py`: signal policy presets, explicit policy parsing, validation, and evaluation.
- `jobs.py`: durable finance job records and process/runtime identity helpers used by Stage 3.
- `evaluation.py`: replay evaluation models and summaries used by Stage 4.

The first stage is behavior-preserving. Public CLI commands and JSON fields should remain compatible except where later stages intentionally add fields.

### Stage 2: Evidence Retrieval Runtime

Introduce an `EvidenceRetriever` contract:

- `build_index(session, feed, options) -> EvidenceIndexMetadata`
- `load_index(session, feed) -> EvidenceIndex`
- `search(index_or_rows, query, top_k, options) -> EvidenceSearchResult`

Default retriever:

- BM25 keyword index over title/summary/body.
- Structured score from feed priority, symbol match, signal state, risk flags, provider errors, and news sentiment labels.
- Recency ranking from event time.
- RRF fusion with score breakdown and rank details.

Semantic retrieval:

- Disabled by default.
- Enabled only by an explicit production provider configuration.
- Metadata must include provider name, model id, embedding/reranker version, dimension when applicable, index version, built time, and row fingerprint.
- Missing provider must surface as `semantic.status=disabled`, not fallback to hash vectors.

### Stage 3: Finance Job Service

Add a Python-layer durable job runtime that the CLI can call. This is not a finance-specific C++ core change.

Job types:

- `watch_session_job`: starts or resumes a durable watch session.
- `evidence_index_job`: builds or refreshes a session/feed index.
- `review_job`: periodically appends watch/intelligence review notes.
- `report_job`: generates scorecards and final research summaries.
- `evaluation_job`: runs replay evaluation over persisted rows.

Job record fields:

- `job_id`, `job_type`, `session_id`, `intelligence_id`, `status`, `pid`, `process_identity`, `started_at`, `updated_at`, `completed_at`, `last_heartbeat`, `last_error`, `command`, `artifact_paths`, and `next_commands`.

CLI surface:

- `finance intelligence jobs --session-id ...`
- `finance intelligence status --session-id ...`
- `finance intelligence stop --session-id ...`
- `finance intelligence resume --session-id ...`
- Existing `start --async-run`, `review`, `supervise`, `index`, `search`, and `report` should use the shared job/runtime helpers where relevant.

### Stage 4: Replay Evaluation

Replay evaluation reads persisted session rows only. It does not refetch providers.

Metrics:

- Signal outcomes: forward return over available future ticks/bars, signal count, signal type distribution, repeated signal suppression, and unavailable outcome reason when future rows are absent.
- Provider quality: row counts, unavailable rows, freshness distribution, source URLs, delay, and error categories.
- Retrieval quality: index version, index hit/rebuild counts, top evidence feed distribution, stale rebuilds, semantic status, and no-hash invariant.
- Runtime quality: job uptime, heartbeat recency, process identity verification, restart count, last error, and artifact availability.

CLI surface:

- `finance intelligence evaluate --session-id ... --format json`
- `finance intelligence eval-report --session-id ... --format json`

Persistence:

- `finance_intelligence_evaluations` stores structured evaluation events.
- Reports include evaluation summaries but keep research-only disclaimers.

## Data Flow

1. Public providers write quote/history/news/fundamental/market context rows with provider evidence.
2. Watch session ingests rows into Velaria `external_event` sources and native stream signal output.
3. Intelligence orchestration reads the same sources for replay, indexing, review, and reports.
4. Evidence indexes materialize keyword/docs metadata under Velaria home and persist index events.
5. Job runtime records process and lifecycle state for long-running watch/review/index/evaluation work.
6. Evaluation reads persisted rows and job records to produce quality metrics and replay summaries.

## Error Handling

All agent-facing failures must be structured:

- `error_type`
- `message`
- `hint`
- optional `details`
- optional `candidates`

Provider failures are persisted as unavailable evidence rows. Job failures update job records and return next diagnostic commands. Semantic provider absence is not an error for the default product path; it is an explicit disabled state.

## Testing And Validation

Required local validation per implementation stage:

- Focused unit tests for moved modules.
- CLI contract tests for command compatibility.
- Finance pack tests for session, retrieval, job, and evaluation behavior.
- Bazel aggregate: `bazel test --cache_test_results=no //python:finance_pack_test //python:python_cli_contract_test //:python_ecosystem_regression`.
- `git diff --check`.
- Changed-file sensitive information scan.

Required smoke validation:

- Public quote/history/news/fundamental provider smoke when reachable.
- Existing watch-session replay smoke against persisted rows.
- Evidence index smoke proving index build, stale rebuild, default hit, semantic disabled without provider, and no `vectors.json` when provider is absent.
- Job runtime smoke proving start/status/stop/resume or a controlled unavailable reason.
- Evaluation smoke proving it reads persisted rows without provider refetch.

## Rollback Strategy

Each stage should preserve the existing CLI behavior until its replacement is verified. If a stage regresses, revert that stage's commit without reverting earlier completed stages. Evidence index v2 already invalidates older hash-vector indexes; future index versions must use metadata versioning for safe rebuilds.

## Open Decisions

- The first implementation checkpoint should be Stage 1 module extraction plus tests.
- Production semantic provider selection is deferred until the retrieval interface exists. Acceptable first provider is the repo's existing local MiniLM path if configured explicitly; no hash fallback is allowed.
- Service runtime remains Python-layer local service/job runtime in this plan. It does not become a distributed C++ runtime feature.

