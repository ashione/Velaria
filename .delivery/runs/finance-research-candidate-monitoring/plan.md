# Finance Research Candidate Monitoring Plan

Status: active

## Selected Design

Implement the feature inside `python/velaria/finance_pack`:

1. Extend provider adapters with `fetch_news`.
2. Register `provider=google-news` backed by public Google News RSS search.
3. Parse RSS with stdlib XML and clean HTML snippets.
4. Add transparent keyword sentiment evidence.
5. Add `finance fetch-news` for direct news validation.
6. Add `finance rank-candidates`:
   - fetch quotes for the candidate pool,
   - fetch history and news per symbol,
   - compute score parts,
   - emit Top N `research_candidates`,
   - write ranking observations to `AgenticStore`.
7. Keep generic service/core domain-neutral.

## Dependency Decision

`existing-toolchain`: Python stdlib `urllib`, `xml.etree.ElementTree`,
`email.utils`, `html`, and `re`; existing finance CLI, provider registry, and
`AgenticStore`. No new dependency.

## Validation Strategy

- TDD unit tests for provider registry, RSS parsing, sentiment, and CLI ranking.
- Real public `fetch-news` smoke against `google-news`.
- Real U.S. `rank-candidates` smoke against `AAPL,MSFT,NVDA`.
- Service visibility check using the same `VELARIA_HOME`.
- Existing finance tests, focused agent/service tests, py_compile, Bazel Python
  ecosystem regression, diff check, and secret scan.

## Acceptance Criteria

- `finance fetch-news --provider google-news --market us --symbol AAPL --limit 2`
  returns real news rows.
- `finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date
  20260501 --end-date 20260518 --top 3 --news-limit 2 --iterations 1 --format
  json` returns three research candidates with quote/history/news/sentiment
  evidence.
- JSON output contains no buy/sell/hold instruction.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G2 Plan | plan | Implementation path, dependency decision, validation strategy explicit | pass | This file | |
| Writing plans | plan | Superpowers writing-plans decision | exception | Mobius plan is used directly; user asked to continue implementation | Avoided second plan artifact |
