# Finance Research Candidate Monitoring Requirements

Status: active

## Goal

Extend the finance CLI chain to continuously rank a candidate pool using public
quote data, historical OHLCV, public news RSS, and transparent sentiment
evidence.

## Success Criteria

- The command emits Top N `research_candidates`, not trading advice.
- The ranking loop can run once or continuously with JSONL output.
- Each candidate includes score parts, quote evidence, history evidence, news
  rows, sentiment evidence, freshness/delay metadata, risk flags, and disclaimer.
- News provider access uses public, keyless RSS where possible and no mock data.
- CLI-created ranking observations are written into `AgenticStore` for generic
  service inspection.

## Scope

- Add `fetch_news` provider capability and `provider=google-news`.
- Add `finance fetch-news` for direct public news RSS inspection.
- Add `finance rank-candidates` for quote/history/news/sentiment ranking.
- Update Python README and user skill.

## Non-Goals

- No buy/sell/hold recommendations.
- No order placement, portfolio allocation, target price, or return promise.
- No finance-specific service routes.
- No social-platform scraping behind login or terms-sensitive APIs.

## Risks

- Public RSS providers can throttle, change payload shape, or return clustered
  redirect links.
- Tencent U.S. quote rows are provider-marked as delayed.
- Keyword sentiment is intentionally transparent and lightweight; it is evidence,
  not a full NLP model.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G1 Requirements | requirements | Goal, criteria, scope, non-goals, risks explicit | pass | This file | |
| Brainstorming | requirements | Design boundary approved | pass | User said continue after adjusted research-candidate boundary | |
