# Finance Stage 2 Retrieval Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn finance evidence search from helper functions into an explicit production-safe retrieval runtime contract.

**Architecture:** `evidence_index.py` keeps the existing function API for CLI compatibility and adds `FinanceEvidenceRetriever`, `EvidenceSearchOptions`, and `EvidenceSearchResult`. The default retriever uses BM25 keyword, structured scoring, recency, and RRF; semantic retrieval remains disabled unless a future explicit production provider is configured.

**Tech Stack:** Python dataclasses, existing Velaria keyword index, existing finance CLI/tests, uv, Bazel.

---

## Files

- Modify `python/velaria/finance_pack/evidence_index.py`
  - Add runtime dataclasses.
  - Add `FinanceEvidenceRetriever`.
  - Route existing functions through the default retriever where useful.
  - Add retriever metadata to index/search output.
- Modify `python/velaria/finance_pack/cli.py`
  - Use retriever search result rather than manually assembling retrieval metadata.
- Modify `python/tests/test_finance_pack.py`
  - Add direct retriever contract tests.

## Tasks

### Task 1: Add Retriever Contract Tests

- [ ] Add imports:

```python
from velaria.finance_pack.evidence_index import (
    EvidenceSearchOptions,
    FinanceEvidenceRetriever,
)
```

- [ ] Add test:

```python
    def test_finance_evidence_retriever_contract_searches_without_semantic_provider(self):
        rows = [
            {
                "feed": "candidates",
                "event_id": "candidate-aapl",
                "event_time": "2026-05-20T14:00:00Z",
                "payload_json": {
                    "watch_session_id": "session_retriever",
                    "event_type": "research_candidate",
                    "symbol": "AAPL",
                    "summary": "AAPL momentum risk evidence",
                    "score": 9.0,
                },
            }
        ]
        retriever = FinanceEvidenceRetriever()
        result = retriever.search_rows(
            intelligence_id="intel_retriever",
            watch_session_id="session_retriever",
            rows=rows,
            query_text="AAPL momentum",
            options=EvidenceSearchOptions(feed="all", top_k=1, index_mode="off"),
        )

        self.assertEqual(result.retrieval["semantic"]["status"], "disabled")
        self.assertEqual(result.retrieval["retriever"], "finance_evidence_retriever")
        self.assertEqual(result.retrieval["retriever_version"], retriever.retriever_version)
        self.assertEqual(len(result.hits), 1)
```

- [ ] Run focused test. Expected: import failure until implementation exists.

### Task 2: Implement Runtime Contract

- [ ] In `evidence_index.py`, add:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class EvidenceSearchOptions:
    feed: str = "all"
    top_k: int = 5
    index_mode: str = "auto"


@dataclass(frozen=True)
class EvidenceSearchResult:
    hits: list[dict[str, Any]]
    index_ref: dict[str, Any]
    retrieval: dict[str, Any]


class FinanceEvidenceRetriever:
    retriever_name = "finance_evidence_retriever"
    retriever_version = "v1"

    def build_index(...): ...
    def load_index(...): ...
    def resolve_index(...): ...
    def search_rows(...): ...
    def search_docs(...): ...
```

- [ ] `search_rows` must call `resolve_index`, then `search_docs`, then return `EvidenceSearchResult`.
- [ ] `retrieval` must include `mode`, `retriever`, `retriever_version`, `keyword`, `semantic`, `fusion`, `rank_constant`, `structured_features`, `index_mode`, `index_status`, `index_path`, `index_fingerprint`, and `doc_count`.
- [ ] `build_index` must add `retriever` and `retriever_version` metadata.
- [ ] Existing top-level helper functions should delegate to `DEFAULT_FINANCE_EVIDENCE_RETRIEVER` so CLI compatibility remains.

### Task 3: Update CLI Search Assembly

- [ ] Import `EvidenceSearchOptions` and `DEFAULT_FINANCE_EVIDENCE_RETRIEVER`.
- [ ] In `_intelligence_search_payload`, replace manual resolve/search/retrieval construction with:

```python
    result = DEFAULT_FINANCE_EVIDENCE_RETRIEVER.search_rows(
        intelligence_id=intelligence_id,
        watch_session_id=watch_session_id,
        rows=rows,
        query_text=query_text,
        options=EvidenceSearchOptions(feed=feed, top_k=top_k, index_mode=index_mode),
    )
```

- [ ] Use `result.hits`, `result.index_ref`, and `result.retrieval` in the returned payload.

### Task 4: Validate

- [ ] Run focused retriever tests.
- [ ] Run full Python finance/CLI tests.
- [ ] Run Bazel aggregate regression.
- [ ] Run persisted-session smoke and verify `retriever=finance_evidence_retriever`, `semantic.status=disabled`.
- [ ] Run `git diff --check` and changed-file sensitive scan.

### Task 5: Commit

- [ ] Commit message:

```bash
git commit -m "feat(finance): add evidence retriever runtime"
```

---

## Self-Review

- Stage 2 does not introduce a real semantic provider yet.
- No hash embedding fallback is permitted.
- Existing CLI payload shape is preserved and gains retriever metadata.

