# Finance Stage 1 Modularization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split finance intelligence retrieval logic out of `finance_pack/cli.py` without changing user-visible CLI behavior.

**Architecture:** Stage 1 starts with the lowest-risk extraction: evidence indexing and retrieval. `cli.py` keeps command parsing and orchestration, while `evidence_index.py` owns finance evidence docs, keyword index persistence, stale detection, and search fusion. Later Stage 1 slices can extract watch-session, intelligence orchestration, signal policy, jobs, and evaluation modules after this boundary is stable.

**Tech Stack:** Python 3.12+, `pyarrow`, Velaria `AgenticStore`, existing `velaria.keyword_index`, `uv`, Bazel `py_library`/`py_test`.

---

## File Structure

- Create `python/velaria/finance_pack/evidence_index.py`
  - Owns constants, evidence doc construction, compact evidence payloads, keyword index build/load/search, fingerprints, RRF fusion, semantic-disabled metadata, and index path helpers.
  - Does not import CLI parser code.
  - Does not import or use `HashEmbeddingProvider`.
- Modify `python/velaria/finance_pack/cli.py`
  - Imports evidence helpers from `evidence_index.py`.
  - Removes duplicated helper implementations after extraction.
  - Keeps command dispatch, persistence to `AgenticStore`, rendering, watch-session orchestration, and intelligence orchestration for this stage.
- Modify `python/tests/test_finance_pack.py`
  - Adds direct tests for `evidence_index.py` no-hash behavior and stale v1 invalidation.
  - Keeps existing CLI behavior tests.
- Modify `python/BUILD.bazel`
  - Adds `velaria/finance_pack/evidence_index.py` to `velaria_py_pkg`.
- Optional docs update is not required for Stage 1 because behavior does not change.

## Acceptance Criteria

- `finance intelligence index/search` behavior remains compatible.
- Search output still includes `retrieval.index_status`, `retrieval.index_path`, `retrieval.semantic.status=disabled`, and hits with score breakdown.
- Index build writes `metadata.json`, `docs.jsonl`, and keyword index files, but no `vectors.json`.
- Existing v1 index metadata is treated as stale and rebuilt as v2.
- `HashEmbeddingProvider` is not imported by finance intelligence retrieval.
- Existing local and Bazel regression commands pass.

---

### Task 1: Add Direct Evidence Index Tests

**Files:**
- Modify: `python/tests/test_finance_pack.py`
- Create later in Task 2: `python/velaria/finance_pack/evidence_index.py`

- [ ] **Step 1: Add imports for direct evidence helpers**

Add this import block near existing finance pack imports:

```python
from velaria.finance_pack.evidence_index import (
    FINANCE_EVIDENCE_INDEX_VERSION,
    build_finance_evidence_index,
    finance_evidence_index_dir,
    load_finance_evidence_index,
)
```

- [ ] **Step 2: Add failing test for no-hash index artifacts**

Add this test next to the intelligence index tests:

```python
    def test_evidence_index_module_builds_no_hash_v2_index(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-evidence-index-module-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                rows = [
                    {
                        "feed": "candidates",
                        "event_id": "candidate-aapl",
                        "event_time": "2026-05-20T14:00:00Z",
                        "payload_json": {
                            "watch_session_id": "session_module_index",
                            "event_time": "2026-05-20T14:00:00Z",
                            "event_type": "research_candidate",
                            "symbol": "AAPL",
                            "summary": "AAPL positive momentum evidence",
                            "score": 9.0,
                        },
                    }
                ]

                index = build_finance_evidence_index(
                    intelligence_id="intel_module_index",
                    watch_session_id="session_module_index",
                    rows=rows,
                    feed="all",
                )

                index_dir = pathlib.Path(index["index_path"])
                self.assertEqual(index["index_version"], FINANCE_EVIDENCE_INDEX_VERSION)
                self.assertEqual(index["semantic_status"], "disabled")
                self.assertTrue((index_dir / "metadata.json").exists())
                self.assertTrue((index_dir / "docs.jsonl").exists())
                self.assertTrue((index_dir / "keyword" / "manifest.json").exists())
                self.assertFalse((index_dir / "vectors.json").exists())
                self.assertNotIn("vectors_path", index)
```

- [ ] **Step 3: Add failing test for stale v1 invalidation**

Add this test after the no-hash artifact test:

```python
    def test_evidence_index_module_rejects_stale_v1_index(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-evidence-index-stale-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index_dir = finance_evidence_index_dir(watch_session_id="session_stale", feed="all")
                index_dir.mkdir(parents=True)
                (index_dir / "metadata.json").write_text(
                    json.dumps({"index_version": 1, "fingerprint": "old"}) + "\n",
                    encoding="utf-8",
                )
                (index_dir / "docs.jsonl").write_text("", encoding="utf-8")
                (index_dir / "keyword").mkdir()

                loaded, reason = load_finance_evidence_index(
                    watch_session_id="session_stale",
                    feed="all",
                    expected_fingerprint="old",
                )

                self.assertIsNone(loaded)
                self.assertEqual(reason, "stale")
```

- [ ] **Step 4: Run tests and verify they fail before implementation**

Run:

```bash
uv run --project python --extra finance python -m unittest \
  python.tests.test_finance_pack.FinancePackTest.test_evidence_index_module_builds_no_hash_v2_index \
  python.tests.test_finance_pack.FinancePackTest.test_evidence_index_module_rejects_stale_v1_index
```

Expected: import failure because `velaria.finance_pack.evidence_index` does not exist yet.

---

### Task 2: Extract Evidence Index Module

**Files:**
- Create: `python/velaria/finance_pack/evidence_index.py`
- Modify: `python/velaria/finance_pack/cli.py`

- [ ] **Step 1: Create `evidence_index.py` with extracted public helpers**

Create the file with these exports:

```python
from __future__ import annotations

import hashlib
import json
import pathlib
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from velaria.keyword_index import build_keyword_index, search_keyword_index

FINANCE_EVIDENCE_INDEX_VERSION = 2
FINANCE_EVIDENCE_SEMANTIC_STATUS = "disabled"
FINANCE_EVIDENCE_SEMANTIC_REASON = "semantic retrieval requires an explicitly configured production embedding provider"
FINANCE_EVIDENCE_FEEDS = (
    "all",
    "quotes",
    "history",
    "news",
    "features",
    "candidates",
    "market_context",
    "fundamentals",
    "native_stream_signals",
)
```

Then move these functions from `cli.py` into this module without behavior changes:

- `hybrid_search_finance_rows`
- `hybrid_search_finance_docs`
- `safe_search_keyword_index`
- `resolve_finance_evidence_search_index`
- `finance_evidence_index_dir`
- `finance_evidence_index_metadata_for_payload`
- `finance_evidence_fingerprint`
- `build_finance_evidence_index`
- `load_finance_evidence_index`
- `finance_evidence_docs`
- `compact_finance_evidence_payload`
- `finance_structured_evidence_score`

Rename the public functions by removing the leading underscore. Keep internal-only helpers with a leading underscore.

- [ ] **Step 2: Include local helper implementations**

The new module must include these local helpers so it does not depend on CLI internals:

```python
def _utc_payload_time() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sql_identifier_suffix(value: str) -> str:
    suffix = "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")
    suffix = suffix or "default"
    if len(suffix) <= 32:
        return suffix
    digest = hashlib.sha1(suffix.encode("utf-8")).hexdigest()[:8]
    return f"{suffix[:23].rstrip('_')}_{digest}"


def _watch_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
    return {**row, **payload}
```

- [ ] **Step 3: Update `cli.py` imports**

Add this import near other finance imports:

```python
from .evidence_index import (
    FINANCE_EVIDENCE_FEEDS,
    FINANCE_EVIDENCE_SEMANTIC_REASON,
    FINANCE_EVIDENCE_SEMANTIC_STATUS,
    build_finance_evidence_index,
    finance_evidence_index_metadata_for_payload,
    hybrid_search_finance_rows,
    resolve_finance_evidence_search_index,
)
```

- [ ] **Step 4: Replace call sites in `cli.py`**

Use these replacements:

```python
_build_finance_evidence_index(...) -> build_finance_evidence_index(...)
_resolve_finance_evidence_search_index(...) -> resolve_finance_evidence_search_index(...)
_hybrid_search_finance_rows(...) -> hybrid_search_finance_rows(...)
_finance_evidence_index_metadata_for_payload(...) -> finance_evidence_index_metadata_for_payload(...)
```

- [ ] **Step 5: Remove extracted implementations from `cli.py`**

Delete the extracted functions and constants from `cli.py` after call sites are updated. Do not remove `_watch_row_payload`, `_sql_identifier_suffix`, or `_utc_payload_time` from `cli.py` yet because other CLI code still uses them.

- [ ] **Step 6: Run focused tests**

Run:

```bash
uv run --project python --extra finance python -m unittest \
  python.tests.test_finance_pack.FinancePackTest.test_evidence_index_module_builds_no_hash_v2_index \
  python.tests.test_finance_pack.FinancePackTest.test_evidence_index_module_rejects_stale_v1_index \
  python.tests.test_finance_pack.FinancePackTest.test_intelligence_index_persists_reusable_hybrid_evidence_index \
  python.tests.test_finance_pack.FinancePackTest.test_intelligence_search_uses_hybrid_evidence_and_persists_query
```

Expected: all 4 tests pass.

---

### Task 3: Update Bazel Package Sources

**Files:**
- Modify: `python/BUILD.bazel`

- [ ] **Step 1: Add new module to `velaria_py_pkg`**

Add this source next to `velaria/finance_pack/cli.py`:

```python
"velaria/finance_pack/evidence_index.py",
```

- [ ] **Step 2: Run Bazel focused test**

Run:

```bash
bazel test --cache_test_results=no //python:finance_pack_test
```

Expected: `//python:finance_pack_test PASSED`.

---

### Task 4: Stage 1 Regression And Smoke

**Files:**
- No new source edits expected unless validation fails.

- [ ] **Step 1: Run Python regression**

Run:

```bash
uv run --project python --extra finance python -m unittest python.tests.test_finance_pack python.tests.test_python_cli_contract
```

Expected: `Ran 95 tests` or more, `OK`.

- [ ] **Step 2: Run Bazel aggregate regression**

Run:

```bash
bazel test --cache_test_results=no //python:finance_pack_test //python:python_cli_contract_test //:python_ecosystem_regression
```

Expected: all targets pass.

- [ ] **Step 3: Run live persisted-session no-hash smoke when prior session exists**

Run:

```bash
VELARIA_HOME=/tmp/velaria-finance-reviewfix-us-20260520 \
uv run --project python --extra finance python python/velaria_cli.py finance intelligence search \
  --intelligence-id intel_stage1_module_20260520 \
  --session-id us_watch_reviewfix_20260520 \
  --query "AAPL momentum risk news fundamentals" \
  --top-k 3 \
  --format json
```

Expected JSON fields:

```json
{
  "search": {
    "retrieval": {
      "semantic": {"status": "disabled"},
      "doc_count": 61
    },
    "top_symbol": "AAPL"
  }
}
```

If the `/tmp` session is unavailable, record the smoke as unavailable and rely on unit/Bazel coverage for this stage.

- [ ] **Step 4: Run formatting and secret checks**

Run:

```bash
git diff --check
rg -n "api[_-]?key|secret|token|password|BEGIN PRIVATE|AKIA|sk-[A-Za-z0-9]" \
  python/velaria/finance_pack/evidence_index.py \
  python/velaria/finance_pack/cli.py \
  python/tests/test_finance_pack.py \
  python/BUILD.bazel
```

Expected: `git diff --check` exits 0. Secret scan should have no real secret values; documented key names in unrelated files are not relevant because this command scans changed files only.

---

### Task 5: Commit Stage 1

**Files:**
- Stage intentional files only:
  - `python/velaria/finance_pack/evidence_index.py`
  - `python/velaria/finance_pack/cli.py`
  - `python/tests/test_finance_pack.py`
  - `python/BUILD.bazel`
  - `docs/superpowers/plans/2026-05-20-finance-stage1-modularization.md`

- [ ] **Step 1: Review local diff**

Run:

```bash
git diff --stat
git diff -- python/velaria/finance_pack/evidence_index.py python/velaria/finance_pack/cli.py | sed -n '1,260p'
```

Expected: evidence logic moved out, no hash embedding import, CLI behavior preserved.

- [ ] **Step 2: Commit**

Use commit message:

```bash
git add python/velaria/finance_pack/evidence_index.py \
  python/velaria/finance_pack/cli.py \
  python/tests/test_finance_pack.py \
  python/BUILD.bazel \
  docs/superpowers/plans/2026-05-20-finance-stage1-modularization.md
git commit -m "refactor(finance): extract intelligence evidence index"
```

- [ ] **Step 3: Push and observe CI**

Run:

```bash
git push
gh pr checks 56 --watch --interval 10
```

Expected: `native-and-python` and `python-wrapper-leak-smoke` pass; wheel jobs may skip.

---

## Self-Review

- Spec coverage: this plan implements Stage 1's evidence-index modularization slice, preserves no-hash invariant from Stage 2, and prepares later modular extraction.
- Placeholder scan: no TBD/TODO placeholders remain.
- Type consistency: exported helper names use public no-underscore names; CLI imports those names directly.
- Scope decision: Stage 1 is deliberately limited to evidence retrieval extraction so the first commit is reviewable and behavior-preserving.

