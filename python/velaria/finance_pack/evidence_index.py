from __future__ import annotations

import hashlib
import json
import pathlib
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from velaria.keyword_index import build_keyword_index, search_keyword_index


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
FINANCE_EVIDENCE_INDEX_VERSION = 2
FINANCE_EVIDENCE_SEMANTIC_STATUS = "disabled"
FINANCE_EVIDENCE_SEMANTIC_REASON = "semantic retrieval requires an explicitly configured production embedding provider"


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

    def build_index(
        self,
        *,
        intelligence_id: str,
        watch_session_id: str,
        rows: list[dict[str, Any]],
        feed: str,
    ) -> dict[str, Any]:
        index = build_finance_evidence_index(
            intelligence_id=intelligence_id,
            watch_session_id=watch_session_id,
            rows=rows,
            feed=feed,
        )
        index.setdefault("retriever", self.retriever_name)
        index.setdefault("retriever_version", self.retriever_version)
        return index

    def load_index(
        self,
        *,
        watch_session_id: str,
        feed: str,
        expected_fingerprint: str,
    ) -> tuple[dict[str, Any] | None, str]:
        return load_finance_evidence_index(watch_session_id=watch_session_id, feed=feed, expected_fingerprint=expected_fingerprint)

    def resolve_index(
        self,
        *,
        intelligence_id: str,
        watch_session_id: str,
        rows: list[dict[str, Any]],
        feed: str,
        index_mode: str,
    ) -> dict[str, Any]:
        return resolve_finance_evidence_search_index(
            intelligence_id=intelligence_id,
            watch_session_id=watch_session_id,
            rows=rows,
            feed=feed,
            index_mode=index_mode,
        )

    def search_rows(
        self,
        *,
        intelligence_id: str,
        watch_session_id: str,
        rows: list[dict[str, Any]],
        query_text: str,
        options: EvidenceSearchOptions | None = None,
    ) -> EvidenceSearchResult:
        effective = options or EvidenceSearchOptions()
        index_ref = self.resolve_index(
            intelligence_id=intelligence_id,
            watch_session_id=watch_session_id,
            rows=rows,
            feed=effective.feed,
            index_mode=effective.index_mode,
        )
        return self.search_docs(
            docs=index_ref["docs"],
            query_text=query_text,
            top_k=effective.top_k,
            index_ref=index_ref,
            index_mode=effective.index_mode,
        )

    def search_docs(
        self,
        *,
        docs: list[dict[str, Any]],
        query_text: str,
        top_k: int,
        index_ref: dict[str, Any] | None = None,
        index_mode: str = "off",
    ) -> EvidenceSearchResult:
        index_ref = index_ref or {
            "index_status": "off",
            "index_path": None,
            "fingerprint": None,
            "docs": docs,
        }
        hits = hybrid_search_finance_docs(
            docs=docs,
            query_text=query_text,
            top_k=top_k,
            keyword_index_dir=index_ref.get("keyword_index_path"),
        )
        retrieval = {
            "mode": "finance_evidence_hybrid_search",
            "retriever": self.retriever_name,
            "retriever_version": self.retriever_version,
            "keyword": "bm25_keyword_index",
            "semantic": {
                "status": FINANCE_EVIDENCE_SEMANTIC_STATUS,
                "reason": FINANCE_EVIDENCE_SEMANTIC_REASON,
            },
            "fusion": "rrf",
            "rank_constant": 60,
            "structured_features": ["feed_priority", "symbol_match", "signal_priority", "recency", "source_score"],
            "index_mode": index_mode,
            "index_status": index_ref["index_status"],
            "index_path": index_ref.get("index_path"),
            "index_fingerprint": index_ref.get("fingerprint"),
            "doc_count": len(docs),
        }
        return EvidenceSearchResult(hits=hits, index_ref=index_ref, retrieval=retrieval)


DEFAULT_FINANCE_EVIDENCE_RETRIEVER = FinanceEvidenceRetriever()


def hybrid_search_finance_rows(*, rows: list[dict[str, Any]], query_text: str, top_k: int) -> list[dict[str, Any]]:
    docs = finance_evidence_docs(rows)
    return hybrid_search_finance_docs(docs=docs, query_text=query_text, top_k=top_k)


def hybrid_search_finance_docs(
    *,
    docs: list[dict[str, Any]],
    query_text: str,
    top_k: int,
    keyword_index_dir: str | pathlib.Path | None = None,
) -> list[dict[str, Any]]:
    if not docs:
        return []
    top_k = max(1, int(top_k))
    window = min(len(docs), max(top_k * 4, top_k))
    keyword_scores: dict[str, float] = {}
    keyword_rank: list[str] = []
    if keyword_index_dir is not None:
        keyword_table = safe_search_keyword_index(keyword_index_dir, query_text=query_text, top_k=window)
        for row in keyword_table.to_pylist():
            doc_id = str(row.get("doc_id"))
            keyword_rank.append(doc_id)
            keyword_scores[doc_id] = float(row.get("keyword_score") or 0.0)
    else:
        with tempfile.TemporaryDirectory(prefix="velaria-finance-evidence-index-") as tmp:
            table = pa.Table.from_pylist(docs)
            build_keyword_index([table], output_dir=tmp, text_columns=["title", "summary", "body"], analyzer="builtin", doc_id_field="doc_id")
            keyword_table = safe_search_keyword_index(tmp, query_text=query_text, top_k=window)
            for row in keyword_table.to_pylist():
                doc_id = str(row.get("doc_id"))
                keyword_rank.append(doc_id)
                keyword_scores[doc_id] = float(row.get("keyword_score") or 0.0)
    semantic_scores = {str(doc["doc_id"]): 0.0 for doc in docs}
    semantic_rank: list[str] = []
    recency_rank = [doc["doc_id"] for doc in sorted(docs, key=lambda doc: str(doc.get("event_time") or ""), reverse=True)[:window]]
    structured_scores = {doc["doc_id"]: finance_structured_evidence_score(doc, query_text=query_text) for doc in docs}
    structured_rank = [doc_id for doc_id, score in sorted(structured_scores.items(), key=lambda item: (-item[1], item[0]))[:window] if score > 0.0]
    rank_lists = {
        "keyword": keyword_rank,
        "semantic": semantic_rank,
        "recency": recency_rank,
        "structured": structured_rank,
    }
    rank_maps = {
        name: {doc_id: rank for rank, doc_id in enumerate(rank_list, start=1)}
        for name, rank_list in rank_lists.items()
    }
    fused: list[dict[str, Any]] = []
    for doc in docs:
        doc_id = doc["doc_id"]
        rrf_score = 0.0
        rank_details: dict[str, int] = {}
        for name, rank_map in rank_maps.items():
            rank = rank_map.get(doc_id)
            if rank is None:
                continue
            rank_details[name] = rank
            rrf_score += 1.0 / (60.0 + rank)
        if rrf_score <= 0.0:
            continue
        keyword_score = keyword_scores.get(doc_id, 0.0)
        semantic_score = semantic_scores.get(doc_id, 0.0)
        structured_score = structured_scores.get(doc_id, 0.0)
        if keyword_score > 0.0:
            reason = "keyword_match"
        elif structured_score > 0.0:
            reason = "structured_match"
        else:
            reason = "recency_match"
        fused.append(
            {
                "target_kind": doc["target_kind"],
                "target_id": doc_id,
                "title": doc["title"],
                "score": round(rrf_score + (structured_score * 0.005), 6),
                "score_breakdown": {
                    "rrf_score": round(rrf_score, 6),
                    "keyword_score": round(keyword_score, 6),
                    "semantic_score": round(semantic_score, 6),
                    "structured_score": round(structured_score, 6),
                    "ranks": rank_details,
                },
                "match_reason": reason,
                "matched_fields": ["title", "summary", "body"],
                "source_ref": doc["source_ref"],
                "snippet": doc["summary"] or doc["body"][:220],
                "row": doc["row"],
            }
        )
    fused.sort(key=lambda item: (-float(item["score"]), str(item["target_id"])))
    return fused[:top_k]


def safe_search_keyword_index(index_dir: str | pathlib.Path, *, query_text: str, top_k: int) -> pa.Table:
    try:
        return search_keyword_index(index_dir, query_text=query_text, top_k=top_k)
    except ValueError:
        return pa.Table.from_pylist([])


def resolve_finance_evidence_search_index(
    *,
    intelligence_id: str,
    watch_session_id: str,
    rows: list[dict[str, Any]],
    feed: str,
    index_mode: str,
) -> dict[str, Any]:
    normalized_mode = index_mode if index_mode in {"auto", "rebuild", "off"} else "auto"
    docs = finance_evidence_docs(rows)
    if normalized_mode == "off":
        return {
            "index_status": "off",
            "index_path": None,
            "fingerprint": finance_evidence_fingerprint(rows),
            "docs": docs,
        }

    fingerprint = finance_evidence_fingerprint(rows)
    if normalized_mode == "auto":
        loaded, reason = load_finance_evidence_index(watch_session_id=watch_session_id, feed=feed, expected_fingerprint=fingerprint)
        if loaded is not None:
            loaded["index_status"] = "hit"
            return loaded
        status = "rebuilt" if reason == "stale" else "built"
    else:
        status = "rebuilt"

    built = build_finance_evidence_index(
        intelligence_id=intelligence_id,
        watch_session_id=watch_session_id,
        rows=rows,
        feed=feed,
    )
    loaded, _ = load_finance_evidence_index(watch_session_id=watch_session_id, feed=feed, expected_fingerprint=fingerprint)
    if loaded is None:
        return {
            "index_status": status,
            "index_path": built.get("index_path"),
            "fingerprint": fingerprint,
            "docs": docs,
        }
    loaded["index_status"] = status
    return loaded


def finance_evidence_index_dir(*, watch_session_id: str, feed: str) -> pathlib.Path:
    from velaria.workspace.paths import get_velaria_home

    return get_velaria_home() / "finance" / "evidence_indexes" / _sql_identifier_suffix(watch_session_id) / _sql_identifier_suffix(feed)


def finance_evidence_index_metadata_for_payload(index_ref: dict[str, Any]) -> dict[str, Any] | None:
    metadata = dict(index_ref.get("metadata") or {})
    for key in ("index_path", "metadata_path", "docs_path", "keyword_index_path", "fingerprint"):
        if index_ref.get(key) is not None:
            metadata.setdefault(key, index_ref.get(key))
    if not metadata:
        return None
    metadata.setdefault("status", "ready")
    metadata.setdefault("doc_count", len(index_ref.get("docs") or []))
    return metadata


def finance_evidence_fingerprint(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        payload = _watch_row_payload(row)
        stable = {
            "feed": row.get("feed") or payload.get("feed"),
            "event_id": row.get("event_id") or payload.get("event_id"),
            "event_time": row.get("event_time") or payload.get("event_time"),
            "ingested_at": row.get("ingested_at"),
            "payload": compact_finance_evidence_payload(payload),
        }
        digest.update(json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def build_finance_evidence_index(
    *,
    intelligence_id: str,
    watch_session_id: str,
    rows: list[dict[str, Any]],
    feed: str,
) -> dict[str, Any]:
    docs = finance_evidence_docs(rows)
    index_dir = finance_evidence_index_dir(watch_session_id=watch_session_id, feed=feed)
    if index_dir.exists():
        shutil.rmtree(index_dir)
    index_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = index_dir / "metadata.json"
    docs_path = index_dir / "docs.jsonl"
    keyword_index_path = index_dir / "keyword"
    fingerprint = finance_evidence_fingerprint(rows)
    built_at = _utc_payload_time()

    with docs_path.open("w", encoding="utf-8") as handle:
        for doc in docs:
            handle.write(json.dumps(doc, ensure_ascii=False, sort_keys=True, default=str) + "\n")

    if docs:
        build_keyword_index(
            [pa.Table.from_pylist(docs)],
            output_dir=keyword_index_path,
            text_columns=["title", "summary", "body"],
            analyzer="builtin",
            doc_id_field="doc_id",
        )
    else:
        keyword_index_path.mkdir(parents=True, exist_ok=True)

    metadata = {
        "status": "ready",
        "index_version": FINANCE_EVIDENCE_INDEX_VERSION,
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "feed": feed,
        "fingerprint": fingerprint,
        "built_at": built_at,
        "row_count": len(rows),
        "doc_count": len(docs),
        "retriever": FinanceEvidenceRetriever.retriever_name,
        "retriever_version": FinanceEvidenceRetriever.retriever_version,
        "semantic_status": FINANCE_EVIDENCE_SEMANTIC_STATUS,
        "semantic_reason": FINANCE_EVIDENCE_SEMANTIC_REASON,
        "index_path": str(index_dir),
        "metadata_path": str(metadata_path),
        "docs_path": str(docs_path),
        "keyword_index_path": str(keyword_index_path),
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return metadata


def load_finance_evidence_index(
    *,
    watch_session_id: str,
    feed: str,
    expected_fingerprint: str,
) -> tuple[dict[str, Any] | None, str]:
    index_dir = finance_evidence_index_dir(watch_session_id=watch_session_id, feed=feed)
    metadata_path = index_dir / "metadata.json"
    docs_path = index_dir / "docs.jsonl"
    keyword_index_path = index_dir / "keyword"
    if not metadata_path.exists() or not docs_path.exists():
        return None, "missing"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, "invalid"
    if int(metadata.get("index_version") or 0) != FINANCE_EVIDENCE_INDEX_VERSION:
        return None, "stale"
    if str(metadata.get("fingerprint") or "") != expected_fingerprint:
        return None, "stale"
    if not keyword_index_path.exists():
        return None, "invalid"

    docs: list[dict[str, Any]] = []
    with docs_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                docs.append(json.loads(text))
    return {
        "index_status": "hit",
        "index_path": str(index_dir),
        "metadata_path": str(metadata_path),
        "docs_path": str(docs_path),
        "keyword_index_path": str(keyword_index_path),
        "fingerprint": metadata.get("fingerprint"),
        "metadata": metadata,
        "docs": docs,
    }, "hit"


def finance_evidence_docs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        payload = _watch_row_payload(row)
        feed = str(row.get("feed") or payload.get("feed") or "unknown")
        symbol = str(payload.get("symbol") or payload.get("source_key") or "")
        event_type = str(payload.get("event_type") or row.get("event_type") or feed)
        title = str(payload.get("title") or payload.get("summary") or f"{feed} {symbol} {event_type}").strip()
        summary = str(payload.get("summary") or payload.get("message") or payload.get("signal_type") or "").strip()
        body = json.dumps(compact_finance_evidence_payload(payload), ensure_ascii=False, sort_keys=True)
        doc_id = f"{feed}:{payload.get('event_id') or row.get('event_id') or index}"
        docs.append(
            {
                "doc_id": doc_id,
                "target_kind": feed,
                "title": title,
                "summary": summary,
                "body": body,
                "search_text": f"{feed}\n{symbol}\n{event_type}\n{title}\n{summary}\n{body}",
                "event_time": payload.get("event_time") or row.get("event_time"),
                "source_ref": {
                    "feed": feed,
                    "source_id": row.get("source_id"),
                    "symbol": symbol or None,
                    "event_type": event_type,
                    "event_time": payload.get("event_time") or row.get("event_time"),
                },
                "row": compact_finance_evidence_payload(payload),
            }
        )
    return docs


def compact_finance_evidence_payload(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "watch_session_id",
        "event_time",
        "event_type",
        "market",
        "symbol",
        "rank",
        "score",
        "period_return_pct",
        "quote_pct_change",
        "news_sentiment_label",
        "momentum_state",
        "signal_type",
        "signal_policy_source",
        "signal_policy_preset",
        "signal_policy_json",
        "provider",
        "source_category",
        "source_type",
        "source_score",
        "source_score_reason",
        "freshness",
        "error_type",
        "message",
        "title",
        "summary",
        "publisher",
        "published_at",
        "fiscal_period_end",
        "revenue",
        "net_income",
    ]
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


def finance_structured_evidence_score(doc: dict[str, Any], *, query_text: str) -> float:
    source_ref = doc.get("source_ref") or {}
    row = doc.get("row") or {}
    query = query_text.lower()
    score = 0.0
    feed = str(source_ref.get("feed") or "")
    score += {"native_stream_signals": 5.0, "candidates": 4.0, "features": 3.0, "news": 2.5, "fundamentals": 2.5}.get(feed, 1.0)
    symbol = str(source_ref.get("symbol") or "").lower()
    if symbol and symbol in query:
        score += 4.0
    if row.get("signal_type"):
        score += 3.0
    if row.get("error_type"):
        score += 2.0
    if row.get("news_sentiment_label") in {"negative", "positive"}:
        score += 1.0
    try:
        score += min(2.0, max(0.0, float(row.get("source_score") or 0.0) * 2.0))
    except (TypeError, ValueError):
        pass
    return score


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
