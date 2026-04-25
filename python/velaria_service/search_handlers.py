from __future__ import annotations

from typing import Any

from velaria.agentic_search import search_datasets, search_events, search_fields, search_templates
from velaria.agentic_store import AgenticStore

from ._helpers import (
    _dataset_docs_from_store,
    _event_docs_from_store,
    _field_docs_from_store,
)


def handle_search_templates(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    hits = search_templates(str(payload.get("query_text") or ""), top_k=int(payload.get("top_k", 10)))
    return {"ok": True, "hits": hits, "query_text": payload.get("query_text"), "retrieval_mode": "hybrid"}


def handle_search_events(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    hits = search_events(str(payload.get("query_text") or ""), _event_docs_from_store(store), top_k=int(payload.get("top_k", 10)))
    return {"ok": True, "hits": hits, "query_text": payload.get("query_text"), "retrieval_mode": "hybrid"}


def handle_search_datasets(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    hits = search_datasets(str(payload.get("query_text") or ""), _dataset_docs_from_store(store), top_k=int(payload.get("top_k", 10)))
    return {"ok": True, "hits": hits, "query_text": payload.get("query_text"), "retrieval_mode": "hybrid"}


def handle_search_fields(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    hits = search_fields(str(payload.get("query_text") or ""), _field_docs_from_store(store), top_k=int(payload.get("top_k", 10)))
    return {"ok": True, "hits": hits, "query_text": payload.get("query_text"), "retrieval_mode": "hybrid"}


def handle_grounding(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    query_text = str(payload.get("query_text") or "")
    top_k = int(payload.get("top_k", 5))
    bundle_payload = {
        "template_hits": search_templates(query_text, top_k=top_k),
        "event_hits": search_events(query_text, _event_docs_from_store(store), top_k=top_k),
        "dataset_hits": search_datasets(query_text, _dataset_docs_from_store(store), top_k=top_k),
        "field_hits": search_fields(query_text, _field_docs_from_store(store), top_k=top_k),
    }
    bundle = store.save_grounding_bundle(query_text, bundle_payload)
    return {"ok": True, **bundle, "payload": bundle_payload}
