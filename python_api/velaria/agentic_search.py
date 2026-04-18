from __future__ import annotations

import math
import re
from typing import Any

from .embedding import HashEmbeddingProvider
from .agentic_templates import builtin_templates


_TOKEN_RE = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


def _tokens(text: str) -> list[str]:
    return [match.group(0).lower() for match in _TOKEN_RE.finditer(text)]


def _keyword_score(query: str, text: str) -> float:
    query_tokens = set(_tokens(query))
    target_tokens = set(_tokens(text))
    if not query_tokens or not target_tokens:
        return 0.0
    return len(query_tokens & target_tokens) / max(1, len(query_tokens))


def _cosine(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def _embed_texts(texts: list[str]) -> list[list[float]]:
    provider = HashEmbeddingProvider(dimension=32)
    return provider.embed(texts, model="hash-agentic")


def _search_docs(query_text: str, docs: list[dict[str, Any]], *, top_k: int = 10) -> list[dict[str, Any]]:
    query_vector = _embed_texts([query_text])[0]
    doc_vectors = _embed_texts([f"{doc.get('title', '')}\n{doc.get('summary', '')}\n{doc.get('body', '')}" for doc in docs])
    results: list[dict[str, Any]] = []
    for idx, doc in enumerate(docs):
        text = f"{doc.get('title', '')}\n{doc.get('summary', '')}\n{doc.get('body', '')}"
        keyword = _keyword_score(query_text, text)
        embedding = max(0.0, _cosine(query_vector, doc_vectors[idx]))
        hybrid = (keyword * 0.55) + (embedding * 0.45)
        if keyword == 0.0 and embedding == 0.0:
            continue
        if keyword > 0.0 and embedding > 0.0:
            reason = "hybrid_match"
        elif keyword > 0.0:
            reason = "keyword_match"
        else:
            reason = "embedding_match"
        results.append(
            {
                "target_kind": doc["target_kind"],
                "target_id": doc["doc_id"],
                "title": doc.get("title", ""),
                "score": hybrid,
                "score_breakdown": {
                    "keyword_score": keyword,
                    "embedding_score": embedding,
                    "hybrid_score": hybrid,
                },
                "match_reason": reason,
                "matched_fields": [field for field in ("title", "summary", "body") if keyword > 0.0 or embedding > 0.0],
                "source_ref": doc.get("metadata", {}),
                "snippet": doc.get("summary") or doc.get("body", "")[:160],
            }
        )
    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:top_k]


def template_search_docs() -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for template in builtin_templates():
        docs.append(
            {
                "doc_id": template["template_id"],
                "target_kind": "template",
                "title": template["name"],
                "summary": template["description"],
                "body": " ".join(template.get("tags", [])),
                "tags": template.get("tags", []),
                "metadata": {"template_id": template["template_id"], "category": template["category"]},
            }
        )
    return docs


def search_templates(query_text: str, *, top_k: int = 10) -> list[dict[str, Any]]:
    return _search_docs(query_text, template_search_docs(), top_k=top_k)


def search_events(query_text: str, docs: list[dict[str, Any]], *, top_k: int = 10) -> list[dict[str, Any]]:
    return _search_docs(query_text, docs, top_k=top_k)


def search_datasets(query_text: str, docs: list[dict[str, Any]], *, top_k: int = 10) -> list[dict[str, Any]]:
    return _search_docs(query_text, docs, top_k=top_k)


def search_fields(query_text: str, docs: list[dict[str, Any]], *, top_k: int = 10) -> list[dict[str, Any]]:
    return _search_docs(query_text, docs, top_k=top_k)
