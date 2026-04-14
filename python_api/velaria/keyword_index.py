from __future__ import annotations

import json
import pathlib
from collections import Counter
from typing import Any, Iterable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


def _normalize_text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(part for part in (str(item).strip() for item in value) if part)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return str(value).strip()


def _tokenize_keyword_text_builtin(text: str) -> list[str]:
    def decode_utf8(input_text: str, offset: int) -> tuple[str, int, int]:
        first = ord(input_text[offset])
        if first < 0x80:
            return input_text[offset], first, offset + 1
        raw = input_text.encode("utf-8")
        byte_offset = len(input_text[:offset].encode("utf-8"))
        first_byte = raw[byte_offset]
        if (first_byte & 0xE0) == 0xC0:
            width = 2
        elif (first_byte & 0xF0) == 0xE0:
            width = 3
        elif (first_byte & 0xF8) == 0xF0:
            width = 4
        else:
            return input_text[offset], first, offset + 1
        token = raw[byte_offset : byte_offset + width].decode("utf-8", errors="ignore")
        if not token:
            return input_text[offset], first, offset + 1
        return token, ord(token[0]), offset + 1

    def is_cjk(codepoint: int) -> bool:
        return (
            0x3400 <= codepoint <= 0x4DBF
            or 0x4E00 <= codepoint <= 0x9FFF
            or 0xF900 <= codepoint <= 0xFAFF
            or 0x3040 <= codepoint <= 0x30FF
            or 0xAC00 <= codepoint <= 0xD7AF
        )

    def append_cjk_tokens(chars: list[str], out: list[str]) -> None:
        if not chars:
            return
        max_window = min(3, len(chars))
        for window in range(1, max_window + 1):
            for index in range(0, len(chars) - window + 1):
                out.append("".join(chars[index : index + window]))
        if 2 <= len(chars) <= 8:
            out.append("".join(chars))

    out: list[str] = []
    ascii_current = ""
    cjk_chars: list[str] = []

    def flush_ascii() -> None:
        nonlocal ascii_current
        if ascii_current:
            out.append(ascii_current)
            ascii_current = ""

    def flush_cjk() -> None:
        nonlocal cjk_chars
        append_cjk_tokens(cjk_chars, out)
        cjk_chars = []

    offset = 0
    while offset < len(text):
        token, codepoint, next_offset = decode_utf8(text, offset)
        offset = next_offset
        if len(token) == 1 and token.isascii() and token.isalnum():
            flush_cjk()
            ascii_current += token.lower()
            continue
        if is_cjk(codepoint):
            flush_ascii()
            cjk_chars.append(token)
            continue
        flush_ascii()
        flush_cjk()

    flush_ascii()
    flush_cjk()
    return out


def _tokenize_keyword_text_jieba(text: str) -> list[str]:
    try:
        import jieba
    except ImportError as exc:  # pragma: no cover - env dependent
        raise ImportError(
            "jieba analyzer requires the `jieba` Python package. "
            "Install it in python_api before building keyword indexes with analyzer=jieba."
        ) from exc
    tokens = [token.strip() for token in jieba.cut_for_search(text) if token.strip()]
    normalized: list[str] = []
    for token in tokens:
        if token.isascii() and token.isalnum():
            normalized.append(token.lower())
        else:
            normalized.append(token)
    return normalized


def tokenize_keyword_text(text: str, *, analyzer: str = "jieba") -> list[str]:
    normalized = (analyzer or "jieba").strip().lower()
    if normalized == "builtin":
        return _tokenize_keyword_text_builtin(text)
    if normalized == "jieba":
        return _tokenize_keyword_text_jieba(text)
    raise ValueError(f"unsupported analyzer: {analyzer}")


def _build_doc_rows(
    tables: Sequence[pa.Table],
    *,
    text_columns: Sequence[str],
    source_labels: Sequence[str] | None = None,
    doc_id_field: str = "doc_id",
    analyzer: str = "jieba",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    labels = list(source_labels) if source_labels is not None else []
    for table_index, table in enumerate(tables):
        label = labels[table_index] if table_index < len(labels) else f"dataset_{table_index + 1}"
        for row_index, row in enumerate(table.to_pylist()):
            doc = dict(row)
            text_parts = []
            for column in text_columns:
                value = _normalize_text_value(doc.get(column))
                if value:
                    text_parts.append(value)
            text = "\n".join(text_parts)
            tokens = tokenize_keyword_text(text, analyzer=analyzer)
            doc_id = _normalize_text_value(doc.get(doc_id_field))
            if not doc_id:
                doc_id = f"doc-{len(rows) + 1}"
            doc["doc_id"] = doc_id
            doc["__doc_id"] = len(rows)
            doc["__source_dataset"] = label
            doc["__source_row_id"] = row_index
            doc["__doc_join_key"] = f"{label}:{row_index}"
            doc["__doc_length"] = len(tokens)
            rows.append(doc)
    return rows


def build_keyword_index(
    tables: Sequence[pa.Table],
    *,
    output_dir: str | pathlib.Path,
    text_columns: Sequence[str],
    source_labels: Sequence[str] | None = None,
    analyzer: str = "jieba",
    doc_id_field: str = "doc_id",
) -> dict[str, Any]:
    if not tables:
        raise ValueError("tables must not be empty")
    if not text_columns:
        raise ValueError("text_columns must not be empty")

    output_path = pathlib.Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    doc_rows = _build_doc_rows(
        tables,
        text_columns=text_columns,
        source_labels=source_labels,
        doc_id_field=doc_id_field,
        analyzer=analyzer,
    )
    if not doc_rows:
        raise ValueError("keyword index input rows must not be empty")

    merged_docs = pa.Table.from_pylist(doc_rows)

    term_to_id: dict[str, int] = {}
    document_frequency: Counter[str] = Counter()
    postings_rows: list[dict[str, Any]] = []

    for doc in doc_rows:
        text_parts = []
        for column in text_columns:
            value = _normalize_text_value(doc.get(column))
            if value:
                text_parts.append(value)
        tokens = tokenize_keyword_text("\n".join(text_parts), analyzer=analyzer)
        term_freq = Counter(tokens)
        for term, tf in term_freq.items():
            if term not in term_to_id:
                term_to_id[term] = len(term_to_id)
            document_frequency[term] += 1
            postings_rows.append(
                {
                    "term_id": term_to_id[term],
                    "doc_id": doc["__doc_id"],
                    "tf": tf,
                }
            )

    terms_rows = [
        {
            "term_id": term_id,
            "term": term,
            "df": document_frequency[term],
        }
        for term, term_id in sorted(term_to_id.items(), key=lambda item: item[1])
    ]

    docs_path = output_path / "docs.parquet"
    postings_path = output_path / "postings.parquet"
    terms_path = output_path / "terms.parquet"
    manifest_path = output_path / "manifest.json"

    pq.write_table(merged_docs, docs_path)
    pq.write_table(pa.Table.from_pylist(postings_rows), postings_path)
    pq.write_table(pa.Table.from_pylist(terms_rows), terms_path)

    manifest = {
        "format": "keyword-index-v1",
        "analyzer": analyzer,
        "text_columns": list(text_columns),
        "doc_count": merged_docs.num_rows,
        "dataset_count": len(tables),
        "term_count": len(terms_rows),
        "posting_count": len(postings_rows),
        "docs_path": docs_path.name,
        "postings_path": postings_path.name,
        "terms_path": terms_path.name,
        "schema": merged_docs.schema.names,
        "source_labels": list(source_labels or []),
        "doc_id_field": doc_id_field,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def load_keyword_index(index_dir: str | pathlib.Path) -> dict[str, Any]:
    base = pathlib.Path(index_dir)
    manifest = json.loads((base / "manifest.json").read_text(encoding="utf-8"))
    docs = pq.read_table(base / manifest["docs_path"])
    postings = pq.read_table(base / manifest["postings_path"])
    terms = pq.read_table(base / manifest["terms_path"])
    return {
        "base": base,
        "manifest": manifest,
        "docs": docs,
        "postings": postings,
        "terms": terms,
    }


def search_keyword_index(
    index_dir: str | pathlib.Path,
    *,
    query_text: str,
    top_k: int,
    allowed_doc_ids: set[int] | None = None,
) -> pa.Table:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    loaded = load_keyword_index(index_dir)
    docs = loaded["docs"]
    postings = loaded["postings"].to_pylist()
    terms_rows = loaded["terms"].to_pylist()
    analyzer = str(loaded["manifest"].get("analyzer") or "jieba")
    query_terms = tokenize_keyword_text(query_text, analyzer=analyzer)
    if not query_terms:
        raise ValueError("query_text must contain at least one token")

    term_to_meta = {row["term"]: row for row in terms_rows}
    query_meta = [term_to_meta[term] for term in dict.fromkeys(query_terms) if term in term_to_meta]
    if not query_meta:
        return pa.Table.from_pylist([])

    docs_rows = docs.to_pylist()
    if allowed_doc_ids is not None:
        docs_rows = [row for row in docs_rows if int(row["__doc_id"]) in allowed_doc_ids]
    doc_length = {int(row["__doc_id"]): int(row.get("__doc_length") or 0) for row in docs_rows}
    doc_count = len(doc_length)
    avg_doc_length = (sum(doc_length.values()) / doc_count) if doc_count else 0.0
    term_ids = {int(row["term_id"]) for row in query_meta}

    per_doc_tf: dict[int, dict[int, int]] = {}
    subset_df: Counter[int] = Counter()
    for row in postings:
        term_id = int(row["term_id"])
        doc_id = int(row["doc_id"])
        if term_id not in term_ids:
            continue
        if allowed_doc_ids is not None and doc_id not in allowed_doc_ids:
            continue
        per_doc_tf.setdefault(doc_id, {})[term_id] = int(row["tf"])
    for tf_map in per_doc_tf.values():
        for term_id in tf_map:
            subset_df[term_id] += 1

    k1 = 1.2
    b = 0.75
    scored_rows: list[tuple[int, float]] = []
    for doc_id, tf_map in per_doc_tf.items():
        score = 0.0
        for term in query_meta:
            term_id = int(term["term_id"])
            tf = tf_map.get(term_id, 0)
            if tf <= 0:
                continue
            df = float(subset_df[term_id] if allowed_doc_ids is not None else term["df"])
            idf = float(__import__("math").log(1.0 + ((doc_count - df + 0.5) / (df + 0.5))))
            length_norm = (1.0 - b + b * (doc_length.get(doc_id, 0) / avg_doc_length)) if avg_doc_length > 0 else 1.0
            score += idf * ((tf * (k1 + 1.0)) / (tf + k1 * length_norm))
        if score > 0.0:
            scored_rows.append((doc_id, score))

    scored_rows.sort(key=lambda item: (-item[1], item[0]))
    doc_by_id = {int(row["__doc_id"]): row for row in docs_rows}
    out_rows = []
    for doc_id, score in scored_rows[:top_k]:
        row = dict(doc_by_id[doc_id])
        row["keyword_score"] = score
        out_rows.append(row)
    return pa.Table.from_pylist(out_rows)
