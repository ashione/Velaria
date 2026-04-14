from __future__ import annotations

import argparse
import json
import os
import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import velaria.cli as cli_impl
from velaria import (
    BitableClient,
    Session,
    __version__,
    DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL,
    annotate_source_arrow_table,
    build_keyword_index,
    build_file_embeddings,
    load_keyword_index,
    read_embedding_table,
    search_keyword_index,
    materialize_mixed_text_embeddings_stream,
    query_file_embeddings,
    read_excel,
)
from velaria.workspace.artifact_index import RunDeleteConflictError
from velaria.workspace import (
    ArtifactIndex,
    append_progress_snapshot,
    append_stderr,
    create_run,
    finalize_run,
    read_run,
    update_run,
    write_explain,
    write_inputs,
)


_EXCEL_SUFFIXES = {".xlsx", ".xlsm", ".xls"}
_PARQUET_SUFFIXES = {".parquet", ".pq"}
_ARROW_SUFFIXES = {".arrow", ".ipc", ".feather"}
_BITABLE_TIMEOUT_SECONDS = 30
_BITABLE_PAGE_SIZE = 200


def _json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def _normalize_path(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _resolve_auto_input_payload(session: Session, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    input_type = str(payload.get("input_type") or "auto").lower()
    effective_input_type = "parquet" if input_type == "bitable" else input_type
    resolved = dict(payload)
    if input_type != "auto":
        return input_type, resolved

    input_path = _normalize_path(payload["input_path"])
    suffix = input_path.suffix.lower()
    if suffix in _EXCEL_SUFFIXES:
        return "excel", resolved
    if suffix in _PARQUET_SUFFIXES:
        return "parquet", resolved
    if suffix in _ARROW_SUFFIXES:
        return "arrow", resolved

    if not hasattr(session, "probe"):
        return "auto", resolved
    try:
        probe = session.probe(str(input_path))
    except Exception:
        return "auto", resolved

    kind = str(probe.get("kind") or "auto").lower()
    if kind == "csv":
        resolved.setdefault("delimiter", probe.get("delimiter") or ",")
        return "csv", resolved
    if kind == "json":
        columns = probe.get("columns") or probe.get("schema") or []
        if columns and not resolved.get("columns"):
            resolved["columns"] = ",".join(str(item) for item in columns)
        resolved.setdefault("json_format", probe.get("format") or probe.get("final_format") or "json_lines")
        return "json", resolved
    if kind == "line":
        mappings = probe.get("mappings") or []
        if mappings and not resolved.get("mappings"):
            resolved["mappings"] = [
                {
                    "name": item["column"],
                    "index": item["source_index"],
                }
                for item in mappings
            ]
        resolved.setdefault("line_mode", probe.get("mode") or "split")
        if resolved.get("line_mode") == "split":
            resolved.setdefault("delimiter", probe.get("delimiter") or "|")
        return "line", resolved
    return "auto", resolved


def _timestamp_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _default_bitable_dataset_name(bitable_url: str) -> str:
    try:
        _, table_id, _ = BitableClient("placeholder", "placeholder").parse_bitable_url(bitable_url)
        return f"bitable-{table_id}"
    except Exception:
        return "bitable"


def _normalize_bitable_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalize_bitable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({str(key): _normalize_bitable_value(value) for key, value in row.items()})
    return normalized


def _resolve_bitable_credentials(payload: dict[str, Any]) -> tuple[str, str]:
    app_id = str(payload.get("app_id") or payload.get("bitable_app_id") or "").strip() or os.environ.get(
        "FEISHU_BITABLE_APP_ID", ""
    )
    app_secret = (
        str(payload.get("app_secret") or payload.get("bitable_app_secret") or "").strip()
        or os.environ.get("FEISHU_BITABLE_APP_SECRET", "")
    )
    if not app_id or not app_secret:
        raise ValueError("bitable app_id and app_secret are required")
    return app_id, app_secret


class ApiRouteNotFoundError(FileNotFoundError):
    pass


def _default_embedding_provider() -> str:
    return "minilm"


def _default_embedding_model(provider: str | None, model: str | None) -> str | None:
    normalized_provider = str(provider or "").strip().lower()
    if model:
        return model
    if normalized_provider == "minilm":
        return DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL
    return model


def _parse_string_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return _parse_csv_list(raw)
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    raise ValueError(f"invalid list payload: {raw}")


def _parse_mappings(raw: Any) -> list[tuple[str, int]]:
    mappings: list[tuple[str, int]] = []
    if not raw:
        return mappings
    if isinstance(raw, str):
        entries: list[Any] = [piece.strip() for piece in raw.split(",") if piece.strip()]
    elif isinstance(raw, (list, tuple)):
        entries = list(raw)
    else:
        raise ValueError(f"invalid mappings payload: {raw}")
    for item in entries:
        if isinstance(item, str):
            if ":" not in item:
                raise ValueError(f"invalid mappings entry: {item}")
            name, index_text = item.split(":", 1)
            mappings.append((name.strip(), int(index_text.strip())))
            continue
        if isinstance(item, dict):
            name = item.get("name") or item.get("column")
            index_value = item.get("index")
            if name is None or index_value is None:
                raise ValueError(f"invalid mappings entry: {item}")
            mappings.append((str(name).strip(), int(index_value)))
            continue
        if isinstance(item, (list, tuple)) and len(item) == 2:
            mappings.append((str(item[0]).strip(), int(item[1])))
            continue
        raise ValueError(f"invalid mappings entry: {item}")
    return mappings


def _load_arrow_table(input_path: Path):
    suffix = input_path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pq.read_table(str(input_path))
    if suffix in {".arrow", ".ipc", ".feather"}:
        with input_path.open("rb") as handle:
            try:
                return pa_ipc.open_file(handle).read_all()
            except Exception:
                handle.seek(0)
                return pa_ipc.open_stream(handle).read_all()
    raise ValueError(f"unsupported arrow-like file: {input_path}")


def _parse_path_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        return [value] if value else []
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    raise ValueError(f"invalid path list payload: {raw}")


def _load_arrow_tables(paths: list[str]) -> list[pa.Table]:
    if not paths:
        raise ValueError("dataset_path or dataset_paths is required")
    tables: list[pa.Table] = []
    for raw_path in paths:
        normalized = _normalize_path(raw_path)
        tables.append(_load_arrow_table(normalized))
    return tables


def _directory_artifact(
    path: Path,
    *,
    format_name: str,
    row_count: int | None,
    schema: list[str] | None,
    preview: dict[str, Any] | None,
    tags: list[str],
) -> dict[str, Any]:
    return {
        "type": "directory",
        "uri": cli_impl._uri_from_path(path),
        "format": format_name,
        "row_count": row_count,
        "schema_json": schema,
        "preview_json": preview,
        "tags_json": tags,
    }


def _run_duration_ms(run: dict[str, Any]) -> int | None:
    created_at = run.get("created_at")
    finished_at = run.get("finished_at")
    if not created_at or not finished_at:
        return None
    started = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return max(0, int((finished - started).total_seconds() * 1000))


def _enrich_run(index: ArtifactIndex, run: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(run)
    enriched["artifact_count"] = len(index.list_artifacts(run_id=run["run_id"], limit=1_000_000))
    enriched["duration_ms"] = _run_duration_ms(run)
    return enriched


def _load_dataframe(session: Session, payload: dict[str, Any]):
    input_type, resolved_payload = _resolve_auto_input_payload(session, payload)
    input_path = _normalize_path(resolved_payload["input_path"])
    suffix = input_path.suffix.lower()
    if input_type == "excel":
        return read_excel(
            session,
            str(input_path),
            sheet_name=int(resolved_payload.get("sheet_name", 0)),
            date_format=resolved_payload.get("date_format", "%Y-%m-%d"),
        )
    if input_type == "parquet":
        return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
    if input_type == "arrow":
        return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
    if input_type == "bitable":
        if suffix in _PARQUET_SUFFIXES | _ARROW_SUFFIXES:
            return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
        raise ValueError("bitable input expects a local parquet/arrow artifact path")
    if input_type == "auto":
        if suffix in {".parquet", ".pq", ".arrow", ".ipc", ".feather"}:
          return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
        return session.read(str(input_path))
    if input_type == "csv":
        delimiter = (resolved_payload.get("delimiter") or ",")[:1]
        return session.read_csv(str(input_path), delimiter=delimiter)
    if input_type == "json":
        columns = _parse_csv_list(resolved_payload.get("columns"))
        if not columns:
            raise ValueError("json input requires columns")
        return session.read_json(
            str(input_path),
            columns=columns,
            format=resolved_payload.get("json_format", "json_lines"),
        )
    if input_type == "line":
        mappings = _parse_mappings(resolved_payload.get("mappings"))
        if not mappings:
            raise ValueError("line input requires mappings")
        kwargs: dict[str, Any] = {"mappings": mappings}
        mode = resolved_payload.get("line_mode", "split")
        if mode == "regex":
            kwargs["mode"] = "regex"
            kwargs["regex_pattern"] = resolved_payload.get("regex_pattern") or ""
        else:
            kwargs["split_delimiter"] = resolved_payload.get("delimiter", "|")
        return session.read_line_file(str(input_path), **kwargs)
    raise ValueError(f"unsupported input_type: {input_type}")


def _preview_from_dataframe(df: Any, limit: int) -> dict[str, Any]:
    table = df.to_arrow()
    preview = cli_impl._preview_payload_from_table(table, limit=limit)
    preview["schema"] = table.schema.names
    preview["row_count"] = table.num_rows
    return preview


def _resolve_json_columns(payload: dict[str, Any], text_columns: list[str]) -> list[str]:
    columns = _parse_string_list(payload.get("columns"))
    if columns:
        return columns
    ordered: list[str] = []
    for column in text_columns:
        if column and column not in ordered:
            ordered.append(column)
    return ordered


def _score_semantics(metric: str) -> tuple[str, str]:
    normalized_metric = str(metric or "cosine").strip().lower()
    if normalized_metric == "dot":
        return "higher_is_better", "similarity"
    return "lower_is_better", "distance"


def _execute_embedding_build(payload: dict[str, Any], run_dir: Path, *, provider: Any, resolved_model: str) -> dict[str, Any]:
    session = Session()
    provider_name = str(payload.get("provider") or _default_embedding_provider())
    input_path = payload.get("input_path")
    if not input_path:
        raise ValueError("input_path is required")

    text_columns = _parse_string_list(payload.get("text_columns"))
    if not text_columns:
        raise ValueError("text_columns must not be empty")

    input_type = str(payload.get("input_type") or "auto").lower()
    effective_input_type = "parquet" if input_type == "bitable" else input_type
    template_version = str(payload.get("template_version") or "text-v1")
    vector_column = str(payload.get("vector_column") or "embedding")
    if vector_column != "embedding":
        raise ValueError("embedding-build currently materializes vectors into the 'embedding' column only")
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "embedding_dataset.parquet"
    )
    normalized_input_path = str(_normalize_path(str(input_path)))
    built_table = build_file_embeddings(
        session,
        normalized_input_path,
        provider=provider,
        model=resolved_model,
        template_version=template_version,
        text_columns=text_columns,
        input_type=effective_input_type,
        delimiter=(payload.get("delimiter") or ",")[:1],
        json_columns=_resolve_json_columns(payload, text_columns) if effective_input_type == "json" else None,
        json_format=str(payload.get("json_format") or "json_lines"),
        mappings=payload.get("mappings") or payload.get("columns"),
        line_mode=str(payload.get("line_mode") or "split"),
        regex_pattern=payload.get("regex_pattern"),
        sheet_name=payload.get("sheet_name", 0),
        date_format=str(payload.get("date_format") or "%Y-%m-%d"),
        doc_id_field=str(payload.get("doc_id_field") or "doc_id"),
        source_updated_at_field=str(payload.get("source_updated_at_field") or "source_updated_at"),
        output_path=output_path,
    )
    preview_limit = int(payload.get("preview_limit", 50))
    preview = cli_impl._preview_payload_from_table(built_table, limit=preview_limit)
    preview["schema"] = built_table.schema.names
    preview["row_count"] = built_table.num_rows
    artifacts = [cli_impl._table_artifact(output_path, built_table, ["dataset", "embedding-build"])]
    return {
        "payload": {
            "dataset_path": str(output_path),
            "input_path": normalized_input_path,
            "input_type": input_type,
            "text_columns": text_columns,
            "provider": provider_name,
            "model": resolved_model,
            "template_version": template_version,
            "vector_column": vector_column,
            "schema": built_table.schema.names,
            "row_count": built_table.num_rows,
        },
        "preview": preview,
        "artifacts": artifacts,
    }


def _execute_keyword_index_build(payload: dict[str, Any], run_dir: Path, *, run_id: str | None = None) -> dict[str, Any]:
    dataset_paths = _parse_path_list(payload.get("dataset_paths"))
    if payload.get("dataset_path"):
        dataset_paths = [str(payload["dataset_path"]), *dataset_paths]

    text_columns = _parse_string_list(payload.get("text_columns"))
    if not text_columns:
        raise ValueError("text_columns must not be empty")

    source_paths = [str(_normalize_path(path)) for path in dataset_paths]
    source_labels: list[str]
    doc_id_field = str(payload.get("doc_id_field") or "doc_id")
    if source_paths:
        tables = _load_arrow_tables(source_paths)
        source_labels = [Path(path).stem for path in source_paths]
    else:
        input_path = payload.get("input_path")
        if not input_path:
            raise ValueError("dataset_path, dataset_paths, or input_path is required")
        session = Session()
        source_df = _load_dataframe(
            session,
            {
                "input_path": str(_normalize_path(str(input_path))),
                "input_type": payload.get("input_type", "auto"),
                "delimiter": payload.get("delimiter", ","),
                "line_mode": payload.get("line_mode", "split"),
                "regex_pattern": payload.get("regex_pattern"),
                "mappings": payload.get("mappings"),
                "columns": payload.get("columns"),
                "json_format": payload.get("json_format", "json_lines"),
            },
        )
        tables = [source_df.to_arrow()]
        source_paths = [str(_normalize_path(str(input_path)))]
        source_labels = [Path(source_paths[0]).stem]
    output_dir = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "keyword_index"
    )
    analyzer = str(payload.get("analyzer") or "jieba")
    if run_id:
        _update_run_progress(
            run_id,
            progress_phase="building_keyword_index",
            keyword_dataset_count=len(tables),
            keyword_text_columns=text_columns,
            keyword_analyzer=analyzer,
        )
    manifest = build_keyword_index(
        tables,
        output_dir=output_dir,
        text_columns=text_columns,
        source_labels=source_labels,
        analyzer=analyzer,
        doc_id_field=doc_id_field,
    )
    docs_table = pq.read_table(output_dir / manifest["docs_path"])
    preview_limit = int(payload.get("preview_limit", 50))
    preview = cli_impl._preview_payload_from_table(docs_table, limit=preview_limit)
    preview["schema"] = docs_table.schema.names
    preview["row_count"] = docs_table.num_rows
    artifact = _directory_artifact(
        output_dir,
        format_name="keyword-index",
        row_count=manifest["doc_count"],
        schema=docs_table.schema.names,
        preview=preview,
        tags=["dataset", "keyword-index-build", "result"],
    )
    result_payload = {
        "index_path": str(output_dir),
        "dataset_paths": source_paths,
        "dataset_count": len(source_paths),
        "text_columns": text_columns,
        "analyzer": analyzer,
        "doc_id_field": doc_id_field,
        "doc_count": manifest["doc_count"],
        "term_count": manifest["term_count"],
        "posting_count": manifest["posting_count"],
        "schema": docs_table.schema.names,
    }
    if run_id:
        _update_run_progress(
            run_id,
            progress_phase="keyword_index_ready",
            keyword_doc_count=manifest["doc_count"],
            keyword_term_count=manifest["term_count"],
        )
    return {
        "payload": result_payload,
        "preview": preview,
        "artifacts": [artifact],
    }


def _execute_hybrid_search(payload: dict[str, Any], run_dir: Path, *, provider: Any, resolved_model: str) -> dict[str, Any]:
    session = Session()
    provider_name = str(payload.get("provider") or _default_embedding_provider())
    query_text = str(payload.get("query_text") or "").strip()
    if not query_text:
        raise ValueError("query_text must not be empty")

    top_k = int(payload.get("top_k", 10))
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    vector_column = str(payload.get("vector_column") or "embedding")
    template_version = str(payload.get("template_version") or "text-v1")
    metric = str(payload.get("metric") or "cosine")
    where_sql = payload.get("where_sql")
    index_path = payload.get("index_path")
    dataset_path = payload.get("dataset_path")
    if not dataset_path:
        raise ValueError(
            "dataset_path is required for hybrid search. Build embeddings first via /api/v1/runs/embedding-build."
        )
    normalized_dataset_path = str(_normalize_path(str(dataset_path)))
    keyword_scores: dict[str, float] = {}
    if index_path:
        normalized_index_path = str(_normalize_path(str(index_path)))
        loaded_index = load_keyword_index(normalized_index_path)
        allowed_doc_ids: set[int] | None = None
        if where_sql:
            docs_df = session.create_dataframe_from_arrow(loaded_index["docs"])
            session.create_temp_view("keyword_hybrid_candidates", docs_df)
            filtered = session.sql(
                f"SELECT __doc_id FROM keyword_hybrid_candidates WHERE {str(where_sql)}"
            ).to_arrow()
            allowed_doc_ids = {int(value) for value in filtered.column("__doc_id").to_pylist()}
        keyword_candidates = search_keyword_index(
            normalized_index_path,
            query_text=query_text,
            top_k=int(payload.get("keyword_top_k", max(top_k * 5, top_k))),
            allowed_doc_ids=allowed_doc_ids,
        )
        if keyword_candidates.num_rows == 0:
            result_table = pa.Table.from_pylist([])
        else:
            keyword_scores = {
                str(row.get("__doc_join_key") or row.get("doc_id")): float(row["keyword_score"])
                for row in keyword_candidates.to_pylist()
                if row.get("__doc_join_key") is not None or row.get("doc_id") is not None
            }
            embedding_table = read_embedding_table(normalized_dataset_path)
            join_column = "__doc_join_key" if "__doc_join_key" in embedding_table.column_names else "doc_id"
            if join_column not in embedding_table.column_names:
                raise ValueError("embedding dataset must contain __doc_join_key or doc_id for keyword+hybrid fusion")
            doc_ids = set(keyword_scores.keys())
            mask = pa.array(
                [str(doc_id) in doc_ids if doc_id is not None else False for doc_id in embedding_table.column(join_column).to_pylist()],
                type=pa.bool_(),
            )
            filtered_table = embedding_table.filter(mask)
            result_df = query_file_embeddings(
                session,
                filtered_table,
                provider=provider,
                model=resolved_model,
                query_text=query_text,
                vector_column=vector_column,
                top_k=top_k,
                metric=metric,
            )
            result_table = result_df.to_arrow()
    else:
        result_df = query_file_embeddings(
            session,
            normalized_dataset_path,
            provider=provider,
            model=resolved_model,
            query_text=query_text,
            vector_column=vector_column,
            top_k=top_k,
            metric=metric,
            where_sql=str(where_sql) if where_sql else None,
        )
        result_table = result_df.to_arrow()

    join_column = "__doc_join_key" if "__doc_join_key" in result_table.column_names else "doc_id"
    if keyword_scores and result_table.num_rows > 0 and join_column in result_table.column_names and "keyword_score" not in result_table.column_names:
        appended_scores = [
            keyword_scores.get(str(doc_id), 0.0)
            for doc_id in result_table.column(join_column).to_pylist()
        ]
        result_table = result_table.append_column("keyword_score", pa.array(appended_scores, type=pa.float64()))
    preview_limit = int(payload.get("preview_limit", 50))
    preview = cli_impl._preview_payload_from_table(result_table, limit=preview_limit)
    preview["schema"] = result_table.schema.names
    preview["row_count"] = result_table.num_rows
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "result.parquet"
    )
    artifacts = [cli_impl._table_artifact(output_path, result_table, ["result", "hybrid-search"])]
    score_order, score_kind = _score_semantics(metric)
    return {
        "payload": {
            "dataset_path": normalized_dataset_path,
            "query_text": query_text,
            "provider": provider_name,
            "model": resolved_model,
            "template_version": template_version,
            "top_k": top_k,
            "metric": metric,
            "where_sql": str(where_sql) if where_sql else None,
            "index_path": str(_normalize_path(str(index_path))) if index_path else None,
            "vector_column": vector_column,
            "score_semantics": score_order,
            "score_kind": score_kind,
            "schema": result_table.schema.names,
            "row_count": result_table.num_rows,
        },
        "preview": preview,
        "artifacts": artifacts,
    }


def _execute_keyword_search(payload: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    session = Session()
    query_text = str(payload.get("query_text") or "").strip()
    if not query_text:
        raise ValueError("query_text must not be empty")

    top_k = int(payload.get("top_k", 10))
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    index_path = payload.get("index_path")
    if not index_path:
        raise ValueError("index_path is required for keyword search. Build keyword index first.")
    normalized_index_path = str(_normalize_path(str(index_path)))
    where_sql = payload.get("where_sql")

    allowed_doc_ids: set[int] | None = None
    loaded_index = load_keyword_index(normalized_index_path)
    docs_table = loaded_index["docs"]
    if where_sql:
        docs_df = session.create_dataframe_from_arrow(docs_table)
        session.create_temp_view("keyword_docs_input", docs_df)
        filtered = session.sql(f"SELECT __doc_id FROM keyword_docs_input WHERE {str(where_sql)}").to_arrow()
        allowed_doc_ids = {int(value) for value in filtered.column("__doc_id").to_pylist()}

    result_table = search_keyword_index(
        normalized_index_path,
        query_text=query_text,
        top_k=top_k,
        allowed_doc_ids=allowed_doc_ids,
    )
    preview_limit = int(payload.get("preview_limit", 50))
    preview = cli_impl._preview_payload_from_table(result_table, limit=preview_limit)
    preview["schema"] = result_table.schema.names
    preview["row_count"] = result_table.num_rows
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "keyword_search_result.parquet"
    )
    artifacts = [cli_impl._table_artifact(output_path, result_table, ["result", "keyword-search"])]
    return {
        "payload": {
            "index_path": normalized_index_path,
            "query_text": query_text,
            "top_k": top_k,
            "where_sql": str(where_sql) if where_sql else None,
            "schema": result_table.schema.names,
            "row_count": result_table.num_rows,
        },
        "preview": preview,
        "artifacts": artifacts,
    }


def _execute_bitable_import(payload: dict[str, Any], run_dir: Path, *, run_id: str | None = None) -> dict[str, Any]:
    bitable_url = str(payload.get("bitable_url") or payload.get("input_path") or "").strip()
    if not bitable_url:
        raise ValueError("bitable_url is required")

    app_id, app_secret = _resolve_bitable_credentials(payload)

    client = BitableClient(
        app_id=app_id,
        app_secret=app_secret,
        request_timeout_seconds=int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
    )
    fetched_pages = 0

    def _on_page(fetched_rows: int, page_rows: int) -> None:
        nonlocal fetched_pages
        fetched_pages += 1
        if run_id:
            _update_run_progress(
                run_id,
                progress_phase="fetching_bitable",
                fetched_rows=fetched_rows,
                pages_fetched=fetched_pages,
                last_page_rows=page_rows,
            )

    rows = client.list_records_from_url(
        bitable_url,
        page_size=int(payload.get("page_size", _BITABLE_PAGE_SIZE)),
        on_page=_on_page,
    )
    normalized_rows = _normalize_bitable_rows(rows)
    table = pa.Table.from_pylist(normalized_rows)
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "bitable_dataset.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(output_path))

    preview_limit = int(payload.get("preview_limit", 50))
    preview = cli_impl._preview_payload_from_table(table, limit=preview_limit)
    preview["schema"] = table.schema.names
    preview["row_count"] = table.num_rows
    dataset_name = str(payload.get("dataset_name") or "").strip() or _default_bitable_dataset_name(bitable_url)
    artifacts = [_register_artifacts_preview_table(output_path, table)]
    result_payload: dict[str, Any] = {
        "dataset_name": dataset_name,
        "source_type": "bitable",
        "source_path": str(output_path),
        "source_label": bitable_url,
        "row_count": table.num_rows,
        "schema": table.schema.names,
        "imported_at": _timestamp_suffix(),
        "fetched_rows": table.num_rows,
        "pages_fetched": fetched_pages,
    }

    raw_keyword_index_config = payload.get("keyword_index_config")
    keyword_index_config = raw_keyword_index_config if isinstance(raw_keyword_index_config, dict) else {}

    worker_errors: list[BaseException] = []
    worker_lock = threading.Lock()
    embedding_result: dict[str, Any] | None = None
    keyword_result: dict[str, Any] | None = None

    raw_embedding_config = payload.get("embedding_config")
    embedding_config = raw_embedding_config if isinstance(raw_embedding_config, dict) else {}

    def record_worker_error(exc: BaseException) -> None:
        with worker_lock:
            worker_errors.append(exc)

    def run_embedding() -> None:
        nonlocal embedding_result
        try:
            text_columns = _parse_string_list(embedding_config.get("text_columns"))
            if not text_columns:
                raise ValueError("embedding_config.text_columns must not be empty")

            provider_name = str(embedding_config.get("provider") or _default_embedding_provider())
            resolved_model = _default_embedding_model(provider_name, embedding_config.get("model"))
            provider, resolved_model = cli_impl._make_embedding_provider(provider_name, resolved_model)
            template_version = str(embedding_config.get("template_version") or "text-v1")
            vector_column = str(embedding_config.get("vector_column") or "embedding")
            if vector_column != "embedding":
                raise ValueError("bitable import currently materializes vectors into the 'embedding' column only")
            embedding_batch_size = int(embedding_config.get("batch_size", 128))
            embedding_output_path = (
                _normalize_path(embedding_config["output_path"])
                if embedding_config.get("output_path")
                else run_dir / "artifacts" / "bitable_embedding_dataset.parquet"
            )
            if run_id:
                _update_run_progress(
                    run_id,
                    progress_phase="building_embedding",
                    embedding_provider=provider_name,
                    embedding_model=resolved_model,
                    embedding_batch_size=embedding_batch_size,
                )

            def _on_batch(total_rows: int, batch_rows: int) -> None:
                if run_id:
                    _update_run_progress(
                        run_id,
                        progress_phase="building_embedding",
                        embedding_rows=total_rows,
                        embedding_last_batch_rows=batch_rows,
                    )

            embedding_source_table = annotate_source_arrow_table(table, source_label=output_path.stem)
            embedding_table = materialize_mixed_text_embeddings_stream(
                embedding_source_table,
                provider=provider,
                model=resolved_model,
                template_version=template_version,
                text_fields=text_columns,
                output_path=embedding_output_path,
                batch_size=embedding_batch_size,
                on_batch=_on_batch,
            )
            embedding_result = {
                "artifact": cli_impl._table_artifact(
                    embedding_output_path,
                    embedding_table,
                    ["dataset", "bitable-import", "embedding-build"],
                ),
                "payload": {
                    "dataset_path": str(embedding_output_path),
                    "schema": embedding_table.schema.names,
                    "row_count": embedding_table.num_rows,
                    "provider": provider_name,
                    "model": resolved_model,
                    "template_version": template_version,
                    "vector_column": vector_column,
                    "text_columns": text_columns,
                },
            }
            if run_id:
                _update_run_progress(
                    run_id,
                    progress_phase="embedding_ready",
                    embedding_rows=embedding_table.num_rows,
                )
        except BaseException as exc:  # noqa: BLE001
            record_worker_error(exc)

    def run_keyword_index() -> None:
        nonlocal keyword_result
        try:
            text_columns = _parse_string_list(keyword_index_config.get("text_columns"))
            if not text_columns:
                raise ValueError("keyword_index_config.text_columns must not be empty")
            analyzer = str(keyword_index_config.get("analyzer") or "jieba")
            keyword_output_dir = (
                _normalize_path(keyword_index_config["output_path"])
                if keyword_index_config.get("output_path")
                else run_dir / "artifacts" / "bitable_keyword_index"
            )
            keyword_result = _execute_keyword_index_build(
                {
                    "dataset_paths": [str(output_path)],
                    "text_columns": text_columns,
                    "output_path": str(keyword_output_dir),
                    "analyzer": analyzer,
                    "preview_limit": int(keyword_index_config.get("preview_limit", payload.get("preview_limit", 50))),
                },
                run_dir,
                run_id=run_id,
            )
        except BaseException as exc:  # noqa: BLE001
            record_worker_error(exc)

    workers: list[threading.Thread] = []
    if embedding_config.get("enabled"):
        workers.append(threading.Thread(target=run_embedding, daemon=True))
    if keyword_index_config.get("enabled"):
        workers.append(threading.Thread(target=run_keyword_index, daemon=True))
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    if worker_errors:
        raise worker_errors[0]

    if embedding_result is not None:
        artifacts.append(embedding_result["artifact"])
        result_payload["embedding_dataset"] = embedding_result["payload"]

    if keyword_result is not None:
        artifacts.extend(keyword_result.get("artifacts", []))
        result_payload["keyword_index"] = {
            "index_path": keyword_result["payload"]["index_path"],
            "dataset_count": keyword_result["payload"]["dataset_count"],
            "text_columns": keyword_result["payload"]["text_columns"],
            "analyzer": keyword_result["payload"]["analyzer"],
            "doc_count": keyword_result["payload"]["doc_count"],
            "term_count": keyword_result["payload"]["term_count"],
            "posting_count": keyword_result["payload"]["posting_count"],
            "schema": keyword_result["payload"]["schema"],
        }

    return {
        "payload": result_payload,
        "preview": preview,
        "artifacts": artifacts,
    }


def _preview_bitable_import(payload: dict[str, Any]) -> dict[str, Any]:
    bitable_url = str(payload.get("bitable_url") or payload.get("input_path") or "").strip()
    if not bitable_url:
        raise ValueError("bitable_url is required")
    app_id, app_secret = _resolve_bitable_credentials(payload)
    client = BitableClient(
        app_id=app_id,
        app_secret=app_secret,
        request_timeout_seconds=int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
    )
    preview_limit = int(payload.get("limit", payload.get("preview_limit", 100)))
    rows = client.list_records_from_url(
        bitable_url,
        page_size=min(max(preview_limit, 1), 100),
        max_rows=preview_limit,
    )
    normalized_rows = _normalize_bitable_rows(rows)
    table = pa.Table.from_pylist(normalized_rows)
    preview = cli_impl._preview_payload_from_table(table, limit=preview_limit)
    preview["schema"] = table.schema.names
    preview["row_count"] = table.num_rows
    dataset_name = str(payload.get("dataset_name") or "").strip() or _default_bitable_dataset_name(bitable_url)
    return {
        "dataset": {
            "name": dataset_name,
            "source_type": "bitable",
            "source_path": bitable_url,
            "source_label": bitable_url,
        },
        "preview": preview,
    }


def _register_artifacts_preview_table(path: Path, table: pa.Table) -> dict[str, Any]:
    return cli_impl._table_artifact(path, table, ["dataset", "bitable-import", "result"])


def _execute_file_sql(payload: dict[str, Any], run_id: str, run_dir: Path) -> dict[str, Any]:
    session = Session()
    input_df = _load_dataframe(session, payload)
    table_name = payload.get("table") or "input_table"
    session.create_temp_view(table_name, input_df)
    query = payload["query"]

    explain = ""
    if hasattr(session, "explain_sql"):
        try:
            explain = session.explain_sql(query)
        except Exception:
            explain = ""

    result_df = session.sql(query)
    logical = result_df.explain() if hasattr(result_df, "explain") else ""
    result_table = result_df.to_arrow()
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "result.parquet"
    )
    artifacts = [cli_impl._table_artifact(output_path, result_table, ["result", "file-sql"])]
    write_explain(
        run_id,
        cli_impl._parse_explain_sections(explain) if explain else cli_impl._build_batch_explain(logical, output_path),
    )
    return {
        "payload": {
            "table": table_name,
            "query": query,
            "input_type": payload.get("input_type", "auto"),
            "schema": result_table.schema.names,
            "preview": cli_impl._preview_payload_from_table(result_table, limit=int(payload.get("preview_limit", 50))),
            "row_count": result_table.num_rows,
        },
        "artifacts": artifacts,
    }


def _register_artifacts(index: ArtifactIndex, run_id: str, artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created: list[dict[str, Any]] = []
    for artifact in artifacts:
        record = dict(artifact)
        record["artifact_id"] = cli_impl._new_artifact_id()
        record["run_id"] = run_id
        record["created_at"] = cli_impl._utc_now()
        index.insert_artifact(record)
        created.append(record)
    return created


def _update_run_progress(run_id: str, **details: Any) -> None:
    current = read_run(run_id)
    merged_details = {
        **(current.get("details") or {}),
        **details,
    }
    updated = update_run(run_id, details=merged_details)
    append_progress_snapshot(
        run_id,
        json.dumps(
            {
                "event": "progress",
                "run_id": run_id,
                **merged_details,
            },
            ensure_ascii=False,
        ),
    )
    index = ArtifactIndex()
    try:
        index.upsert_run(updated)
    finally:
        index.close()


def _finalize_bitable_import_run(
    *,
    run_id: str,
    run_dir: Path,
    action_args: dict[str, Any],
) -> None:
    index = ArtifactIndex()
    try:
        index.upsert_run(read_run(run_id))
        result = _execute_bitable_import(action_args, run_dir, run_id=run_id)
        created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
        result_payload = result.get("payload") or {}
        if created_artifacts:
            result_payload = {
                **result_payload,
                "artifact_uri": created_artifacts[0].get("uri", ""),
                "artifact_id": created_artifacts[0].get("artifact_id", ""),
            }
            embedding_dataset = result_payload.get("embedding_dataset")
            if isinstance(embedding_dataset, dict):
                for artifact in created_artifacts:
                    tags = artifact.get("tags_json") or []
                    if "embedding-build" not in tags:
                        continue
                    result_payload["embedding_dataset"] = {
                        **embedding_dataset,
                        "artifact_uri": artifact.get("uri", ""),
                        "artifact_id": artifact.get("artifact_id", ""),
                        "run_id": run_id,
                    }
                    break
            keyword_index = result_payload.get("keyword_index")
            if isinstance(keyword_index, dict):
                for artifact in created_artifacts:
                    tags = artifact.get("tags_json") or []
                    if "keyword-index-build" not in tags:
                        continue
                    result_payload["keyword_index"] = {
                        **keyword_index,
                        "artifact_uri": artifact.get("uri", ""),
                        "artifact_id": artifact.get("artifact_id", ""),
                        "run_id": run_id,
                    }
                    break
        finalized = finalize_run(run_id, "succeeded", details=result_payload)
        index.upsert_run(finalized)
    except Exception as exc:
        append_stderr(run_id, traceback.format_exc())
        finalized = finalize_run(
            run_id,
            "failed",
            error=str(exc),
            details=cli_impl._error_payload_from_exception(exc),
        )
        index.upsert_run(finalized)
    finally:
        index.close()


def _api_parts(path: str) -> tuple[str, ...] | None:
    parts = tuple(part for part in path.split("/") if part)
    if not parts or parts[0] != "api":
        return None
    if len(parts) >= 2 and parts[1] == "v1":
        return parts[2:]
    return parts[1:]


def _build_run_response(
    index: ArtifactIndex,
    run: dict[str, Any],
    *,
    run_dir: Path | None = None,
    result: dict[str, Any] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    artifact_list = list(artifacts or [])
    payload: dict[str, Any] = {
        "ok": True,
        "run_id": run["run_id"],
        "run": _enrich_run(index, run),
        "artifacts": artifact_list,
        "artifact": artifact_list[0] if artifact_list else None,
    }
    if run_dir is not None:
        payload["run_dir"] = str(run_dir)
    if result is not None:
        payload["result"] = result["payload"]
        preview = result.get("preview") or result["payload"].get("preview")
        if preview is not None:
            payload["preview"] = preview
    return payload


def _error_response(exc: Exception) -> tuple[HTTPStatus, dict[str, Any]]:
    if isinstance(exc, ApiRouteNotFoundError):
        return (
            HTTPStatus.NOT_FOUND,
            cli_impl._error_payload(
                str(exc),
                error_type="api_not_found",
                phase="api",
            ),
        )
    if isinstance(exc, RunDeleteConflictError):
        return (
            HTTPStatus.CONFLICT,
            cli_impl._error_payload(
                str(exc),
                error_type="run_conflict",
                phase="run_lifecycle",
                details={
                    "run_id": exc.run_id,
                    "status": exc.status,
                },
            ),
        )
    if isinstance(exc, FileNotFoundError):
        return HTTPStatus.NOT_FOUND, cli_impl._error_payload_from_exception(exc)
    if isinstance(exc, ValueError):
        return HTTPStatus.BAD_REQUEST, cli_impl._error_payload_from_exception(exc)
    return HTTPStatus.INTERNAL_SERVER_ERROR, cli_impl._error_payload_from_exception(exc)


@dataclass
class VelariaService:
    host: str
    port: int
    _embedding_provider_cache: dict[tuple[str, str | None], tuple[Any, str]] = field(default_factory=dict)
    _embedding_provider_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_embedding_provider(self, provider_name: str, model_name: str | None):
        cache_key = (provider_name.strip().lower(), model_name or None)
        with self._embedding_provider_lock:
            cached = self._embedding_provider_cache.get(cache_key)
            if cached is not None:
                return cached
            created = cli_impl._make_embedding_provider(provider_name, model_name)
            self._embedding_provider_cache[cache_key] = created
            return created

    def build_handler(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "VelariaService/0.1"

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                body = _json_dumps(payload)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
                self.end_headers()
                self.wfile.write(body)

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                if not raw:
                    return {}
                return json.loads(raw.decode("utf-8"))

            def do_OPTIONS(self) -> None:  # noqa: N802
                self.send_response(HTTPStatus.NO_CONTENT)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
                self.end_headers()

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    if path == "/health":
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "service": "velaria-service",
                                "version": __version__,
                                "port": self.server.server_port,
                            },
                        )
                        return
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if parts == ("runs",):
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            runs = index.list_runs(limit=limit)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "runs": runs,
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 3 and parts[0] == "runs" and parts[2] == "result":
                        run_id = parts[1]
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            run = index.get_run(run_id)
                            if run is None:
                                raise FileNotFoundError(f"run not found: {run_id}")
                            artifact = cli_impl._find_run_result_artifact(index, run_id)
                            preview = cli_impl._read_preview_for_artifact(artifact, limit=limit)
                            if artifact.get("preview_json") is None:
                                index.update_artifact_preview(artifact["artifact_id"], preview)
                                artifact = index.get_artifact(artifact["artifact_id"]) or artifact
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "run": _enrich_run(index, run),
                                    "artifact": artifact,
                                    "preview": preview,
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 2 and parts[0] == "runs":
                        run_id = parts[1]
                        index = ArtifactIndex()
                        try:
                            run = index.get_run(run_id)
                            if run is None:
                                raise FileNotFoundError(f"run not found: {run_id}")
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "run": _enrich_run(index, run),
                                    "artifacts": index.list_artifacts(run_id=run_id, limit=100),
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 3 and parts[0] == "artifacts" and parts[2] == "preview":
                        artifact_id = parts[1]
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            artifact = index.get_artifact(artifact_id)
                            if artifact is None:
                                raise FileNotFoundError(f"artifact not found: {artifact_id}")
                            preview = cli_impl._read_preview_for_artifact(artifact, limit=limit)
                            if artifact.get("preview_json") is None:
                                index.update_artifact_preview(artifact_id, preview)
                                artifact = index.get_artifact(artifact_id) or artifact
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "artifact": artifact,
                                    "preview": preview,
                                },
                            )
                            return
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    payload = self._read_json()
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if parts == ("import", "preview"):
                        if str(payload.get("input_type") or "auto").lower() == "bitable":
                            preview_payload = _preview_bitable_import(payload)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    **preview_payload,
                                },
                            )
                            return
                        session = Session()
                        resolved_input_type, _ = _resolve_auto_input_payload(session, payload)
                        df = _load_dataframe(session, payload)
                        preview = _preview_from_dataframe(df, limit=int(payload.get("limit", 50)))
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "dataset": {
                                    "name": payload.get("dataset_name")
                                    or Path(payload["input_path"]).stem,
                                    "source_type": resolved_input_type,
                                    "source_path": str(_normalize_path(payload["input_path"])),
                                },
                                "preview": preview,
                            },
                        )
                        return
                    if parts == ("runs", "bitable-import"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        request_app_secret = payload.get("app_secret") or payload.get("bitable_app_secret")
                        embedding_config = payload.get("embedding_config")
                        keyword_index_config = payload.get("keyword_index_config")
                        action_args = {
                            "bitable_url": payload.get("bitable_url") or payload.get("input_path"),
                            "app_id": payload.get("app_id") or payload.get("bitable_app_id"),
                            "dataset_name": payload.get("dataset_name"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                            "timeout_seconds": int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
                            "page_size": int(payload.get("page_size", _BITABLE_PAGE_SIZE)),
                            "embedding_config": embedding_config,
                            "keyword_index_config": keyword_index_config,
                        }
                        worker_args = {
                            **action_args,
                            "app_secret": request_app_secret,
                        }
                        run_id, run_dir = create_run(
                            "bitable-import",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name") or f"bitable-import-{_timestamp_suffix()}",
                            description=payload.get("description") or "Bitable import run",
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "bitable-import",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            created = read_run(run_id)
                            index.upsert_run(created)
                            worker = threading.Thread(
                                target=_finalize_bitable_import_run,
                                kwargs={
                                    "run_id": run_id,
                                    "run_dir": run_dir,
                                    "action_args": worker_args,
                                },
                                daemon=True,
                            )
                            worker.start()
                            self._send_json(
                                HTTPStatus.ACCEPTED,
                                {
                                    "ok": True,
                                    "run_id": run_id,
                                    "run": _enrich_run(index, created),
                                    "run_dir": str(run_dir),
                                },
                            )
                            return
                        finally:
                            index.close()
                    if parts == ("runs", "file-sql"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        action_args = {
                            "input_path": str(_normalize_path(payload["input_path"])),
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "table": payload.get("table", "input_table"),
                            "query": payload["query"],
                            "output_path": payload.get("output_path"),
                        }
                        run_id, run_dir = create_run(
                            "file-sql",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "file-sql",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_file_sql(action_args | {"preview_limit": payload.get("preview_limit", 50)}, run_id, run_dir)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(HTTPStatus.OK, _build_run_response(index, finalized, run_dir=run_dir, result=result, artifacts=created_artifacts))
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "embedding-build"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        provider_name = payload.get("provider") or _default_embedding_provider()
                        resolved_model_name = _default_embedding_model(provider_name, payload.get("model"))
                        action_args = {
                            "input_path": str(_normalize_path(payload["input_path"])),
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "text_columns": payload.get("text_columns"),
                            "provider": provider_name,
                            "model": resolved_model_name,
                            "template_version": payload.get("template_version", "text-v1"),
                            "vector_column": payload.get("vector_column", "embedding"),
                            "doc_id_field": payload.get("doc_id_field", "doc_id"),
                            "source_updated_at_field": payload.get("source_updated_at_field", "source_updated_at"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                            "sheet_name": payload.get("sheet_name", 0),
                            "date_format": payload.get("date_format", "%Y-%m-%d"),
                        }
                        run_id, run_dir = create_run(
                            "embedding-build",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "embedding-build",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            provider, resolved_model = service.get_embedding_provider(
                                str(provider_name),
                                resolved_model_name,
                            )
                            result = _execute_embedding_build(action_args, run_dir, provider=provider, resolved_model=resolved_model)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "keyword-index-build"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        dataset_paths = _parse_path_list(payload.get("dataset_paths"))
                        if payload.get("dataset_path"):
                            dataset_paths = [payload["dataset_path"], *dataset_paths]
                        action_args = {
                            "dataset_paths": [str(_normalize_path(path)) for path in dataset_paths],
                            "input_path": str(_normalize_path(payload["input_path"])) if payload.get("input_path") else None,
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "text_columns": payload.get("text_columns"),
                            "analyzer": payload.get("analyzer", "jieba"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "keyword-index-build",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "keyword-index-build",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_keyword_index_build(action_args, run_dir, run_id=run_id)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded", details=result.get("payload"))
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "keyword-search"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        action_args = {
                            "index_path": str(_normalize_path(str(payload["index_path"])))
                            if payload.get("index_path")
                            else None,
                            "query_text": payload.get("query_text"),
                            "top_k": int(payload.get("top_k", 10)),
                            "where_sql": payload.get("where_sql"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "keyword-search",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "keyword-search",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_keyword_search(action_args, run_dir)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts in {("search", "hybrid"), ("runs", "hybrid-search")}:
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        provider_name = payload.get("provider") or _default_embedding_provider()
                        resolved_model_name = _default_embedding_model(provider_name, payload.get("model"))
                        action_args = {
                            "dataset_path": str(_normalize_path(str(payload["dataset_path"])))
                            if payload.get("dataset_path")
                            else None,
                            "query_text": payload["query_text"],
                            "provider": provider_name,
                            "model": resolved_model_name,
                            "template_version": payload.get("template_version", "text-v1"),
                            "top_k": int(payload.get("top_k", 10)),
                            "metric": payload.get("metric", "cosine"),
                            "where_sql": payload.get("where_sql"),
                            "vector_column": payload.get("vector_column", "embedding"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "hybrid-search",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "hybrid-search",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            provider, resolved_model = service.get_embedding_provider(
                                str(provider_name),
                                resolved_model_name,
                            )
                            result = _execute_hybrid_search(action_args, run_dir, provider=provider, resolved_model=resolved_model)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(HTTPStatus.OK, _build_run_response(index, finalized, run_dir=run_dir, result=result, artifacts=created_artifacts))
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def do_DELETE(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if len(parts) == 2 and parts[0] == "runs":
                        run_id = parts[1]
                        index = ArtifactIndex()
                        try:
                            deleted = index.delete_run(run_id, delete_files=True)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    **deleted,
                                },
                            )
                            return
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

        return Handler

    def serve_forever(self) -> None:
        server = ThreadingHTTPServer((self.host, self.port), self.build_handler())
        print(
            json.dumps(
                {
                    "event": "ready",
                    "host": self.host,
                    "port": server.server_port,
                    "service": "velaria-service",
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        try:
            server.serve_forever()
        finally:
            server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Velaria local app service.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=37491)
    args = parser.parse_args()
    service = VelariaService(host=args.host, port=args.port)
    service.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
