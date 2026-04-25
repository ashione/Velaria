from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import velaria.cli as cli_impl
from velaria import (
    Session,
    build_file_embeddings,
    load_keyword_index,
    read_embedding_table,
    search_keyword_index,
    query_file_embeddings,
)

from ._helpers import (
    _default_embedding_model,
    _default_embedding_provider,
    _normalize_path,
    _parse_path_list,
    _parse_string_list,
    _resolve_json_columns,
    _score_semantics,
    _update_run_progress,
)


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
