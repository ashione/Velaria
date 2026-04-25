from __future__ import annotations

import csv  # noqa: F401 – kept for original parity
import json
import os
import re
import traceback
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any

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
    stream_mixed_text_embeddings_to_parquet,
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
from velaria.agentic_store import AgenticStore
from velaria.agentic_dsl import compile_rule_spec, compile_template_rule, parse_rule_spec
from velaria.agentic_search import search_datasets, search_events, search_fields, search_templates
from velaria.agentic_runtime import execute_monitor_once
from velaria.agentic_runtime import coerce_result_rows, process_signal_rows

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


def _safe_sql_identifier(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", str(raw).strip())
    if not cleaned:
        return "input_table"
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


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


def _event_docs_from_store(store: AgenticStore) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for event in store.list_focus_events(limit=200):
        docs.append(
            {
                "doc_id": event["event_id"],
                "target_kind": "event",
                "title": event["title"],
                "summary": event["summary"],
                "body": json.dumps(event.get("key_fields") or {}, ensure_ascii=False),
                "tags": [event["severity"]],
                "metadata": {"event_id": event["event_id"], "monitor_id": event["monitor_id"]},
            }
        )
    return docs


def _dataset_docs_from_store(store: AgenticStore) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for source in store.list_sources():
        if source["kind"] not in {"saved_dataset", "local_file", "bitable"}:
            continue
        path = source["spec"].get("path") or source["spec"].get("input_path") or source.get("event_log_path")
        docs.append(
            {
                "doc_id": source["source_id"],
                "target_kind": "dataset",
                "title": source["name"],
                "summary": str(path or source["kind"]),
                "body": json.dumps(source.get("metadata") or {}, ensure_ascii=False),
                "tags": [source["kind"]],
                "metadata": {"source_id": source["source_id"], "kind": source["kind"]},
            }
        )
    return docs


def get_ai_config() -> dict[str, Any]:
    """Read AI provider config from ``~/.velaria/config.json``."""
    config_path = Path.home() / ".velaria" / "config.json"
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {
            "provider": config.get("aiProvider", "openai"),
            "api_key": config.get("aiApiKey", ""),
            "base_url": config.get("aiBaseUrl", "https://api.openai.com/v1"),
            "model": config.get("aiModel", ""),
            "runtime": config.get("aiRuntime", "codex"),
            "runtime_path": config.get("aiRuntimePath", ""),
            "claude_runtime_path": config.get("aiClaudeRuntimePath", ""),
            "codex_runtime_path": config.get("aiCodexRuntimePath", ""),
            "runtime_workspace": config.get("aiRuntimeWorkspace", ""),
            "reuse_local_config": bool(config.get("aiReuseLocalConfig", True)),
            "runtime_config_path": config.get("aiRuntimeConfigPath", ""),
            "network_access": config.get("aiCodexNetworkAccess", config.get("aiNetworkAccess", True)),
        }
    return {}


def _field_docs_from_store(store: AgenticStore) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for source in store.list_sources():
        schema_binding = source.get("schema_binding") or {}
        for name in (schema_binding.get("field_mappings") or {}).keys():
            key = (source["source_id"], name)
            if key in seen:
                continue
            seen.add(key)
            docs.append(
                {
                    "doc_id": f"{source['source_id']}:{name}",
                    "target_kind": "field",
                    "title": name,
                    "summary": f"field from {source['name']}",
                    "body": source["kind"],
                    "tags": [source["kind"]],
                    "metadata": {"source_id": source["source_id"], "field": name},
                }
            )
    return docs
