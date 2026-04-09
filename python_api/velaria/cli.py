from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import multiprocessing
import pathlib
import secrets
import shlex
import sys
import traceback
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from velaria import (
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    HashEmbeddingProvider,
    Session,
    SentenceTransformerEmbeddingProvider,
    __version__,
    build_file_embeddings,
    query_file_embeddings,
    run_file_mixed_text_hybrid_search,
)
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
from velaria.workspace.types import PREVIEW_LIMIT_BYTES, PREVIEW_LIMIT_ROWS


class CliStructuredError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        error_type: str = "value_error",
        phase: str | None = None,
        details: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.phase = phase
        self.details = details or {}
        self.run_id = run_id


class CliUsageError(CliStructuredError):
    def __init__(
        self,
        message: str,
        *,
        phase: str = "argument_parse",
        details: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            error_type="usage_error",
            phase=phase,
            details=details,
            run_id=run_id,
        )


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CliUsageError(message)

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if status == 0:
            raise SystemExit(0)
        raise CliUsageError(message.strip() if message else "invalid arguments")


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)


def _emit_json(payload: Any) -> int:
    print(_json_dumps(payload))
    return 0


def _error_payload(
    error: str,
    *,
    error_type: str = "cli_error",
    phase: str | None = None,
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": error,
        "error_type": error_type,
    }
    if phase is not None:
        payload["phase"] = phase
    if details:
        payload["details"] = details
    if run_id is not None:
        payload["run_id"] = run_id
    return payload


def _error_payload_from_exception(
    exc: BaseException,
    *,
    error_type: str | None = None,
    phase: str | None = None,
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload_details = dict(details or {})
    payload_run_id = run_id
    if isinstance(exc, CliStructuredError):
        payload_details = {**exc.details, **payload_details}
        payload_run_id = payload_run_id or exc.run_id
        return _error_payload(
            str(exc),
            error_type=error_type or exc.error_type,
            phase=phase or exc.phase,
            details=payload_details,
            run_id=payload_run_id,
        )
    if isinstance(exc, FileNotFoundError):
        if getattr(exc, "filename", None):
            payload_details.setdefault("path", exc.filename)
        return _error_payload(
            str(exc),
            error_type=error_type or "file_not_found",
            phase=phase or "filesystem",
            details=payload_details,
            run_id=payload_run_id,
        )
    if isinstance(exc, ValueError):
        return _error_payload(
            str(exc),
            error_type=error_type or "value_error",
            phase=phase,
            details=payload_details,
            run_id=payload_run_id,
        )
    return _error_payload(
        str(exc),
        error_type=error_type or "internal_error",
        phase=phase,
        details=payload_details,
        run_id=payload_run_id,
    )


def _emit_error_json(
    error: str,
    *,
    error_type: str = "cli_error",
    phase: str | None = None,
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> int:
    print(
        _json_dumps(
            _error_payload(
                error,
                error_type=error_type,
                phase=phase,
                details=details,
                run_id=run_id,
            )
        )
    )
    return 1


def _interactive_banner() -> int:
    print("Velaria interactive mode. Type 'help' for usage, 'exit' to quit.")
    return 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_artifact_id() -> str:
    return f"artifact_{secrets.token_hex(8)}"


def _normalize_metric(metric: str) -> str:
    return "cosine" if metric in ("cosine", "cosin") else metric


def _normalize_path(path: pathlib.Path) -> pathlib.Path:
    return path.expanduser().resolve()


def _escape_sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _sql_literal(value: str) -> str:
    return f"'{_escape_sql_literal(value)}'"


def _sql_literal_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return _sql_literal(str(value))


def _build_batch_source_create_sql(
    *,
    table: str,
    input_path: pathlib.Path,
    input_type: str,
    delimiter: str,
    line_mode: str,
    regex_pattern: str | None,
    mappings: str | None,
    columns: str | None,
    json_format: str,
) -> str:
    options = [f"path: {_sql_literal(str(_normalize_path(input_path)))}"]
    if input_type == "auto":
        if columns:
            options.append(f"columns: {_sql_literal(columns)}")
    elif input_type == "csv":
        options.append(f"delimiter: {_sql_literal(delimiter)}")
    elif input_type == "line":
        options.append(f"mode: {_sql_literal(line_mode)}")
        if line_mode == "split":
            options.append(f"delimiter: {_sql_literal(delimiter)}")
        else:
            if not regex_pattern:
                raise CliUsageError(
                    "--regex-pattern is required when --input-type=line and --line-mode=regex",
                    details={"input_type": input_type, "line_mode": line_mode},
                )
            options.append(f"regex_pattern: {_sql_literal(regex_pattern)}")
        if mappings:
            options.append(f"mappings: {_sql_literal(mappings)}")
        elif columns:
            options.append(f"columns: {_sql_literal(columns)}")
        else:
            raise CliUsageError(
                "--input-type=line requires --columns or --mappings",
                details={"input_type": input_type},
            )
    elif input_type == "json":
        if not columns:
            raise CliUsageError(
                "--input-type=json requires --columns",
                details={"input_type": input_type},
            )
        options.append(f"format: {_sql_literal(json_format)}")
        options.append(f"columns: {_sql_literal(columns)}")
    else:
        raise CliUsageError(
            "unsupported input type",
            details={"input_type": input_type},
        )
    if input_type == "auto":
        return f"CREATE TABLE {table} OPTIONS({', '.join(options)})"
    return f"CREATE TABLE {table} USING {input_type} OPTIONS({', '.join(options)})"


def _uri_from_path(path: pathlib.Path) -> str:
    return _normalize_path(path).as_uri()


def _path_from_uri(uri: str) -> pathlib.Path:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return pathlib.Path(parsed.path)
    raise CliStructuredError(
        f"unsupported artifact uri: {uri}",
        phase="artifact_preview",
        details={"uri": uri},
    )


def _parse_vector_text(text: str) -> list[float]:
    value = text.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1].strip()
    if not value:
        return []
    if "," in value:
        parts = [part.strip() for part in value.split(",")]
    else:
        parts = value.split()
    try:
        return [float(part) for part in parts if part]
    except ValueError as exc:
        raise CliStructuredError(
            "--query-vector contains invalid numeric value",
            phase="argument_parse",
            details={"query_vector": text},
        ) from exc


def _split_csv_values(text: str | None) -> list[str]:
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _normalize_embedding_provider(provider: str) -> str:
    return provider.strip().lower()


def _make_embedding_provider(provider: str, model: str | None):
    normalized = _normalize_embedding_provider(provider)
    if normalized == "minilm":
        return SentenceTransformerEmbeddingProvider(
            model_name=model or DEFAULT_LOCAL_EMBEDDING_MODEL
        ), (model or DEFAULT_LOCAL_EMBEDDING_MODEL)
    if normalized == "hash":
        return HashEmbeddingProvider(), (model or "hash-demo")
    raise CliStructuredError(
        "unsupported embedding provider",
        phase="argument_parse",
        details={"provider": provider},
    )


def _infer_embedding_json_columns(
    *,
    text_columns: list[str],
    doc_id_field: str,
    source_updated_at_field: str,
    where_column: str | None = None,
) -> list[str]:
    ordered = []
    for item in [doc_id_field, source_updated_at_field, where_column, *text_columns]:
        if item and item not in ordered:
            ordered.append(item)
    return ordered


def _parse_scalar_text(text: str) -> Any:
    value = text.strip()
    upper = value.upper()
    if upper == "NULL":
        return None
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_explain_sections(explain: str) -> dict[str, str]:
    sections = {
        "logical": "",
        "physical": "",
        "strategy": "",
    }
    current: str | None = None
    for line in explain.splitlines():
        stripped = line.strip()
        if stripped in sections:
            current = stripped
            continue
        if current is None:
            continue
        if sections[current]:
            sections[current] += "\n"
        sections[current] += line
    return sections


def _build_batch_explain(logical: str, output_path: pathlib.Path) -> dict[str, str]:
    return {
        "logical": logical,
        "physical": (
            "local-batch\n"
            f"result_sink=file://{_normalize_path(output_path)}\n"
            "materialization=pyarrow-table"
        ),
        "strategy": (
            "selected_mode=single-process\n"
            "transport_mode=inproc\n"
            "execution_reason=batch query executed through DataflowSession"
        ),
    }


def _preview_payload_from_table(
    table: pa.Table,
    limit: int = PREVIEW_LIMIT_ROWS,
    max_bytes: int = PREVIEW_LIMIT_BYTES,
) -> dict[str, Any]:
    rows = table.slice(0, limit).to_pylist()
    preview: dict[str, Any] = {
        "schema": table.schema.names,
        "rows": rows,
        "row_count": table.num_rows,
        "truncated": table.num_rows > limit,
    }
    encoded = json.dumps(preview, ensure_ascii=False)
    while len(encoded.encode("utf-8")) > max_bytes and preview["rows"]:
        preview["rows"].pop()
        preview["truncated"] = True
        encoded = json.dumps(preview, ensure_ascii=False)
    if len(encoded.encode("utf-8")) > max_bytes:
        preview = {
            "schema": table.schema.names,
            "rows": [],
            "row_count": table.num_rows,
            "truncated": True,
            "message": "preview truncated to satisfy size limit",
        }
    return preview


def _preview_payload_from_csv(
    csv_path: pathlib.Path,
    limit: int = PREVIEW_LIMIT_ROWS,
    max_bytes: int = PREVIEW_LIMIT_BYTES,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    row_count = 0
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        schema = reader.fieldnames or []
        for row in reader:
            row_count += 1
            if len(rows) < limit:
                rows.append(dict(row))
        truncated = row_count > len(rows)
    preview: dict[str, Any] = {
        "schema": schema,
        "rows": rows,
        "row_count": row_count,
        "truncated": truncated,
    }
    encoded = json.dumps(preview, ensure_ascii=False)
    while len(encoded.encode("utf-8")) > max_bytes and preview["rows"]:
        preview["rows"].pop()
        preview["truncated"] = True
        encoded = json.dumps(preview, ensure_ascii=False)
    return preview


def _finalize_preview_payload(
    preview: dict[str, Any],
    artifact_row_count: int | None,
) -> dict[str, Any]:
    if artifact_row_count is not None:
        preview["row_count"] = artifact_row_count
        preview["truncated"] = preview.get("truncated", False) or artifact_row_count > len(
            preview.get("rows", [])
        )
    return preview


def _limit_preview_payload(preview: dict[str, Any], limit: int) -> dict[str, Any]:
    rows = list(preview.get("rows", []))
    limited = dict(preview)
    limited["rows"] = rows[:limit]
    row_count = preview.get("row_count")
    limited["truncated"] = (
        preview.get("truncated", False)
        or len(rows) > limit
        or (
            row_count is not None
            and isinstance(row_count, int)
            and row_count > len(limited["rows"])
        )
    )
    return limited


def _infer_format(path: pathlib.Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in (".parquet", ".pq"):
        return "parquet"
    if suffix in (".arrow", ".feather"):
        return "arrow"
    raise CliStructuredError(
        f"unsupported output format for path: {path}",
        phase="artifact_materialize",
        details={"path": str(path)},
    )


def _write_table(path: pathlib.Path, table: pa.Table) -> str:
    fmt = _infer_format(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        pa_csv.write_csv(table, str(path))
    elif fmt == "parquet":
        pq.write_table(table, str(path))
    elif fmt == "arrow":
        with pa.OSFile(str(path), "wb") as sink:
            with pa_ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
    return fmt


def _table_artifact(path: pathlib.Path, table: pa.Table, tags: list[str]) -> dict[str, Any]:
    fmt = _write_table(path, table)
    preview = _preview_payload_from_table(table)
    return {
        "type": "file",
        "uri": _uri_from_path(path),
        "format": fmt,
        "row_count": table.num_rows,
        "schema_json": table.schema.names,
        "preview_json": preview,
        "tags_json": tags,
    }


def _text_artifact(path: pathlib.Path, text: str, tags: list[str]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return {
        "type": "file",
        "uri": _uri_from_path(path),
        "format": "text",
        "row_count": None,
        "schema_json": None,
        "preview_json": None,
        "tags_json": tags,
    }


def _read_preview_for_artifact(
    artifact: dict[str, Any],
    limit: int = PREVIEW_LIMIT_ROWS,
) -> dict[str, Any]:
    if artifact.get("preview_json") is not None:
        return _limit_preview_payload(artifact["preview_json"], limit)
    path = _path_from_uri(artifact["uri"])
    fmt = artifact["format"]
    if fmt == "csv":
        return _finalize_preview_payload(
            _preview_payload_from_csv(path, limit=limit),
            artifact.get("row_count"),
        )
    if fmt == "parquet":
        return _finalize_preview_payload(
            _preview_payload_from_table(pq.read_table(str(path)), limit=limit),
            artifact.get("row_count"),
        )
    if fmt == "arrow":
        with path.open("rb") as handle:
            try:
                table = pa_ipc.open_file(handle).read_all()
            except pa.ArrowInvalid:
                handle.seek(0)
                table = pa_ipc.open_stream(handle).read_all()
        return _finalize_preview_payload(
            _preview_payload_from_table(table, limit=limit),
            artifact.get("row_count"),
        )
    raise CliStructuredError(
        f"preview unsupported for format: {fmt}",
        phase="artifact_preview",
        details={
            "artifact_uri": artifact["uri"],
            "format": fmt,
        },
    )


def _execute_csv_sql(
    input_path: pathlib.Path,
    table: str,
    query: str,
    input_type: str = "csv",
    delimiter: str = ",",
    line_mode: str = "split",
    regex_pattern: str | None = None,
    mappings: str | None = None,
    columns: str | None = None,
    json_format: str = "json_lines",
    output_path: pathlib.Path | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    session = Session()
    create_sql = _build_batch_source_create_sql(
        table=table,
        input_path=input_path,
        input_type=input_type,
        delimiter=delimiter,
        line_mode=line_mode,
        regex_pattern=regex_pattern,
        mappings=mappings,
        columns=columns,
        json_format=json_format,
    )
    try:
        session.sql(create_sql)
    except Exception as exc:
        raise CliStructuredError(
            "failed to register sql input table",
            phase="sql_input_register",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "create_sql": create_sql,
            },
            run_id=run_id,
        ) from exc
    batch_explain = ""
    if hasattr(session, "explain_sql"):
        try:
            batch_explain = session.explain_sql(query)
        except Exception:
            batch_explain = ""
    try:
        result_df = session.sql(query)
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute sql query",
            phase="sql_execute",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "query": query,
            },
            run_id=run_id,
        ) from exc
    logical = result_df.explain() if hasattr(result_df, "explain") else ""
    try:
        result = result_df.to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to materialize sql result",
            phase="result_materialize",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "query": query,
            },
            run_id=run_id,
        ) from exc
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "file-sql"]))
        if run_id is not None:
            write_explain(
                run_id,
                _parse_explain_sections(batch_explain)
                if batch_explain
                else _build_batch_explain(logical, output_path),
            )
    return {
        "payload": {
            "table": table,
            "query": query,
            "input_type": input_type,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
        },
        "artifacts": artifacts,
    }


def _execute_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    score_threshold: float | None = None,
    output_path: pathlib.Path | None = None,
    explain_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    session = Session()
    try:
        df = session.read_csv(str(csv_path))
    except Exception as exc:
        raise CliStructuredError(
            "failed to read csv input",
            phase="csv_read",
            details={"csv": str(csv_path), "vector_column": vector_column},
        ) from exc
    try:
        session.create_temp_view("input_table", df)
    except Exception as exc:
        raise CliStructuredError(
            "failed to register temp view",
            phase="vector_register_view",
            details={"csv": str(csv_path), "table": "input_table"},
        ) from exc
    needle = _parse_vector_text(query_vector)
    if not needle:
        raise CliStructuredError(
            "--query-vector must not be empty",
            phase="argument_parse",
            details={"query_vector": query_vector},
        )

    base_df = df
    if where_column or where_op or where_value is not None:
        if not where_column or not where_op or where_value is None:
            raise CliStructuredError(
                "--where-column, --where-op, and --where-value must be provided together",
                phase="argument_parse",
                details={
                    "where_column": where_column,
                    "where_op": where_op,
                    "where_value": where_value,
                },
            )
        try:
            parsed_where_value = _parse_scalar_text(where_value)
            base_df = session.sql(
                f"SELECT * FROM input_table WHERE {where_column} {where_op} "
                f"{_sql_literal_value(parsed_where_value)}"
            )
        except Exception as exc:
            raise CliStructuredError(
                "failed to apply hybrid search filter",
                phase="vector_filter",
                details={
                    "where_column": where_column,
                    "where_op": where_op,
                    "where_value": where_value,
                },
            ) from exc

    try:
        if where_column or score_threshold is not None:
            session.create_temp_view("input_table_hybrid", base_df)
            result = session.hybrid_search(
                table="input_table_hybrid",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
                score_threshold=score_threshold,
            ).to_arrow()
        else:
            result = session.vector_search(
                table="input_table",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
            ).to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute hybrid search" if (where_column or score_threshold is not None)
            else "failed to execute vector search",
            phase="hybrid_search" if (where_column or score_threshold is not None) else "vector_search",
            details={
                "csv": str(csv_path),
                "vector_column": vector_column,
                "metric": _normalize_metric(metric),
                "top_k": top_k,
                "where_column": where_column,
                "where_op": where_op,
                "score_threshold": score_threshold,
            },
        ) from exc
    try:
        if where_column or score_threshold is not None:
            explain = session.explain_hybrid_search(
                table="input_table_hybrid",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
                score_threshold=score_threshold,
            )
        else:
            explain = session.explain_vector_search(
                table="input_table",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
            )
    except Exception as exc:
        raise CliStructuredError(
            "failed to explain hybrid search" if (where_column or score_threshold is not None)
            else "failed to explain vector search",
            phase="hybrid_explain" if (where_column or score_threshold is not None) else "vector_explain",
            details={
                "csv": str(csv_path),
                "vector_column": vector_column,
                "metric": _normalize_metric(metric),
                "top_k": top_k,
                "where_column": where_column,
                "where_op": where_op,
                "score_threshold": score_threshold,
            },
        ) from exc
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(
            _table_artifact(
                output_path,
                result,
                ["result", "hybrid-search" if (where_column or score_threshold is not None) else "vector-search"],
            )
        )
    if explain_path is not None:
        artifacts.append(
            _text_artifact(
                explain_path,
                explain,
                ["explain", "hybrid-search" if (where_column or score_threshold is not None) else "vector-search"],
            )
        )
    return {
        "payload": {
            "metric": _normalize_metric(metric),
            "top_k": top_k,
            "where_column": where_column,
            "where_op": where_op,
            "where_value": where_value,
            "score_threshold": score_threshold,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
            "explain": explain,
        },
        "artifacts": artifacts,
    }


def _execute_embedding_build(
    input_path: pathlib.Path,
    *,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    output_path: pathlib.Path,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
) -> dict[str, Any]:
    session = Session()
    embedding_provider, resolved_model = _make_embedding_provider(provider, model)
    text_fields = _split_csv_values(text_columns)
    if not text_fields:
        raise CliStructuredError(
            "--text-columns must not be empty",
            phase="argument_parse",
            details={"text_columns": text_columns},
        )
    json_field_list = (
        _split_csv_values(json_columns)
        if json_columns
        else (
            _infer_embedding_json_columns(
                text_columns=text_fields,
                doc_id_field=doc_id_field,
                source_updated_at_field=source_updated_at_field,
            )
            if input_type == "json"
            else None
        )
    )
    try:
        table = build_file_embeddings(
            session,
            input_path,
            provider=embedding_provider,
            model=resolved_model,
            template_version=template_version,
            text_columns=text_fields,
            input_type=input_type,
            delimiter=delimiter,
            json_columns=json_field_list,
            json_format=json_format,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            output_path=output_path,
        )
    except Exception as exc:
        raise CliStructuredError(
            "failed to build embedding dataset",
            phase="embedding_build",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "text_columns": text_fields,
                "provider": provider,
                "model": resolved_model,
                "output_path": str(output_path),
            },
        ) from exc
    artifacts = [_table_artifact(output_path, table, ["result", "embedding-build"])]
    return {
        "payload": {
            "input_path": str(input_path),
            "input_type": input_type,
            "text_columns": text_fields,
            "provider": provider,
            "model": resolved_model,
            "output_path": str(output_path),
            "schema": table.schema.names,
            "row_count": table.num_rows,
        },
        "artifacts": artifacts,
    }


def _execute_embedding_query(
    *,
    dataset_path: pathlib.Path | None,
    input_path: pathlib.Path | None,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    query_text: str,
    top_k: int,
    metric: str,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    output_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    session = Session()
    embedding_provider, resolved_model = _make_embedding_provider(provider, model)
    text_fields = _split_csv_values(text_columns)
    if dataset_path is None and input_path is None:
        raise CliStructuredError(
            "either --dataset-path or --input-path must be provided",
            phase="argument_parse",
        )
    if dataset_path is None and not text_fields:
        raise CliStructuredError(
            "--text-columns must not be empty when --input-path is used",
            phase="argument_parse",
            details={"text_columns": text_columns},
        )
    if where_sql is not None and (
        where_column is not None or where_op is not None or where_value is not None
    ):
        raise CliStructuredError(
            "--where-sql cannot be used together with --where-column/--where-op/--where-value",
            phase="argument_parse",
        )
    if where_sql is None and (where_column is not None or where_op is not None or where_value is not None) and (
        where_column is None or where_op is None or where_value is None
    ):
        raise CliStructuredError(
            "--where-column, --where-op, and --where-value must be provided together",
            phase="argument_parse",
            details={
                "where_column": where_column,
                "where_op": where_op,
                "where_value": where_value,
            },
        )

    try:
        if dataset_path is not None:
            result_df = query_file_embeddings(
                session,
                dataset_path,
                provider=embedding_provider,
                model=resolved_model,
                query_text=query_text,
                top_k=top_k,
                metric=metric,
                where_sql=where_sql,
                where_column=where_column,
                where_op=where_op,
                where_value=_parse_scalar_text(where_value) if where_value is not None else None,
            )
        else:
            json_field_list = (
                _split_csv_values(json_columns)
                if json_columns
                else (
                    _infer_embedding_json_columns(
                        text_columns=text_fields,
                        doc_id_field=doc_id_field,
                        source_updated_at_field=source_updated_at_field,
                        where_column=where_column,
                    )
                    if input_type == "json"
                    else None
                )
            )
            result_df = run_file_mixed_text_hybrid_search(
                session,
                input_path,
                provider=embedding_provider,
                model=resolved_model,
                query_text=query_text,
                template_version=template_version,
                text_columns=text_fields,
                input_type=input_type,
                delimiter=delimiter,
                json_columns=json_field_list,
                json_format=json_format,
                doc_id_field=doc_id_field,
                source_updated_at_field=source_updated_at_field,
                top_k=top_k,
                metric=metric,
                where_sql=where_sql,
                where_column=where_column,
                where_op=where_op,
                where_value=_parse_scalar_text(where_value) if where_value is not None else None,
            )
        result = result_df.to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute embedding query",
            phase="embedding_query",
            details={
                "dataset_path": str(dataset_path) if dataset_path else None,
                "input_path": str(input_path) if input_path else None,
                "input_type": input_type,
                "provider": provider,
                "model": resolved_model,
                "query_text": query_text,
            },
        ) from exc

    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "embedding-query"]))
    return {
        "payload": {
            "dataset_path": str(dataset_path) if dataset_path else None,
            "input_path": str(input_path) if input_path else None,
            "input_type": input_type,
            "text_columns": text_fields,
            "provider": provider,
            "model": resolved_model,
            "query_text": query_text,
            "top_k": top_k,
            "metric": metric,
            "where_sql": where_sql,
            "where_column": where_column,
            "where_op": where_op,
            "where_value": where_value,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
        },
        "artifacts": artifacts,
    }


def _execute_stream_sql_once(
    source_csv_dir: pathlib.Path,
    source_table: str,
    source_delimiter: str,
    sink_table: str,
    sink_schema: str,
    sink_path: pathlib.Path,
    sink_delimiter: str,
    query: str,
    trigger_interval_ms: int,
    checkpoint_delivery_mode: str,
    execution_mode: str,
    local_workers: int,
    max_inflight_partitions: int,
    max_batches: int,
    run_id: str,
) -> dict[str, Any]:
    if not query.lstrip().upper().startswith("INSERT INTO"):
        raise CliStructuredError(
            "--query for stream-sql-once must start with INSERT INTO",
            phase="argument_parse",
            details={"query": query},
            run_id=run_id,
        )
    effective_max_batches = max_batches if max_batches > 0 else 1
    session = Session()
    try:
        stream_df = session.read_stream_csv_dir(str(source_csv_dir), delimiter=source_delimiter)
    except Exception as exc:
        raise CliStructuredError(
            "failed to read stream csv directory",
            phase="stream_source_read",
            details={"source_csv_dir": str(source_csv_dir), "delimiter": source_delimiter},
            run_id=run_id,
        ) from exc
    try:
        session.create_temp_view(source_table, stream_df)
    except Exception as exc:
        raise CliStructuredError(
            "failed to register stream temp view",
            phase="stream_register_view",
            details={"source_table": source_table, "source_csv_dir": str(source_csv_dir)},
            run_id=run_id,
        ) from exc
    try:
        session.sql(
            f"CREATE SINK TABLE {sink_table} ({sink_schema}) "
            f"USING csv OPTIONS(path: '{_normalize_path(sink_path)}', delimiter: '{sink_delimiter}')"
        )
    except Exception as exc:
        raise CliStructuredError(
            "failed to create sink table",
            phase="stream_setup",
            details={
                "sink_table": sink_table,
                "sink_schema": sink_schema,
                "sink_path": str(sink_path),
            },
            run_id=run_id,
        ) from exc
    try:
        explain = session.explain_stream_sql(
            query,
            trigger_interval_ms=trigger_interval_ms,
            checkpoint_delivery_mode=checkpoint_delivery_mode,
            execution_mode=execution_mode,
            local_workers=local_workers,
            max_inflight_partitions=max_inflight_partitions,
        )
    except Exception as exc:
        raise CliStructuredError(
            "failed to explain stream sql",
            phase="stream_explain",
            details={"query": query, "execution_mode": execution_mode},
            run_id=run_id,
        ) from exc
    write_explain(run_id, _parse_explain_sections(explain))
    try:
        streaming_query = session.start_stream_sql(
            query,
            trigger_interval_ms=trigger_interval_ms,
            checkpoint_delivery_mode=checkpoint_delivery_mode,
            execution_mode=execution_mode,
            local_workers=local_workers,
            max_inflight_partitions=max_inflight_partitions,
        )
    except Exception as exc:
        raise CliStructuredError(
            "failed to start stream sql",
            phase="stream_execute",
            details={"query": query, "execution_mode": execution_mode},
            run_id=run_id,
        ) from exc
    try:
        streaming_query.start()
    except Exception as exc:
        raise CliStructuredError(
            "failed to start stream execution",
            phase="stream_start",
            details={"query": query, "execution_mode": execution_mode},
            run_id=run_id,
        ) from exc
    append_progress_snapshot(run_id, streaming_query.snapshot_json())
    try:
        processed = streaming_query.await_termination(max_batches=effective_max_batches)
    except Exception as exc:
        raise CliStructuredError(
            "failed while waiting for stream execution",
            phase="stream_wait",
            details={
                "query": query,
                "execution_mode": execution_mode,
                "max_batches": effective_max_batches,
            },
            run_id=run_id,
        ) from exc
    append_progress_snapshot(run_id, streaming_query.snapshot_json())
    try:
        result = session.read_csv(str(sink_path)).to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to read stream sink result",
            phase="result_materialize",
            details={"sink_path": str(sink_path), "sink_table": sink_table},
            run_id=run_id,
        ) from exc
    artifacts = [_table_artifact(sink_path, result, ["result", "stream-sql-once"])]
    return {
        "payload": {
            "processed_batches": processed,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
            "progress": streaming_query.progress(),
        },
        "artifacts": artifacts,
    }


def _run_csv_sql(
    input_path: pathlib.Path,
    table: str,
    query: str,
    *,
    input_type: str = "csv",
    delimiter: str = ",",
    line_mode: str = "split",
    regex_pattern: str | None = None,
    mappings: str | None = None,
    columns: str | None = None,
    json_format: str = "json_lines",
) -> int:
    return _emit_json(
        _execute_csv_sql(
            input_path,
            table,
            query,
            input_type=input_type,
            delimiter=delimiter,
            line_mode=line_mode,
            regex_pattern=regex_pattern,
            mappings=mappings,
            columns=columns,
            json_format=json_format,
        )["payload"]
    )


def _run_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    score_threshold: float | None = None,
) -> int:
    return _emit_json(
        _execute_vector_search(
            csv_path,
            vector_column,
            query_vector,
            metric,
            top_k,
            where_column=where_column,
            where_op=where_op,
            where_value=where_value,
            score_threshold=score_threshold,
        )["payload"]
    )


def _run_embedding_build(
    input_path: pathlib.Path,
    *,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    output_path: pathlib.Path,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
) -> int:
    return _emit_json(
        _execute_embedding_build(
            input_path,
            input_type=input_type,
            text_columns=text_columns,
            provider=provider,
            model=model,
            output_path=output_path,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            delimiter=delimiter,
            json_columns=json_columns,
            json_format=json_format,
            template_version=template_version,
        )["payload"]
    )


def _run_embedding_query(
    *,
    dataset_path: pathlib.Path | None,
    input_path: pathlib.Path | None,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    query_text: str,
    top_k: int,
    metric: str,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
) -> int:
    return _emit_json(
        _execute_embedding_query(
            dataset_path=dataset_path,
            input_path=input_path,
            input_type=input_type,
            text_columns=text_columns,
            provider=provider,
            model=model,
            query_text=query_text,
            top_k=top_k,
            metric=metric,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            delimiter=delimiter,
            json_columns=json_columns,
            json_format=json_format,
            template_version=template_version,
            where_sql=where_sql,
            where_column=where_column,
            where_op=where_op,
            where_value=where_value,
        )["payload"]
    )


def _child_result_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "_action_result.json"


def _worker_stdout_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "stdout.log"


def _worker_stderr_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "stderr.log"


def _execute_action_for_run(spec: dict[str, Any]) -> dict[str, Any]:
    action = spec["action"]
    args = spec["action_args"]
    run_id = spec["run_id"]
    run_dir = pathlib.Path(spec["run_dir"])
    artifacts_dir = run_dir / "artifacts"
    if action == "file-sql":
        output_path = (
            pathlib.Path(args["output_path"]) if args.get("output_path") else artifacts_dir / "result.parquet"
        )
        return _execute_csv_sql(
            input_path=pathlib.Path(args["input_path"]),
            table=args["table"],
            query=args["query"],
            input_type=args["input_type"],
            delimiter=args["delimiter"],
            line_mode=args["line_mode"],
            regex_pattern=args.get("regex_pattern"),
            mappings=args.get("mappings"),
            columns=args.get("columns"),
            json_format=args["json_format"],
            output_path=output_path,
            run_id=run_id,
        )
    if action == "vector-search":
        output_path = (
            pathlib.Path(args["output_path"]) if args.get("output_path") else artifacts_dir / "result.parquet"
        )
        explain_path = artifacts_dir / "vector_explain.txt"
        return _execute_vector_search(
            csv_path=pathlib.Path(args["csv"]),
            vector_column=args["vector_column"],
            query_vector=args["query_vector"],
            metric=args["metric"],
            top_k=args["top_k"],
            where_column=args.get("where_column"),
            where_op=args.get("where_op"),
            where_value=args.get("where_value"),
            score_threshold=args.get("score_threshold"),
            output_path=output_path,
            explain_path=explain_path,
        )
    if action == "embedding-build":
        output_path = pathlib.Path(args["output_path"]) if args.get("output_path") else artifacts_dir / "embeddings.parquet"
        return _execute_embedding_build(
            pathlib.Path(args["input_path"]),
            input_type=args["input_type"],
            text_columns=args["text_columns"],
            provider=args["provider"],
            model=args.get("model"),
            output_path=output_path,
            doc_id_field=args["doc_id_field"],
            source_updated_at_field=args["source_updated_at_field"],
            delimiter=args["delimiter"],
            json_columns=args.get("json_columns"),
            json_format=args["json_format"],
            template_version=args["template_version"],
        )
    if action == "embedding-query":
        output_path = (
            pathlib.Path(args["output_path"]) if args.get("output_path") else artifacts_dir / "result.parquet"
        )
        return _execute_embedding_query(
            dataset_path=pathlib.Path(args["dataset_path"]) if args.get("dataset_path") else None,
            input_path=pathlib.Path(args["input_path"]) if args.get("input_path") else None,
            input_type=args["input_type"],
            text_columns=args.get("text_columns") or "",
            provider=args["provider"],
            model=args.get("model"),
            query_text=args["query_text"],
            top_k=args["top_k"],
            metric=args["metric"],
            doc_id_field=args["doc_id_field"],
            source_updated_at_field=args["source_updated_at_field"],
            delimiter=args["delimiter"],
            json_columns=args.get("json_columns"),
            json_format=args["json_format"],
            template_version=args["template_version"],
            where_sql=args.get("where_sql"),
            where_column=args.get("where_column"),
            where_op=args.get("where_op"),
            where_value=args.get("where_value"),
            output_path=output_path,
        )
    if action == "stream-sql-once":
        sink_path = (
            pathlib.Path(args["sink_path"]) if args.get("sink_path") else artifacts_dir / "stream_result.csv"
        )
        return _execute_stream_sql_once(
            source_csv_dir=pathlib.Path(args["source_csv_dir"]),
            source_table=args["source_table"],
            source_delimiter=args["source_delimiter"],
            sink_table=args["sink_table"],
            sink_schema=args["sink_schema"],
            sink_path=sink_path,
            sink_delimiter=args["sink_delimiter"],
            query=args["query"],
            trigger_interval_ms=args["trigger_interval_ms"],
            checkpoint_delivery_mode=args["checkpoint_delivery_mode"],
            execution_mode=args["execution_mode"],
            local_workers=args["local_workers"],
            max_inflight_partitions=args["max_inflight_partitions"],
            max_batches=args["max_batches"],
            run_id=run_id,
        )
    raise CliStructuredError(
        f"unsupported run action: {action}",
        phase="run_action_dispatch",
        details={"action": action},
        run_id=run_id,
    )


def _run_action_subprocess_target(
    spec: dict[str, Any],
    result_path: str,
    stdout_path: str,
    stderr_path: str,
) -> None:
    with open(stdout_path, "a", encoding="utf-8") as stdout_handle, open(
        stderr_path, "a", encoding="utf-8"
    ) as stderr_handle, contextlib.redirect_stdout(stdout_handle), contextlib.redirect_stderr(
        stderr_handle
    ):
        try:
            result = _execute_action_for_run(spec)
            pathlib.Path(result_path).write_text(_json_dumps(result), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - exercised via parent behavior
            traceback.print_exc(file=stderr_handle)
            failure = _error_payload_from_exception(exc, run_id=spec.get("run_id"))
            failure["traceback"] = traceback.format_exc()
            pathlib.Path(result_path).write_text(_json_dumps(failure), encoding="utf-8")
            raise


def _run_action_with_timeout(spec: dict[str, Any], timeout_ms: int | None) -> dict[str, Any]:
    result_path = _child_result_path(pathlib.Path(spec["run_dir"]))
    stdout_path = _worker_stdout_path(pathlib.Path(spec["run_dir"]))
    stderr_path = _worker_stderr_path(pathlib.Path(spec["run_dir"]))
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_run_action_subprocess_target,
        args=(spec, str(result_path), str(stdout_path), str(stderr_path)),
    )
    process.start()
    process.join(None if timeout_ms is None else timeout_ms / 1000.0)
    if process.is_alive():
        process.terminate()
        process.join()
        return {
            "timed_out": True,
            "error": f"run timed out after {timeout_ms} ms",
            "error_type": "timeout",
            "phase": "run_timeout",
            "details": {"timeout_ms": timeout_ms},
            "run_id": spec.get("run_id"),
        }
    if not result_path.exists():
        return {
            "timed_out": False,
            "error": f"action process exited with code {process.exitcode}",
            "error_type": "internal_error",
            "phase": "run_subprocess",
            "details": {"exit_code": process.exitcode},
            "run_id": spec.get("run_id"),
        }
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if process.exitcode not in (0, None):
        payload.setdefault("error", f"action process exited with code {process.exitcode}")
        payload.setdefault("error_type", "internal_error")
        payload.setdefault("phase", "run_subprocess")
        payload.setdefault("details", {"exit_code": process.exitcode})
        payload.setdefault("run_id", spec.get("run_id"))
    return payload


def _register_artifacts(
    index: ArtifactIndex,
    run_id: str,
    artifacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    created: list[dict[str, Any]] = []
    for artifact in artifacts:
        record = dict(artifact)
        record["artifact_id"] = _new_artifact_id()
        record["run_id"] = run_id
        record["created_at"] = _utc_now()
        index.insert_artifact(record)
        created.append(record)
    return created


def _add_csv_sql_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--csv", "--input-path", dest="input_path", required=True, help="Input file path.")
    parser.add_argument(
        "--table",
        default="input_table",
        help="Temporary table name exposed to session.sql(...).",
    )
    parser.add_argument(
        "--input-type",
        default="auto",
        choices=["auto", "csv", "line", "json"],
        help="Input file type. Defaults to auto probe.",
    )
    parser.add_argument("--delimiter", default=",", help="Delimiter for csv or line split sources.")
    parser.add_argument(
        "--line-mode",
        default="split",
        choices=["split", "regex"],
        help="Line source mode when --input-type=line.",
    )
    parser.add_argument("--regex-pattern", help="Regex pattern for line regex mode.")
    parser.add_argument("--mappings", help="Line source mappings like 'uid:1,action:2'.")
    parser.add_argument(
        "--columns",
        help="Comma-separated source column names for json or sequential line parsing.",
    )
    parser.add_argument(
        "--json-format",
        default="json_lines",
        choices=["json_lines", "json_array"],
        help="JSON source format when --input-type=json.",
    )
    parser.add_argument("--query", required=True, help="SQL query text.")


def _add_vector_search_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--csv", required=True, help="CSV file path.")
    parser.add_argument(
        "--vector-column",
        required=True,
        help="Vector column name. CSV row value format should use bracketed vectors like '[1 2 3]' or '[1,2,3]'.",
    )
    parser.add_argument("--query-vector", required=True, help="Query vector, e.g. '0.1,0.2,0.3'.")
    parser.add_argument(
        "--metric",
        default="cosine",
        choices=["cosine", "cosin", "dot", "l2"],
        help="Distance metric.",
    )
    parser.add_argument("--top-k", type=int, default=5, help="Return top-k nearest rows.")
    parser.add_argument("--where-column", help="Optional column filter for hybrid search mode.")
    parser.add_argument("--where-op", help="Optional filter operator such as =, !=, <, >, <=, >=.")
    parser.add_argument("--where-value", help="Optional filter literal for hybrid search mode.")
    parser.add_argument("--score-threshold", type=float, help="Optional score threshold for hybrid search mode.")


def _add_embedding_common_arguments(parser: argparse.ArgumentParser, *, require_input: bool) -> None:
    if require_input:
        parser.add_argument("--input-path", required=True, help="Input batch file path.")
    else:
        parser.add_argument("--input-path", help="Optional source file path when querying without a prebuilt dataset.")
    parser.add_argument(
        "--input-type",
        default="auto",
        choices=["auto", "csv", "json"],
        help="Input file type for source data.",
    )
    parser.add_argument("--delimiter", default=",", help="Delimiter for CSV input.")
    parser.add_argument("--json-columns", help="Comma-separated JSON columns to read.")
    parser.add_argument(
        "--json-format",
        default="json_lines",
        choices=["json_lines", "json_array"],
        help="JSON source format.",
    )
    parser.add_argument("--text-columns", required=require_input, help="Comma-separated columns used to build embedding text.")
    parser.add_argument("--provider", choices=["hash", "minilm"], default="hash")
    parser.add_argument("--model", help="Embedding model name. Defaults to provider-specific default.")
    parser.add_argument("--template-version", default="text-v1")
    parser.add_argument("--doc-id-field", default="doc_id")
    parser.add_argument("--source-updated-at-field", default="source_updated_at")


def _add_embedding_build_arguments(parser: argparse.ArgumentParser) -> None:
    _add_embedding_common_arguments(parser, require_input=True)
    parser.add_argument("--output-path", required=True, help="Output embedding dataset path (.parquet/.arrow/.ipc).")


def _add_embedding_query_arguments(parser: argparse.ArgumentParser) -> None:
    _add_embedding_common_arguments(parser, require_input=False)
    parser.add_argument("--dataset-path", help="Prebuilt embedding dataset path.")
    parser.add_argument("--query-text", required=True, help="Query text to embed and search.")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--metric", choices=["cosine", "dot", "l2"], default="cosine")
    parser.add_argument("--where-sql", help="Optional SQL WHERE fragment, e.g. \"bucket = 1 AND region = 'apac'\".")
    parser.add_argument("--where-column")
    parser.add_argument("--where-op")
    parser.add_argument("--where-value")


def _add_stream_sql_once_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-csv-dir", required=True, help="Streaming source CSV directory.")
    parser.add_argument("--source-table", default="input_stream", help="Streaming source temp view name.")
    parser.add_argument("--source-delimiter", default=",", help="Streaming source CSV delimiter.")
    parser.add_argument("--sink-table", default="output_sink", help="Sink table name.")
    parser.add_argument("--sink-schema", required=True, help="Sink schema body used in CREATE SINK TABLE.")
    parser.add_argument("--sink-path", help="Sink CSV path. Defaults to run_dir/artifacts/stream_result.csv.")
    parser.add_argument("--sink-delimiter", default=",", help="Sink CSV delimiter.")
    parser.add_argument("--query", required=True, help="INSERT INTO ... SELECT ... streaming SQL.")
    parser.add_argument("--trigger-interval-ms", type=int, default=0)
    parser.add_argument("--checkpoint-delivery-mode", default="at-least-once")
    parser.add_argument("--execution-mode", default="single-process")
    parser.add_argument("--local-workers", type=int, default=1)
    parser.add_argument("--max-inflight-partitions", type=int, default=0)
    parser.add_argument("--max-batches", type=int, default=1)


def _parse_action_args(action: str, argv: list[str]) -> dict[str, Any]:
    parser = JsonArgumentParser(prog=f"velaria-cli {action}")
    if action == "file-sql":
        _add_csv_sql_arguments(parser)
        parser.add_argument("--output-path")
    elif action == "vector-search":
        _add_vector_search_arguments(parser)
        parser.add_argument("--output-path")
    elif action == "stream-sql-once":
        _add_stream_sql_once_arguments(parser)
    else:
        raise CliStructuredError(
            f"unsupported run action: {action}",
            phase="run_action_parse",
            details={"action": action},
        )
    return vars(parser.parse_args(argv))


def _parse_passthrough(command_args: list[str]) -> tuple[str, dict[str, Any]]:
    if command_args and command_args[0] == "--":
        command_args = command_args[1:]
    if not command_args:
        raise CliUsageError("run start requires an action after '--'")
    return command_args[0], _parse_action_args(command_args[0], command_args[1:])


def _normalize_tags(tags: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for raw_tag in tags or []:
        for piece in raw_tag.split(","):
            tag = piece.strip()
            if tag and tag not in normalized:
                normalized.append(tag)
    return normalized


def _load_latest_progress(run_id: str) -> dict[str, Any] | None:
    progress_path = pathlib.Path(_read_run_or_raise(run_id)["run_dir"]) / "progress.jsonl"
    if not progress_path.exists():
        return None
    last_line = ""
    with progress_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                last_line = line.strip()
    return json.loads(last_line) if last_line else None


def _read_run_or_raise(run_id: str) -> dict[str, Any]:
    try:
        return read_run(run_id)
    except FileNotFoundError as exc:
        raise CliStructuredError(
            f"run not found: {run_id}",
            error_type="file_not_found",
            phase="run_lookup",
            details={"run_id": run_id},
            run_id=run_id,
        ) from exc


def _run_duration_ms(run: dict[str, Any]) -> int | None:
    created_at = run.get("created_at")
    finished_at = run.get("finished_at")
    if not created_at or not finished_at:
        return None
    started = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return max(0, int((finished - started).total_seconds() * 1000))


def _enrich_run_summary(index: ArtifactIndex, run: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(run)
    enriched["artifact_count"] = len(index.list_artifacts(run_id=run["run_id"], limit=1000000))
    enriched["duration_ms"] = _run_duration_ms(run)
    return enriched


def _find_run_result_artifact(index: ArtifactIndex, run_id: str) -> dict[str, Any]:
    artifacts = index.list_artifacts(run_id=run_id, limit=1000000, tag="result")
    if not artifacts:
        raise CliStructuredError(
            f"result artifact not found for run: {run_id}",
            error_type="file_not_found",
            phase="run_result",
            details={"run_id": run_id},
            run_id=run_id,
        )
    return artifacts[0]


def _diff_values(left: Any, right: Any) -> dict[str, Any] | None:
    if left == right:
        return None
    return {
        "left": left,
        "right": right,
    }


def _build_run_metadata_diff(left_run: dict[str, Any], right_run: dict[str, Any]) -> dict[str, Any]:
    diffs: dict[str, Any] = {}
    for field in (
        "status",
        "action",
        "run_name",
        "description",
        "tags",
        "artifact_count",
        "duration_ms",
        "error",
    ):
        diff = _diff_values(left_run.get(field), right_run.get(field))
        if diff is not None:
            diffs[field] = diff
    return diffs


def _build_result_diff(
    index: ArtifactIndex,
    left_run_id: str,
    right_run_id: str,
    limit: int,
) -> dict[str, Any]:
    left_artifact = _find_run_result_artifact(index, left_run_id)
    right_artifact = _find_run_result_artifact(index, right_run_id)
    left_preview = _read_preview_for_artifact(left_artifact, limit=limit)
    right_preview = _read_preview_for_artifact(right_artifact, limit=limit)
    return {
        "left_artifact": left_artifact,
        "right_artifact": right_artifact,
        "comparison": {
            "format": _diff_values(left_artifact.get("format"), right_artifact.get("format")),
            "row_count": {
                "left": left_artifact.get("row_count"),
                "right": right_artifact.get("row_count"),
                "delta": (
                    (right_artifact.get("row_count") or 0) - (left_artifact.get("row_count") or 0)
                    if left_artifact.get("row_count") is not None
                    and right_artifact.get("row_count") is not None
                    else None
                ),
            },
            "schema": {
                "left": left_artifact.get("schema_json"),
                "right": right_artifact.get("schema_json"),
                "equal": left_artifact.get("schema_json") == right_artifact.get("schema_json"),
            },
            "preview": {
                "left": left_preview,
                "right": right_preview,
            },
        },
    }


def _run_start(args: argparse.Namespace) -> int:
    action, action_args = _parse_passthrough(args.command_args)
    tags = _normalize_tags(args.tag)
    run_id, run_dir = create_run(
        action,
        action_args,
        __version__,
        run_name=args.run_name,
        description=args.description,
        tags=tags,
    )
    write_inputs(
        run_id,
        {
            "action": action,
            "action_args": action_args,
            "run_name": args.run_name,
            "description": args.description,
            "tags": tags,
            "timeout_ms": args.timeout_ms,
        },
    )
    index = ArtifactIndex()
    try:
        index.upsert_run(read_run(run_id))
        spec = {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "action": action,
            "action_args": action_args,
        }
        result = _run_action_with_timeout(spec, args.timeout_ms)
        if result.get("timed_out"):
            append_stderr(run_id, f"{result['error']}\n")
            finalized = finalize_run(
                run_id,
                "timed_out",
                error=result["error"],
                details={
                    "error_type": result.get("error_type"),
                    "phase": result.get("phase"),
                    "error_details": result.get("details", {}),
                },
            )
            index.upsert_run(finalized)
            _emit_json(
                {
                    "ok": False,
                    "run_id": run_id,
                    "run_dir": str(run_dir),
                    "status": "timed_out",
                    "error": result["error"],
                    "error_type": result.get("error_type", "timeout"),
                    "phase": result.get("phase"),
                    "details": result.get("details", {}),
                }
            )
            return 1
        if "error" in result and "payload" not in result:
            append_stderr(run_id, f"{result['error']}\n")
            if result.get("traceback"):
                append_stderr(run_id, result["traceback"])
            finalized = finalize_run(
                run_id,
                "failed",
                error=result["error"],
                details={
                    "error_type": result.get("error_type"),
                    "phase": result.get("phase"),
                    "error_details": result.get("details", {}),
                },
            )
            index.upsert_run(finalized)
            _emit_json(
                {
                    "ok": False,
                    "run_id": run_id,
                    "run_dir": str(run_dir),
                    "status": "failed",
                    "error": result["error"],
                    "error_type": result.get("error_type", "value_error"),
                    "phase": result.get("phase"),
                    "details": result.get("details", {}),
                }
            )
            return 1
        created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
        details: dict[str, Any] = {}
        for artifact in created_artifacts:
            if "explain" in artifact.get("tags_json", []):
                details["explain_artifact_id"] = artifact["artifact_id"]
                details["explain_artifact_uri"] = artifact["uri"]
        if details:
            update_run(run_id, details=details)
        finalized = finalize_run(run_id, "succeeded")
        index.upsert_run(finalized)
        return _emit_json(
            {
                "ok": True,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "status": "succeeded",
                "action": action,
                "tags": tags,
                "result": result["payload"],
                "artifacts": created_artifacts,
            }
        )
    finally:
        index.close()


def _run_list(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        return _emit_json(
            {
                "ok": True,
                "runs": index.list_runs(
                    limit=args.limit,
                    status=args.status,
                    action=args.action,
                    tag=args.tag,
                    query=args.query,
                ),
            }
        )
    finally:
        index.close()


def _run_show(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        run = _enrich_run_summary(index, _read_run_or_raise(args.run_id))
        artifacts = index.list_artifacts(run_id=args.run_id, limit=args.limit)
        return _emit_json(
            {
                "ok": True,
                "run": run,
                "artifacts": artifacts,
            }
        )
    finally:
        index.close()


def _run_result(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        run = _enrich_run_summary(index, _read_run_or_raise(args.run_id))
        artifact = _find_run_result_artifact(index, args.run_id)
        preview = _read_preview_for_artifact(artifact, limit=args.limit)
        if artifact.get("preview_json") is None:
            index.update_artifact_preview(artifact["artifact_id"], preview)
            artifact = index.get_artifact(artifact["artifact_id"]) or artifact
        return _emit_json(
            {
                "ok": True,
                "run": run,
                "artifact_id": artifact["artifact_id"],
                "artifact": artifact,
                "preview": preview,
            }
        )
    finally:
        index.close()


def _run_diff(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        left_run = _enrich_run_summary(index, _read_run_or_raise(args.run_id))
        right_run = _enrich_run_summary(index, _read_run_or_raise(args.other_run_id))
        return _emit_json(
            {
                "ok": True,
                "left_run": left_run,
                "right_run": right_run,
                "metadata_diff": _build_run_metadata_diff(left_run, right_run),
                "result_diff": _build_result_diff(
                    index,
                    args.run_id,
                    args.other_run_id,
                    args.limit,
                ),
            }
        )
    finally:
        index.close()


def _run_status(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        run = _enrich_run_summary(index, _read_run_or_raise(args.run_id))
        payload: dict[str, Any] = {
            "ok": True,
            "run_id": args.run_id,
            "status": run["status"],
            "action": run["action"],
            "artifacts": index.list_artifacts(run_id=args.run_id, limit=args.limit),
            "artifact_count": run["artifact_count"],
            "duration_ms": run["duration_ms"],
        }
        latest_progress = _load_latest_progress(args.run_id)
        if latest_progress is not None:
            payload["latest_progress"] = latest_progress
        return _emit_json(payload)
    finally:
        index.close()


def _artifacts_list(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        return _emit_json(
            {
                "ok": True,
                "artifacts": index.list_artifacts(limit=args.limit, run_id=args.run_id),
            }
        )
    finally:
        index.close()


def _artifacts_preview(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        artifact = index.get_artifact(args.artifact_id)
        if artifact is None:
            raise CliStructuredError(
                f"artifact not found: {args.artifact_id}",
                error_type="file_not_found",
                phase="artifact_lookup",
                details={"artifact_id": args.artifact_id},
            )
        preview = _read_preview_for_artifact(artifact, limit=args.limit)
        if artifact.get("preview_json") is None:
            index.update_artifact_preview(args.artifact_id, preview)
            artifact = index.get_artifact(args.artifact_id) or artifact
        return _emit_json(
            {
                "ok": True,
                "artifact_id": args.artifact_id,
                "preview": preview,
                "artifact": artifact,
            }
        )
    finally:
        index.close()


def _run_cleanup(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        payload = index.cleanup_runs(
            keep_last_n=args.keep_last,
            ttl_days=args.ttl_days,
            delete_files=args.delete_files,
        )
        payload["ok"] = True
        return _emit_json(payload)
    finally:
        index.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(
        prog="velaria-cli",
        description="Velaria CLI for SQL query execution and workspace run management.",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Enter interactive mode.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    csv_sql = subparsers.add_parser(
        "file-sql",
        help="Register a batch file source and run a SQL query through DataflowSession.",
    )
    _add_csv_sql_arguments(csv_sql)

    vector_search = subparsers.add_parser(
        "vector-search",
        help="Read CSV and run fixed-length vector nearest search.",
    )
    _add_vector_search_arguments(vector_search)

    embedding_build = subparsers.add_parser(
        "embedding-build",
        help="Read a batch file, mix selected columns into text, and materialize an embedding dataset.",
    )
    _add_embedding_build_arguments(embedding_build)

    embedding_query = subparsers.add_parser(
        "embedding-query",
        help="Run mixed-text embedding plus hybrid search from a dataset or directly from an input file.",
    )
    _add_embedding_query_arguments(embedding_query)

    run_parser = subparsers.add_parser("run", help="Run management commands.")
    run_subparsers = run_parser.add_subparsers(dest="run_command", required=True)

    run_start = run_subparsers.add_parser("start", help="Start a tracked run.")
    run_start.add_argument("--run-name")
    run_start.add_argument("--description", "--run-description", dest="description")
    run_start.add_argument(
        "--tag",
        action="append",
        help="Attach a tag to the run. Repeat or use comma-separated values.",
    )
    run_start.add_argument("--timeout-ms", type=int)
    run_start.add_argument("command_args", nargs=argparse.REMAINDER)

    run_list = run_subparsers.add_parser("list", help="List tracked runs.")
    run_list.add_argument("--status")
    run_list.add_argument("--action")
    run_list.add_argument("--tag")
    run_list.add_argument("--query")
    run_list.add_argument("--limit", type=int, default=50)

    run_result = run_subparsers.add_parser(
        "result",
        help="Show the primary result artifact for a run.",
    )
    run_result.add_argument("--run-id", required=True)
    run_result.add_argument("--limit", type=int, default=PREVIEW_LIMIT_ROWS)

    run_diff = run_subparsers.add_parser(
        "diff",
        help="Compare two runs and their primary result artifacts.",
    )
    run_diff.add_argument("--run-id", required=True)
    run_diff.add_argument("--other-run-id", required=True)
    run_diff.add_argument("--limit", type=int, default=PREVIEW_LIMIT_ROWS)

    run_show = run_subparsers.add_parser("show", help="Show a run and its artifacts.")
    run_show.add_argument("--run-id", required=True)
    run_show.add_argument("--limit", type=int, default=50)

    run_status = run_subparsers.add_parser("status", help="Show run status.")
    run_status.add_argument("--run-id", required=True)
    run_status.add_argument("--limit", type=int, default=20)

    run_cleanup = run_subparsers.add_parser("cleanup", help="Cleanup indexed runs.")
    run_cleanup.add_argument("--keep-last", type=int)
    run_cleanup.add_argument("--ttl-days", type=int)
    run_cleanup.add_argument("--delete-files", action="store_true")

    artifacts_parser = subparsers.add_parser("artifacts", help="Artifact index commands.")
    artifacts_subparsers = artifacts_parser.add_subparsers(dest="artifacts_command", required=True)

    artifacts_list = artifacts_subparsers.add_parser("list", help="List artifacts.")
    artifacts_list.add_argument("--run-id")
    artifacts_list.add_argument("--limit", type=int, default=50)

    artifacts_preview = artifacts_subparsers.add_parser("preview", help="Preview an artifact.")
    artifacts_preview.add_argument("--artifact-id", required=True)
    artifacts_preview.add_argument("--limit", type=int, default=PREVIEW_LIMIT_ROWS)

    return parser


def _run_interactive_loop() -> int:
    _interactive_banner()
    while True:
        try:
            line = input("velaria> ")
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            continue
        command = line.strip()
        if not command:
            continue
        if command in {"exit", "quit"}:
            return 0
        if command == "help":
            main(["--help"])
            continue
        if command.startswith("help "):
            help_args = shlex.split(command[len("help ") :].strip())
            main([*help_args, "--help"])
            continue
        if command in {"-i", "--interactive"}:
            _emit_error_json(
                "interactive mode is already active",
                error_type="usage_error",
                phase="interactive",
            )
            continue
        main(shlex.split(command))


def _wants_interactive(argv: list[str]) -> bool:
    return argv in (["-i"], ["--interactive"])


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    if _wants_interactive(argv):
        return _run_interactive_loop()
    try:
        parser = _build_parser()
        args = parser.parse_args(argv)

        if args.command == "file-sql":
            return _run_csv_sql(
                pathlib.Path(args.input_path),
                args.table,
                args.query,
                input_type=args.input_type,
                delimiter=args.delimiter,
                line_mode=args.line_mode,
                regex_pattern=args.regex_pattern,
                mappings=args.mappings,
                columns=args.columns,
                json_format=args.json_format,
            )
        if args.command == "vector-search":
            return _run_vector_search(
                csv_path=pathlib.Path(args.csv),
                vector_column=args.vector_column,
                query_vector=args.query_vector,
                metric=args.metric,
                top_k=args.top_k,
                where_column=args.where_column,
                where_op=args.where_op,
                where_value=args.where_value,
                score_threshold=args.score_threshold,
            )
        if args.command == "embedding-build":
            return _run_embedding_build(
                pathlib.Path(args.input_path),
                input_type=args.input_type,
                text_columns=args.text_columns,
                provider=args.provider,
                model=args.model,
                output_path=pathlib.Path(args.output_path),
                doc_id_field=args.doc_id_field,
                source_updated_at_field=args.source_updated_at_field,
                delimiter=args.delimiter,
                json_columns=args.json_columns,
                json_format=args.json_format,
                template_version=args.template_version,
            )
        if args.command == "embedding-query":
            return _run_embedding_query(
                dataset_path=pathlib.Path(args.dataset_path) if args.dataset_path else None,
                input_path=pathlib.Path(args.input_path) if args.input_path else None,
                input_type=args.input_type,
                text_columns=args.text_columns or "",
                provider=args.provider,
                model=args.model,
                query_text=args.query_text,
                top_k=args.top_k,
                metric=args.metric,
                doc_id_field=args.doc_id_field,
                source_updated_at_field=args.source_updated_at_field,
                delimiter=args.delimiter,
                json_columns=args.json_columns,
                json_format=args.json_format,
                template_version=args.template_version,
                where_sql=args.where_sql,
                where_column=args.where_column,
                where_op=args.where_op,
                where_value=args.where_value,
            )
        if args.command == "run":
            if args.run_command == "start":
                return _run_start(args)
            if args.run_command == "list":
                return _run_list(args)
            if args.run_command == "result":
                return _run_result(args)
            if args.run_command == "diff":
                return _run_diff(args)
            if args.run_command == "show":
                return _run_show(args)
            if args.run_command == "status":
                return _run_status(args)
            if args.run_command == "cleanup":
                return _run_cleanup(args)
        if args.command == "artifacts":
            if args.artifacts_command == "list":
                return _artifacts_list(args)
            if args.artifacts_command == "preview":
                return _artifacts_preview(args)

        raise CliUsageError("unsupported command", phase="command_dispatch")
    except SystemExit as exc:
        if exc.code in (0, None):
            return 0
        raise
    except CliUsageError as exc:
        return _emit_error_json(
            str(exc),
            error_type=exc.error_type,
            phase=exc.phase,
            details=exc.details,
            run_id=exc.run_id,
        )
    except CliStructuredError as exc:
        return _emit_error_json(
            str(exc),
            error_type=exc.error_type,
            phase=exc.phase,
            details=exc.details,
            run_id=exc.run_id,
        )
    except ValueError as exc:
        payload = _error_payload_from_exception(exc)
        return _emit_error_json(
            payload["error"],
            error_type=payload["error_type"],
            phase=payload.get("phase"),
            details=payload.get("details"),
            run_id=payload.get("run_id"),
        )
    except FileNotFoundError as exc:
        payload = _error_payload_from_exception(exc)
        return _emit_error_json(
            payload["error"],
            error_type=payload["error_type"],
            phase=payload.get("phase"),
            details=payload.get("details"),
            run_id=payload.get("run_id"),
        )
    except Exception as exc:
        payload = _error_payload_from_exception(exc)
        return _emit_error_json(
            payload["error"],
            error_type=payload["error_type"],
            phase=payload.get("phase"),
            details=payload.get("details"),
            run_id=payload.get("run_id"),
        )


if __name__ == "__main__":
    raise SystemExit(main())
