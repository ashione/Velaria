from __future__ import annotations

import argparse
import csv
import json
import pathlib
import secrets
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
    SentenceTransformerEmbeddingProvider,
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
    print("Velaria interactive mode. Type '/help' for usage, '/exit' to quit.")
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
