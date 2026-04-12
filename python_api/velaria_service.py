from __future__ import annotations

import argparse
import json
import traceback
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import velaria.cli as cli_impl
from velaria import (
    Session,
    __version__,
    query_file_embeddings,
    read_excel,
    run_file_mixed_text_hybrid_search,
)
from velaria.workspace.artifact_index import RunDeleteConflictError
from velaria.workspace import (
    ArtifactIndex,
    append_stderr,
    create_run,
    finalize_run,
    read_run,
    update_run,
    write_explain,
    write_inputs,
)


def _json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def _normalize_path(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


class ApiRouteNotFoundError(FileNotFoundError):
    pass


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
    input_type = (payload.get("input_type") or "auto").lower()
    input_path = _normalize_path(payload["input_path"])
    suffix = input_path.suffix.lower()
    if input_type == "excel":
        return read_excel(
            session,
            str(input_path),
            sheet_name=int(payload.get("sheet_name", 0)),
            date_format=payload.get("date_format", "%Y-%m-%d"),
        )
    if input_type == "parquet":
        return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
    if input_type == "arrow":
        return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
    if input_type == "auto":
        if suffix in {".parquet", ".pq", ".arrow", ".ipc", ".feather"}:
          return session.create_dataframe_from_arrow(_load_arrow_table(input_path))
        return session.read(str(input_path))
    if input_type == "csv":
        delimiter = (payload.get("delimiter") or ",")[:1]
        return session.read_csv(str(input_path), delimiter=delimiter)
    if input_type == "json":
        columns = _parse_csv_list(payload.get("columns"))
        if not columns:
            raise ValueError("json input requires columns")
        return session.read_json(
            str(input_path),
            columns=columns,
            format=payload.get("json_format", "json_lines"),
        )
    if input_type == "line":
        mappings = _parse_mappings(payload.get("mappings"))
        if not mappings:
            raise ValueError("line input requires mappings")
        kwargs: dict[str, Any] = {"mappings": mappings}
        mode = payload.get("line_mode", "split")
        if mode == "regex":
            kwargs["mode"] = "regex"
            kwargs["regex_pattern"] = payload.get("regex_pattern") or ""
        else:
            kwargs["split_delimiter"] = payload.get("delimiter", "|")
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


def _execute_hybrid_search(payload: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    session = Session()
    provider_name = str(payload.get("provider") or "hash")
    provider, resolved_model = cli_impl._make_embedding_provider(provider_name, payload.get("model"))
    query_text = str(payload.get("query_text") or "").strip()
    if not query_text:
        raise ValueError("query_text must not be empty")

    top_k = int(payload.get("top_k", 10))
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    vector_column = str(payload.get("vector_column") or "embedding")
    input_type = str(payload.get("input_type") or "auto").lower()
    text_columns = _parse_string_list(payload.get("text_columns"))
    template_version = str(payload.get("template_version") or "text-v1")
    metric = str(payload.get("metric") or "cosine")
    dataset_path = payload.get("dataset_path")

    if dataset_path:
        normalized_dataset_path = str(_normalize_path(str(dataset_path)))
        result_df = query_file_embeddings(
            session,
            normalized_dataset_path,
            provider=provider,
            model=resolved_model,
            query_text=query_text,
            vector_column=vector_column,
            top_k=top_k,
            metric=metric,
        )
        normalized_input_path = None
    else:
        input_path = payload.get("input_path")
        if not input_path:
            raise ValueError("input_path or dataset_path is required")
        if not text_columns:
            raise ValueError("text_columns must not be empty when input_path is used")
        normalized_dataset_path = None
        normalized_input_path = str(_normalize_path(str(input_path)))
        result_df = run_file_mixed_text_hybrid_search(
            session,
            normalized_input_path,
            provider=provider,
            model=resolved_model,
            query_text=query_text,
            template_version=template_version,
            text_columns=text_columns,
            input_type=input_type,
            delimiter=(payload.get("delimiter") or ",")[:1],
            json_columns=_resolve_json_columns(payload, text_columns) if input_type == "json" else None,
            json_format=str(payload.get("json_format") or "json_lines"),
            mappings=payload.get("mappings") or payload.get("columns"),
            line_mode=str(payload.get("line_mode") or "split"),
            regex_pattern=payload.get("regex_pattern"),
            sheet_name=payload.get("sheet_name", 0),
            date_format=str(payload.get("date_format") or "%Y-%m-%d"),
            vector_column=vector_column,
            top_k=top_k,
            metric=metric,
        )

    result_table = result_df.to_arrow()
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
    return {
        "payload": {
            "dataset_path": normalized_dataset_path,
            "input_path": normalized_input_path,
            "input_type": input_type,
            "query_text": query_text,
            "text_columns": text_columns,
            "provider": provider_name,
            "model": resolved_model,
            "template_version": template_version,
            "top_k": top_k,
            "metric": metric,
            "vector_column": vector_column,
            "schema": result_table.schema.names,
            "row_count": result_table.num_rows,
        },
        "preview": preview,
        "artifacts": artifacts,
    }


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
                        session = Session()
                        df = _load_dataframe(session, payload)
                        preview = _preview_from_dataframe(df, limit=int(payload.get("limit", 50)))
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "dataset": {
                                    "name": payload.get("dataset_name")
                                    or Path(payload["input_path"]).stem,
                                    "source_type": payload.get("input_type", "auto"),
                                    "source_path": str(_normalize_path(payload["input_path"])),
                                },
                                "preview": preview,
                            },
                        )
                        return
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
                    if parts in {("search", "hybrid"), ("runs", "hybrid-search")}:
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        action_args = {
                            "dataset_path": str(_normalize_path(str(payload["dataset_path"])))
                            if payload.get("dataset_path")
                            else None,
                            "input_path": str(_normalize_path(str(payload["input_path"])))
                            if payload.get("input_path")
                            else None,
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "columns": payload.get("columns"),
                            "mappings": payload.get("mappings"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "line_mode": payload.get("line_mode", "split"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "query_text": payload["query_text"],
                            "text_columns": payload.get("text_columns"),
                            "provider": payload.get("provider", "hash"),
                            "model": payload.get("model"),
                            "template_version": payload.get("template_version", "text-v1"),
                            "top_k": int(payload.get("top_k", 10)),
                            "metric": payload.get("metric", "cosine"),
                            "vector_column": payload.get("vector_column", "embedding"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                            "sheet_name": payload.get("sheet_name", 0),
                            "date_format": payload.get("date_format", "%Y-%m-%d"),
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
                            result = _execute_hybrid_search(action_args, run_dir)
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
