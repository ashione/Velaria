from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import multiprocessing
import pathlib
import secrets
import sys
import traceback
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from velaria import Session, __version__
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


class CliUsageError(ValueError):
    pass


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


def _emit_error_json(error: str, *, error_type: str = "cli_error") -> int:
    print(
        _json_dumps(
            {
                "ok": False,
                "error": error,
                "error_type": error_type,
            }
        )
    )
    return 1


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_artifact_id() -> str:
    return f"artifact_{secrets.token_hex(8)}"


def _normalize_metric(metric: str) -> str:
    return "cosine" if metric in ("cosine", "cosin") else metric


def _normalize_path(path: pathlib.Path) -> pathlib.Path:
    return path.expanduser().resolve()


def _uri_from_path(path: pathlib.Path) -> str:
    return _normalize_path(path).as_uri()


def _path_from_uri(uri: str) -> pathlib.Path:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return pathlib.Path(parsed.path)
    raise ValueError(f"unsupported artifact uri: {uri}")


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
    return [float(part) for part in parts if part]


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


def _infer_format(path: pathlib.Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in (".parquet", ".pq"):
        return "parquet"
    if suffix in (".arrow", ".feather"):
        return "arrow"
    raise ValueError(f"unsupported output format for path: {path}")


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
        return artifact["preview_json"]
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
    raise ValueError(f"preview unsupported for format: {fmt}")


def _execute_csv_sql(
    csv_path: pathlib.Path,
    table: str,
    query: str,
    output_path: pathlib.Path | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    session = Session()
    df = session.read_csv(str(csv_path))
    session.create_temp_view(table, df)
    result_df = session.sql(query)
    logical = result_df.explain() if hasattr(result_df, "explain") else ""
    result = result_df.to_arrow()
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "csv-sql"]))
        if run_id is not None:
            write_explain(run_id, _build_batch_explain(logical, output_path))
    return {
        "payload": {
            "table": table,
            "query": query,
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
    output_path: pathlib.Path | None = None,
    explain_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    session = Session()
    df = session.read_csv(str(csv_path))
    session.create_temp_view("input_table", df)
    needle = _parse_vector_text(query_vector)
    if not needle:
        raise ValueError("--query-vector must not be empty")

    result = session.vector_search(
        table="input_table",
        vector_column=vector_column,
        query_vector=needle,
        top_k=top_k,
        metric=metric,
    ).to_arrow()
    explain = session.explain_vector_search(
        table="input_table",
        vector_column=vector_column,
        query_vector=needle,
        top_k=top_k,
        metric=metric,
    )
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "vector-search"]))
    if explain_path is not None:
        artifacts.append(_text_artifact(explain_path, explain, ["explain", "vector-search"]))
    return {
        "payload": {
            "metric": _normalize_metric(metric),
            "top_k": top_k,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
            "explain": explain,
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
        raise ValueError("--query for stream-sql-once must start with INSERT INTO")
    effective_max_batches = max_batches if max_batches > 0 else 1
    session = Session()
    stream_df = session.read_stream_csv_dir(str(source_csv_dir), delimiter=source_delimiter)
    session.create_temp_view(source_table, stream_df)
    session.sql(
        f"CREATE SINK TABLE {sink_table} ({sink_schema}) "
        f"USING csv OPTIONS(path '{_normalize_path(sink_path)}', delimiter '{sink_delimiter}')"
    )
    explain = session.explain_stream_sql(
        query,
        trigger_interval_ms=trigger_interval_ms,
        checkpoint_delivery_mode=checkpoint_delivery_mode,
        execution_mode=execution_mode,
        local_workers=local_workers,
        max_inflight_partitions=max_inflight_partitions,
    )
    write_explain(run_id, _parse_explain_sections(explain))
    streaming_query = session.start_stream_sql(
        query,
        trigger_interval_ms=trigger_interval_ms,
        checkpoint_delivery_mode=checkpoint_delivery_mode,
        execution_mode=execution_mode,
        local_workers=local_workers,
        max_inflight_partitions=max_inflight_partitions,
    )
    streaming_query.start()
    append_progress_snapshot(run_id, streaming_query.snapshot_json())
    processed = streaming_query.await_termination(max_batches=effective_max_batches)
    append_progress_snapshot(run_id, streaming_query.snapshot_json())
    result = session.read_csv(str(sink_path)).to_arrow()
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


def _run_csv_sql(csv_path: pathlib.Path, table: str, query: str) -> int:
    return _emit_json(_execute_csv_sql(csv_path, table, query)["payload"])


def _run_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
) -> int:
    return _emit_json(
        _execute_vector_search(csv_path, vector_column, query_vector, metric, top_k)["payload"]
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
    if action == "csv-sql":
        output_path = (
            pathlib.Path(args["output_path"]) if args.get("output_path") else artifacts_dir / "result.parquet"
        )
        return _execute_csv_sql(
            csv_path=pathlib.Path(args["csv"]),
            table=args["table"],
            query=args["query"],
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
            output_path=output_path,
            explain_path=explain_path,
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
    raise ValueError(f"unsupported run action: {action}")


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
            failure = {
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
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
        }
    if not result_path.exists():
        return {
            "timed_out": False,
            "error": f"action process exited with code {process.exitcode}",
        }
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if process.exitcode not in (0, None):
        payload.setdefault("error", f"action process exited with code {process.exitcode}")
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
    parser.add_argument("--csv", required=True, help="CSV file path.")
    parser.add_argument(
        "--table",
        default="input_table",
        help="Temporary table name exposed to session.sql(...).",
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
    if action == "csv-sql":
        _add_csv_sql_arguments(parser)
        parser.add_argument("--output-path")
    elif action == "vector-search":
        _add_vector_search_arguments(parser)
        parser.add_argument("--output-path")
    elif action == "stream-sql-once":
        _add_stream_sql_once_arguments(parser)
    else:
        raise ValueError(f"unsupported run action: {action}")
    return vars(parser.parse_args(argv))


def _parse_passthrough(command_args: list[str]) -> tuple[str, dict[str, Any]]:
    if command_args and command_args[0] == "--":
        command_args = command_args[1:]
    if not command_args:
        raise ValueError("run start requires an action after '--'")
    return command_args[0], _parse_action_args(command_args[0], command_args[1:])


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
        raise ValueError(f"run not found: {run_id}") from exc


def _run_start(args: argparse.Namespace) -> int:
    action, action_args = _parse_passthrough(args.command_args)
    run_id, run_dir = create_run(action, action_args, __version__, run_name=args.run_name)
    write_inputs(
        run_id,
        {
            "action": action,
            "action_args": action_args,
            "timeout_ms": args.timeout_ms,
        },
    )
    index = ArtifactIndex()
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
        finalized = finalize_run(run_id, "timed_out", error=result["error"])
        index.upsert_run(finalized)
        _emit_json(
            {
                "ok": False,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "status": "timed_out",
                "error": result["error"],
            }
        )
        return 1
    if "error" in result and "payload" not in result:
        append_stderr(run_id, f"{result['error']}\n")
        if result.get("traceback"):
            append_stderr(run_id, result["traceback"])
        finalized = finalize_run(run_id, "failed", error=result["error"])
        index.upsert_run(finalized)
        _emit_json(
            {
                "ok": False,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "status": "failed",
                "error": result["error"],
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
            "result": result["payload"],
            "artifacts": created_artifacts,
        }
    )


def _run_show(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    return _emit_json(
        {
            "ok": True,
            "run": _read_run_or_raise(args.run_id),
            "artifacts": index.list_artifacts(run_id=args.run_id, limit=args.limit),
        }
    )


def _run_status(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    run = _read_run_or_raise(args.run_id)
    payload: dict[str, Any] = {
        "ok": True,
        "run_id": args.run_id,
        "status": run["status"],
        "action": run["action"],
        "artifacts": index.list_artifacts(run_id=args.run_id, limit=args.limit),
    }
    latest_progress = _load_latest_progress(args.run_id)
    if latest_progress is not None:
        payload["latest_progress"] = latest_progress
    return _emit_json(payload)


def _artifacts_list(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    return _emit_json(
        {
            "ok": True,
            "artifacts": index.list_artifacts(limit=args.limit, run_id=args.run_id),
        }
    )


def _artifacts_preview(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    artifact = index.get_artifact(args.artifact_id)
    if artifact is None:
        raise ValueError(f"artifact not found: {args.artifact_id}")
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


def _run_cleanup(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    payload = index.cleanup_runs(
        keep_last_n=args.keep_last,
        ttl_days=args.ttl_days,
        delete_files=args.delete_files,
    )
    payload["ok"] = True
    return _emit_json(payload)


def _build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(
        prog="velaria-cli",
        description="Velaria CLI for SQL query execution and workspace run management.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    csv_sql = subparsers.add_parser(
        "csv-sql",
        help="Read CSV and run a SQL query through DataflowSession.",
    )
    _add_csv_sql_arguments(csv_sql)

    vector_search = subparsers.add_parser(
        "vector-search",
        help="Read CSV and run fixed-length vector nearest search.",
    )
    _add_vector_search_arguments(vector_search)

    run_parser = subparsers.add_parser("run", help="Run management commands.")
    run_subparsers = run_parser.add_subparsers(dest="run_command", required=True)

    run_start = run_subparsers.add_parser("start", help="Start a tracked run.")
    run_start.add_argument("--run-name")
    run_start.add_argument("--timeout-ms", type=int)
    run_start.add_argument("command_args", nargs=argparse.REMAINDER)

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


def main(argv: list[str] | None = None) -> int:
    try:
        parser = _build_parser()
        args = parser.parse_args(argv)

        if args.command == "csv-sql":
            return _run_csv_sql(pathlib.Path(args.csv), args.table, args.query)
        if args.command == "vector-search":
            return _run_vector_search(
                csv_path=pathlib.Path(args.csv),
                vector_column=args.vector_column,
                query_vector=args.query_vector,
                metric=args.metric,
                top_k=args.top_k,
            )
        if args.command == "run":
            if args.run_command == "start":
                return _run_start(args)
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

        raise CliUsageError("unsupported command")
    except CliUsageError as exc:
        return _emit_error_json(str(exc), error_type="usage_error")
    except ValueError as exc:
        return _emit_error_json(str(exc), error_type="value_error")
    except FileNotFoundError as exc:
        return _emit_error_json(str(exc), error_type="file_not_found")
    except Exception as exc:
        return _emit_error_json(str(exc), error_type="internal_error")


if __name__ == "__main__":
    raise SystemExit(main())
