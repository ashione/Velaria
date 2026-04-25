from __future__ import annotations

import argparse
import contextlib
import json
import multiprocessing
import pathlib
import traceback
from datetime import datetime, timezone
from typing import Any

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
from velaria.workspace.types import PREVIEW_LIMIT_ROWS

from velaria.cli._common import (
    CliStructuredError,
    CliUsageError,
    JsonArgumentParser,
    _emit_json,
    _error_payload_from_exception,
    _json_dumps,
    _new_artifact_id,
    _normalize_path,
    _parse_explain_sections,
    _read_preview_for_artifact,
    _table_artifact,
    _utc_now,
)
from velaria.cli.file_sql import _add_csv_sql_arguments, _execute_csv_sql
from velaria.cli.vector_search import _add_vector_search_arguments, _execute_vector_search
from velaria.cli.embedding import _execute_embedding_build, _execute_embedding_query


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


def _child_result_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "_action_result.json"


def _worker_stdout_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "stdout.log"


def _worker_stderr_path(run_dir: pathlib.Path) -> pathlib.Path:
    return run_dir / "stderr.log"


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


def register(subparsers: argparse._SubParsersAction) -> None:
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
