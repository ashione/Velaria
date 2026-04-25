from __future__ import annotations

import pathlib
import sys
from typing import Any

from velaria.cli._common import *  # noqa: F401,F403 -- re-export all shared utilities

# Explicit re-exports for IDE/type checking and backward compatibility.
from velaria.cli._common import (  # noqa: F811
    CliStructuredError,
    CliUsageError,
    JsonArgumentParser,
    _build_batch_explain,
    _build_batch_source_create_sql,
    _emit_error_json,
    _emit_json,
    _error_payload,
    _error_payload_from_exception,
    _escape_sql_literal,
    _finalize_preview_payload,
    _infer_embedding_json_columns,
    _infer_format,
    _interactive_banner,
    _json_dumps,
    _limit_preview_payload,
    _make_embedding_provider,
    _new_artifact_id,
    _normalize_embedding_provider,
    _normalize_metric,
    _normalize_path,
    _parse_explain_sections,
    _parse_scalar_text,
    _parse_vector_text,
    _path_from_uri,
    _preview_payload_from_csv,
    _preview_payload_from_table,
    _read_preview_for_artifact,
    _split_csv_values,
    _sql_literal,
    _sql_literal_value,
    _table_artifact,
    _text_artifact,
    _uri_from_path,
    _utc_now,
    _write_table,
)

from velaria.cli.interactive import _run_interactive_loop, _wants_interactive  # noqa: F401
from velaria.cli.run_cmd import (  # noqa: F401
    _execute_stream_sql_once as _run_cmd_execute_stream_sql_once,
    _find_run_result_artifact,
    _run_action_with_timeout,
)
from velaria.cli.vector_search import _run_vector_search as _vector_search_run_vector_search

# Re-export symbols that tests mock on the velaria.cli module
from velaria import (  # noqa: F401
    Session,
    build_file_embeddings,
    query_file_embeddings,
    run_file_mixed_text_hybrid_search,
)

from velaria.cli import file_sql as _file_sql
from velaria.cli import vector_search as _vector_search
from velaria.cli import embedding as _embedding
from velaria.cli import run_cmd as _run_cmd
from velaria.cli import artifacts as _artifacts
from velaria.cli import agentic as _agentic
from velaria.cli import ai_cmd as _ai_cmd


def _sync_compat_bindings() -> None:
    _file_sql.Session = Session
    _vector_search.Session = Session
    _embedding.Session = Session
    _embedding.build_file_embeddings = build_file_embeddings
    _embedding.query_file_embeddings = query_file_embeddings
    _embedding.run_file_mixed_text_hybrid_search = run_file_mixed_text_hybrid_search
    _run_cmd.Session = Session
    _run_cmd._run_action_with_timeout = _run_action_with_timeout


def _run_vector_search(*args: Any, **kwargs: Any) -> int:
    _sync_compat_bindings()
    return _vector_search_run_vector_search(*args, **kwargs)


def _execute_stream_sql_once(*args: Any, **kwargs: Any) -> dict[str, Any]:
    _sync_compat_bindings()
    return _run_cmd_execute_stream_sql_once(*args, **kwargs)


def _build_parser():
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

    _file_sql.register(subparsers)
    _vector_search.register(subparsers)
    _embedding.register(subparsers)
    _run_cmd.register(subparsers)
    _artifacts.register(subparsers)
    _agentic.register(subparsers)
    _ai_cmd.register(subparsers)

    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    _sync_compat_bindings()
    if _wants_interactive(argv):
        return _run_interactive_loop()
    try:
        parser = _build_parser()
        args = parser.parse_args(argv)

        if args.command == "file-sql":
            return _file_sql._run_csv_sql(
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
            return _vector_search._run_vector_search(
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
            return _embedding._run_embedding_build(
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
            return _embedding._run_embedding_query(
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
                return _run_cmd._run_start(args)
            if args.run_command == "list":
                return _run_cmd._run_list(args)
            if args.run_command == "result":
                return _run_cmd._run_result(args)
            if args.run_command == "diff":
                return _run_cmd._run_diff(args)
            if args.run_command == "show":
                return _run_cmd._run_show(args)
            if args.run_command == "status":
                return _run_cmd._run_status(args)
            if args.run_command == "cleanup":
                return _run_cmd._run_cleanup(args)
        if args.command == "artifacts":
            if args.artifacts_command == "list":
                return _artifacts._artifacts_list(args)
            if args.artifacts_command == "preview":
                return _artifacts._artifacts_preview(args)
        if args.command == "source":
            if args.source_command == "create":
                return _agentic._source_create_cli(args)
            if args.source_command == "list":
                return _agentic._source_list_cli(args)
            if args.source_command == "ingest":
                return _agentic._source_ingest_cli(args)
        if args.command == "search":
            if args.search_command == "templates":
                return _agentic._search_templates_cli(args)
            if args.search_command == "events":
                return _agentic._search_events_cli(args)
            if args.search_command == "datasets":
                return _agentic._search_datasets_cli(args)
            if args.search_command == "fields":
                return _agentic._search_fields_cli(args)
        if args.command == "grounding":
            return _agentic._grounding_cli(args)
        if args.command == "monitor":
            if args.monitor_command == "create":
                return _agentic._monitor_create_cli(args)
            if args.monitor_command == "create-from-intent":
                return _agentic._monitor_create_from_intent_cli(args)
            if args.monitor_command == "list":
                return _agentic._monitor_list_cli(args)
            if args.monitor_command == "show":
                return _agentic._monitor_show_cli(args)
            if args.monitor_command == "validate":
                return _agentic._monitor_validate_cli(args)
            if args.monitor_command == "enable":
                return _agentic._monitor_enable_cli(args)
            if args.monitor_command == "disable":
                return _agentic._monitor_disable_cli(args)
            if args.monitor_command == "run":
                return _agentic._monitor_run_cli(args)
            if args.monitor_command == "delete":
                return _agentic._monitor_delete_cli(args)
        if args.command == "focus-events":
            if args.focus_events_command == "poll":
                return _agentic._focus_events_poll_cli(args)
            if args.focus_events_command == "consume":
                return _agentic._focus_events_update_cli(args, status_name="consumed")
            if args.focus_events_command == "archive":
                return _agentic._focus_events_update_cli(args, status_name="archived")

        if args.command == "ai":
            return _ai_cmd._dispatch_ai(args)

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
