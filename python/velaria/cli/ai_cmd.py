from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import urllib.request
from typing import Any

from velaria.cli._common import CliStructuredError, _emit_json, _emit_error_json

_DEFAULT_SERVICE_URL = "http://127.0.0.1:37491"
_runtime_instance = None


def _service_url() -> str:
    return os.environ.get("VELARIA_SERVICE_URL", _DEFAULT_SERVICE_URL)


def _use_service() -> bool:
    return os.environ.get("VELARIA_AI_USE_SERVICE", "").lower() in {"1", "true", "yes"}


def _request_timeout_seconds() -> float:
    raw = os.environ.get("VELARIA_AI_CLI_TIMEOUT_SECONDS", "60")
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 60.0


def _api_call(method: str, path: str, payload: dict | None = None) -> dict:
    if not _use_service():
        return _local_ai_call(method, path, payload or {})
    return _http_api_call(method, path, payload)


def _http_api_call(method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{_service_url()}{path}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=_request_timeout_seconds()) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(body)
        except (json.JSONDecodeError, ValueError):
            detail = {"raw": body}
        raise CliStructuredError(
            f"AI service returned HTTP {exc.code}",
            error_type="ai_service_error",
            phase="ai_api_call",
            details={"status": exc.code, "response": detail, "url": url},
        ) from exc
    except urllib.error.URLError as exc:
        raise CliStructuredError(
            f"cannot reach AI service at {url}: {exc.reason}",
            error_type="ai_service_unavailable",
            phase="ai_api_call",
            details={"url": url},
        ) from exc
    except (TimeoutError, socket.timeout) as exc:
        raise CliStructuredError(
            f"AI service request timed out after {_request_timeout_seconds():.0f} seconds",
            error_type="ai_service_timeout",
            phase="ai_api_call",
            details={"url": url, "timeout_seconds": _request_timeout_seconds()},
        ) from exc


def _get_runtime():
    global _runtime_instance
    if _runtime_instance is None:
        from velaria.ai_runtime import create_runtime, load_ai_config

        _runtime_instance = create_runtime(load_ai_config())
    return _runtime_instance


def _local_ai_call(method: str, path: str, payload: dict) -> dict:
    try:
        return _local_ai_call_impl(method, path, payload)
    except (RuntimeError, ImportError) as exc:
        return {"ok": False, "error": str(exc), "error_type": "runtime_unavailable"}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "error_type": "internal_error"}


def _local_ai_call_impl(method: str, path: str, payload: dict) -> dict:
    runtime = _get_runtime()
    parts = tuple(part for part in path.strip("/").split("/") if part)
    if parts[:2] == ("api", "v1"):
        parts = parts[2:]

    if method == "POST" and parts == ("ai", "sessions"):
        session_id = asyncio.run(runtime.create_session(payload.get("dataset_context", {})))
        return {"ok": True, "session_id": session_id}

    if method == "GET" and parts == ("ai", "sessions"):
        return {"ok": True, "sessions": asyncio.run(runtime.list_sessions())}

    if method == "GET" and len(parts) == 3 and parts[:2] == ("ai", "sessions"):
        session_id = parts[2]
        sessions = asyncio.run(runtime.list_sessions())
        for session in sessions:
            if session.get("session_id") == session_id:
                return {"ok": True, "session": session}
        return {"ok": False, "error": f"session not found: {session_id}"}

    if method == "DELETE" and len(parts) == 3 and parts[:2] == ("ai", "sessions"):
        session_id = parts[2]
        asyncio.run(runtime.close_session(session_id))
        return {"ok": True, "session_id": session_id, "closed": True}

    if method == "POST" and parts == ("ai", "generate-sql"):
        prompt = payload.get("prompt")
        schema = payload.get("schema")
        if not prompt:
            return {"ok": False, "error": "prompt is required", "error_type": "validation_error"}
        if not schema or not isinstance(schema, list):
            return {
                "ok": False,
                "error": "schema (list of column names) is required",
                "error_type": "validation_error",
            }
        table_name = str(payload.get("table_name") or "input_table")
        session_id = asyncio.run(
            runtime.create_session({"schema": schema, "table_name": table_name})
        )
        result = asyncio.run(
            runtime.generate_sql(
                session_id,
                str(prompt),
                list(schema),
                table_name,
                payload.get("sample_rows"),
            )
        )
        return {"ok": not bool(result.get("error")), "session_id": session_id, **result}

    if (
        method == "POST"
        and len(parts) == 4
        and parts[:2] == ("ai", "sessions")
        and parts[3] == "generate-sql"
    ):
        session_id = parts[2]
        prompt = payload.get("prompt")
        if not prompt:
            return {"ok": False, "error": "prompt is required", "error_type": "validation_error"}
        sessions = asyncio.run(runtime.list_sessions())
        session = next((s for s in sessions if s.get("session_id") == session_id), None)
        context = session.get("dataset_context", {}) if session else {}
        schema = context.get("schema") or payload.get("schema") or []
        table_name = str(context.get("table_name") or payload.get("table_name") or "input_table")
        result = asyncio.run(
            runtime.generate_sql(session_id, str(prompt), list(schema), table_name, payload.get("sample_rows"))
        )
        return {"ok": not bool(result.get("error")), "session_id": session_id, **result}

    if (
        method == "POST"
        and len(parts) == 4
        and parts[:2] == ("ai", "sessions")
        and parts[3] == "analyze"
    ):
        session_id = parts[2]
        prompt = payload.get("prompt")
        if not prompt:
            return {"ok": False, "error": "prompt is required", "error_type": "validation_error"}
        messages: list[dict[str, Any]] = []

        async def _collect() -> None:
            async for message in runtime.analyze(session_id, str(prompt)):
                messages.append(message)

        asyncio.run(_collect())
        return {"ok": True, "session_id": session_id, "messages": messages}

    return {"ok": False, "error": f"unsupported local AI endpoint: {method} {path}"}


def _ai_generate_sql(args: argparse.Namespace) -> int:
    schema = [s.strip() for s in args.schema.split(",")]
    result = _api_call("POST", "/api/v1/ai/generate-sql", {
        "prompt": args.prompt,
        "schema": schema,
        "table_name": args.table or "input_table",
    })
    return _emit_json(result)


def _ai_session_start(args: argparse.Namespace) -> int:
    schema = [s.strip() for s in args.schema.split(",")] if args.schema else []
    result = _api_call("POST", "/api/v1/ai/sessions", {
        "dataset_context": {
            "schema": schema,
            "source_path": args.source_path or "",
            "table_name": args.table or "input_table",
        },
    })
    return _emit_json(result)


def _ai_session_list(_args: argparse.Namespace) -> int:
    result = _api_call("GET", "/api/v1/ai/sessions")
    return _emit_json(result)


def _ai_session_close(args: argparse.Namespace) -> int:
    result = _api_call("DELETE", f"/api/v1/ai/sessions/{args.session_id}")
    return _emit_json(result)


def _ai_analyze(args: argparse.Namespace) -> int:
    path = f"/api/v1/ai/sessions/{args.session_id}/analyze"
    result = _api_call("POST", path, {"prompt": args.prompt})
    return _emit_json(result)


def _dispatch_ai(args: argparse.Namespace) -> int:
    if args.ai_command == "generate-sql":
        return _ai_generate_sql(args)
    if args.ai_command == "session":
        if args.session_command == "start":
            return _ai_session_start(args)
        if args.session_command == "list":
            return _ai_session_list(args)
        if args.session_command == "close":
            return _ai_session_close(args)
    if args.ai_command == "analyze":
        return _ai_analyze(args)
    return _emit_error_json(
        f"unsupported ai command: {args.ai_command}",
        error_type="usage_error",
        phase="command_dispatch",
    )


def register(subparsers: argparse._SubParsersAction) -> None:
    ai_parser = subparsers.add_parser("ai", help="AI-assisted analysis commands.")
    ai_subparsers = ai_parser.add_subparsers(dest="ai_command", required=True)

    gen = ai_subparsers.add_parser("generate-sql", help="Generate SQL from natural language.")
    gen.add_argument("--prompt", required=True, help="Natural language description of the query.")
    gen.add_argument("--schema", required=True, help="Comma-separated column names.")
    gen.add_argument("--table", default="input_table", help="Table name (default: input_table).")

    sess_parser = ai_subparsers.add_parser("session", help="AI session management.")
    sess_sub = sess_parser.add_subparsers(dest="session_command", required=True)

    start = sess_sub.add_parser("start", help="Create a new AI session.")
    start.add_argument("--schema", help="Comma-separated column names.")
    start.add_argument("--source-path", help="Dataset source path.")
    start.add_argument("--table", default="input_table", help="Table name (default: input_table).")

    sess_sub.add_parser("list", help="List active AI sessions.")

    close = sess_sub.add_parser("close", help="Close an AI session.")
    close.add_argument("--session-id", required=True, help="Session ID to close.")

    analyze = ai_subparsers.add_parser("analyze", help="Run AI analysis on a session.")
    analyze.add_argument("--session-id", required=True, help="Session ID for analysis.")
    analyze.add_argument("--prompt", required=True, help="Analysis prompt.")
