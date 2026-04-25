from __future__ import annotations

import argparse
import json
import os
import urllib.request
from typing import Any

from velaria.cli._common import CliStructuredError, _emit_json, _emit_error_json

_DEFAULT_SERVICE_URL = "http://127.0.0.1:37491"


def _service_url() -> str:
    return os.environ.get("VELARIA_SERVICE_URL", _DEFAULT_SERVICE_URL)


def _api_call(method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{_service_url()}{path}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
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
