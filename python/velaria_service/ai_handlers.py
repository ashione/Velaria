"""AI runtime HTTP handlers for Velaria service."""
from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from ._helpers import get_ai_config


_runtime_instance = None
_runtime_lock = threading.Lock()


def _get_runtime():
    global _runtime_instance
    with _runtime_lock:
        if _runtime_instance is None:
            from velaria.ai_runtime import create_runtime

            config = get_ai_config()
            _runtime_instance = create_runtime(config)
        return _runtime_instance


def handle_create_session(payload: dict[str, Any]) -> dict[str, Any]:
    runtime = _get_runtime()
    dataset_context = payload.get("dataset_context", {})
    session_id = asyncio.run(runtime.create_session(dataset_context))
    return {"ok": True, "session_id": session_id}


def handle_resume_session(session_id: str) -> dict[str, Any]:
    runtime = _get_runtime()
    ok = asyncio.run(runtime.resume_session(session_id))
    if not ok:
        return {"ok": False, "error": f"session not found or closed: {session_id}"}
    return {"ok": True, "session_id": session_id}


def handle_generate_sql(payload: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
    prompt = payload.get("prompt")
    if not prompt:
        return {"ok": False, "error": "prompt is required", "error_type": "validation_error"}
    schema = payload.get("schema")
    if not schema or not isinstance(schema, list):
        return {"ok": False, "error": "schema (list of column names) is required", "error_type": "validation_error"}

    runtime = _get_runtime()
    table_name = str(payload.get("table_name") or "input_table")
    sample_rows = payload.get("sample_rows")

    # If no session_id, create a temporary one
    if not session_id:
        session_id = asyncio.run(
            runtime.create_session(
                {
                    "schema": schema,
                    "table_name": table_name,
                }
            )
        )

    result = asyncio.run(
        runtime.generate_sql(
            session_id,
            str(prompt),
            list(schema),
            table_name,
            sample_rows,
        )
    )
    if result.get("error"):
        return {"ok": False, **result}
    return {"ok": True, "session_id": session_id, **result}


def handle_analyze(payload: dict[str, Any], session_id: str) -> dict[str, Any]:
    prompt = payload.get("prompt")
    if not prompt:
        return {"ok": False, "error": "prompt is required"}

    runtime = _get_runtime()
    results: list[dict[str, Any]] = []

    async def _collect():
        async for msg in runtime.analyze(session_id, str(prompt)):
            results.append(msg)

    asyncio.run(_collect())
    return {"ok": True, "session_id": session_id, "messages": results}


def handle_list_sessions() -> dict[str, Any]:
    runtime = _get_runtime()
    sessions = asyncio.run(runtime.list_sessions())
    return {"ok": True, "sessions": sessions}


def handle_get_session(session_id: str) -> dict[str, Any]:
    runtime = _get_runtime()
    sessions = asyncio.run(runtime.list_sessions())
    for s in sessions:
        if s["session_id"] == session_id:
            return {"ok": True, "session": s}
    return {"ok": False, "error": f"session not found: {session_id}"}


def handle_close_session(session_id: str) -> dict[str, Any]:
    runtime = _get_runtime()
    asyncio.run(runtime.close_session(session_id))
    return {"ok": True, "session_id": session_id, "closed": True}
