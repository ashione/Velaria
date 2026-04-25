"""Codex App Server runtime -- runs Codex as a sub-process."""
from __future__ import annotations

import json
from typing import Any, AsyncIterator

from . import AiRuntime, generate_session_id
from ._tools import VELARIA_TOOLS, VELARIA_SQL_SYSTEM_PROMPT, execute_tool
from .session_registry import SessionRegistry


class CodexRuntime:
    """Codex App Server runtime -- runs Codex as a sub-process."""

    def __init__(self, model: str = "gpt-5"):
        from codex_app_server import Codex  # noqa: F401 -- validated at init

        self.model = model
        self.codex = Codex()
        self.codex.start()
        self.registry = SessionRegistry()
        self._threads: dict[str, Any] = {}

    async def create_session(self, dataset_context: dict[str, Any]) -> str:
        session_id = generate_session_id()
        thread = self.codex.thread_start(model=self.model)
        thread_id = getattr(thread, "thread_id", session_id)
        self.registry.register(session_id, "codex", str(thread_id), dataset_context)
        self._threads[session_id] = thread
        return session_id

    async def resume_session(self, session_id: str) -> bool:
        entry = self.registry.lookup(session_id)
        if not entry or entry["status"] != "active":
            return False
        thread = self.codex.thread_resume(entry["runtime_session_ref"])
        self._threads[session_id] = thread
        self.registry.update_activity(session_id)
        return True

    async def generate_sql(
        self,
        session_id: str,
        prompt: str,
        schema: list[str],
        table_name: str,
        sample_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        thread = self._threads.get(session_id)
        if not thread:
            return {"error": f"session not found: {session_id}"}

        user_msg = _build_sql_user_message(prompt, schema, table_name, sample_rows)
        full_prompt = (
            VELARIA_SQL_SYSTEM_PROMPT
            + "\n\n"
            "Generate a SQL query and return ONLY a JSON object: "
            '{"sql": "<query>", "explanation": "<brief explanation>"}\n\n'
            + user_msg
        )
        result = thread.run(full_prompt)
        self.registry.update_activity(session_id)
        return _parse_sql_json(result.final_response)

    async def analyze(self, session_id: str, prompt: str) -> AsyncIterator[dict[str, Any]]:
        thread = self._threads.get(session_id)
        if not thread:
            yield {"type": "error", "content": f"session not found: {session_id}"}
            return

        for event in thread.turn(prompt, stream=True):
            yield {
                "type": getattr(event, "type", "text"),
                "content": getattr(event, "data", "") or getattr(event, "text", ""),
                "session_id": session_id,
            }

        self.registry.update_activity(session_id)

    async def list_sessions(self) -> list[dict[str, Any]]:
        return self.registry.list_active()

    async def close_session(self, session_id: str) -> None:
        self.registry.close_session(session_id)
        self._threads.pop(session_id, None)

    def available_tools(self) -> list[str]:
        return [t["name"] for t in VELARIA_TOOLS]

    def shutdown(self) -> None:
        self.codex.stop()


def _build_sql_user_message(
    prompt: str,
    schema: list[str],
    table_name: str,
    sample_rows: list[dict[str, Any]] | None = None,
) -> str:
    parts = [f"Table: {table_name}", f"Columns: {', '.join(schema)}"]
    if sample_rows:
        parts.append("Sample rows:")
        for row in sample_rows[:5]:
            parts.append(f"  {json.dumps(row, ensure_ascii=False)}")
    parts.append(f"\nRequest: {prompt}")
    return "\n".join(parts)


def _parse_sql_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        parsed = json.loads(text)
        return {
            "sql": str(parsed.get("sql", "")),
            "explanation": str(parsed.get("explanation", "")),
        }
    except (json.JSONDecodeError, ValueError):
        return {"sql": text, "explanation": ""}
