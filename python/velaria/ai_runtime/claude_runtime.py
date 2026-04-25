"""Claude Agent SDK runtime -- runs Claude as a sub-process with custom Velaria tools."""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from . import AiRuntime, generate_session_id
from ._tools import VELARIA_TOOLS, VELARIA_SQL_SYSTEM_PROMPT, execute_tool
from .session_registry import SessionRegistry


class ClaudeAgentRuntime:
    """Claude Agent SDK runtime -- runs Claude as a sub-process with custom Velaria tools."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        from claude_agent_sdk import ClaudeSDKClient  # noqa: F401 -- validated at init

        self.model = model
        self.api_key = api_key
        self.registry = SessionRegistry()
        self._sessions: dict[str, dict[str, Any]] = {}  # in-memory session state

        # Build tool name -> definition lookup
        self._tool_functions: dict[str, dict[str, Any]] = {}
        for tool_def in VELARIA_TOOLS:
            self._tool_functions[tool_def["name"]] = tool_def

    async def create_session(self, dataset_context: dict[str, Any]) -> str:
        session_id = generate_session_id()
        self.registry.register(session_id, "claude", session_id, dataset_context)
        self._sessions[session_id] = {"dataset_context": dataset_context, "session": None}
        return session_id

    async def resume_session(self, session_id: str) -> bool:
        entry = self.registry.lookup(session_id)
        if not entry or entry["status"] != "active":
            return False
        self._sessions[session_id] = {"dataset_context": entry["dataset_context"], "session": None}
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
        from claude_agent_sdk import query as claude_query
        from claude_agent_sdk.types import ClaudeAgentOptions

        user_msg = _build_sql_user_message(prompt, schema, table_name, sample_rows)
        sql_system = (
            VELARIA_SQL_SYSTEM_PROMPT
            + "\n\n"
            "For this request, generate a SQL query and return it as JSON: "
            '{"sql": "<query>", "explanation": "<brief explanation>"}\n'
            "Do not use tools for this request -- just output the JSON directly."
        )

        result_text = ""
        async for msg in claude_query(
            prompt=user_msg,
            options=ClaudeAgentOptions(
                system_prompt=sql_system,
                model=self.model,
                max_turns=1,
                permission_mode="bypassPermissions",
            ),
        ):
            # AssistantMessage.content is a list of TextBlock/ThinkingBlock
            if hasattr(msg, "content") and isinstance(msg.content, list):
                text_parts = [block.text for block in msg.content if hasattr(block, "text")]
                if text_parts:
                    # Take only this message's text (overwrite, not accumulate)
                    result_text = "".join(text_parts)
            # ResultMessage.result may contain the final text
            elif hasattr(msg, "result") and isinstance(msg.result, str) and msg.result.strip():
                result_text = msg.result

        self.registry.update_activity(session_id)
        return _parse_sql_json(result_text)

    async def analyze(self, session_id: str, prompt: str) -> AsyncIterator[dict[str, Any]]:
        from claude_agent_sdk import ClaudeSDKClient
        from claude_agent_sdk.types import ClaudeAgentOptions

        session_data = self._sessions.get(session_id)
        if not session_data:
            yield {"type": "error", "content": f"session not found: {session_id}"}
            return

        dataset_context = session_data["dataset_context"]

        # Build custom tool handlers
        def handle_tool(tool_name: str, tool_input: dict) -> str:
            try:
                # Lazy import Session to avoid circular imports
                from velaria import Session

                session = Session()
                result = execute_tool(
                    tool_name, tool_input, session=session, dataset_context=dataset_context
                )
                return json.dumps(result, ensure_ascii=False)
            except Exception as exc:
                return json.dumps({"error": str(exc)})

        # Create custom tools for ClaudeSDKClient
        custom_tools = []
        for tool_def in VELARIA_TOOLS:
            name = tool_def["name"]
            custom_tools.append(
                {
                    "name": name,
                    "description": tool_def["description"],
                    "input_schema": tool_def["input_schema"],
                    "handler": lambda inp, n=name: handle_tool(n, inp),
                }
            )

        client = ClaudeSDKClient(
            options=ClaudeAgentOptions(
                model=self.model,
                system_prompt=VELARIA_SQL_SYSTEM_PROMPT,
                permission_mode="bypassPermissions",
            ),
            custom_tools=custom_tools,
        )

        async for msg in client.query(prompt=prompt):
            msg_type = type(msg).__name__
            content = ""
            if hasattr(msg, "content") and isinstance(msg.content, list):
                for block in msg.content:
                    if hasattr(block, "text"):
                        content += block.text
            elif hasattr(msg, "result") and isinstance(msg.result, str):
                content = msg.result
            yield {
                "type": msg_type,
                "content": content,
                "session_id": session_id,
            }

        self.registry.update_activity(session_id)

    async def list_sessions(self) -> list[dict[str, Any]]:
        return self.registry.list_active()

    async def close_session(self, session_id: str) -> None:
        self.registry.close_session(session_id)
        self._sessions.pop(session_id, None)

    def available_tools(self) -> list[str]:
        return [t["name"] for t in VELARIA_TOOLS]


def _build_sql_user_message(
    prompt: str,
    schema: list[str],
    table_name: str,
    sample_rows: list[dict[str, Any]] | None = None,
) -> str:
    parts = [f"Table: {table_name}", f"Columns: {', '.join(schema)}"]
    if sample_rows:
        parts.append(f"Sample rows (first {len(sample_rows)}):")
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
        # If it's not JSON, treat the whole text as SQL
        return {"sql": text, "explanation": ""}
