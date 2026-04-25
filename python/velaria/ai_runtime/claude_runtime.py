"""Claude Agent SDK runtime -- runs Claude as a sub-process with custom Velaria tools."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pathlib
from typing import Any, AsyncIterator

from . import AiRuntime, generate_session_id
from .agent import AgentEvent, normalize_runtime_event
from .functions import execute_local_function, tool_definitions, velaria_agent_instructions
from .session_registry import SessionRegistry


class ClaudeAgentRuntime:
    """Claude Agent SDK runtime -- runs Claude as a sub-process with custom Velaria tools."""

    def __init__(
        self,
        api_key: str,
        provider: str = "anthropic",
        auth_mode: str = "oauth",
        base_url: str = "",
        model: str = "claude-sonnet-4-20250514",
        runtime_path: str = "",
        runtime_workspace: str = "",
        reuse_local_config: bool = True,
        runtime_config_path: str = "",
        skill_dir: str = "",
        skill_path: str = "",
        cwd: str = "",
    ):
        self.runtime_path = _claude_runtime_path(runtime_path)
        self.launch_cwd = pathlib.Path(cwd).expanduser().resolve() if cwd else pathlib.Path.cwd().resolve()
        self.runtime_workspace = _runtime_workspace(runtime_workspace, self.launch_cwd)
        self.cwd = _runtime_cwd(self.runtime_workspace)
        self.skill_dir = pathlib.Path(skill_dir).expanduser().resolve() if skill_dir else None
        self.skill_path = _resolve_velaria_skill_path(skill_path, self.skill_dir)
        self.reuse_local_config = reuse_local_config
        self.runtime_config_path = runtime_config_path
        from claude_agent_sdk import ClaudeSDKClient  # noqa: F401 -- validated at init

        self.model = model
        self.provider = provider
        self.auth_mode = auth_mode
        self.api_key = api_key
        self.base_url = base_url
        self.registry = SessionRegistry(pathlib.Path(self.runtime_workspace) / "sessions.sqlite")
        self._sessions: dict[str, dict[str, Any]] = {}  # in-memory session state

        # Build tool name -> definition lookup
        self._tool_functions: dict[str, dict[str, Any]] = {}
        for tool_def in tool_definitions():
            self._tool_functions[tool_def["name"]] = tool_def

    async def create_session(self, dataset_context: dict[str, Any]) -> str:
        return await self.start_thread(dataset_context)

    async def start_thread(self, dataset_context: dict[str, Any] | None = None) -> str:
        session_id = generate_session_id()
        self.registry.register(session_id, "claude", session_id, dataset_context or {})
        self._sessions[session_id] = {"dataset_context": dataset_context or {}, "session": None}
        return session_id

    async def resume_session(self, session_id: str) -> bool:
        return await self.resume_thread(session_id)

    async def resume_thread(self, session_id: str) -> bool:
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
            velaria_agent_instructions()
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
                cli_path=self.runtime_path or None,
                env=self._runtime_env(),
                cwd=str(self.cwd),
                settings=self.runtime_config_path or None,
                session_id=session_id,
                skills=["velaria-python-local"],
                setting_sources=["project", "user"],
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
        async for event in self.send_message(session_id, prompt):
            yield {
                "type": event.type,
                "content": event.content,
                "session_id": event.session_id,
                "data": event.data,
            }

    async def send_message(self, session_id: str, prompt: str) -> AsyncIterator[AgentEvent]:
        from claude_agent_sdk import ClaudeSDKClient
        from claude_agent_sdk.types import ClaudeAgentOptions

        session_data = self._sessions.get(session_id)
        if not session_data:
            yield AgentEvent("error", f"session not found: {session_id}", session_id=session_id)
            return

        dataset_context = session_data["dataset_context"]

        # Build custom tool handlers
        def handle_tool(tool_name: str, tool_input: dict) -> str:
            merged = dict(tool_input or {})
            for key in ("source_path", "table_name", "schema"):
                if key in dataset_context and key not in merged:
                    merged[key] = dataset_context[key]
            return json.dumps(execute_local_function(tool_name, merged), ensure_ascii=False)

        # Create custom tools for ClaudeSDKClient
        custom_tools = []
        for tool_def in tool_definitions():
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
                system_prompt=velaria_agent_instructions(),
                permission_mode="bypassPermissions",
                cli_path=self.runtime_path or None,
                env=self._runtime_env(),
                cwd=str(self.cwd),
                settings=self.runtime_config_path or None,
                session_id=session_id,
                skills=["velaria-python-local"],
                setting_sources=["project", "user"],
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
            yield normalize_runtime_event(msg_type, content, session_id=session_id)

        self.registry.update_activity(session_id)

    async def prewarm(self) -> None:
        return None

    async def list_sessions(self) -> list[dict[str, Any]]:
        return self.registry.list_active()

    async def list_threads(self) -> list[dict[str, Any]]:
        return await self.list_sessions()

    async def close_session(self, session_id: str) -> None:
        await self.close_thread(session_id)

    async def close_thread(self, session_id: str) -> None:
        self.registry.close_session(session_id)
        self._sessions.pop(session_id, None)

    def status(self, session_id: str | None = None) -> dict[str, Any]:
        entry = self.registry.lookup(session_id) if session_id else self.registry.most_recent_active()
        return {
            "runtime": "claude",
            "provider": self.provider,
            "model": self.model,
            "auth_mode": self.auth_mode,
            "reuse_local_config": self.reuse_local_config,
            "workspace": self.runtime_workspace,
            "cwd": str(self.cwd),
            "session": entry,
            "tools": self.available_tools(),
        }

    def available_tools(self) -> list[str]:
        return [t["name"] for t in tool_definitions()]

    def shutdown(self) -> None:
        self.registry.close()

    def _runtime_env(self) -> dict[str, str]:
        env = {} if self.reuse_local_config else dict(os.environ)
        if not self.reuse_local_config:
            env["HOME"] = str(self.runtime_workspace)
            env["CLAUDE_CONFIG_DIR"] = str(pathlib.Path(self.runtime_workspace) / ".claude")
        env["VELARIA_WORKSPACE"] = str(self.runtime_workspace)
        env["VELARIA_RUNTIME_WORKSPACE"] = str(self.runtime_workspace)
        if self.skill_dir is not None:
            env["VELARIA_SKILL_DIR"] = str(self.skill_dir)
        if self.skill_path is not None:
            env["VELARIA_SKILL_PATH"] = str(self.skill_path)
        if self.api_key:
            env["ANTHROPIC_API_KEY"] = self.api_key
        if self.base_url:
            env["ANTHROPIC_BASE_URL"] = self.base_url
        return env


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


def _claude_runtime_path(runtime_path: str) -> str:
    if not runtime_path:
        return ""
    path = pathlib.Path(runtime_path).expanduser()
    if not path.exists():
        raise RuntimeError(f"Claude runtime path does not exist: {path}")
    if not path.is_file():
        raise RuntimeError(f"Claude runtime path must be an executable file: {path}")
    if not os.access(path, os.X_OK):
        raise RuntimeError(f"Claude runtime path is not executable: {path}")
    return str(path)


def _runtime_workspace(runtime_workspace: str, cwd: pathlib.Path) -> str:
    path = pathlib.Path(runtime_workspace).expanduser() if runtime_workspace else (
        pathlib.Path.home() / ".velaria" / "ai-runtime" / _workspace_key(cwd)
    )
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _runtime_cwd(runtime_workspace: str) -> pathlib.Path:
    path = pathlib.Path(runtime_workspace) / "workspace"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_velaria_skill_path(skill_path: str, skill_dir: pathlib.Path | None) -> pathlib.Path | None:
    if skill_path:
        path = pathlib.Path(skill_path).expanduser().resolve()
        return path if path.exists() else None
    if skill_dir is None:
        return None
    candidate = skill_dir / "velaria_python_local" / "SKILL.md"
    return candidate if candidate.exists() else None


def _workspace_key(cwd: pathlib.Path) -> str:
    digest = hashlib.sha1(str(cwd).encode("utf-8")).hexdigest()[:12]
    return f"{cwd.name}-{digest}"


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
