"""Claude Agent SDK runtime -- runs Claude as a sub-process with custom Velaria tools."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import pathlib
import sys
import time
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
        auth_mode: str = "local",
        base_url: str = "",
        model: str = "claude-sonnet-4-20250514",
        runtime_path: str = "",
        runtime_workspace: str = "",
        reuse_local_config: bool = True,
        runtime_config_path: str = "",
        skill_dir: str = "",
        skill_path: str = "",
        cwd: str = "",
        proxy_env: dict[str, str] | None = None,
        reasoning_effort: str = "none",
        network_access: bool = True,
    ):
        self.runtime_path = _claude_runtime_path(runtime_path)
        self.launch_cwd = pathlib.Path(cwd).expanduser().resolve() if cwd else pathlib.Path.cwd().resolve()
        self.runtime_workspace = _runtime_workspace(runtime_workspace, self.launch_cwd)
        self.cwd = _runtime_cwd(self.runtime_workspace)
        self.skill_dir = pathlib.Path(skill_dir).expanduser().resolve() if skill_dir else None
        self.skill_path = _resolve_velaria_skill_path(skill_path, self.skill_dir)
        self.reuse_local_config = reuse_local_config
        self.runtime_config_path = runtime_config_path
        self._proxy_env = _normalize_proxy_env(proxy_env or {})
        self.reasoning_effort = reasoning_effort or "none"
        self.network_access = _coerce_bool(network_access, True)
        self._trace_start = time.perf_counter()
        from claude_agent_sdk import ClaudeSDKClient  # noqa: F401 -- validated at init

        self.model = model
        self.provider = provider
        self.auth_mode = auth_mode
        self.api_key = api_key
        self.base_url = base_url
        self.registry = SessionRegistry(pathlib.Path(self.runtime_workspace) / "sessions.sqlite")
        self._sessions: dict[str, dict[str, Any]] = {}  # in-memory session state
        self._prewarm_complete = False
        self._prewarm_lock: asyncio.Lock | None = None

        # Build tool name -> definition lookup
        self._tool_functions: dict[str, dict[str, Any]] = {}
        for tool_def in tool_definitions():
            self._tool_functions[tool_def["name"]] = tool_def

    async def create_session(self, dataset_context: dict[str, Any]) -> str:
        return await self.start_thread(dataset_context)

    async def start_thread(self, dataset_context: dict[str, Any] | None = None) -> str:
        session_id = generate_session_id()
        with self._trace_span(f"start_thread session={session_id}"):
            self.registry.register(session_id, "claude", session_id, dataset_context or {})
        self._sessions[session_id] = {"dataset_context": dataset_context or {}, "session": None}
        return session_id

    async def resume_session(self, session_id: str) -> bool:
        return await self.resume_thread(session_id)

    async def resume_thread(self, session_id: str) -> bool:
        with self._trace_span(f"resume_thread session={session_id}"):
            entry = self.registry.lookup(session_id)
            if not entry or entry["status"] != "active":
                self._trace("resume_thread not_found_or_inactive")
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
        with self._trace_span(f"generate_sql session={session_id} prompt_len={len(prompt)}"):
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
                content, _ = _claude_sdk_event_text(msg)
                if content:
                    result_text = content
                self._trace(f"generate_sql msg type={type(msg).__name__} content_len={len(content)}")

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
        from claude_agent_sdk import ClaudeSDKClient, create_sdk_mcp_server
        from claude_agent_sdk.types import ClaudeAgentOptions

        session_data = self._sessions.get(session_id)
        if not session_data:
            yield AgentEvent("error", f"session not found: {session_id}", session_id=session_id)
            return

        dataset_context = session_data["dataset_context"]

        # Build SDK MCP tools from Velaria tool definitions
        sdk_tools = _build_sdk_mcp_tools(dataset_context)

        with self._trace_span(f"send_message session={session_id} prompt_len={len(prompt)}"):
            velaria_mcp = create_sdk_mcp_server(name="velaria", version="1.0.0", tools=sdk_tools)
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
                    mcp_servers={"velaria": velaria_mcp},
                ),
            )

            async for msg in client.query(prompt=prompt):
                event = _claude_sdk_event(msg, session_id)
                self._trace(
                    f"send_message yield raw_type={type(msg).__name__} "
                    f"normalized={event.type} content_len={len(event.content)}"
                )
                yield event

        self.registry.update_activity(session_id)

    async def prewarm(self) -> None:
        if self._prewarm_complete:
            self._trace("prewarm skipped already_complete")
            return
        if self._prewarm_lock is None:
            self._prewarm_lock = asyncio.Lock()
        async with self._prewarm_lock:
            if self._prewarm_complete:
                return
            with self._trace_span("prewarm"):
                await self._prewarm_runtime()

    async def _prewarm_runtime(self) -> None:
        from claude_agent_sdk import ClaudeSDKClient
        from claude_agent_sdk.types import ClaudeAgentOptions

        with self._trace_span("prewarm.create_client"):
            client = ClaudeSDKClient(
                options=ClaudeAgentOptions(
                    model=self.model,
                    system_prompt=velaria_agent_instructions(),
                    permission_mode="bypassPermissions",
                    cli_path=self.runtime_path or None,
                    env=self._runtime_env(),
                    cwd=str(self.cwd),
                    skills=["velaria-python-local"],
                    setting_sources=["project", "user"],
                ),
            )

        if os.environ.get("VELARIA_PREWARM_TURN"):
            with self._trace_span("prewarm.chat_once"):
                async for _msg in client.query(
                    prompt="Velaria runtime warmup. Reply with exactly: READY"
                ):
                    pass
        else:
            self._trace("prewarm.chat_once skipped")

        self._prewarm_complete = True

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
            "reasoning_effort": self.reasoning_effort,
            "auth_mode": self.auth_mode,
            "network_access": self.network_access,
            "reuse_local_config": self.reuse_local_config,
            "proxy": bool(_proxy_env_from_process(self._proxy_env)),
            "workspace": self.runtime_workspace,
            "cwd": str(self.cwd),
            "session": entry,
            "tools": self.available_tools(),
        }

    def available_tools(self) -> list[str]:
        return [t["name"] for t in tool_definitions()]

    def shutdown(self) -> None:
        self.registry.close()
        self._sessions.clear()

    def _runtime_env(self) -> dict[str, str]:
        env = {} if self.reuse_local_config else dict(os.environ)
        if not self.reuse_local_config:
            env["HOME"] = str(self.runtime_workspace)
            env["CLAUDE_CONFIG_DIR"] = str(pathlib.Path(self.runtime_workspace) / ".claude")
        uv_cache_dir = self.cwd / ".cache" / "uv"
        uv_cache_dir.mkdir(parents=True, exist_ok=True)
        env["UV_CACHE_DIR"] = str(uv_cache_dir)
        env["VELARIA_HOME"] = str(self.runtime_workspace)
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
        _apply_proxy_env(env, self._proxy_env)
        return env

    @contextlib.contextmanager
    def _trace_span(self, name: str):
        if not _trace_enabled():
            yield
            return
        start = time.perf_counter()
        self._trace(f"{name} start")
        try:
            yield
        finally:
            elapsed = (time.perf_counter() - start) * 1000.0
            self._trace(f"{name} end {elapsed:.1f}ms")

    def _trace(self, message: str) -> None:
        if not _trace_enabled():
            return
        elapsed = time.perf_counter() - self._trace_start
        print(f"[velaria-trace {elapsed:8.3f}s] claude.{message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Event normalization
# ---------------------------------------------------------------------------

def _claude_sdk_event_text(msg: Any) -> tuple[str, str]:
    msg_type = type(msg).__name__
    if hasattr(msg, "content") and isinstance(msg.content, list):
        text_parts = [block.text for block in msg.content if hasattr(block, "text") and isinstance(block.text, str)]
        if text_parts:
            return "".join(text_parts), msg_type
        thinking_parts = [block.thinking for block in msg.content if hasattr(block, "thinking") and isinstance(block.thinking, str)]
        if thinking_parts:
            return "".join(thinking_parts), "thinking"
        tool_blocks = [block for block in msg.content if hasattr(block, "name") and hasattr(block, "input")]
        if tool_blocks:
            return getattr(tool_blocks[0], "name", ""), "ToolUseBlock"
    if hasattr(msg, "result") and isinstance(msg.result, str) and msg.result.strip():
        return msg.result, "ResultMessage"
    return "", msg_type


def _claude_sdk_event(msg: Any, session_id: str = "") -> AgentEvent:
    content, msg_type = _claude_sdk_event_text(msg)
    event_type = msg_type
    if msg_type == "AssistantMessage":
        event_type = "assistant_text"
    elif "thinking" in msg_type.lower():
        event_type = "thinking"
    elif msg_type in ("ToolUseBlock",):
        event_type = "tool_call"
    elif msg_type in ("ToolResultBlock",):
        event_type = "tool_result"
    elif content and msg_type not in ("ResultMessage",):
        event_type = "assistant_text"
    return normalize_runtime_event(event_type, content, session_id=session_id)


# ---------------------------------------------------------------------------
# SDK MCP tool builders
# ---------------------------------------------------------------------------

def _build_sdk_mcp_tools(dataset_context: dict[str, Any] | None = None) -> list:
    """Build claude_agent_sdk SdkMcpTool instances from Velaria tool definitions."""
    from claude_agent_sdk import SdkMcpTool

    ctx = dataset_context or {}
    sdk_tools = []
    for tool_def in tool_definitions():
        name = tool_def["name"]

        def make_handler(n=name):
            async def handler(input_data: dict) -> dict:
                merged = dict(input_data or {})
                for key in ("source_path", "table_name", "schema"):
                    if key in ctx and key not in merged:
                        merged[key] = ctx[key]
                return execute_local_function(n, merged)
            return handler

        sdk_tools.append(SdkMcpTool(
            name=name,
            description=tool_def["description"],
            input_schema=tool_def["input_schema"],
            handler=make_handler(name),
        ))
    return sdk_tools


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_sql_user_message(prompt, schema, table_name, sample_rows=None):
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
    candidate = (skill_dir / "velaria_python_local" / "SKILL.md").resolve()
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
        return {"sql": text, "explanation": ""}


def _normalize_proxy_env(proxy_env: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in proxy_env.items():
        key_text = str(key).strip()
        value_text = str(value).strip()
        if not key_text or not value_text:
            continue
        key_lower = key_text.lower()
        if key_lower in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}:
            normalized[key_lower] = value_text
    return normalized


def _proxy_env_from_process(configured: dict[str, str]) -> dict[str, str]:
    merged = dict(configured)
    for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        if key not in merged:
            value = os.environ.get(key) or os.environ.get(key.upper())
            if value:
                merged[key] = value
    return merged


def _apply_proxy_env(env: dict[str, str], configured: dict[str, str]) -> None:
    proxies = _proxy_env_from_process(configured)
    if not proxies:
        return
    proxies.setdefault("no_proxy", "127.0.0.1,localhost,::1")
    for key, value in proxies.items():
        env[key] = value
        env[key.upper()] = value


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _trace_enabled() -> bool:
    return bool(os.environ.get("VELARIA_TRACE"))
