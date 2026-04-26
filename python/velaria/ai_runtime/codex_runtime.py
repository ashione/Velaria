"""Codex App Server runtime -- runs Codex as a sub-process."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pathlib
import queue
import shutil
import sys
import threading
import time
import contextlib
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator

from . import AiRuntime, generate_session_id
from .agent import AgentEvent, normalize_runtime_event
from .functions import tool_definitions, velaria_agent_instructions, velaria_turn_instructions
from .session_registry import SessionRegistry


_STREAM_DONE = object()


@dataclass
class _ChatEventStream:
    events: queue.Queue
    future: Any


class CodexRuntime:
    """Codex App Server runtime -- runs Codex as a sub-process."""

    def __init__(
        self,
        provider: str = "openai",
        model: str = "gpt-5.4-mini",
        reasoning_effort: str = "none",
        network_access: bool = True,
        api_key: str = "",
        base_url: str = "",
        auth_mode: str = "local",
        runtime_path: str = "",
        runtime_workspace: str = "",
        reuse_local_config: bool = True,
        runtime_config_path: str = "",
        skill_dir: str = "",
        skill_path: str = "",
        cwd: str = "",
        proxy_env: dict[str, str] | None = None,
    ):
        self._command = _codex_command(runtime_path)
        self._launch_cwd = pathlib.Path(cwd).expanduser().resolve() if cwd else pathlib.Path.cwd().resolve()
        self._workspace = _runtime_workspace(runtime_workspace, self._launch_cwd)
        self._cwd = _runtime_cwd(self._workspace)
        self._codex_home = self._workspace / ".codex"
        self._skill_dir = pathlib.Path(skill_dir).expanduser().resolve() if skill_dir else None
        self._skill_path = _resolve_velaria_skill_path(skill_path, self._skill_dir)
        self._reuse_local_config = reuse_local_config
        self._runtime_config_path = runtime_config_path
        self._proxy_env = _normalize_proxy_env(proxy_env or {})
        self._provider = provider
        self._api_key = api_key
        self._base_url = base_url
        self._auth_mode = auth_mode
        from codex_app_server_sdk import CodexClient, ThreadConfig  # noqa: F401 -- validated at init

        self.model = model
        self.reasoning_effort = reasoning_effort or "none"
        self.network_access = _coerce_bool(network_access, True)
        self.registry = SessionRegistry(self._workspace / "sessions.sqlite")
        self._threads: dict[str, Any] = {}
        self._client: Any | None = None
        self._client_lock: asyncio.Lock | None = None
        self._thread_config_cls = ThreadConfig
        self._client_cls = CodexClient
        self._prewarm_complete = False
        self._prewarm_lock: asyncio.Lock | None = None
        self._trace_start = time.perf_counter()
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop,
            name="velaria-codex-runtime",
            daemon=True,
        )
        self._loop_thread.start()

    async def create_session(self, dataset_context: dict[str, Any]) -> str:
        return await self.start_thread(dataset_context)

    async def start_thread(self, dataset_context: dict[str, Any] | None = None) -> str:
        session_id = generate_session_id()
        with self._trace_span(f"start_thread session={session_id}"):
            thread = await self._submit(self._create_thread())
        thread_id = getattr(thread, "thread_id", session_id)
        self.registry.register(session_id, "codex", str(thread_id), dataset_context or {})
        self._threads[session_id] = thread
        return session_id

    async def resume_session(self, session_id: str) -> bool:
        return await self.resume_thread(session_id)

    async def resume_thread(self, session_id: str) -> bool:
        entry = self.registry.lookup(session_id, "codex")
        if not entry or entry["status"] != "active":
            return False
        if not _is_uuid_text(session_id):
            self._trace("resume_thread skipped legacy_non_uuid_session")
            return False
        with self._trace_span(f"resume_thread session={session_id}"):
            thread = await self._submit(self._resume_thread(entry["runtime_session_ref"]))
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
        thread = await self._get_thread(session_id)
        if not thread:
            return {"error": f"session not found: {session_id}"}

        user_msg = _build_sql_user_message(prompt, schema, table_name, sample_rows)
        full_prompt = (
            velaria_agent_instructions()
            + "\n\n"
            "Generate a SQL query and return ONLY a JSON object: "
            '{"sql": "<query>", "explanation": "<brief explanation>"}\n\n'
            + user_msg
        )
        result = await self._submit(thread.chat_once(full_prompt))
        self.registry.update_activity(session_id)
        return _parse_sql_json(result.final_text)

    async def analyze(self, session_id: str, prompt: str) -> AsyncIterator[dict[str, Any]]:
        async for event in self.send_message(session_id, prompt):
            yield {
                "type": event.type,
                "content": event.content,
                "session_id": event.session_id,
                "data": event.data,
            }

    async def send_message(self, session_id: str, prompt: str) -> AsyncIterator[AgentEvent]:
        thread = await self._get_thread(session_id)
        if not thread:
            yield AgentEvent("error", f"session not found: {session_id}", session_id=session_id)
            return

        with self._trace_span(f"send_message stream session={session_id} prompt_len={len(prompt)}"):
            stream = self._stream_chat_events(thread, velaria_turn_instructions(prompt))
            try:
                while True:
                    event = await asyncio.to_thread(stream.events.get)
                    if event is _STREAM_DONE:
                        break
                    if not isinstance(event, dict):
                        continue
                    self._trace(f"send_message yield type={event.get('type')}")
                    yield normalize_runtime_event(
                        event.get("type", "assistant_text"),
                        event.get("content", ""),
                        session_id=session_id,
                        data=event.get("data") or {},
                    )
            finally:
                if not stream.future.done():
                    stream.future.cancel()

        self.registry.update_activity(session_id)

    def _stream_chat_events(self, thread, prompt: str) -> _ChatEventStream:
        events: queue.Queue = queue.Queue()

        async def _producer() -> None:
            try:
                async for event in thread.chat(prompt):
                    normalized_events = _codex_sdk_events(event)
                    self._trace(
                        f"thread.chat raw={getattr(event, 'step_type', '')} "
                        f"normalized={','.join(e.get('type', '') for e in normalized_events) or 'filtered'}"
                    )
                    self._trace_event_payload(event)
                    for normalized in normalized_events:
                        events.put(normalized)
            except Exception as exc:
                events.put({"type": "error", "content": str(exc), "data": {}})
            finally:
                events.put(_STREAM_DONE)

        future = asyncio.run_coroutine_threadsafe(_producer(), self._loop)
        return _ChatEventStream(events=events, future=future)

    async def prewarm(self) -> None:
        if self._prewarm_complete:
            self._trace("prewarm skipped already_complete")
            return
        with self._trace_span("prewarm submit"):
            await self._submit(self._prewarm_runtime())

    async def list_sessions(self) -> list[dict[str, Any]]:
        return [
            entry
            for entry in self.registry.list_active("codex")
            if _is_uuid_text(str(entry.get("session_id") or ""))
        ]

    async def list_threads(self) -> list[dict[str, Any]]:
        return await self.list_sessions()

    async def close_session(self, session_id: str) -> None:
        await self.close_thread(session_id)

    async def close_thread(self, session_id: str) -> None:
        self.registry.close_session(session_id)
        self._threads.pop(session_id, None)

    def status(self, session_id: str | None = None) -> dict[str, Any]:
        entry = self._session_status_entry(session_id)
        return {
            "runtime": "codex",
            "provider": self._provider,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "auth_mode": self._auth_mode,
            "network_access": self.network_access,
            "reuse_local_config": self._reuse_local_config,
            "proxy": bool(_proxy_env_from_process(self._proxy_env)),
            "codex_home": str(self._codex_home),
            "workspace": str(self._workspace),
            "cwd": str(self._cwd),
            "session": entry,
            "tools": self.available_tools(),
        }

    def available_tools(self) -> list[str]:
        return [t["name"] for t in tool_definitions()]

    def shutdown(self) -> None:
        close_future = asyncio.run_coroutine_threadsafe(self._close_client(), self._loop)
        close_future.result(timeout=10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=10)
        self.registry.close()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
        self._loop.close()

    async def _submit(self, coro):
        self._trace(f"submit {getattr(coro, '__name__', type(coro).__name__)}")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return await asyncio.wrap_future(future)

    async def _ensure_client(self):
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()
        async with self._client_lock:
            if self._client is not None:
                self._trace("ensure_client reuse")
                return self._client
            with self._trace_span("connect_stdio"):
                client = self._client_cls.connect_stdio(
                    command=self._command,
                    cwd=str(self._cwd),
                    env=self._runtime_env(),
                    inactivity_timeout=180.0,
                )
            try:
                with self._trace_span("client.start"):
                    await client.start()
            except Exception:
                self._client = None
                raise
            self._client = client
            return self._client

    async def _create_thread(self):
        with self._trace_span("_create_thread.ensure_client"):
            client = await self._ensure_client()
        with self._trace_span("_create_thread.config"):
            config = self._thread_config()
        with self._trace_span("_create_thread.client.start_thread"):
            return await client.start_thread(config)

    async def _resume_thread(self, thread_id: str):
        with self._trace_span("_resume_thread.ensure_client"):
            client = await self._ensure_client()
        with self._trace_span("_resume_thread.config"):
            config = self._thread_config()
        with self._trace_span("_resume_thread.client.resume_thread"):
            return await client.resume_thread(thread_id, overrides=config)

    def _thread_config(self, *, ephemeral: bool = False):
        kwargs: dict[str, Any] = {
            "model": self.model,
            "base_instructions": velaria_agent_instructions(),
            "developer_instructions": velaria_agent_instructions(),
            "approval_policy": "never",
            "sandbox": "workspace-write",
            "cwd": str(self._cwd),
            "ephemeral": ephemeral,
            "config": _codex_config(self._runtime_config_path, self._mcp_config()),
        }
        return self._thread_config_cls(**kwargs)

    async def _prewarm_runtime(self) -> None:
        if self._prewarm_complete:
            return
        if self._prewarm_lock is None:
            self._prewarm_lock = asyncio.Lock()
        async with self._prewarm_lock:
            if self._prewarm_complete:
                return
            with self._trace_span("prewarm.ensure_client"):
                client = await self._ensure_client()
            with self._trace_span("prewarm.start_thread"):
                thread = await client.start_thread(self._thread_config(ephemeral=True))
            if os.environ.get("VELARIA_PREWARM_TURN") and hasattr(thread, "chat_once"):
                with self._trace_span("prewarm.chat_once"):
                    await thread.chat_once(
                        "Velaria runtime warmup. Reply with exactly: READY"
                    )
            else:
                self._trace("prewarm.chat_once skipped")
            self._prewarm_complete = True

    async def _get_thread(self, session_id: str):
        thread = self._threads.get(session_id)
        if thread is not None:
            return thread
        entry = self.registry.lookup(session_id, "codex")
        if not entry or entry["status"] != "active":
            return None
        if not _is_uuid_text(session_id):
            self._trace("_get_thread skipped legacy_non_uuid_session")
            return None
        with self._trace_span(f"_get_thread.resume session={session_id}"):
            thread = await self._submit(self._resume_thread(entry["runtime_session_ref"]))
        self._threads[session_id] = thread
        self.registry.update_activity(session_id)
        return thread

    async def _collect_chat_events(self, thread, prompt: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        idx = 0
        with self._trace_span("thread.chat"):
            async for event in thread.chat(prompt):
                idx += 1
                normalized = _codex_sdk_event(event)
                self._trace(
                    f"thread.chat event#{idx} raw={getattr(event, 'step_type', '')} "
                    f"normalized={normalized.get('type') if normalized else 'filtered'}"
                )
                if normalized is not None:
                    events.append(normalized)
        return events

    async def _close_client(self) -> None:
        if self._client is not None:
            with self._trace_span("client.close"):
                await self._client.close()
            self._client = None

    def _session_status_entry(self, session_id: str | None) -> dict[str, Any] | None:
        if session_id:
            if not _is_uuid_text(session_id):
                return None
            return self.registry.lookup(session_id, "codex")
        for entry in self.registry.list_active("codex"):
            if _is_uuid_text(str(entry.get("session_id") or "")):
                return entry
        return None

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
        print(f"[velaria-trace {elapsed:8.3f}s] codex.{message}", file=sys.stderr, flush=True)

    def _trace_event_payload(self, event: Any) -> None:
        if not os.environ.get("VELARIA_TRACE_EVENTS"):
            return
        payload = {
            "raw": str(getattr(event, "step_type", "") or ""),
            "text": str(getattr(event, "text", "") or ""),
            "data": getattr(event, "data", None) or {},
        }
        try:
            rendered = json.dumps(payload, ensure_ascii=False, default=str)
        except Exception:
            rendered = str(payload)
        if len(rendered) > 4000:
            rendered = rendered[:3997] + "..."
        self._trace(f"event_payload {rendered}")

    def _runtime_env(self) -> dict[str, str] | None:
        env = dict(os.environ)
        _prepare_isolated_codex_home(
            self._codex_home,
            reuse_local_auth=self._reuse_local_config,
            model=self.model,
            reasoning_effort=self.reasoning_effort,
            network_access=self.network_access,
            mcp_config=self._mcp_config(),
        )
        env["HOME"] = str(self._workspace)
        env["CODEX_HOME"] = str(self._codex_home)
        uv_cache_dir = self._cwd / ".cache" / "uv"
        uv_cache_dir.mkdir(parents=True, exist_ok=True)
        env["UV_CACHE_DIR"] = str(uv_cache_dir)
        env["VELARIA_HOME"] = str(self._workspace)
        env["VELARIA_WORKSPACE"] = str(self._workspace)
        env["VELARIA_RUNTIME_WORKSPACE"] = str(self._workspace)
        if self._api_key:
            env["OPENAI_API_KEY"] = self._api_key
        if self._base_url:
            env["OPENAI_BASE_URL"] = self._base_url
        _apply_proxy_env(env, self._proxy_env)
        return env

    def _mcp_config(self) -> dict[str, Any]:
        env = {
            "VELARIA_HOME": str(self._workspace),
            "VELARIA_WORKSPACE": str(self._workspace),
            "VELARIA_RUNTIME_WORKSPACE": str(self._workspace),
            "UV_CACHE_DIR": str(self._cwd / ".cache" / "uv"),
            "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
        }
        if self._api_key:
            env["OPENAI_API_KEY"] = self._api_key
        if self._base_url:
            env["OPENAI_BASE_URL"] = self._base_url
        _apply_proxy_env(env, self._proxy_env)
        if self._skill_dir is not None:
            env["VELARIA_SKILL_DIR"] = str(self._skill_dir)
        if self._skill_path is not None:
            env["VELARIA_SKILL_PATH"] = str(self._skill_path)
        return {
            "model_reasoning_effort": self.reasoning_effort,
            "sandbox_workspace_write": {
                "network_access": self.network_access,
            },
            "mcp_servers": {
                "velaria": {
                    "command": sys.executable,
                    "args": ["-m", "velaria.ai_runtime.mcp_server"],
                    "env": env,
                    "default_tools_approval_mode": "approve",
                    "enabled_tools": self.available_tools(),
                }
            }
        }


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


def _codex_sdk_event(event: Any) -> dict[str, Any] | None:
    events = _codex_sdk_events(event)
    return events[-1] if events else None


def _codex_sdk_events(event: Any) -> list[dict[str, Any]]:
    data = getattr(event, "data", None) or {}
    text = str(getattr(event, "text", "") or "")
    raw_type = str(getattr(event, "step_type", "") or "")
    item = _codex_event_item(data)
    item_type = str(item.get("type") or raw_type)
    event_data = _codex_tool_event_data(data, item)

    if item_type in {"userMessage", "message"} and item.get("role") == "user":
        return []
    if item_type in {"assistantMessage", "message"}:
        content = text or _codex_text_from_blocks(item.get("content"))
        return [{"type": "assistant_text", "content": content, "data": data}] if content else []
    if item_type == "reasoning":
        content = text or _codex_reasoning_text(item)
        return [{"type": "thinking", "content": content, "data": data}] if content else []
    if item_type == "mcpToolCall" and item.get("result"):
        content = text or _codex_tool_result_text(item)
        name = _codex_tool_name(item)
        return [
            {"type": "tool_call", "content": name, "data": event_data},
            {"type": "tool_result", "content": content, "data": event_data},
        ]
    if item_type in {"functionCall", "toolCall", "mcpToolCall", "function_call", "tool_search_call"}:
        name = _codex_tool_name(item)
        return [{"type": "tool_call", "content": name, "data": event_data}]
    if item_type in {
        "functionCallOutput",
        "toolResult",
        "mcpToolCallOutput",
        "function_call_output",
        "tool_search_output",
    }:
        content = text or _codex_tool_result_text(item)
        return [{"type": "tool_result", "content": content, "data": event_data}]
    if item_type in {"commandExecution", "exec"}:
        content = text or str(item.get("command") or item.get("cmd") or "")
        return [{"type": "command", "content": content, "data": {**data, "item": item}}]
    if item_type in {"error", "failed"}:
        content = text or str(item.get("message") or item.get("error") or "")
        return [{"type": "error", "content": content, "data": {**data, "item": item}}]
    if raw_type in {"done", "completed", "turnDone"}:
        return [{"type": "done", "content": "", "data": data}]
    if text:
        return [{"type": raw_type or "assistant_text", "content": text, "data": data}]
    return []


def _codex_tool_event_data(data: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    event_data = {**data, "item": item}
    name = _codex_tool_name(item)
    if name:
        event_data["tool_name"] = name
    status = item.get("status")
    if isinstance(status, str) and status:
        event_data["tool_status"] = status
    arguments = item.get("arguments")
    if arguments is not None:
        event_data["tool_arguments"] = arguments
    duration = item.get("durationMs")
    if duration is not None:
        event_data["duration_ms"] = duration
    return event_data


def _codex_event_item(data: dict[str, Any]) -> dict[str, Any]:
    found = _find_codex_item(data)
    if found is not None:
        return found
    item = data.get("item")
    if isinstance(item, dict):
        return item
    params = data.get("params")
    if isinstance(params, dict) and isinstance(params.get("item"), dict):
        return params["item"]
    return {}


def _find_codex_item(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        item_type = value.get("type")
        if item_type in {
            "functionCall",
            "toolCall",
            "mcpToolCall",
            "function_call",
            "tool_search_call",
            "functionCallOutput",
            "toolResult",
            "mcpToolCallOutput",
            "function_call_output",
            "tool_search_output",
            "message",
            "assistantMessage",
            "userMessage",
            "reasoning",
            "error",
            "failed",
        }:
            return value
        if value.get("name") and (value.get("namespace") or value.get("serverName") or value.get("toolName")):
            return value
        for key in ("payload", "item", "params", "event", "data"):
            found = _find_codex_item(value.get(key))
            if found is not None:
                return found
        for nested in value.values():
            found = _find_codex_item(nested)
            if found is not None:
                return found
    if isinstance(value, list):
        for nested in value:
            found = _find_codex_item(nested)
            if found is not None:
                return found
    return None


def _codex_tool_name(item: dict[str, Any]) -> str:
    name = str(item.get("name") or item.get("toolName") or item.get("tool") or "")
    namespace = str(item.get("namespace") or item.get("serverName") or item.get("server") or "")
    function = item.get("function")
    if not name and isinstance(function, dict):
        name = str(function.get("name") or "")
    if namespace == "velaria" and name:
        return name
    if namespace and name and not name.startswith(namespace):
        return f"{namespace}.{name}"
    return name or namespace


def _codex_text_from_blocks(blocks: Any) -> str:
    if not isinstance(blocks, list):
        return ""
    parts: list[str] = []
    for block in blocks:
        if isinstance(block, dict):
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts)


def _codex_reasoning_text(item: dict[str, Any]) -> str:
    summary = item.get("summary")
    content = _codex_text_from_blocks(item.get("content"))
    if isinstance(summary, list):
        summary_text = _codex_text_from_blocks(summary)
        return summary_text or content
    if isinstance(summary, str):
        return summary or content
    return content


def _codex_tool_result_text(item: dict[str, Any]) -> str:
    for key in ("output", "result", "content"):
        value = item.get(key)
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            structured = value.get("structuredContent")
            if isinstance(structured, dict):
                return json.dumps(structured, ensure_ascii=False)
            content = value.get("content")
            if isinstance(content, list):
                text = _codex_text_from_blocks(content)
                if text:
                    return text
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, list):
            text = _codex_text_from_blocks(value)
            if text:
                return text
    return ""


def _codex_command(runtime_path: str) -> list[str] | None:
    if not runtime_path:
        return None
    path = pathlib.Path(runtime_path).expanduser()
    if not path.exists():
        raise RuntimeError(f"Codex runtime path does not exist: {path}")
    if not path.is_file():
        raise RuntimeError(f"Codex runtime path must be an executable file: {path}")
    if not os.access(path, os.X_OK):
        raise RuntimeError(f"Codex runtime path is not executable: {path}")
    return [str(path), "app-server"]


def _runtime_workspace(runtime_workspace: str, cwd: pathlib.Path) -> pathlib.Path:
    path = pathlib.Path(runtime_workspace).expanduser() if runtime_workspace else (
        pathlib.Path.home() / ".velaria" / "ai-runtime" / _workspace_key(cwd)
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _runtime_cwd(runtime_workspace: pathlib.Path) -> pathlib.Path:
    path = runtime_workspace / "workspace"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _prepare_isolated_codex_home(
    codex_home: pathlib.Path,
    *,
    reuse_local_auth: bool,
    model: str,
    reasoning_effort: str,
    network_access: bool,
    mcp_config: dict[str, Any] | None = None,
) -> None:
    codex_home.mkdir(parents=True, exist_ok=True)
    if reuse_local_auth:
        source_home = _local_codex_home()
        for filename in ("auth.json", "installation_id"):
            source = source_home / filename
            target = codex_home / filename
            if source.exists() and source.is_file():
                shutil.copy2(source, target)
    _write_minimal_codex_config(
        codex_home / "config.toml",
        model,
        reasoning_effort,
        network_access,
        mcp_config or {},
    )


def _local_codex_home() -> pathlib.Path:
    configured = os.environ.get("CODEX_HOME")
    if configured:
        return pathlib.Path(configured).expanduser()
    return pathlib.Path.home() / ".codex"


def _write_minimal_codex_config(
    path: pathlib.Path,
    model: str,
    reasoning_effort: str,
    network_access: bool,
    mcp_config: dict[str, Any] | None = None,
) -> None:
    lines = [
        "# Generated by Velaria. Keep this runtime config minimal.",
        f'model = "{_toml_string(model)}"',
        f'model_reasoning_effort = "{_toml_string(reasoning_effort or "none")}"',
        'approval_policy = "never"',
        'sandbox_mode = "workspace-write"',
        "[sandbox_workspace_write]",
        f"network_access = {_toml_bool(network_access)}",
        "",
    ]
    lines.extend(_mcp_config_toml_lines(mcp_config or {}))
    lines.append("")
    content = "\n".join(lines)
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return
    path.write_text(content, encoding="utf-8")


def _mcp_config_toml_lines(config: dict[str, Any]) -> list[str]:
    servers = config.get("mcp_servers")
    if not isinstance(servers, dict) or not servers:
        return []
    lines: list[str] = []
    for name, raw_server in sorted(servers.items()):
        if not isinstance(raw_server, dict):
            continue
        server_name = _toml_string(str(name))
        lines.append(f'[mcp_servers."{server_name}"]')
        command = str(raw_server.get("command") or "").strip()
        if command:
            lines.append(f'command = "{_toml_string(command)}"')
        args = raw_server.get("args")
        if isinstance(args, list):
            lines.append(f"args = {_toml_string_array([str(item) for item in args])}")
        enabled_tools = raw_server.get("enabled_tools")
        if isinstance(enabled_tools, list):
            lines.append(f"enabled_tools = {_toml_string_array([str(item) for item in enabled_tools])}")
        approval_mode = str(raw_server.get("default_tools_approval_mode") or "").strip()
        if approval_mode:
            lines.append(f'default_tools_approval_mode = "{_toml_string(approval_mode)}"')
        env = raw_server.get("env")
        if isinstance(env, dict) and env:
            lines.append("")
            lines.append(f'[mcp_servers."{server_name}".env]')
            for key, value in sorted(env.items()):
                key_text = str(key).strip()
                if not key_text:
                    continue
                lines.append(f'"{_toml_string(key_text)}" = "{_toml_string(str(value))}"')
        lines.append("")
    return lines


def _toml_string_array(values: list[str]) -> str:
    return "[" + ", ".join(f'"{_toml_string(value)}"' for value in values) + "]"


def _toml_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _toml_bool(value: bool) -> str:
    return "true" if value else "false"


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


def _resolve_velaria_skill_path(skill_path: str, skill_dir: pathlib.Path | None) -> pathlib.Path | None:
    if skill_path:
        path = pathlib.Path(skill_path).expanduser().resolve()
        return path if path.exists() else None
    if skill_dir is None:
        return None
    candidate = skill_dir / "velaria_python_local" / "SKILL.md"
    return candidate if candidate.exists() else None


def _is_uuid_text(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (TypeError, ValueError):
        return False


def _trace_enabled() -> bool:
    return bool(os.environ.get("VELARIA_TRACE"))


def _workspace_key(cwd: pathlib.Path) -> str:
    digest = hashlib.sha1(str(cwd).encode("utf-8")).hexdigest()[:12]
    return f"{cwd.name}-{digest}"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _codex_config(runtime_config_path: str, generated_config: dict[str, Any]) -> dict[str, Any]:
    if not runtime_config_path:
        return generated_config
    path = pathlib.Path(runtime_config_path).expanduser()
    if not path.exists():
        raise RuntimeError(f"Codex runtime config path does not exist: {path}")
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to read Codex runtime config path: {path}") from exc
    return _deep_merge(loaded, generated_config)


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
