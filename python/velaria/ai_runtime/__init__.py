from __future__ import annotations
import secrets
from typing import Any, AsyncIterator, Protocol

class AiRuntime(Protocol):
    async def create_session(self, dataset_context: dict[str, Any]) -> str: ...
    async def resume_session(self, session_id: str) -> bool: ...
    async def generate_sql(self, session_id: str, prompt: str, schema: list[str], table_name: str, sample_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]: ...
    async def analyze(self, session_id: str, prompt: str) -> AsyncIterator[dict[str, Any]]: ...
    async def list_sessions(self) -> list[dict[str, Any]]: ...
    async def close_session(self, session_id: str) -> None: ...
    def available_tools(self) -> list[str]: ...

def generate_session_id() -> str:
    return f"ai_session_{secrets.token_hex(8)}"

def create_runtime(config: dict[str, Any]) -> AiRuntime:
    runtime_type = str(config.get("runtime", "auto")).strip().lower()
    api_key = str(config.get("api_key", ""))
    model = str(config.get("model", ""))

    if runtime_type == "claude" or (runtime_type == "auto" and _has_claude_sdk()):
        from .claude_runtime import ClaudeAgentRuntime
        return ClaudeAgentRuntime(api_key=api_key, model=model or "claude-sonnet-4-20250514")

    if runtime_type == "codex" or (runtime_type == "auto" and _has_codex_sdk()):
        from .codex_runtime import CodexRuntime
        return CodexRuntime(model=model or "gpt-5")

    raise RuntimeError(
        "No AI runtime SDK available. Install claude-agent-sdk or codex-app-server-sdk: "
        "uv pip install claude-agent-sdk  OR  uv pip install codex-app-server-sdk"
    )

def _has_claude_sdk() -> bool:
    try:
        import claude_agent_sdk  # noqa: F401
        return True
    except ImportError:
        return False

def _has_codex_sdk() -> bool:
    try:
        import codex_app_server  # noqa: F401
        return True
    except ImportError:
        return False
