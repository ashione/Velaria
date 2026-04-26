from __future__ import annotations
import json
import secrets
from pathlib import Path
from typing import Any, AsyncIterator, Protocol

from .agent import AgentEvent

class AiRuntime(Protocol):
    async def create_session(self, dataset_context: dict[str, Any]) -> str: ...
    async def resume_session(self, session_id: str) -> bool: ...
    async def generate_sql(self, session_id: str, prompt: str, schema: list[str], table_name: str, sample_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]: ...
    async def analyze(self, session_id: str, prompt: str) -> AsyncIterator[dict[str, Any]]: ...
    async def list_sessions(self) -> list[dict[str, Any]]: ...
    async def close_session(self, session_id: str) -> None: ...
    def available_tools(self) -> list[str]: ...
    async def start_thread(self, dataset_context: dict[str, Any] | None = None) -> str: ...
    async def resume_thread(self, session_id: str) -> bool: ...
    async def send_message(self, session_id: str, prompt: str) -> AsyncIterator[AgentEvent]: ...
    async def list_threads(self) -> list[dict[str, Any]]: ...
    async def close_thread(self, session_id: str) -> None: ...
    def status(self, session_id: str | None = None) -> dict[str, Any]: ...

def generate_session_id() -> str:
    return f"ai_session_{secrets.token_hex(8)}"

def create_runtime(config: dict[str, Any]) -> AiRuntime:
    runtime_type = str(config.get("runtime", "codex")).strip().lower()
    provider = str(config.get("provider", "openai") or "openai").strip().lower()
    auth_mode = _normalize_auth_mode(config.get("auth_mode", "oauth"))
    api_key = str(config.get("api_key", "")) if auth_mode == "api_key" else ""
    base_url = str(config.get("base_url", "")) if auth_mode == "api_key" else ""
    model = str(config.get("model", ""))
    reasoning_effort = str(config.get("reasoning_effort") or "none")
    network_access = _bool_config(config, "network_access", True)
    reuse_local_config = auth_mode == "local"

    if runtime_type == "claude" or (runtime_type == "auto" and _has_claude_sdk()):
        from .claude_runtime import ClaudeAgentRuntime
        return ClaudeAgentRuntime(
            provider=provider,
            auth_mode=auth_mode,
            api_key=api_key,
            base_url=base_url,
            model=model or "claude-sonnet-4-20250514",
            reasoning_effort=reasoning_effort,
            network_access=network_access,
            runtime_path=_runtime_path_for(config, "claude"),
            runtime_workspace=str(config.get("runtime_workspace") or ""),
            reuse_local_config=reuse_local_config,
            runtime_config_path=str(config.get("runtime_config_path") or ""),
            skill_dir=str(config.get("skill_dir") or ""),
            skill_path=str(config.get("skill_path") or ""),
            cwd=str(config.get("cwd") or ""),
            proxy_env=dict(config.get("proxy_env") or {}),
        )

    if runtime_type == "codex" or (runtime_type == "auto" and _has_codex_sdk()):
        from .codex_runtime import CodexRuntime
        return CodexRuntime(
            provider=provider,
            model=model or "gpt-5.4-mini",
            reasoning_effort=reasoning_effort,
            network_access=network_access,
            api_key=api_key,
            base_url=base_url,
            auth_mode=auth_mode,
            runtime_path=_runtime_path_for(config, "codex"),
            runtime_workspace=str(config.get("runtime_workspace") or ""),
            reuse_local_config=reuse_local_config,
            runtime_config_path=str(config.get("runtime_config_path") or ""),
            skill_dir=str(config.get("skill_dir") or ""),
            skill_path=str(config.get("skill_path") or ""),
            cwd=str(config.get("cwd") or ""),
            proxy_env=dict(config.get("proxy_env") or {}),
        )

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
        import codex_app_server_sdk  # noqa: F401
        return True
    except ImportError:
        return False

def _runtime_path_for(config: dict[str, Any], runtime_type: str) -> str:
    if runtime_type == "claude":
        return str(config.get("claude_runtime_path") or config.get("runtime_path") or "")
    if runtime_type == "codex":
        return str(config.get("codex_runtime_path") or config.get("runtime_path") or "")
    return str(config.get("runtime_path") or "")

def _bool_config(config: dict[str, Any], key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)

def _normalize_auth_mode(value: Any) -> str:
    normalized = str(value or "local").strip().lower()
    if normalized == "api_key":
        return "api_key"
    # "oauth" is a legacy alias for "local" (reuse local CLI config)
    if normalized in ("oauth", "local"):
        return "local"
    return "local"

def load_ai_config() -> dict[str, Any]:
    config_path = Path.home() / ".velaria" / "config.json"
    if not config_path.exists():
        return {}
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    auth_mode = _normalize_auth_mode(config.get("agentAuthMode", "local"))
    return {
        "provider": config.get("agentProvider", "openai"),
        "auth_mode": auth_mode,
        "api_key": config.get("agentApiKey", "") if auth_mode == "api_key" else "",
        "base_url": config.get("agentBaseUrl", "https://api.openai.com/v1"),
        "model": config.get("agentModel", ""),
        "reasoning_effort": config.get("agentReasoningEffort", "none"),
        "runtime": config.get("agentRuntime", "codex"),
        "runtime_path": config.get("agentRuntimePath", ""),
        "claude_runtime_path": config.get("agentClaudeRuntimePath", ""),
        "codex_runtime_path": config.get("agentCodexRuntimePath", ""),
        "runtime_workspace": config.get("agentRuntimeWorkspace", ""),
        "reuse_local_config": auth_mode == "local",
        "runtime_config_path": config.get("agentRuntimeConfigPath", ""),
        "network_access": config.get("agentCodexNetworkAccess", config.get("agentNetworkAccess", True)),
        "proxy_env": _proxy_env_from_config(config),
        "skill_dir": config.get("agentSkillDir", ""),
        "skill_path": config.get("agentSkillPath", ""),
        "cwd": str(Path.cwd()),
    }


def _proxy_env_from_config(config: dict[str, Any]) -> dict[str, str]:
    proxy_env: dict[str, str] = {}
    shared_proxy = str(config.get("agentProxy") or "").strip()
    http_proxy = str(config.get("agentHttpProxy") or shared_proxy).strip()
    https_proxy = str(config.get("agentHttpsProxy") or shared_proxy).strip()
    all_proxy = str(config.get("agentAllProxy") or "").strip()
    no_proxy = str(config.get("agentNoProxy") or "").strip()
    if http_proxy:
        proxy_env["http_proxy"] = http_proxy
    if https_proxy:
        proxy_env["https_proxy"] = https_proxy
    if all_proxy:
        proxy_env["all_proxy"] = all_proxy
    if no_proxy:
        proxy_env["no_proxy"] = no_proxy
    return proxy_env
