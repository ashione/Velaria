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
    api_key = str(config.get("api_key", ""))
    model = str(config.get("model", ""))
    reasoning_effort = str(config.get("reasoning_effort") or "none")
    network_access = _bool_config(config, "network_access", True)

    if runtime_type == "claude" or (runtime_type == "auto" and _has_claude_sdk()):
        from .claude_runtime import ClaudeAgentRuntime
        return ClaudeAgentRuntime(
            api_key=api_key,
            model=model or "claude-sonnet-4-20250514",
            runtime_path=_runtime_path_for(config, "claude"),
            runtime_workspace=str(config.get("runtime_workspace") or ""),
            reuse_local_config=bool(config.get("reuse_local_config", True)),
            runtime_config_path=str(config.get("runtime_config_path") or ""),
            skill_dir=str(config.get("skill_dir") or ""),
            skill_path=str(config.get("skill_path") or ""),
            cwd=str(config.get("cwd") or ""),
        )

    if runtime_type == "codex" or (runtime_type == "auto" and _has_codex_sdk()):
        from .codex_runtime import CodexRuntime
        return CodexRuntime(
            model=model or "gpt-5.4-mini",
            reasoning_effort=reasoning_effort,
            network_access=network_access,
            runtime_path=_runtime_path_for(config, "codex"),
            runtime_workspace=str(config.get("runtime_workspace") or ""),
            reuse_local_config=bool(config.get("reuse_local_config", True)),
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

def load_ai_config() -> dict[str, Any]:
    config_path = Path.home() / ".velaria" / "config.json"
    if not config_path.exists():
        return {}
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {
        "provider": _config_value(config, "agentProvider", "aiProvider", default="openai"),
        "api_key": _config_value(config, "agentApiKey", "aiApiKey", default=""),
        "base_url": _config_value(config, "agentBaseUrl", "aiBaseUrl", default="https://api.openai.com/v1"),
        "model": _config_value(config, "agentModel", "aiModel", default=""),
        "reasoning_effort": _config_value(config, "agentReasoningEffort", "aiReasoningEffort", default="none"),
        "runtime": _config_value(config, "agentRuntime", "aiRuntime", default="codex"),
        "runtime_path": _config_value(config, "agentRuntimePath", "aiRuntimePath", default=""),
        "claude_runtime_path": _config_value(
            config,
            "agentClaudeRuntimePath",
            "aiClaudeRuntimePath",
            default="",
        ),
        "codex_runtime_path": _config_value(
            config,
            "agentCodexRuntimePath",
            "aiCodexRuntimePath",
            default="",
        ),
        "runtime_workspace": _config_value(
            config,
            "agentRuntimeWorkspace",
            "aiRuntimeWorkspace",
            default="",
        ),
        "reuse_local_config": _bool_config_value(
            config,
            "agentReuseLocalConfig",
            "aiReuseLocalConfig",
            default=True,
        ),
        "runtime_config_path": _config_value(
            config,
            "agentRuntimeConfigPath",
            "aiRuntimeConfigPath",
            default="",
        ),
        "network_access": _config_value(
            config,
            "agentCodexNetworkAccess",
            "agentNetworkAccess",
            "aiCodexNetworkAccess",
            "aiNetworkAccess",
            default=True,
        ),
        "proxy_env": _proxy_env_from_config(config),
        "skill_dir": _config_value(config, "agentSkillDir", "aiSkillDir", default=""),
        "skill_path": _config_value(config, "agentSkillPath", "aiSkillPath", default=""),
        "cwd": str(Path.cwd()),
    }


def _config_value(config: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in config:
            return config[key]
    return default


def _bool_config_value(config: dict[str, Any], *keys: str, default: bool) -> bool:
    return _bool_value(_config_value(config, *keys, default=default), default)


def _proxy_env_from_config(config: dict[str, Any]) -> dict[str, str]:
    proxy_env: dict[str, str] = {}
    shared_proxy = str(_config_value(config, "agentProxy", "aiProxy", default="") or "").strip()
    http_proxy = str(
        _config_value(config, "agentHttpProxy", "aiHttpProxy", default=shared_proxy) or ""
    ).strip()
    https_proxy = str(
        _config_value(config, "agentHttpsProxy", "aiHttpsProxy", default=shared_proxy) or ""
    ).strip()
    all_proxy = str(_config_value(config, "agentAllProxy", "aiAllProxy", default="") or "").strip()
    no_proxy = str(_config_value(config, "agentNoProxy", "aiNoProxy", default="") or "").strip()
    if http_proxy:
        proxy_env["http_proxy"] = http_proxy
    if https_proxy:
        proxy_env["https_proxy"] = https_proxy
    if all_proxy:
        proxy_env["all_proxy"] = all_proxy
    if no_proxy:
        proxy_env["no_proxy"] = no_proxy
    return proxy_env


def _bool_value(value: Any, default: bool) -> bool:
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
