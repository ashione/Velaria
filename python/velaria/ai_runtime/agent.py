"""Agent runtime adapter types used by the Velaria interactive CLI."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass(frozen=True)
class AgentEvent:
    """A runtime-neutral event emitted while an agent turn is running."""

    type: str
    content: str = ""
    session_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)


class AgentRuntimeAdapter(Protocol):
    """Minimal wrapper over an existing agent runtime such as Codex or Claude Code."""

    async def start_thread(self, dataset_context: dict[str, Any] | None = None) -> str: ...

    async def resume_thread(self, session_id: str) -> bool: ...

    async def send_message(self, session_id: str, prompt: str) -> AsyncIterator[AgentEvent]: ...

    async def prewarm(self) -> None: ...

    async def list_threads(self) -> list[dict[str, Any]]: ...

    async def close_thread(self, session_id: str) -> None: ...

    def status(self, session_id: str | None = None) -> dict[str, Any]: ...

    def shutdown(self) -> None: ...


def normalize_runtime_event(
    raw_type: str,
    content: str = "",
    *,
    session_id: str = "",
    data: dict[str, Any] | None = None,
) -> AgentEvent:
    """Map runtime-specific event names to Velaria's small interactive event set."""

    event_type = raw_type
    if raw_type in {"codex", "assistant", "AssistantMessage", "agentMessage"}:
        event_type = "assistant_text"
    elif raw_type in {"tool", "mcpToolCall", "ToolUseBlock"}:
        event_type = "tool_call"
    elif raw_type in {"tool_result", "ToolResultBlock"}:
        event_type = "tool_result"
    elif raw_type in {"exec", "commandExecution"}:
        event_type = "command"
    elif raw_type in {"thinking", "reasoning"}:
        event_type = "thinking"
    elif raw_type in {"error", "failed"}:
        event_type = "error"
    return AgentEvent(
        type=event_type,
        content=content,
        session_id=session_id,
        data=data or {},
    )
