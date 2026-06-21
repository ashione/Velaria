from __future__ import annotations

from typing import Any, AsyncIterator

from velaria.ai_runtime.agent import AgentEvent

from .events import VelariaAgentEvent
from .state import AgentUiState


class InteractiveController:
    """Runtime-neutral Velaria Agent controller.

    This is the headless state machine behind both the Textual TUI and scripted
    agent modes. Runtime adapters may be Codex or Claude; UI code only sees
    VelariaAgentEvent instances and AgentUiState.
    """

    def __init__(self, runtime: Any):
        self.runtime = runtime
        self.state = AgentUiState()

    async def ensure_session(self, *, session_id: str | None = None, new_session: bool = False) -> VelariaAgentEvent:
        if session_id and not new_session:
            resumed = await self.runtime.resume_thread(session_id)
            if resumed:
                self.state.session_id = session_id
                self.refresh_status()
                return VelariaAgentEvent("session.resumed", session_id=session_id)
        if self.state.session_id and not new_session:
            return VelariaAgentEvent("session.current", session_id=self.state.session_id)
        created = await self.runtime.start_thread({})
        self.state.session_id = created
        self.refresh_status()
        return VelariaAgentEvent("session.started", session_id=created)

    async def send_turn(self, prompt: str) -> AsyncIterator[VelariaAgentEvent]:
        if not self.state.session_id:
            yield await self.ensure_session()
        self.state.turn_state = "running"
        async for event in self.runtime.send_message(self.state.session_id, prompt):
            velaria_event = self._normalize_event(event)
            self._update_state_from_event(velaria_event)
            yield velaria_event
        if self.state.turn_state == "running":
            self.state.turn_state = "done"

    async def list_sessions(self) -> list[dict[str, Any]]:
        return await self.runtime.list_threads()

    async def close_session(self, session_id: str | None = None) -> None:
        target = session_id or self.state.session_id
        if not target:
            return
        await self.runtime.close_thread(target)
        if target == self.state.session_id:
            self.state.session_id = ""

    def refresh_status(self) -> dict[str, Any]:
        status = self.runtime.status(self.state.session_id or None)
        self.state.runtime = str(status.get("runtime") or "")
        self.state.model = str(status.get("model") or "")
        tools = status.get("tools") or []
        self.state.tools = [str(tool) for tool in tools]
        return status

    def shutdown(self) -> None:
        shutdown = getattr(self.runtime, "shutdown", None)
        if callable(shutdown):
            shutdown()

    def _normalize_event(self, event: AgentEvent | Any) -> VelariaAgentEvent:
        event_type = str(getattr(event, "type", "") or "")
        content = str(getattr(event, "content", "") or "")
        session_id = str(getattr(event, "session_id", "") or self.state.session_id)
        data = getattr(event, "data", {}) or {}
        if not isinstance(data, dict):
            data = {"payload": data}
        return VelariaAgentEvent(event_type, content, session_id=session_id, data=data)

    def _update_state_from_event(self, event: VelariaAgentEvent) -> None:
        if event.type in {"done", "turn.completed"}:
            self.state.turn_state = "done"
            return
        if event.type in {"error", "turn.failed"}:
            self.state.turn_state = "failed"
            return
        if event.type in {"tool_call", "tool_result"}:
            tool_name = _tool_name(event.data)
            if tool_name:
                self.state.last_tool = tool_name
                self.state.last_function = tool_name
        if event.type == "tool_result":
            self._update_state_from_payload(event.data)

    def _update_state_from_payload(self, payload: dict[str, Any]) -> None:
        function_name = str(payload.get("function") or payload.get("tool_name") or "")
        if function_name:
            self.state.last_function = function_name
            self.state.last_tool = function_name
        dataset = payload.get("dataset")
        if isinstance(dataset, dict):
            self.state.dataset_name = str(dataset.get("name") or self.state.dataset_name)
            self.state.source_path = str(dataset.get("path") or dataset.get("uri") or self.state.source_path)
        if isinstance(payload.get("source_path"), str):
            self.state.source_path = str(payload["source_path"])
        if isinstance(payload.get("table_name"), str):
            self.state.table_name = str(payload["table_name"])
        schema = payload.get("schema")
        if isinstance(schema, list):
            self.state.schema = [str(value) for value in schema]
            self.state.result_schema = [str(value) for value in schema]
        if isinstance(payload.get("row_count"), int):
            self.state.row_count = int(payload["row_count"])
            self.state.result_row_count = int(payload["row_count"])
        if isinstance(payload.get("result_row_count"), int):
            self.state.result_row_count = int(payload["result_row_count"])
        if isinstance(payload.get("run_id"), str):
            self.state.last_run_id = payload["run_id"]
        if isinstance(payload.get("artifact_id"), str):
            self.state.last_artifact_id = payload["artifact_id"]
        artifacts = payload.get("artifacts")
        if isinstance(artifacts, list) and artifacts:
            first = artifacts[0]
            if isinstance(first, dict) and isinstance(first.get("artifact_id"), str):
                self.state.last_artifact_id = first["artifact_id"]


def _tool_name(data: dict[str, Any]) -> str:
    for key in ("function", "tool_name", "name", "tool"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    item = data.get("item")
    if isinstance(item, dict):
        value = item.get("name") or item.get("toolName") or item.get("tool")
        if isinstance(value, str):
            return value
    return ""
