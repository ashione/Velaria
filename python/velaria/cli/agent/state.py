from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AgentUiState:
    session_id: str = ""
    runtime: str = ""
    model: str = ""
    tools: list[str] = field(default_factory=list)
    turn_state: str = "idle"
    turn_activity: str = "agent"
    dataset_name: str = ""
    source_path: str = ""
    table_name: str = "input_table"
    schema: list[str] = field(default_factory=list)
    row_count: int | None = None
    result_schema: list[str] = field(default_factory=list)
    result_row_count: int | None = None
    last_run_id: str = ""
    last_artifact_id: str = ""
    last_tool: str = ""
    last_function: str = ""
