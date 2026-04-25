"""Compatibility exports for Velaria agent tools."""
from __future__ import annotations

from typing import Any

from .functions import (
    execute_local_function,
    tool_definitions,
    velaria_agent_instructions,
)

VELARIA_AGENT_SYSTEM_PROMPT = velaria_agent_instructions()
VELARIA_TOOLS = tool_definitions()


def execute_tool(tool_name: str, args: dict, *, session: Any = None, dataset_context: dict | None = None) -> dict:
    """Execute a Velaria local function.

    The ``session`` and ``dataset_context`` parameters are accepted for older
    Claude custom-tool call sites. The shared function registry is now the
    source of truth.
    """

    merged = dict(args or {})
    if dataset_context:
        for key in ("source_path", "table_name", "schema"):
            if key in dataset_context and key not in merged:
                merged[key] = dataset_context[key]
    return execute_local_function(tool_name, merged)
