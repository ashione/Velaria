"""Stdio MCP server exposing Velaria local functions."""
from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

from .functions import (
    execute_local_function,
    load_velaria_skill_text,
    tool_definitions,
)


PROTOCOL_VERSION = "2024-11-05"
SKILL_URI = "velaria://skills/velaria-python-local"


def _response(msg_id: Any, result: dict[str, Any] | None = None, error: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {"jsonrpc": "2.0", "id": msg_id}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result or {}
    return payload


def _tool_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(result, ensure_ascii=False),
            }
        ],
        "isError": not bool(result.get("ok", False)),
    }


def _handle_request(message: dict[str, Any]) -> dict[str, Any] | None:
    msg_id = message.get("id")
    method = message.get("method")
    params = message.get("params") or {}
    if msg_id is None:
        return None

    if method == "initialize":
        return _response(
            msg_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "serverInfo": {"name": "velaria", "version": "0.1.0"},
                "capabilities": {
                    "tools": {},
                    "resources": {},
                },
            },
        )
    if method == "tools/list":
        return _response(
            msg_id,
            {
                "tools": [
                    {
                        "name": t["name"],
                        "description": t["description"],
                        "inputSchema": t["input_schema"],
                        "annotations": t.get("annotations") or {},
                    }
                    for t in tool_definitions()
                ]
            },
        )
    if method == "tools/call":
        name = str(params.get("name") or "")
        arguments = params.get("arguments") or {}
        return _response(msg_id, _tool_result(execute_local_function(name, arguments)))
    if method == "resources/list":
        return _response(
            msg_id,
            {
                "resources": [
                    {
                        "uri": SKILL_URI,
                        "name": "velaria-python-local",
                        "description": "Default Velaria local Python usage skill.",
                        "mimeType": "text/markdown",
                    }
                ]
            },
        )
    if method == "resources/read":
        if params.get("uri") != SKILL_URI:
            return _response(
                msg_id,
                error={"code": -32602, "message": f"unknown resource: {params.get('uri')}"},
            )
        return _response(
            msg_id,
            {
                "contents": [
                    {
                        "uri": SKILL_URI,
                        "mimeType": "text/markdown",
                        "text": load_velaria_skill_text(),
                    }
                ]
            },
        )
    return _response(msg_id, error={"code": -32601, "message": f"method not found: {method}"})


def _create_server():
    from mcp.server import Server
    from mcp.server.lowlevel.server import NotificationOptions
    from mcp.server.models import InitializationOptions
    import mcp.types as types

    server = Server(
        "velaria",
        version="0.1.0",
        instructions=(
            "Velaria local function bridge. Use these tools for Velaria dataset "
            "download/localization, import, schema inspection, SQL processing, "
            "runs, and artifacts. HTTP(S) dataset URLs should be handled through "
            "Velaria tools before generic shell or Python download code."
        ),
    )

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=t["name"],
                description=t["description"],
                inputSchema=t["input_schema"],
                annotations=types.ToolAnnotations(**(t.get("annotations") or {})),
            )
            for t in tool_definitions()
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any] | None) -> types.CallToolResult:
        result = execute_local_function(name, arguments)
        text = json.dumps(result, ensure_ascii=False)
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=text)],
            structuredContent=result,
            isError=not bool(result.get("ok", False)),
        )

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        return [
            types.Resource(
                uri=SKILL_URI,
                name="velaria-python-local",
                description="Default Velaria local Python usage skill.",
                mimeType="text/markdown",
            )
        ]

    @server.read_resource()
    async def read_resource(uri) -> str:
        if str(uri) != SKILL_URI:
            raise ValueError(f"unknown resource: {uri}")
        return load_velaria_skill_text()

    return server, InitializationOptions(
        server_name="velaria",
        server_version="0.1.0",
        capabilities=server.get_capabilities(
            notification_options=NotificationOptions(),
            experimental_capabilities={},
        ),
        instructions=server.instructions,
    )


async def _run_mcp_stdio() -> None:
    from mcp.server.stdio import stdio_server

    server, initialization_options = _create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            initialization_options,
            raise_exceptions=False,
        )


def _run_line_json_stdio() -> int:
    """Compatibility harness for old tests/debugging, not the Codex transport."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
            response = _handle_request(message)
        except Exception as exc:
            response = _response(
                None,
                error={"code": -32603, "message": str(exc)},
            )
        if response is not None:
            print(json.dumps(response, ensure_ascii=False), flush=True)
    return 0


def main() -> int:
    if "--line-json" in sys.argv:
        return _run_line_json_stdio()
    try:
        asyncio.run(_run_mcp_stdio())
    except ModuleNotFoundError as exc:
        if exc.name != "mcp":
            raise
        print(
            "Velaria MCP bridge requires the 'mcp' package. "
            "Run `uv sync --project python` before starting velaria_cli.py -i.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
