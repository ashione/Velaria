from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Any

from velaria.cli._common import _json_dumps

from .controller import InteractiveController
from .events import VelariaAgentEvent


@dataclass
class AgentCommandArgs:
    runtime: str = ""
    model: str = ""
    prompt: str | None = None
    print_prompt: str | None = None
    stream_json_prompt: str | None = None
    session: str = ""
    new: bool = False


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "agent",
        help="Start the Velaria Agent TUI or run a headless agent turn.",
    )
    _add_agent_arguments(parser)
    parser.set_defaults(_handler=_run_agent_namespace)


def wants_agent_alias(argv: list[str]) -> bool:
    return bool(argv) and argv[0] in {"-i", "--interactive"}


def run_default_agent() -> int:
    return run_agent_args(AgentCommandArgs())


def run_agent_argv(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="velaria-cli agent", add_help=True)
    _add_agent_arguments(parser, include_interactive_alias=True)
    namespace = parser.parse_args(argv)
    return run_agent_args(_args_from_namespace(namespace))


def run_agent_args(args: AgentCommandArgs) -> int:
    if args.print_prompt is not None:
        return _run_agent_print(args)
    if args.stream_json_prompt is not None:
        return _run_agent_stream_json(args)
    if not _stdio_is_tty():
        print(
            _json_dumps(
                {
                    "ok": False,
                    "error": "Velaria Agent TUI requires an interactive terminal.",
                    "error_type": "non_tty_agent_entry",
                    "message": "Velaria Agent TUI requires an interactive terminal.",
                    "hint": "Use `velaria-cli agent --print \"...\"`, `velaria-cli agent --stream-json \"...\"`, or a non-interactive Velaria subcommand.",
                }
            )
        )
        return 1
    return run_agent_tui(args)


def run_agent_tui(args: AgentCommandArgs) -> int:
    from .tui_app import run_tui

    return run_tui(args)


def _run_agent_namespace(args: argparse.Namespace) -> int:
    return run_agent_args(_args_from_namespace(args))


def _args_from_namespace(namespace: argparse.Namespace) -> AgentCommandArgs:
    return AgentCommandArgs(
        runtime=str(getattr(namespace, "runtime", "") or ""),
        model=str(getattr(namespace, "model", "") or ""),
        prompt=getattr(namespace, "prompt", None),
        print_prompt=getattr(namespace, "print_prompt", None),
        stream_json_prompt=getattr(namespace, "stream_json_prompt", None),
        session=str(getattr(namespace, "session", "") or ""),
        new=bool(getattr(namespace, "new", False)),
    )


def _add_agent_arguments(parser: argparse.ArgumentParser, *, include_interactive_alias: bool = False) -> None:
    if include_interactive_alias:
        parser.add_argument("-i", "--interactive", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--runtime", choices=["codex", "claude"], default="", help="Override configured Agent runtime.")
    parser.add_argument("--model", default="", help="Override configured Agent model for this session.")
    parser.add_argument("--new", action="store_true", help="Start a fresh Agent session.")
    parser.add_argument("--session", help="Resume a specific Agent session.")
    parser.add_argument(
        "--print",
        dest="print_prompt",
        metavar="PROMPT",
        help="Run one Velaria Agent turn and print only the final assistant text.",
    )
    parser.add_argument(
        "--stream-json",
        dest="stream_json_prompt",
        metavar="PROMPT",
        help="Run one Velaria Agent turn and stream JSONL events.",
    )
    parser.add_argument("prompt", nargs="?", help="Initial prompt for the Velaria Agent TUI.")


def _runtime_from_args(args: AgentCommandArgs):
    from velaria.ai_runtime import create_runtime, load_ai_config

    config = load_ai_config()
    if args.runtime:
        config["runtime"] = args.runtime
        config["configured_runtime"] = args.runtime
    if args.model:
        config["model"] = args.model
    return create_runtime(config)


def _controller_from_args(args: AgentCommandArgs) -> InteractiveController:
    return InteractiveController(_runtime_from_args(args))


def _run_agent_print(args: AgentCommandArgs) -> int:
    async def _run() -> str:
        controller = _controller_from_args(args)
        try:
            await controller.ensure_session(session_id=args.session or None, new_session=args.new)
            chunks: list[str] = []
            async for event in controller.send_turn(args.print_prompt or ""):
                if event.type == "assistant_text" and event.content:
                    chunks.append(event.content)
            return "".join(chunks)
        finally:
            controller.shutdown()

    print(asyncio.run(_run()))
    return 0


def _run_agent_stream_json(args: AgentCommandArgs) -> int:
    async def _run() -> None:
        controller = _controller_from_args(args)
        try:
            session_event = await controller.ensure_session(session_id=args.session or None, new_session=args.new)
            _emit_event_json(session_event)
            async for event in controller.send_turn(args.stream_json_prompt or ""):
                if event.type == "session.current":
                    continue
                _emit_event_json(event)
        finally:
            controller.shutdown()

    asyncio.run(_run())
    return 0


def _emit_event_json(event: VelariaAgentEvent) -> None:
    print(json.dumps(event.to_json(), ensure_ascii=False), flush=True)


def _stdio_is_tty() -> bool:
    return (
        hasattr(sys.stdin, "isatty")
        and hasattr(sys.stdout, "isatty")
        and sys.stdin.isatty()
        and sys.stdout.isatty()
    )
