from __future__ import annotations

import argparse
import asyncio
import contextlib
import queue
import io
import json
import os
import shlex
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from velaria.cli._common import _json_dumps
from velaria.ai_runtime.agent import AgentEvent

_current_session_id: str | None = None
_runtime: Any | None = None
_runtime_override: str | None = None
_prompt_session: Any | None = None
_prompt_active: bool = False
_prewarm_thread: threading.Thread | None = None
_session_start_thread: threading.Thread | None = None
_session_start_error: str = ""
_trace_start = time.perf_counter()
_turn_status_thread: threading.Thread | None = None
_turn_status_stop: threading.Event | None = None

_SPINNER_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
_SPINNER_INTERVAL_SECONDS = 0.12
_turn_status_frame = _SPINNER_FRAMES[0]


@dataclass
class VelariaInteractiveState:
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
    turn_state: str = "idle"
    turn_activity: str = "agent"
    turn_started_at: float | None = None
    runtime_warmup: str = "idle"


_state = VelariaInteractiveState()

_cached_status: dict[str, Any] = {}
_cached_status_at: float = 0.0
_STATUS_CACHE_TTL = 2.0

_SLASH_COMMANDS = [
    "/help",
    "/status",
    "/dataset",
    "/runs",
    "/artifacts",
    "/shortcuts",
    "/keys",
    "/new",
    "/sessions",
    "/resume",
    "/close",
    "/exit",
]


_INTERACTIVE_FLAGS = {"-i", "--interactive", "--new", "--runtime", "--session"}


def _wants_interactive(argv: list[str]) -> bool:
    if not argv:
        return True
    return bool(_INTERACTIVE_FLAGS.intersection(argv))


def _run_interactive_loop(argv: list[str] | None = None) -> int:
    global _state, _current_session_id, _session_start_thread, _session_start_error, _runtime_override
    _state = VelariaInteractiveState()
    _current_session_id = None
    _session_start_thread = None
    _session_start_error = ""
    args = _parse_interactive_args(argv or [])
    _runtime_override = getattr(args, "runtime", None)
    from velaria.cli import main

    _print_banner()
    if args.session:
        with _trace_span("interactive.ensure_session"):
            _ensure_agent_session(new_session=True, session_id=args.session)
    elif args.new:
        with _trace_span("interactive.ensure_new_session"):
            _ensure_agent_session(new_session=True)
    else:
        with _trace_span("interactive.start_session_background"):
            _start_agent_session_async()
    with _trace_span("interactive.print_startup_status"):
        _print_startup_status()
    with _trace_span("interactive.start_prewarm"):
        _start_runtime_prewarm()
    global _prompt_session
    _prompt_session = _build_prompt_session()
    while True:
        global _prompt_active
        _prompt_active = True
        try:
            line = _read_prompt(_prompt_session)
        except EOFError:
            print()
            _shutdown_runtime()
            _prompt_session = None
            return 0
        except KeyboardInterrupt:
            print()
            continue
        finally:
            _prompt_active = False

        command = line.strip()
        if not command:
            continue
        if command in {"/exit", "/quit", "exit", "quit"}:
            _shutdown_runtime()
            return 0
        if command in {"/help", "help"}:
            _print_help()
            continue
        if command.startswith(":help "):
            help_args = shlex.split(command[len(":help "):].strip())
            main([*help_args, "--help"])
            continue
        if command == ":help":
            main(["--help"])
            continue
        if command.startswith(":"):
            _run_cli_escape(shlex.split(command[1:].strip()))
            continue
        if command.startswith("/"):
            _handle_control_command(command)
            continue
        if _try_local_fast_response(command):
            continue
        try:
            _send_agent_message(command)
        except KeyboardInterrupt:
            _stop_turn_status()
            _state.turn_state = "cancelled"
            _print_note("cancelled", "turn interrupted")


def _parse_interactive_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-i", "--interactive", action="store_true")
    parser.add_argument("--new", action="store_true")
    parser.add_argument("--session")
    parser.add_argument("--runtime", choices=["codex", "claude"],
                        help="Select AI runtime (overrides agentRuntime in config.json).")
    parsed, _unknown = parser.parse_known_args(argv)
    return parsed


def _print_help() -> None:
    _print_section(
        "Agent",
        [
            ("<prompt>", "send a message to the active agent thread"),
            ("/status", "show runtime and Velaria state"),
            ("/shortcuts", "show keyboard shortcuts"),
            ("/keys", "alias for /shortcuts"),
        ],
    )
    _print_section(
        "Velaria",
        [
            ("/dataset", "show current dataset, schema, and last result"),
            ("/runs", "show recent workspace runs"),
            ("/artifacts", "show recent/current artifacts"),
            (":<command>", "run a non-interactive Velaria CLI command"),
        ],
    )
    _print_section(
        "Session",
        [
            ("/new", "start a new agent thread"),
            ("/sessions", "list active saved threads"),
            ("/resume <id>", "resume a saved thread"),
            ("/close [id]", "close a thread"),
            ("/exit", "exit interactive mode"),
        ],
    )


def _get_runtime():
    global _runtime
    if _runtime is None:
        from velaria.ai_runtime import create_runtime, load_ai_config

        with _trace_span("interactive.load_config_create_runtime"):
            config = load_ai_config()
            if _runtime_override:
                config["runtime"] = _runtime_override
            _runtime = create_runtime(config)
    return _runtime


def _shutdown_runtime() -> None:
    global _runtime, _prewarm_thread, _session_start_thread, _session_start_error
    _stop_turn_status()
    _wait_agent_session_start(timeout=0.2, quiet=True)
    if _current_session_id and _runtime is not None:
        try:
            asyncio.run(_runtime.close_thread(_current_session_id))
        except Exception:
            pass
    if _prewarm_thread is not None and _prewarm_thread.is_alive():
        with _trace_span("interactive.shutdown_join_prewarm"):
            _prewarm_thread.join(timeout=0.2)
    if _runtime is not None and hasattr(_runtime, "shutdown"):
        try:
            with _trace_span("interactive.runtime_shutdown"):
                _runtime.shutdown()
        except Exception:
            pass
    _runtime = None
    _prewarm_thread = None
    _session_start_thread = None
    _session_start_error = ""


def _start_runtime_prewarm() -> None:
    global _prewarm_thread
    runtime = _runtime
    if runtime is None or not hasattr(runtime, "prewarm"):
        return
    if _session_start_thread is not None and _session_start_thread.is_alive():
        _state.runtime_warmup = "warming"
        return
    if _prewarm_thread is not None and _prewarm_thread.is_alive():
        return
    _state.runtime_warmup = "warming"
    _print_note("warming", "runtime prewarm started in background")

    def _target() -> None:
        try:
            with _trace_span("interactive.prewarm_thread"):
                asyncio.run(runtime.prewarm())
            _state.runtime_warmup = "warm"
        except Exception as exc:
            _state.runtime_warmup = f"failed: {exc}"

    _prewarm_thread = threading.Thread(
        target=_target,
        name="velaria-runtime-prewarm",
        daemon=True,
    )
    _prewarm_thread.start()


def _wait_runtime_prewarm(timeout: float = 5.0) -> None:
    thread = _prewarm_thread
    if thread is None or not thread.is_alive():
        return
    _state.turn_activity = "runtime warmup"
    _print_note("warming", "waiting for runtime prewarm")
    with _trace_span(f"interactive.wait_prewarm timeout={timeout}"):
        thread.join(timeout=timeout)
    if thread.is_alive():
        _trace("interactive.wait_prewarm timeout_reached")


def _ensure_agent_session(*, new_session: bool = False, session_id: str | None = None) -> None:
    global _current_session_id
    try:
        with _trace_span("interactive.get_runtime"):
            runtime = _get_runtime()
        if session_id:
            with _trace_span("interactive.resume_explicit_session"):
                resumed = _try_resume(runtime, session_id)
            if resumed:
                _current_session_id = session_id
                _print_note("resumed", session_id)
                return
            _print_note("not found", session_id, level="warn")
        if not new_session:
            sessions = asyncio.run(runtime.list_threads())
            for session in sessions:
                candidate = str(session["session_id"])
                if _try_resume(runtime, candidate):
                    _current_session_id = candidate
                    _print_note("resumed", _current_session_id)
                    return
        with _trace_span("interactive.start_thread"):
            _current_session_id = asyncio.run(runtime.start_thread({}))
        _print_note("started", _current_session_id)
    except Exception as exc:
        _current_session_id = None
        _print_note("runtime unavailable", str(exc), level="error")


def _start_agent_session_async(dataset_context: dict[str, Any] | None = None) -> None:
    global _session_start_thread, _session_start_error
    if _current_session_id:
        return
    if _session_start_thread is not None and _session_start_thread.is_alive():
        return
    try:
        with _trace_span("interactive.get_runtime_background_start"):
            runtime = _get_runtime()
    except Exception as exc:
        _session_start_error = str(exc)
        _print_note("runtime unavailable", str(exc), level="error")
        return
    _session_start_error = ""
    _state.runtime_warmup = "warming"
    _print_note("starting", "agent thread is starting in background")

    def _target() -> None:
        global _current_session_id, _session_start_error
        try:
            with _trace_span("interactive.background_start_thread"):
                session_id = asyncio.run(runtime.start_thread(dataset_context or {}))
            _current_session_id = session_id
            _state.runtime_warmup = "warm"
            _print_note("started", session_id)
        except Exception as exc:
            _current_session_id = None
            _session_start_error = str(exc)
            _state.runtime_warmup = f"failed: {exc}"
            _print_note("runtime unavailable", str(exc), level="error")

    _session_start_thread = threading.Thread(
        target=_target,
        name="velaria-agent-session-start",
        daemon=True,
    )
    _session_start_thread.start()


def _wait_agent_session_start(timeout: float | None = None, *, quiet: bool = False) -> bool:
    thread = _session_start_thread
    if thread is None:
        return bool(_current_session_id)
    if thread.is_alive():
        if not quiet:
            _print_note("starting", "waiting for agent thread")
        with _trace_span(f"interactive.wait_session_start timeout={timeout}"):
            thread.join(timeout=timeout)
    if thread.is_alive():
        return False
    if _session_start_error:
        if not quiet:
            _print_note("runtime unavailable", _session_start_error, level="error")
        return False
    return bool(_current_session_id)


def _try_resume(runtime: Any, session_id: str) -> bool:
    try:
        return bool(asyncio.run(runtime.resume_thread(session_id)))
    except Exception as exc:
        try:
            asyncio.run(runtime.close_thread(session_id))
        except Exception:
            pass
        _print_note("skipped stale session", f"{session_id}: {exc}", level="warn")
        return False


def _handle_control_command(command: str) -> None:
    global _current_session_id
    parts = command.split(None, 1)
    cmd = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/status":
        runtime = _safe_runtime()
        if runtime is None:
            return
        status = runtime.status(_current_session_id or "__velaria_pending_session__")
        if not _current_session_id:
            status["session"] = None
        _print_status(status)
        _print_velaria_state()
        return
    if cmd == "/dataset":
        _print_dataset_state()
        return
    if cmd == "/runs":
        _print_runs()
        return
    if cmd == "/artifacts":
        _print_artifacts()
        return
    if cmd in {"/shortcuts", "/keys"}:
        _print_shortcuts()
        return
    if cmd == "/new":
        runtime = _safe_runtime()
        if runtime is None:
            return
        _current_session_id = None
        _start_agent_session_async({})
        return
    if cmd == "/sessions":
        runtime = _safe_runtime()
        if runtime is None:
            return
        sessions = asyncio.run(runtime.list_threads())
        if not sessions:
            _print_note("sessions", "no active sessions")
            return
        _print_sessions(sessions, current_session_id=_current_session_id)
        return
    if cmd == "/resume":
        if not rest:
            _print_note("usage", "/resume <session_id>", level="warn")
            return
        runtime = _safe_runtime()
        if runtime is None:
            return
        if asyncio.run(runtime.resume_thread(rest)):
            _current_session_id = rest
            _print_note("resumed", _current_session_id)
            _print_startup_status()
        else:
            _print_note("not found", rest, level="warn")
        return
    if cmd == "/close":
        session_id = rest or _current_session_id
        if not session_id:
            _print_note("session", "no active session", level="warn")
            return
        runtime = _safe_runtime()
        if runtime is None:
            return
        asyncio.run(runtime.close_thread(session_id))
        _print_note("closed", session_id)
        if session_id == _current_session_id:
            _current_session_id = None
        return
    _print_note("unknown command", cmd, level="warn")
    print(_muted("Run /help for available commands."))


def _try_local_fast_response(command: str) -> bool:
    normalized = command.strip().lower()
    if normalized in {"hello", "hi", "hey", "你好", "您好"}:
        _state.turn_state = "done"
        print("Hello, I am Velaria Agent.")
        return True
    return False


def _get_cached_status() -> dict[str, Any]:
    global _cached_status, _cached_status_at
    now = time.time()
    if now - _cached_status_at < _STATUS_CACHE_TTL and _cached_status:
        return _cached_status
    try:
        _cached_status = _runtime.status(_current_session_id or "__velaria_pending_session__") if _runtime is not None else {}
    except Exception:
        _cached_status = {}
    _cached_status_at = now
    return _cached_status


def _safe_runtime():
    try:
        return _get_runtime()
    except Exception as exc:
        _print_note("runtime unavailable", str(exc), level="error")
        return None


def _send_agent_message(prompt: str) -> None:
    global _current_session_id
    if not _current_session_id:
        _start_agent_session_async()
        _wait_agent_session_start(timeout=None)
    if not _current_session_id:
        _print_note("session", "no active session", level="warn")
        return
    runtime = _safe_runtime()
    if runtime is None:
        return

    _state.turn_state = "running"
    _state.turn_activity = "agent"
    _state.turn_started_at = time.time()
    _wait_runtime_prewarm()
    print(_muted("─" * _terminal_width()))

    show_spinner = _should_show_turn_status()
    if show_spinner:
        _start_spinner_thread()

    event_queue: queue.Queue[Any | None] = queue.Queue()

    async def _produce() -> None:
        global _current_session_id
        try:
            async for event in runtime.send_message(_current_session_id, prompt):
                event_queue.put(event)
        except Exception as exc:
            event_queue.put(AgentEvent("error", str(exc), session_id=_current_session_id, data={"runtime_failure": True}))
        event_queue.put(None)

    def _run_producer() -> None:
        try:
            asyncio.run(_produce())
        except Exception as exc:
            event_queue.put(AgentEvent("error", str(exc), session_id=_current_session_id, data={"runtime_failure": True}))
            event_queue.put(None)

    producer = threading.Thread(target=_run_producer, name="velaria-agent-stream", daemon=True)
    producer.start()

    failed = False
    saw_done = False
    runtime_failed = False
    try:
        while True:
            # Poll with timeout so we can animate the spinner between events
            try:
                event = event_queue.get(timeout=_SPINNER_INTERVAL_SECONDS)
            except queue.Empty:
                if show_spinner:
                    _write_status_bar()
                continue
            if event is None:
                break
            event_type = getattr(event, "type", "")
            data = getattr(event, "data", {}) or {}
            if event_type == "error":
                failed = True
                if data.get("runtime_failure"):
                    runtime_failed = True
            if event_type == "done":
                saw_done = True
            _trace(f"interactive.event type={event_type}")
            _clear_status_bar()
            _render_event(event)
            if show_spinner and _state.turn_state == "running":
                _write_status_bar()
    except KeyboardInterrupt:
        _clear_status_bar()
        _stop_spinner_thread()
        _state.turn_state = "cancelled"
        _print_note("cancelled", "turn interrupted")
        while not event_queue.empty():
            try:
                event_queue.get_nowait()
            except queue.Empty:
                break
        return

    _clear_status_bar()
    _stop_spinner_thread()
    _state.turn_state = "failed" if failed else "done"
    if failed or not saw_done:
        _print_note(_state.turn_state, _elapsed_turn())
    print(_muted("─" * _terminal_width()))
    if runtime_failed:
        _current_session_id = None


def _start_spinner_thread() -> None:
    global _turn_status_thread, _turn_status_stop, _turn_status_frame
    _stop_spinner_thread()
    _turn_status_stop = threading.Event()
    _turn_status_frame = _SPINNER_FRAMES[0]

    def _target() -> None:
        index = 0
        while _turn_status_stop is not None and not _turn_status_stop.wait(_SPINNER_INTERVAL_SECONDS):
            _turn_status_frame = _SPINNER_FRAMES[index % len(_SPINNER_FRAMES)]
            index += 1

    _turn_status_thread = threading.Thread(
        target=_target,
        name="velaria-turn-status",
        daemon=True,
    )
    _turn_status_thread.start()


def _stop_spinner_thread() -> None:
    global _turn_status_thread, _turn_status_stop
    stop = _turn_status_stop
    thread = _turn_status_thread
    if stop is not None:
        stop.set()
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=0.4)
    _turn_status_thread = None
    _turn_status_stop = None


def _should_show_turn_status() -> bool:
    """Show a live status bar while the agent is running."""
    if os.environ.get("VELARIA_INTERACTIVE_NO_SPINNER"):
        return False
    return (
        hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
    )


def _should_print_turn_fallback() -> bool:
    if os.environ.get("VELARIA_INTERACTIVE_NO_SPINNER"):
        return False
    if _should_use_prompt_toolkit():
        return False
    return not (
        hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
    )


def _status_bar_text() -> str:
    frame = _turn_status_frame
    runtime = "-"
    model = "-"
    tool_count = 0
    try:
        status = _get_cached_status()
        runtime = str(status.get("runtime") or "-")
        model = str(status.get("model") or "-")
        tool_count = len(status.get("tools") or [])
    except Exception:
        pass
    session = _short_id(_current_session_id) or "-"
    dataset = _state.dataset_name or pathlib_basename(_state.source_path) or "no dataset"
    run = _short_id(_state.last_run_id) if _state.last_run_id else "no run"
    elapsed = _elapsed_turn()
    activity = _state.turn_activity or "agent"
    state_text = f"{_state.runtime_warmup}/{_state.turn_state}"
    return (
        f"────── {frame} running {elapsed} | {runtime} {model} | "
        f"session {session} | dataset {dataset} | run {run} | "
        f"tools {tool_count} | {activity} | {state_text} "
    )


def _write_status_bar() -> None:
    """Write the status bar at the current cursor position (last line). Main thread only."""
    if not _should_show_turn_status():
        return
    text = _compact_line(_status_bar_text(), limit=_terminal_width())
    sys.stdout.write("\r" + _style(text, "event") + "\x1b[K")
    sys.stdout.flush()


def _clear_status_bar() -> None:
    """Clear the status bar line. Main thread only."""
    sys.stdout.write("\r\x1b[2K")
    sys.stdout.flush()


def _terminal_width() -> int:
    try:
        return max(40, os.get_terminal_size().columns)
    except OSError:
        return 100


@contextlib.contextmanager
def _trace_span(name: str):
    if not _trace_enabled():
        yield
        return
    start = time.perf_counter()
    _trace(f"{name} start")
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000.0
        _trace(f"{name} end {elapsed:.1f}ms")


def _trace(message: str) -> None:
    if not _trace_enabled():
        return
    elapsed = time.perf_counter() - _trace_start
    print(f"[velaria-trace {elapsed:8.3f}s] {message}", file=sys.stderr, flush=True)


def _trace_enabled() -> bool:
    return bool(os.environ.get("VELARIA_TRACE"))


def _render_event(event: Any) -> None:
    event_type = getattr(event, "type", "")
    content = getattr(event, "content", "")
    data = getattr(event, "data", {}) or {}
    _update_state_from_event(event_type, content, data)

    if event_type == "done":
        _state.turn_state = "done"
        _print_note("done", _elapsed_turn())
        return
    if event_type == "error":
        _print_note("error", content or _json_dumps(data), level="error")
        return

    if event_type == "assistant_text":
        if content:
            _print_assistant_text(content)
    elif event_type == "thinking":
        if content:
            _print_event("thinking", content)
    elif event_type == "tool_call":
        _print_event("tool", _format_tool_call(content, data))
    elif event_type == "tool_result":
        summary = _summarize_tool_result(content, data)
        if summary:
            _print_event("tool result", summary)
    elif event_type == "command":
        if content:
            _print_event("command", content)
    elif event_type == "file":
        if content:
            _print_event("file", content)
    elif not _looks_like_runtime_payload(content) and content:
        _print_assistant_text(content)


def _extract_tool_name(data: dict[str, Any]) -> str:
    value = data.get("tool_name") if isinstance(data, dict) else None
    if isinstance(value, str) and value:
        return value
    for key in ("name", "toolName", "tool", "function"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    payload = data.get("payload") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        value = _extract_tool_name(payload)
        if value:
            return value
    item = data.get("item") if isinstance(data, dict) else None
    if isinstance(item, dict):
        name = str(item.get("name") or item.get("toolName") or item.get("tool") or "")
        namespace = str(item.get("namespace") or item.get("serverName") or item.get("server") or "")
        if namespace == "velaria" and name:
            return name
        if namespace and name and not name.startswith(namespace):
            return f"{namespace}.{name}"
        if name or namespace:
            return name or namespace
    return ""


def _run_cli_escape(argv: list[str], *, summarize: bool = False) -> int:
    from velaria.cli import main

    if not argv:
        _print_note("usage", ":<velaria cli command>", level="warn")
        return 1
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = main(argv)
    out = stdout.getvalue()
    err = stderr.getvalue()
    if out:
        _update_state_from_text(out)
        if summarize:
            payload = _parse_json_payload(out)
            if payload and _print_cli_payload_summary(payload):
                pass
            else:
                print(out, end="" if out.endswith("\n") else "\n")
        else:
            print(out, end="" if out.endswith("\n") else "\n")
    if err:
        print(err, end="" if err.endswith("\n") else "\n", file=sys.stderr)
    _state.last_function = f":{' '.join(argv)}"
    return exit_code


def _build_prompt_session():
    if not _should_use_prompt_toolkit():
        return None
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.completion import WordCompleter
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.key_binding import KeyBindings
    except Exception:
        return None

    bindings = KeyBindings()

    @bindings.add("c-c")
    def _(event):
        buffer = event.app.current_buffer
        if getattr(buffer, "text", ""):
            buffer.reset()
            return
        event.app.exit(exception=KeyboardInterrupt)

    @bindings.add("c-d")
    def _(event):
        buffer = event.app.current_buffer
        if getattr(buffer, "text", ""):
            buffer.delete()
            return
        event.app.exit(exception=EOFError)

    @bindings.add("c-l")
    def _(event):
        event.app.renderer.clear()

    @bindings.add("tab")
    def _(event):
        event.app.current_buffer.start_completion(select_first=False)

    @bindings.add("escape", "enter")
    def _(event):
        event.app.current_buffer.validate_and_handle()

    from prompt_toolkit.styles import Style

    return PromptSession(
        completer=WordCompleter(_SLASH_COMMANDS, ignore_case=True),
        history=InMemoryHistory(),
        key_bindings=bindings,
        bottom_toolbar=_statusline,
        style=Style.from_dict({
            "bottom-toolbar": "bg:#1a1a2e",
            "bottom-toolbar.text": "#6a6a7a",
        }),
        complete_while_typing=False,
        reserve_space_for_menu=0,
        multiline=False,
    )


def _should_use_prompt_toolkit() -> bool:
    return (
        hasattr(sys.stdin, "isatty")
        and hasattr(sys.stdout, "isatty")
        and sys.stdin.isatty()
        and sys.stdout.isatty()
        and not os.environ.get("VELARIA_INTERACTIVE_STDLIB")
    )


def _read_prompt(prompt_session: Any | None) -> str:
    if prompt_session is not None:
        return prompt_session.prompt("› ")
    return input(_style("› ", "prompt"))


def _statusline() -> Any:
    runtime = "-"
    model = "-"
    tool_count = 0
    try:
        status = _get_cached_status()
        runtime = str(status.get("runtime") or "-")
        model = str(status.get("model") or "-")
        tool_count = len(status.get("tools") or [])
    except Exception:
        pass
    session = _short_id(_current_session_id) or "-"
    dataset = _state.dataset_name or pathlib_basename(_state.source_path) or "no dataset"
    run = _short_id(_state.last_run_id) if _state.last_run_id else "no run"
    spinner = _turn_status_frame if _state.turn_state == "running" else ""
    elapsed = _elapsed_turn() if _state.turn_state == "running" else ""
    state_text = f"{_state.runtime_warmup}/{_state.turn_state}"
    sep = "─" * 6 + " "

    if _prompt_session is not None:
        parts: list[tuple[str, str]] = []
        parts.append(("class:bottom-toolbar.text", sep))
        if spinner:
            parts.append(("class:bottom-toolbar.text bold", f"{spinner} running {elapsed} "))
        parts.append(("class:bottom-toolbar.text", f"{runtime} {model} | session {session} | dataset {dataset} | run {run} | tools {tool_count} | {state_text} "))
        return parts

    if spinner:
        return f"{sep}{spinner} running {elapsed} | {runtime} {model} | session {session} | dataset {dataset} | run {run} | tools {tool_count} | {state_text} "
    return f"{sep}{runtime} {model} | session {session} | dataset {dataset} | run {run} | tools {tool_count} | {state_text} "


def _print_velaria_state() -> None:
    _print_dataset_state(title="Velaria State")


def _print_dataset_state(*, title: str = "Dataset") -> None:
    rows = [
        ("dataset", _state.dataset_name or "-"),
        ("source", _state.source_path or "-"),
        ("table", _state.table_name or "-"),
        ("schema", ", ".join(_state.schema) if _state.schema else "-"),
        ("rows", str(_state.row_count) if _state.row_count is not None else "-"),
        ("result", _format_result_state()),
        ("last run", _state.last_run_id or "-"),
        ("last artifact", _state.last_artifact_id or "-"),
        ("last tool", _state.last_tool or "-"),
        ("runtime warmup", _state.runtime_warmup),
        ("turn", _state.turn_state),
    ]
    _print_section(title, rows)


def _print_runs() -> None:
    _run_cli_escape(["run", "list", "--limit", "5"], summarize=True)


def _print_artifacts() -> None:
    argv = ["artifacts", "list", "--limit", "5"]
    if _state.last_run_id:
        argv.extend(["--run-id", _state.last_run_id])
    _run_cli_escape(argv, summarize=True)


def _print_cli_payload_summary(payload: dict[str, Any]) -> bool:
    if isinstance(payload.get("runs"), list):
        _print_run_rows(payload["runs"])
        return True
    if isinstance(payload.get("artifacts"), list):
        _print_artifact_rows(payload["artifacts"])
        return True
    return False


def _print_run_rows(runs: list[Any]) -> None:
    if not runs:
        _print_note("runs", "no recent runs")
        return
    rows: list[tuple[str, str, str, str, str]] = []
    for item in runs:
        if not isinstance(item, dict):
            continue
        run_id = str(item.get("run_id") or "-")
        rows.append(
            (
                _short_id(run_id) or "-",
                str(item.get("status") or "-"),
                str(item.get("action") or "-"),
                str(item.get("run_name") or "-"),
                str(item.get("artifact_count") if item.get("artifact_count") is not None else "-"),
            )
        )
    _print_table("Runs", ("run", "status", "action", "name", "artifacts"), rows)


def _print_artifact_rows(artifacts: list[Any]) -> None:
    if not artifacts:
        _print_note("artifacts", "no recent artifacts")
        return
    rows: list[tuple[str, str, str, str, str, str]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        artifact_id = str(item.get("artifact_id") or "-")
        run_id = str(item.get("run_id") or "-")
        schema = _artifact_schema_summary(item)
        rows.append(
            (
                _short_id(artifact_id) or "-",
                _short_id(run_id) or "-",
                str(item.get("type") or "-"),
                str(item.get("format") or "-"),
                str(item.get("row_count") if item.get("row_count") is not None else "-"),
                schema,
            )
        )
    _print_table("Artifacts", ("artifact", "run", "type", "format", "rows", "schema"), rows)


def _artifact_schema_summary(artifact: dict[str, Any]) -> str:
    schema = artifact.get("schema_json")
    preview = artifact.get("preview_json")
    if not isinstance(schema, list) and isinstance(preview, dict):
        schema = preview.get("schema")
    if not isinstance(schema, list):
        return "-"
    names = [str(v) for v in schema[:6]]
    suffix = ", ..." if len(schema) > 6 else ""
    return ", ".join(names) + suffix


def _print_shortcuts() -> None:
    _print_section(
        "Shortcuts",
        [
            ("Ctrl-C", "cancel current input; during a turn, interrupt the active stream"),
            ("Ctrl-D", "exit interactive mode"),
            ("Ctrl-L", "clear screen in prompt_toolkit mode"),
            ("Esc Enter", "submit current input in prompt_toolkit mode"),
            ("Up/Down", "navigate input history"),
            ("Tab", "complete slash commands"),
        ],
    )


def _format_result_state() -> str:
    if not _state.result_schema and _state.result_row_count is None:
        return "-"
    schema = ", ".join(_state.result_schema) if _state.result_schema else "unknown schema"
    rows = _state.result_row_count if _state.result_row_count is not None else "?"
    return f"{rows} rows [{schema}]"


def _update_state_from_event(event_type: str, content: str, data: dict[str, Any]) -> None:
    if event_type == "thinking":
        _state.turn_activity = "thinking"
    if event_type == "assistant_text":
        _state.turn_activity = "answering"
    if event_type in {"tool_call", "tool_result"}:
        tool_name = _extract_tool_name(data) or _extract_tool_name_from_payload(content)
        if tool_name:
            _state.turn_activity = tool_name
        if tool_name and (event_type == "tool_call" or not _state.last_tool):
            _state.last_tool = tool_name
    if event_type == "command" and content:
        _state.turn_activity = "command"
        _state.last_tool = "command"
        _state.last_function = content
    if event_type == "tool_result":
        payload = _payload_from_content_or_data(content, data)
        if payload:
            _update_state_from_payload(payload)
    if event_type == "error":
        _state.turn_state = "failed"


def _update_state_from_text(text: str) -> None:
    payload = _parse_json_payload(text)
    if payload:
        _update_state_from_payload(payload)


def _update_state_from_payload(payload: dict[str, Any]) -> None:
    function = str(payload.get("function") or payload.get("tool") or payload.get("name") or "")
    if function:
        _state.last_function = function
    if function == "velaria_read" or ("source_path" in payload and "schema" in payload and "query" not in payload):
        _state.source_path = str(payload.get("source_path") or _state.source_path)
        _state.dataset_name = pathlib_basename(_state.source_path) or _state.dataset_name
        _state.table_name = str(payload.get("table_name") or _state.table_name or "input_table")
        _state.schema = [str(v) for v in payload.get("schema") or []]
        if isinstance(payload.get("row_count"), int):
            _state.row_count = int(payload["row_count"])
    if function == "velaria_dataset_normalize":
        _state.source_path = str(payload.get("source_path") or payload.get("normalized_path") or _state.source_path)
        _state.dataset_name = pathlib_basename(_state.source_path) or _state.dataset_name
        _state.table_name = str(payload.get("table_name") or _state.table_name or "input_table")
        _state.schema = [str(v) for v in payload.get("schema") or []]
        if isinstance(payload.get("row_count"), int):
            _state.row_count = int(payload["row_count"])
    if function == "velaria_sql" or ("query" in payload and "schema" in payload):
        _state.result_schema = [str(v) for v in payload.get("schema") or []]
        if isinstance(payload.get("row_count"), int):
            _state.result_row_count = int(payload["row_count"])
        if payload.get("source_path"):
            _state.source_path = str(payload["source_path"])
            _state.dataset_name = pathlib_basename(_state.source_path) or _state.dataset_name
        if payload.get("table_name"):
            _state.table_name = str(payload["table_name"])
    _update_run_artifact_state(payload)
    stdout = payload.get("stdout")
    if isinstance(stdout, str):
        nested = _parse_json_payload(stdout)
        if nested:
            _update_state_from_payload(nested)


def _update_run_artifact_state(payload: dict[str, Any]) -> None:
    run_id = payload.get("run_id")
    if isinstance(run_id, str) and run_id:
        _state.last_run_id = run_id
    artifact_id = payload.get("artifact_id")
    if isinstance(artifact_id, str) and artifact_id:
        _state.last_artifact_id = artifact_id
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list):
        for item in artifacts:
            if isinstance(item, dict) and isinstance(item.get("artifact_id"), str):
                _state.last_artifact_id = item["artifact_id"]
                if isinstance(item.get("run_id"), str) and item["run_id"]:
                    _state.last_run_id = item["run_id"]
                break
    runs = payload.get("runs")
    if isinstance(runs, list):
        for item in runs:
            if isinstance(item, dict) and isinstance(item.get("run_id"), str):
                _state.last_run_id = item["run_id"]
                break
    artifact = payload.get("artifact")
    if isinstance(artifact, dict) and isinstance(artifact.get("artifact_id"), str):
        _state.last_artifact_id = artifact["artifact_id"]
        if isinstance(artifact.get("run_id"), str) and artifact["run_id"]:
            _state.last_run_id = artifact["run_id"]


def _payload_from_content_or_data(content: str, data: dict[str, Any]) -> dict[str, Any] | None:
    for candidate in (
        _parse_json_payload(content),
        data if isinstance(data, dict) and ("schema" in data or "run_id" in data or "artifact_id" in data or "function" in data) else None,
        _first_dict_value(data, {"result", "output", "payload"}),
        _first_dict_value(data.get("item") if isinstance(data.get("item"), dict) else {}, {"result", "output", "payload"}),
    ):
        if candidate:
            return candidate
    return None


def _parse_json_payload(text: str) -> dict[str, Any] | None:
    if not text or not text.strip():
        return None
    raw = text.strip()
    for candidate in _json_payload_candidates(raw):
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _json_payload_candidates(raw: str) -> list[str]:
    candidates = [raw]
    marker = "Output:"
    if marker in raw:
        candidates.append(raw.rsplit(marker, 1)[1].strip())
    lines = [line for line in raw.splitlines() if line.strip()]
    if lines:
        candidates.append(lines[-1].strip())
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw[start : end + 1])
    deduped: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _first_dict_value(data: dict[str, Any], keys: set[str]) -> dict[str, Any] | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            parsed = _parse_json_payload(value)
            if parsed:
                return parsed
    return None


def _extract_tool_name_from_payload(content: str) -> str:
    payload = _parse_json_payload(content)
    if not payload:
        return ""
    return str(payload.get("function") or payload.get("tool") or payload.get("name") or "")


def _elapsed_turn() -> str:
    if _state.turn_started_at is None:
        return "turn complete"
    elapsed = max(0.0, time.time() - _state.turn_started_at)
    return f"{elapsed:.1f}s"


def _short_id(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 12:
        return value
    if "_" in value:
        prefix, suffix = value.rsplit("_", 1)
        if suffix:
            if "T" in prefix and prefix.endswith("Z"):
                time_part = prefix.split("T", 1)[1].rstrip("Z")[:6]
                return f"{time_part}_{suffix[:6]}"
            label = prefix.split("_", 1)[0]
            return f"{label[:3]}_{suffix[:8]}"
    return value[:8]


def pathlib_basename(path: str) -> str:
    if not path:
        return ""
    return os.path.basename(path.rstrip("/"))


def _print_banner() -> None:
    print(_bold("Velaria Agent"))
    print(_muted("Interactive agent runtime. Type /help for commands, /exit to quit."))
    print()


def _print_startup_status() -> None:
    runtime = _safe_runtime()
    if runtime is None:
        return
    if _current_session_id:
        status = runtime.status(_current_session_id)
    else:
        status = runtime.status("__velaria_pending_session__")
        status["session"] = None
    _print_status(status, title="Session")
    _print_dataset_state(title="Velaria State")


def _print_status(status: dict[str, Any], *, title: str = "Status") -> None:
    session = status.get("session") if isinstance(status.get("session"), dict) else {}
    rows = [
        ("runtime", str(status.get("runtime") or "-")),
        ("provider", str(status.get("provider") or "-")),
        ("auth", str(status.get("auth_mode") or "-")),
        ("model", str(status.get("model") or "-")),
        ("network", "enabled" if status.get("network_access", True) else "disabled"),
        ("local config", "reuse" if status.get("reuse_local_config", True) else "isolated"),
        ("session", str(session.get("session_id") or "-")),
        ("cwd", str(status.get("cwd") or "-")),
        ("workspace", str(status.get("workspace") or "-")),
        ("tools", ", ".join(str(t) for t in status.get("tools") or []) or "-"),
    ]
    _print_section(title, rows)


def _print_sessions(sessions: list[dict[str, Any]], *, current_session_id: str | None) -> None:
    print(_bold("Sessions"))
    headers = ("current", "session", "runtime", "status", "last active")
    rows = []
    for item in sessions:
        session_id = str(item.get("session_id") or "?")
        rows.append(
            (
                "*" if session_id == current_session_id else "",
                session_id,
                str(item.get("runtime_type") or "-"),
                str(item.get("status") or "-"),
                str(item.get("last_active_at") or "-"),
            )
        )
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers))
    ]
    print("  " + "  ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    for row in rows:
        print("  " + "  ".join(row[i].ljust(widths[i]) for i in range(len(row))))
    print()


def _print_section(title: str, rows: list[tuple[str, str]]) -> None:
    print(_bold(title))
    width = max((len(k) for k, _ in rows), default=0)
    for key, value in rows:
        print(f"  {key.ljust(width)}  {_wrap_value(value, width + 4)}")
    print()


def _print_table(title: str, headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    print(_bold(title))
    if not rows:
        print("  -")
        print()
        return
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers))
    ]
    print("  " + "  ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    for row in rows:
        print("  " + "  ".join(row[i].ljust(widths[i]) for i in range(len(row))))
    print()


def _print_note(label: str, message: str, *, level: str = "info") -> None:
    print(f"{_style(label.ljust(8), level)} {message}", flush=True)


def _print_event(label: str, message: str) -> None:
    print(f"{_style(label.ljust(12), 'event')} {_wrap_value(message, 13)}", flush=True)


def _print_assistant_text(message: str) -> None:
    if not _should_render_markdown():
        print(message, flush=True)
        return
    try:
        from rich.console import Console
        from rich.markdown import Markdown
    except Exception:
        print(message, flush=True)
        return
    console = Console(
        file=sys.stdout,
        force_terminal=_supports_color(),
        soft_wrap=True,
        highlight=False,
    )
    console.print(Markdown(message))
    sys.stdout.flush()


def _should_render_markdown() -> bool:
    configured = os.environ.get("VELARIA_MARKDOWN")
    if configured is not None:
        return configured.strip().lower() not in {"0", "false", "no", "off"}
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _wrap_value(value: str, indent: int) -> str:
    text = str(value)
    if "\n" not in text:
        return text
    pad = "\n" + " " * indent
    return pad.join(text.splitlines())


def _compact_content(content: str) -> str:
    text = content.strip()
    if len(text) <= 600:
        return text
    return text[:597] + "..."


def _format_tool_call(content: str, data: dict[str, Any]) -> str:
    name = _extract_tool_name(data) or content or "tool"
    args = _tool_arguments(data)
    status = _tool_status(data)
    parts = [name]
    arg_summary = _format_tool_arguments(args)
    if arg_summary:
        parts.append(arg_summary)
    if status and status not in {"completed", "complete", "success"}:
        parts.append(f"status={status}")
    return _compact_line(" ".join(parts), limit=240)


def _tool_status(data: dict[str, Any]) -> str:
    value = data.get("tool_status") if isinstance(data, dict) else None
    if isinstance(value, str) and value:
        return value
    item = data.get("item") if isinstance(data, dict) else None
    if isinstance(item, dict):
        status = item.get("status")
        if isinstance(status, str):
            return status
    return ""


def _tool_arguments(data: dict[str, Any]) -> dict[str, Any] | str | None:
    for value in (
        data.get("tool_arguments") if isinstance(data, dict) else None,
        data.get("arguments") if isinstance(data, dict) else None,
    ):
        parsed = _parse_tool_arguments(value)
        if parsed is not None:
            return parsed
    item = data.get("item") if isinstance(data, dict) else None
    if isinstance(item, dict):
        for key in ("arguments", "input"):
            parsed = _parse_tool_arguments(item.get(key))
            if parsed is not None:
                return parsed
    return None


def _parse_tool_arguments(value: Any) -> dict[str, Any] | str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return raw
        if isinstance(parsed, dict):
            return parsed
        return raw
    return str(value)


def _format_tool_arguments(args: dict[str, Any] | str | None) -> str:
    if args is None:
        return ""
    if isinstance(args, str):
        return _compact_line(args, limit=180)
    preferred = [
        "path",
        "url",
        "source_path",
        "source_url",
        "source_id",
        "table_name",
        "query",
        "artifact_id",
        "run_id",
        "save_run",
        "limit",
    ]
    pieces: list[str] = []
    used: set[str] = set()
    for key in preferred:
        if key in args:
            pieces.append(f"{key}={_format_tool_argument_value(args[key])}")
            used.add(key)
    for key, value in args.items():
        if key in used:
            continue
        pieces.append(f"{key}={_format_tool_argument_value(value)}")
        if len(pieces) >= 5:
            break
    return _compact_line(" ".join(pieces), limit=190)


def _format_tool_argument_value(value: Any) -> str:
    if isinstance(value, str):
        text = value.replace("\n", " ").strip()
    elif isinstance(value, (int, float, bool)) or value is None:
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = json.dumps(value, ensure_ascii=False)
    if len(text) > 72:
        text = text[:69] + "..."
    if " " in text:
        return json.dumps(text, ensure_ascii=False)
    return text


def _compact_line(content: str, *, limit: int) -> str:
    text = " ".join(content.strip().split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _looks_like_runtime_payload(content: str) -> bool:
    text = content.strip()
    if not text.startswith("{"):
        return False
    try:
        payload = json.loads(text)
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    return "params" in payload and ("item" in payload or isinstance(payload.get("params"), dict))


def _summarize_tool_result(content: str, data: dict[str, Any]) -> str:
    payload = _payload_from_content_or_data(content, data)
    if not payload:
        return _compact_content(content)
    function = str(
        payload.get("function")
        or payload.get("tool")
        or payload.get("name")
        or _state.last_tool
        or "tool"
    )
    if payload.get("ok") is False:
        return f"{function}: failed {payload.get('error') or payload.get('phase') or 'unknown'}"
    if function == "velaria_dataset_import":
        source = pathlib_basename(str(payload.get("source_path") or ""))
        schema = ", ".join(str(v) for v in payload.get("schema") or [])
        source_payload = payload.get("source")
        source_id = payload.get("source_id") or (
            source_payload.get("source_id") if isinstance(source_payload, dict) else None
        )
        return (
            f"{function}: {source_id or source or 'dataset'} "
            f"{payload.get('row_count', '?')} rows [{schema or 'no schema'}]"
        )
    if function == "velaria_dataset_normalize":
        source = pathlib_basename(str(payload.get("source_path") or payload.get("normalized_path") or ""))
        schema = ", ".join(str(v) for v in payload.get("schema") or [])
        return f"{function}: {source or 'normalized dataset'} {payload.get('row_count', '?')} rows [{schema or 'no schema'}]"
    if function == "velaria_dataset_process":
        schema = ", ".join(str(v) for v in payload.get("schema") or [])
        run = payload.get("run_id")
        prefix = f"{function}: run {run}" if run else function
        return f"{prefix}: {payload.get('row_count', '?')} rows [{schema or 'no schema'}]"
    if function == "velaria_read":
        source = pathlib_basename(str(payload.get("source_path") or ""))
        schema = ", ".join(str(v) for v in payload.get("schema") or [])
        return f"{function}: {payload.get('row_count', '?')} rows from {source or 'dataset'} [{schema or 'no schema'}]"
    if function == "velaria_sql":
        schema = ", ".join(str(v) for v in payload.get("schema") or [])
        return f"{function}: {payload.get('row_count', '?')} rows [{schema or 'no schema'}]"
    if function == "velaria_artifact_preview" or payload.get("artifact_id"):
        return f"{function}: artifact {payload.get('artifact_id') or _state.last_artifact_id}"
    if function == "velaria_cli_run":
        nested = _parse_json_payload(str(payload.get("stdout") or ""))
        if nested and nested.get("run_id"):
            return f"{function}: run {nested.get('run_id')}"
        return f"{function}: exit {payload.get('exit_code', '?')}"
    if payload.get("run_id"):
        return f"{function}: run {payload.get('run_id')}"
    return _compact_content(json.dumps(payload, ensure_ascii=False))


def _style(text: str, style: str) -> str:
    if not _supports_color():
        return text
    colors = {
        "prompt": "\033[36m",
        "info": "\033[36m",
        "warn": "\033[33m",
        "error": "\033[31m",
        "event": "\033[35m",
        "bold": "\033[1m",
        "muted": "\033[2m",
    }
    color = colors.get(style)
    if not color:
        return text
    return f"{color}{text}\033[0m"


def _bold(text: str) -> str:
    return _style(text, "bold")


def _muted(text: str) -> str:
    return _style(text, "muted")


def _supports_color() -> bool:
    return (
        hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
        and not os.environ.get("NO_COLOR")
    )
