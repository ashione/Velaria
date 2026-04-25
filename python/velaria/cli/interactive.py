from __future__ import annotations

import json
import os
import shlex
import urllib.request
from typing import Any

from velaria.cli._common import _emit_error_json, _interactive_banner, _json_dumps

_DEFAULT_SERVICE_URL = "http://127.0.0.1:37491"
_current_ai_session: str | None = None


def _service_url() -> str:
    return os.environ.get("VELARIA_SERVICE_URL", _DEFAULT_SERVICE_URL)


def _ai_api(method: str, path: str, payload: dict | None = None) -> dict | None:
    url = f"{_service_url()}{path}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[ai] error: {exc}")
        return None


def _handle_ai_command(text: str) -> None:
    global _current_ai_session
    parts = text.split(None, 1)
    cmd = parts[0] if parts else ""
    rest = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "help" or not cmd:
        print("AI commands:")
        print("  ai <prompt>              Generate SQL from natural language")
        print("  ai analyze <prompt>      Run agent analysis (needs session)")
        print("  ai session start         Create a new AI session")
        print("  ai session list          List active sessions")
        print("  ai session resume <id>   Resume a session")
        print("  ai session close [id]    Close current or specified session")
        print("  ai status                Show current session info")
        print("  ai help                  Show this help")
        return

    if cmd == "session":
        _handle_ai_session(rest)
        return

    if cmd == "status":
        if _current_ai_session:
            result = _ai_api("GET", f"/api/v1/ai/sessions/{_current_ai_session}")
            if result and result.get("ok"):
                print(f"[ai] active session: {_current_ai_session}")
                print(_json_dumps(result.get("session", {})))
            else:
                print(f"[ai] session {_current_ai_session} not found, clearing")
                _current_ai_session = None
        else:
            print("[ai] no active session")
        return

    if cmd == "analyze":
        if not _current_ai_session:
            print("[ai] no active session -- run 'ai session start' first")
            return
        if not rest:
            print("[ai] usage: ai analyze <prompt>")
            return
        result = _ai_api(
            "POST",
            f"/api/v1/ai/sessions/{_current_ai_session}/analyze",
            {"prompt": rest},
        )
        if result:
            if result.get("ok"):
                for msg in result.get("messages", []):
                    print(f"[{msg.get('type', '?')}] {msg.get('content', '')}")
            else:
                print(f"[ai] error: {result.get('error', 'unknown')}")
        return

    # Default: treat everything as a SQL generation prompt.
    prompt = text  # full text including cmd
    payload: dict[str, Any] = {"prompt": prompt}

    if _current_ai_session:
        result = _ai_api(
            "POST",
            f"/api/v1/ai/sessions/{_current_ai_session}/generate-sql",
            payload,
        )
    else:
        result = _ai_api("POST", "/api/v1/ai/generate-sql", payload)

    if result and result.get("ok"):
        sql = result.get("sql", "")
        explanation = result.get("explanation", "")
        if explanation:
            print(f"[ai] {explanation}")
        print(f"\n{sql}\n")
        # Offer to run the generated SQL.
        try:
            answer = input("Run this SQL? [y/N] ").strip().lower()
            if answer in ("y", "yes"):
                from velaria.cli import main as cli_main

                cli_main(["file-sql", "--input-path", ".", "--query", sql])
        except (EOFError, KeyboardInterrupt):
            print()
    elif result:
        print(f"[ai] error: {result.get('error', 'unknown')}")


def _handle_ai_session(text: str) -> None:
    global _current_ai_session
    parts = text.split(None, 1)
    cmd = parts[0] if parts else ""
    rest = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "start":
        result = _ai_api("POST", "/api/v1/ai/sessions", {"dataset_context": {}})
        if result and result.get("ok"):
            _current_ai_session = result["session_id"]
            print(f"[ai] session started: {_current_ai_session}")
        return

    if cmd == "list":
        result = _ai_api("GET", "/api/v1/ai/sessions")
        if result and result.get("ok"):
            sessions = result.get("sessions", [])
            if not sessions:
                print("[ai] no active sessions")
            for s in sessions:
                marker = " <-- current" if s.get("session_id") == _current_ai_session else ""
                print(
                    f"  {s.get('session_id', '?')}  "
                    f"{s.get('runtime_type', '')}  "
                    f"{s.get('status', '')}  "
                    f"{s.get('last_active_at', '')}"
                    f"{marker}"
                )
        return

    if cmd == "resume":
        if not rest:
            print("[ai] usage: ai session resume <session_id>")
            return
        _current_ai_session = rest
        print(f"[ai] session resumed: {_current_ai_session}")
        return

    if cmd == "close":
        sid = rest or _current_ai_session
        if not sid:
            print("[ai] no session to close")
            return
        _ai_api("DELETE", f"/api/v1/ai/sessions/{sid}")
        print(f"[ai] session closed: {sid}")
        if sid == _current_ai_session:
            _current_ai_session = None
        return

    print(f"[ai] unknown session command: {cmd}")
    print("  ai session start|list|resume <id>|close [id]")


def _wants_interactive(argv: list[str]) -> bool:
    return argv in (["-i"], ["--interactive"])


def _run_interactive_loop() -> int:
    from velaria.cli import main

    _interactive_banner()
    while True:
        try:
            line = input("velaria> ")
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            continue
        command = line.strip()
        if not command:
            continue
        if command in {"exit", "quit"}:
            return 0
        if command == "help":
            main(["--help"])
            print("\nAI commands: type 'ai help' for AI-assisted analysis.")
            continue
        if command.startswith("help "):
            help_args = shlex.split(command[len("help "):].strip())
            main([*help_args, "--help"])
            continue
        if command in {"-i", "--interactive"}:
            _emit_error_json(
                "interactive mode is already active",
                error_type="usage_error",
                phase="interactive",
            )
            continue
        if command.startswith("ai ") or command == "ai":
            ai_text = command[3:].strip() if command.startswith("ai ") else ""
            _handle_ai_command(ai_text)
            continue
        main(shlex.split(command))
