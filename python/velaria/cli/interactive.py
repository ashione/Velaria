from __future__ import annotations

import shlex

from velaria.cli._common import _emit_error_json, _interactive_banner


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
            continue
        if command.startswith("help "):
            help_args = shlex.split(command[len("help ") :].strip())
            main([*help_args, "--help"])
            continue
        if command in {"-i", "--interactive"}:
            _emit_error_json(
                "interactive mode is already active",
                error_type="usage_error",
                phase="interactive",
            )
            continue
        main(shlex.split(command))
