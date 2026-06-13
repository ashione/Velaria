"""Velaria-owned Agent CLI and TUI entrypoints."""

from .entry import register, run_agent_argv, run_default_agent, wants_agent_alias

__all__ = ["register", "run_agent_argv", "run_default_agent", "wants_agent_alias"]
