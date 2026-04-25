import io
import json
import importlib
import os
import pathlib
import sys
import tempfile
import types
import unittest
from builtins import EOFError
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

try:
    velaria_cli = importlib.import_module("velaria_cli")
    velaria_cli_impl = importlib.import_module("velaria.cli")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_cli = importlib.import_module("velaria_cli")
    velaria_cli_impl = importlib.import_module("velaria.cli")


class _FakeArrowResult:
    @property
    def schema(self):
        return mock.Mock(names=["row_id", "score"])

    def to_pylist(self):
        return [{"row_id": 0, "score": 0.0}]


class _FakeDataFrame:
    def to_arrow(self):
        return _FakeArrowResult()


class _FakeAgentRuntime:
    def __init__(self, events=None):
        self.sessions = []
        self.closed = []
        self.messages = []
        self.events = events

    async def start_thread(self, dataset_context=None):
        session_id = f"agent-session-{len(self.sessions) + 1}"
        self.sessions.insert(
            0,
            {
                "session_id": session_id,
                "runtime_type": "codex",
                "runtime_session_ref": f"thread-{len(self.sessions) + 1}",
                "status": "active",
                "last_active_at": "2026-04-25T00:00:00Z",
                "dataset_context": dataset_context or {},
            },
        )
        return session_id

    async def resume_thread(self, session_id):
        return any(s["session_id"] == session_id for s in self.sessions)

    async def list_threads(self):
        return list(self.sessions)

    async def close_thread(self, session_id):
        self.closed.append(session_id)
        self.sessions = [s for s in self.sessions if s["session_id"] != session_id]

    async def send_message(self, session_id, prompt):
        from velaria.ai_runtime.agent import AgentEvent

        self.messages.append((session_id, prompt))
        if self.events is None:
            yield AgentEvent("assistant_text", f"agent heard: {prompt}", session_id=session_id)
            return
        for event in self.events:
            if isinstance(event, AgentEvent):
                yield event
            else:
                yield AgentEvent(session_id=session_id, **event)

    def status(self, session_id=None):
        return {
            "runtime": "codex",
            "model": "gpt-5.4-mini",
            "cwd": "/tmp/workspace",
            "workspace": "/tmp/runtime",
            "session": {"session_id": session_id},
            "tools": ["velaria_read", "velaria_sql"],
        }

    def shutdown(self):
        pass


def _mock_agent_runtime(fake):
    return mock.patch("velaria.ai_runtime.create_runtime", return_value=fake)


class PythonCliContractTest(unittest.TestCase):
    def test_agentic_search_templates_cli_returns_hits(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = velaria_cli.main(["search", "templates", "--query-text", "count events", "--top-k", "2"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertGreaterEqual(len(payload["hits"]), 1)

    def test_agentic_source_create_and_list_cli(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-agentic-source-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = velaria_cli.main(
                        [
                            "source",
                            "create",
                            "--source-id",
                            "ticks",
                            "--kind",
                            "external_event",
                            "--name",
                            "ticks",
                            "--schema-binding",
                            json.dumps({"time_field": "ts", "type_field": "kind", "key_field": "symbol"}),
                        ]
                    )
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["source"]["source_id"], "ticks")

                list_stdout = io.StringIO()
                with redirect_stdout(list_stdout):
                    list_exit = velaria_cli.main(["source", "list"])
                self.assertEqual(list_exit, 0)
                list_payload = json.loads(list_stdout.getvalue())
                self.assertEqual(len(list_payload["sources"]), 1)
                self.assertEqual(list_payload["sources"][0]["source_id"], "ticks")

    def test_interactive_mode_accepts_commands_and_exits_cleanly(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-interactive-") as tmp:
            fake = _FakeAgentRuntime()
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with mock.patch("velaria.cli.interactive._runtime", None):
                    with _mock_agent_runtime(fake):
                        with mock.patch(
                            "builtins.input",
                            side_effect=["/status", ":run list --limit 1", ":help run diff", "/exit"],
                        ):
                            with redirect_stdout(stdout), redirect_stderr(stderr):
                                exit_code = velaria_cli.main(["-i"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                output = stdout.getvalue()
                self.assertIn("Velaria Agent", output)
                self.assertIn("starting agent thread is starting in background", output)
                self.assertRegex(output, r"runtime\s+codex")
                self.assertRegex(output, r"network\s+enabled")
                self.assertIn("Velaria State", output)
                self.assertIn('"ok": true', output)
                self.assertIn("usage:", output)

    def test_interactive_mode_starts_new_thread_by_default(self):
        fake = _FakeAgentRuntime()
        fake.sessions.append(
            {
                "session_id": "agent-session-old",
                "runtime_type": "codex",
                "runtime_session_ref": "thread-old",
                "status": "active",
                "last_active_at": "2026-04-24T00:00:00Z",
                "dataset_context": {},
            }
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("starting agent thread is starting in background", stdout.getvalue())
        self.assertNotIn("resumed  agent-session-old", stdout.getvalue())

    def test_interactive_mode_resumes_only_when_session_is_explicit(self):
        fake = _FakeAgentRuntime()
        fake.sessions.append(
            {
                "session_id": "agent-session-old",
                "runtime_type": "codex",
                "runtime_session_ref": "thread-old",
                "status": "active",
                "last_active_at": "2026-04-24T00:00:00Z",
                "dataset_context": {},
            }
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i", "--session", "agent-session-old"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("resumed  agent-session-old", stdout.getvalue())
        self.assertNotIn("started  agent-session-2", stdout.getvalue())

    def test_interactive_mode_reports_command_errors_without_exiting_session(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-interactive-errors-") as tmp:
            fake = _FakeAgentRuntime()
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with mock.patch("velaria.cli.interactive._runtime", None):
                    with _mock_agent_runtime(fake):
                        with mock.patch(
                            "builtins.input",
                            side_effect=[
                                ":run show --run-id missing-run",
                                ":help run result",
                                ":run list --limit 1",
                                "/exit",
                            ],
                        ):
                            with redirect_stdout(stdout), redirect_stderr(stderr):
                                exit_code = velaria_cli.main(["-i"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                output = stdout.getvalue()
                self.assertIn("Velaria Agent", output)
                self.assertIn('"ok": false', output)
                self.assertIn('"phase": "run_lookup"', output)
                self.assertIn('"ok": true', output)
                self.assertIn("usage:", output)

    def test_interactive_mode_eof_exits_cleanly(self):
        fake = _FakeAgentRuntime()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=EOFError):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("Velaria Agent", stdout.getvalue())

    def test_interactive_mode_sends_plain_text_to_agent(self):
        fake = _FakeAgentRuntime()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["analyze this dataset", "/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(fake.messages, [("agent-session-1", "analyze this dataset")])
        self.assertIn("agent heard: analyze this dataset", stdout.getvalue())

    def test_interactive_mode_answers_simple_greeting_locally(self):
        fake = _FakeAgentRuntime()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["hello", "/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(fake.messages, [])
        self.assertIn("Hello, I am Velaria Agent.", stdout.getvalue())

    def test_interactive_mode_streams_events_and_updates_velaria_state(self):
        events = [
            {"type": "thinking", "content": "checking dataset"},
            {"type": "tool_call", "content": "velaria_read", "data": {"name": "velaria_read"}},
            {
                "type": "tool_result",
                "content": json.dumps(
                    {
                        "ok": True,
                        "function": "velaria_read",
                        "source_path": "/tmp/sales.csv",
                        "schema": ["region", "amount"],
                        "row_count": 3,
                    }
                ),
            },
            {"type": "command", "content": "run list --limit 5"},
            {
                "type": "tool_result",
                "content": json.dumps(
                    {
                        "ok": True,
                        "function": "velaria_sql",
                        "source_path": "/tmp/sales.csv",
                        "table_name": "input_table",
                        "query": "SELECT region FROM input_table",
                        "schema": ["region"],
                        "row_count": 2,
                    }
                ),
            },
            {"type": "assistant_text", "content": "done"},
        ]
        fake = _FakeAgentRuntime(events=events)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["analyze sales", "/status", "/dataset", "/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        output = stdout.getvalue()
        self.assertIn("thinking     checking dataset", output)
        self.assertIn("tool         velaria_read", output)
        self.assertIn("tool result  velaria_read: 3 rows from sales.csv [region, amount]", output)
        self.assertIn("command      run list --limit 5", output)
        self.assertIn("tool result  velaria_sql: 2 rows [region]", output)
        self.assertRegex(output, r"dataset\s+sales\.csv")
        self.assertRegex(output, r"schema\s+region, amount")
        self.assertRegex(output, r"result\s+2 rows \[region\]")

    def test_interactive_mode_parses_codex_mcp_function_events(self):
        tool_output = (
            "Wall time: 0.1 seconds\n"
            "Output:\n"
            + json.dumps(
                {
                    "ok": True,
                    "function": "velaria_dataset_process",
                    "source_path": "/tmp/sales.csv",
                    "source_url": "https://example.test/sales.csv",
                    "table_name": "input_table",
                    "query": "SELECT region FROM input_table",
                    "schema": ["region", "total_amount"],
                    "row_count": 2,
                    "rows": [{"region": "cn", "total_amount": 15.0}],
                    "run_id": "run_dataset",
                    "artifacts": [{"artifact_id": "artifact_dataset", "run_id": "run_dataset"}],
                }
            )
        )
        events = [
            {
                "type": "tool_call",
                "data": {
                    "item": {
                        "type": "function_call",
                        "namespace": "mcp__velaria__",
                        "name": "velaria_dataset_process",
                    }
                },
            },
            {
                "type": "tool_result",
                "content": tool_output,
                "data": {"item": {"type": "function_call_output", "call_id": "call_1"}},
            },
            {"type": "assistant_text", "content": "done"},
        ]
        fake = _FakeAgentRuntime(events=events)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.interactive._runtime", None):
            with _mock_agent_runtime(fake):
                with mock.patch("builtins.input", side_effect=["process url", "/status", "/exit"]):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        output = stdout.getvalue()
        self.assertIn("tool         mcp__velaria__.velaria_dataset_process", output)
        self.assertIn("tool result  velaria_dataset_process: run run_dataset: 2 rows [region, total_amount]", output)
        self.assertRegex(output, r"last run\s+run_dataset")
        self.assertRegex(output, r"last artifact\s+artifact_dataset")
        self.assertRegex(output, r"last tool\s+mcp__velaria__\.velaria_dataset_process")

    def test_interactive_mode_shortcuts_dataset_runs_and_artifacts_commands(self):
        fake = _FakeAgentRuntime()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory(prefix="velaria-cli-interactive-state-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.cli.interactive._runtime", None):
                    with _mock_agent_runtime(fake):
                        with mock.patch(
                            "builtins.input",
                            side_effect=["/shortcuts", "/keys", "/dataset", "/runs", "/artifacts", "/exit"],
                        ):
                            with redirect_stdout(stdout), redirect_stderr(stderr):
                                exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        output = stdout.getvalue()
        self.assertIn("Shortcuts", output)
        self.assertIn("Ctrl-C", output)
        self.assertIn("Dataset", output)
        self.assertTrue("runs     no recent runs" in output or '"runs": []' in output)
        self.assertIn("artifacts no recent artifacts", output)

    def test_interactive_prompt_toolkit_path_is_optional_and_wired(self):
        interactive = importlib.import_module("velaria.cli.interactive")

        class FakePromptSession:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeWordCompleter:
            def __init__(self, words, ignore_case=False):
                self.words = words
                self.ignore_case = ignore_case

        class FakeHistory:
            pass

        class FakeKeyBindings:
            def __init__(self):
                self.bindings = []
                self.handlers = {}

            def add(self, *keys):
                self.bindings.append(keys)

                def decorator(fn):
                    self.handlers[keys] = fn
                    return fn

                return decorator

        modules = {
            "prompt_toolkit": types.SimpleNamespace(PromptSession=FakePromptSession),
            "prompt_toolkit.completion": types.SimpleNamespace(WordCompleter=FakeWordCompleter),
            "prompt_toolkit.history": types.SimpleNamespace(InMemoryHistory=FakeHistory),
            "prompt_toolkit.key_binding": types.SimpleNamespace(KeyBindings=FakeKeyBindings),
        }
        with mock.patch.dict(sys.modules, modules):
            with mock.patch.object(interactive, "_should_use_prompt_toolkit", return_value=True):
                session = interactive._build_prompt_session()
        self.assertIsInstance(session, FakePromptSession)
        self.assertIn("bottom_toolbar", session.kwargs)
        self.assertIn("completer", session.kwargs)
        self.assertIn("key_bindings", session.kwargs)
        self.assertFalse(session.kwargs["multiline"])
        self.assertFalse(session.kwargs["complete_while_typing"])
        self.assertEqual(session.kwargs["reserve_space_for_menu"], 3)
        bindings = session.kwargs["key_bindings"]
        self.assertIn(("c-c",), bindings.bindings)
        self.assertIn(("c-d",), bindings.bindings)
        self.assertIn(("c-l",), bindings.bindings)
        self.assertIn(("tab",), bindings.bindings)
        self.assertIn(("escape", "enter"), bindings.bindings)
        self.assertIn("/status", session.kwargs["completer"].words)

        class FakeBuffer:
            def __init__(self, text=""):
                self.text = text
                self.reset_called = False
                self.delete_called = False
                self.completion_started = False
                self.accepted = False

            def reset(self):
                self.reset_called = True

            def delete(self):
                self.delete_called = True

            def start_completion(self, select_first=False):
                self.completion_started = True
                self.select_first = select_first

            def validate_and_handle(self):
                self.accepted = True

        class FakeApp:
            def __init__(self, buffer):
                self.current_buffer = buffer
                self.exited_with = None
                self.renderer = mock.Mock()

            def exit(self, *, exception=None):
                self.exited_with = exception

        text_buffer = FakeBuffer("draft")
        bindings.handlers[("c-c",)](types.SimpleNamespace(app=FakeApp(text_buffer)))
        self.assertTrue(text_buffer.reset_called)
        empty_app = FakeApp(FakeBuffer(""))
        bindings.handlers[("c-d",)](types.SimpleNamespace(app=empty_app))
        self.assertIs(empty_app.exited_with, EOFError)
        tab_buffer = FakeBuffer("/")
        bindings.handlers[("tab",)](types.SimpleNamespace(app=FakeApp(tab_buffer)))
        self.assertTrue(tab_buffer.completion_started)
        enter_buffer = FakeBuffer("line 1\nline 2")
        bindings.handlers[("escape", "enter")](types.SimpleNamespace(app=FakeApp(enter_buffer)))
        self.assertTrue(enter_buffer.accepted)

        with mock.patch.object(interactive, "_should_use_prompt_toolkit", return_value=False):
            self.assertIsNone(interactive._build_prompt_session())

    def test_prompt_toolkit_prompt_uses_plain_text_prompt(self):
        interactive = importlib.import_module("velaria.cli.interactive")

        class FakePromptSession:
            def __init__(self):
                self.message = None

            def prompt(self, message):
                self.message = message
                return "/status"

        session = FakePromptSession()
        self.assertEqual(interactive._read_prompt(session), "/status")
        self.assertEqual(session.message, "› ")
        self.assertNotIn("\033", session.message)

    def test_assistant_text_renders_markdown_when_enabled(self):
        interactive = importlib.import_module("velaria.cli.interactive")
        from velaria.ai_runtime.agent import AgentEvent

        stdout = io.StringIO()
        with mock.patch.object(interactive, "_should_render_markdown", return_value=True):
            with redirect_stdout(stdout):
                interactive._render_event(
                    AgentEvent(
                        "assistant_text",
                        "**Summary**\n\n- region: CN",
                        session_id="agent-session-1",
                    )
                )
        output = stdout.getvalue()
        self.assertIn("Summary", output)
        self.assertIn("region: CN", output)
        self.assertNotIn("**Summary**", output)

    def test_interactive_turn_keyboard_interrupt_marks_cancelled(self):
        interactive = importlib.import_module("velaria.cli.interactive")

        class InterruptingRuntime(_FakeAgentRuntime):
            async def send_message(self, session_id, prompt):
                self.messages.append((session_id, prompt))
                raise KeyboardInterrupt
                yield

        interactive._state = interactive.VelariaInteractiveState()
        interactive._current_session_id = "agent-session-1"
        interactive._runtime = InterruptingRuntime()
        stdout = io.StringIO()
        try:
            with redirect_stdout(stdout):
                interactive._send_agent_message("long running")
            self.assertEqual(interactive._state.turn_state, "cancelled")
            self.assertIn("cancelled turn interrupted", stdout.getvalue())
        finally:
            interactive._current_session_id = None
            interactive._runtime = None

    def test_interactive_state_updates_from_function_payloads(self):
        interactive = importlib.import_module("velaria.cli.interactive")
        interactive._state = interactive.VelariaInteractiveState()

        interactive._update_state_from_payload(
            {
                "function": "velaria_read",
                "source_path": "/tmp/sales.csv",
                "table_name": "input_table",
                "schema": ["region", "amount"],
                "row_count": 3,
            }
        )
        self.assertEqual(interactive._state.dataset_name, "sales.csv")
        self.assertEqual(interactive._state.schema, ["region", "amount"])
        self.assertEqual(interactive._state.row_count, 3)

        interactive._update_state_from_payload(
            {
                "function": "velaria_sql",
                "source_path": "/tmp/sales.csv",
                "table_name": "input_table",
                "query": "SELECT region FROM input_table",
                "schema": ["region"],
                "row_count": 2,
            }
        )
        self.assertEqual(interactive._state.result_schema, ["region"])
        self.assertEqual(interactive._state.result_row_count, 2)

        interactive._update_state_from_payload(
            {
                "function": "velaria_cli_run",
                "stdout": json.dumps(
                    {
                        "ok": True,
                        "run_id": "run_state",
                        "artifacts": [{"artifact_id": "artifact_state"}],
                    }
                ),
            }
        )
        self.assertEqual(interactive._state.last_run_id, "run_state")
        self.assertEqual(interactive._state.last_artifact_id, "artifact_state")

    def test_help_is_available_for_top_level_and_subcommands(self):
        cases = [
            [],
            ["run"],
            ["run", "start"],
            ["run", "list"],
            ["run", "result"],
            ["run", "diff"],
            ["run", "show"],
            ["run", "status"],
            ["run", "cleanup"],
            ["artifacts"],
            ["artifacts", "list"],
            ["artifacts", "preview"],
            ["file-sql"],
            ["embedding-build"],
            ["embedding-query"],
            ["vector-search"],
        ]
        for argv in cases:
            stdout = io.StringIO()
            stderr = io.StringIO()
            with self.subTest(argv=argv):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = velaria_cli.main([*argv, "--help"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                self.assertIn("usage:", stdout.getvalue())

    def test_run_start_persists_metadata_and_run_list_filters(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-meta-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={"payload": {"rows": []}, "artifacts": []},
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--run-name",
                                "cn-slow-query",
                                "--description",
                                "CN slow query snapshot for cache replay triage",
                                "--tag",
                                "cn",
                                "--tag",
                                "slow-query,cache",
                                "--",
                                "file-sql",
                                "--csv",
                                "/tmp/input.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["run_name"], "cn-slow-query")
                self.assertEqual(
                    run_meta["description"],
                    "CN slow query snapshot for cache replay triage",
                )
                self.assertEqual(run_meta["tags"], ["cn", "slow-query", "cache"])
                inputs = json.loads(
                    (pathlib.Path(run_meta["run_dir"]) / "inputs.json").read_text(encoding="utf-8")
                )
                self.assertEqual(inputs["run_name"], "cn-slow-query")
                self.assertEqual(
                    inputs["description"],
                    "CN slow query snapshot for cache replay triage",
                )
                self.assertEqual(inputs["tags"], ["cn", "slow-query", "cache"])

                list_stdout = io.StringIO()
                with redirect_stdout(list_stdout):
                    list_exit_code = velaria_cli.main(
                        [
                            "run",
                            "list",
                            "--status",
                            "succeeded",
                            "--tag",
                            "slow-query",
                            "--query",
                            "cache replay",
                        ]
                    )
                self.assertEqual(list_exit_code, 0)
                list_payload = json.loads(list_stdout.getvalue())
                self.assertEqual(len(list_payload["runs"]), 1)
                self.assertEqual(list_payload["runs"][0]["run_id"], payload["run_id"])
                self.assertEqual(list_payload["runs"][0]["tags"], ["cn", "slow-query", "cache"])
                self.assertEqual(list_payload["runs"][0]["artifact_count"], 0)
                self.assertIsNotNone(list_payload["runs"][0]["duration_ms"])

    def test_run_start_normalizes_duplicate_and_blank_tags(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-tags-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={"payload": {"rows": []}, "artifacts": []},
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--tag",
                                " cn , slow-query , cn , ",
                                "--tag",
                                "analytics",
                                "--tag",
                                "slow-query",
                                "--",
                                "file-sql",
                                "--csv",
                                "/tmp/input.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["tags"], ["cn", "slow-query", "analytics"])
                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["tags"], ["cn", "slow-query", "analytics"])

    def test_workspace_errors_return_json_without_stderr_noise(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-errors-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                cases = [
                    (["run", "start", "--"], "run start requires an action", "argument_parse"),
                    (
                        ["run", "show", "--run-id", "missing-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["run", "result", "--run-id", "missing-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["run", "diff", "--run-id", "missing-run", "--other-run-id", "other-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["artifacts", "preview", "--artifact-id", "missing-artifact"],
                        "artifact not found: missing-artifact",
                        "artifact_lookup",
                    ),
                ]
                for argv, expected_error, expected_phase in cases:
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    with self.subTest(argv=argv):
                        with redirect_stdout(stdout), redirect_stderr(stderr):
                            exit_code = velaria_cli.main(argv)
                        self.assertEqual(exit_code, 1)
                        self.assertEqual(stderr.getvalue(), "")
                        payload = json.loads(stdout.getvalue())
                        self.assertFalse(payload["ok"])
                        self.assertIn(expected_error, payload["error"])
                        self.assertIn("error_type", payload)
                        self.assertIn("phase", payload)
                        self.assertEqual(payload["phase"], expected_phase)
                        self.assertIsInstance(payload.get("details", {}), dict)

    def test_run_start_failure_returns_structured_error_and_persists_details(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-failure-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={
                        "error": "failed to read csv input",
                        "error_type": "value_error",
                        "phase": "csv_read",
                        "details": {
                            "csv": "/tmp/missing.csv",
                            "table": "input_table",
                        },
                    },
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--description",
                                "failure case",
                                "--",
                                "file-sql",
                                "--csv",
                                "/tmp/missing.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 1)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["error_type"], "value_error")
                self.assertEqual(payload["phase"], "csv_read")
                self.assertEqual(payload["details"]["csv"], "/tmp/missing.csv")
                self.assertIsNotNone(payload["run_id"])

                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["status"], "failed")
                self.assertEqual(run_meta["details"]["error_type"], "value_error")
                self.assertEqual(run_meta["details"]["phase"], "csv_read")
                self.assertEqual(
                    run_meta["details"]["error_details"]["csv"],
                    "/tmp/missing.csv",
                )

    def test_run_result_and_diff_fail_when_result_artifact_is_missing(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-missing-result-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = workspace.ArtifactIndex()
                run_one_id, run_one_dir = workspace.create_run("file-sql", {"query": "SELECT 1"}, "0.0.test")
                run_two_id, run_two_dir = workspace.create_run("file-sql", {"query": "SELECT 2"}, "0.0.test")
                index.upsert_run(workspace.read_run(run_one_id))
                index.upsert_run(workspace.read_run(run_two_id))
                pathlib.Path(run_two_dir, "artifacts", "explain.txt").write_text("logical", encoding="utf-8")
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-explain",
                        "run_id": run_two_id,
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": pathlib.Path(run_two_dir, "artifacts", "explain.txt").resolve().as_uri(),
                        "format": "text",
                        "row_count": None,
                        "schema_json": None,
                        "preview_json": None,
                        "tags_json": ["explain"],
                    }
                )

                for argv in (
                    ["run", "result", "--run-id", run_one_id],
                    ["run", "diff", "--run-id", run_one_id, "--other-run-id", run_two_id],
                ):
                    stdout = io.StringIO()
                    with self.subTest(argv=argv):
                        with redirect_stdout(stdout):
                            exit_code = velaria_cli.main(argv)
                        self.assertEqual(exit_code, 1)
                        payload = json.loads(stdout.getvalue())
                        self.assertEqual(payload["error_type"], "file_not_found")
                        self.assertEqual(payload["phase"], "run_result")
                        self.assertEqual(payload["run_id"], run_one_id)

    def test_csv_sql_sql_input_registration_failure_reports_phase(self):
        fake_session = mock.Mock()
        fake_session.sql.side_effect = RuntimeError("boom")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-csv-view-fail-") as tmp:
            csv_path = pathlib.Path(tmp) / "input.csv"
            csv_path.write_text("name,score\nalice,1\n", encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        [
                            "file-sql",
                            "--csv",
                            str(csv_path),
                            "--table",
                            "input_table",
                            "--query",
                            "SELECT * FROM input_table",
                        ]
                    )
            self.assertEqual(exit_code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["phase"], "sql_input_register")
            self.assertEqual(payload["details"]["input_path"], str(csv_path))
            self.assertEqual(payload["details"]["input_type"], "auto")
            self.assertEqual(payload["details"]["table"], "input_table")

    def test_csv_sql_json_input_builds_create_table_sql(self):
        fake_session = mock.Mock()
        fake_session.sql.side_effect = [mock.Mock(name="create_result"), _FakeDataFrame()]
        with tempfile.TemporaryDirectory(prefix="velaria-cli-json-sql-") as tmp:
            json_path = pathlib.Path(tmp) / "input.jsonl"
            json_path.write_text('{"row_id":1,"score":0.5}\n', encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        [
                            "file-sql",
                            "--input-path",
                            str(json_path),
                            "--input-type",
                            "json",
                            "--columns",
                            "row_id,score",
                            "--query",
                            "SELECT row_id, score FROM input_table",
                        ]
                    )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["input_type"], "json")
        create_sql = fake_session.sql.call_args_list[0].args[0]
        self.assertIn("CREATE TABLE input_table USING json OPTIONS(", create_sql)
        self.assertIn("columns: 'row_id,score'", create_sql)

    def test_stream_sql_once_start_and_wait_failures_report_phase(self):
        workspace = importlib.import_module("velaria.workspace")

        class _FailingQuery:
            def __init__(self, fail_on: str) -> None:
                self.fail_on = fail_on

            def start(self):
                if self.fail_on == "start":
                    raise RuntimeError("start failed")

            def snapshot_json(self):
                return "{\"status\":\"running\"}"

            def await_termination(self, max_batches: int):
                if self.fail_on == "wait":
                    raise RuntimeError("wait failed")
                return max_batches

            def progress(self):
                return {"status": "running"}

        def _make_session(fail_on: str):
            fake_session = mock.Mock()
            fake_session.read_stream_csv_dir.return_value = mock.Mock(name="stream_df")
            fake_session.explain_stream_sql.return_value = "logical\nscan\nphysical\nplan\nstrategy\nselected_mode=single-process"
            fake_session.start_stream_sql.return_value = _FailingQuery(fail_on)
            return fake_session

        with tempfile.TemporaryDirectory(prefix="velaria-cli-stream-fail-") as tmp:
            run_id, run_dir = workspace.create_run("stream-sql-once", {"query": "demo"}, "0.0.test")
            source_dir = pathlib.Path(tmp) / "source"
            source_dir.mkdir(parents=True)
            for fail_on, expected_phase in (("start", "stream_start"), ("wait", "stream_wait")):
                fake_session = _make_session(fail_on)
                with self.subTest(fail_on=fail_on):
                    with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                        with self.assertRaises(velaria_cli.CliStructuredError) as ctx:
                            velaria_cli._execute_stream_sql_once(
                                source_csv_dir=source_dir,
                                source_table="input_stream",
                                source_delimiter=",",
                                sink_table="output_sink",
                                sink_schema="key STRING, value_sum INT",
                                sink_path=pathlib.Path(run_dir) / "artifacts" / "stream_result.csv",
                                sink_delimiter=",",
                                query="INSERT INTO output_sink SELECT key, 1 AS value_sum FROM input_stream",
                                trigger_interval_ms=0,
                                checkpoint_delivery_mode="best-effort",
                                execution_mode="single-process",
                                local_workers=1,
                                max_inflight_partitions=0,
                                max_batches=1,
                                run_id=run_id,
                            )
                    self.assertEqual(ctx.exception.phase, expected_phase)

    def test_artifact_preview_cache_miss_reports_full_row_count_for_parquet(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-preview-") as tmp:
            parquet_path = pathlib.Path(tmp) / "artifact.parquet"
            table = pa.table({"name": ["alice", "bob", "carol"], "score": [1, 2, 3]})
            pq.write_table(table, parquet_path)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = workspace.ArtifactIndex()
                run_dir = pathlib.Path(tmp) / "runs" / "run-1"
                run_dir.mkdir(parents=True)
                index.upsert_run(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": "2026-04-01T10:00:01Z",
                        "status": "succeeded",
                        "action": "file-sql",
                        "cli_args": {},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                    }
                )
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-1",
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": parquet_path.resolve().as_uri(),
                        "format": "parquet",
                        "row_count": 3,
                        "schema_json": ["name", "score"],
                        "preview_json": None,
                        "tags_json": ["result"],
                    }
                )
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        ["artifacts", "preview", "--artifact-id", "artifact-1", "--limit", "2"]
                    )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["preview"]["row_count"], 3)
                self.assertTrue(payload["preview"]["truncated"])
                self.assertEqual(len(payload["preview"]["rows"]), 2)

    def test_artifact_preview_cached_payload_still_respects_requested_limit(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-preview-cache-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = workspace.ArtifactIndex()
                run_dir = pathlib.Path(tmp) / "runs" / "run-1"
                run_dir.mkdir(parents=True)
                index.upsert_run(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": "2026-04-01T10:00:01Z",
                        "status": "succeeded",
                        "action": "file-sql",
                        "cli_args": {},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                    }
                )
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-1",
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": pathlib.Path(tmp, "artifact.json").resolve().as_uri(),
                        "format": "json",
                        "row_count": 3,
                        "schema_json": ["name"],
                        "preview_json": {
                            "schema": ["name"],
                            "rows": [{"name": "alice"}, {"name": "bob"}, {"name": "carol"}],
                            "row_count": 3,
                            "truncated": False,
                        },
                        "tags_json": ["result"],
                    }
                )
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        ["artifacts", "preview", "--artifact-id", "artifact-1", "--limit", "1"]
                    )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["preview"]["row_count"], 3)
                self.assertTrue(payload["preview"]["truncated"])
                self.assertEqual(len(payload["preview"]["rows"]), 1)

    def test_vector_cli_delegates_to_session_contract(self):
        fake_session = mock.Mock()
        fake_session.read_csv.return_value = mock.Mock(name="df")
        fake_session.vector_search.return_value = _FakeDataFrame()
        fake_session.explain_vector_search.return_value = (
            "mode=exact-scan\n"
            "metric=cosine\n"
            "dimension=3\n"
            "top_k=2\n"
            "candidate_rows=3\n"
            "filter_pushdown=false\n"
            "acceleration=flat-buffer+simd-topk\n"
            "backend=neon\n"
        )

        with tempfile.TemporaryDirectory(prefix="velaria-cli-contract-") as tmp:
            csv_path = pathlib.Path(tmp) / "vectors.csv"
            csv_path.write_text("id,embedding\n1,[1 0 0]\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli._run_vector_search(
                        csv_path=csv_path,
                        vector_column="embedding",
                        query_vector="1.0,0.0,0.0",
                        metric="cosine",
                        top_k=2,
                    )

        self.assertEqual(exit_code, 0)
        fake_session.read_csv.assert_called_once_with(str(csv_path))
        fake_session.create_temp_view.assert_called_once()
        fake_session.vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )
        fake_session.explain_vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["metric"], "cosine")
        self.assertEqual(payload["top_k"], 2)
        self.assertEqual(payload["schema"], ["row_id", "score"])
        self.assertEqual(payload["rows"], [{"row_id": 0, "score": 0.0}])
        self.assertIn("mode=exact-scan", payload["explain"])
        self.assertIn("candidate_rows=3", payload["explain"])
        self.assertIn("filter_pushdown=false", payload["explain"])

    def test_vector_cli_hybrid_search_delegates_to_session_contract(self):
        fake_session = mock.Mock()
        fake_df = mock.Mock(name="df")
        fake_df.filter.return_value = mock.Mock(name="filtered_df")
        fake_session.read_csv.return_value = fake_df
        fake_session.hybrid_search.return_value = _FakeDataFrame()
        fake_session.explain_hybrid_search.return_value = (
            "mode=exact-scan-hybrid-search\n"
            "metric=cosine\n"
            "top_k=2\n"
            "score_threshold=0.02\n"
            "candidate_rows=2\n"
            "returned_rows=1\n"
            "column_filter_execution=post-load-filter\n"
            "backend=neon\n"
        )

        with tempfile.TemporaryDirectory(prefix="velaria-cli-contract-") as tmp:
            csv_path = pathlib.Path(tmp) / "vectors.csv"
            csv_path.write_text("id,bucket,embedding\n1,1,[1 0 0]\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli._run_vector_search(
                        csv_path=csv_path,
                        vector_column="embedding",
                        query_vector="1.0,0.0,0.0",
                        metric="cosine",
                        top_k=2,
                        where_column="bucket",
                        where_op="=",
                        where_value="1",
                        score_threshold=0.02,
                    )

        self.assertEqual(exit_code, 0)
        fake_session.read_csv.assert_called_once_with(str(csv_path))
        self.assertEqual(fake_session.create_temp_view.call_count, 2)
        fake_session.create_temp_view.assert_any_call("input_table", fake_df)
        fake_session.sql.assert_called_once_with("SELECT * FROM input_table WHERE bucket = 1")
        fake_session.create_temp_view.assert_any_call("input_table_hybrid", fake_session.sql.return_value)
        fake_session.hybrid_search.assert_called_once_with(
            table="input_table_hybrid",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
            score_threshold=0.02,
        )
        fake_session.explain_hybrid_search.assert_called_once_with(
            table="input_table_hybrid",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
            score_threshold=0.02,
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["metric"], "cosine")
        self.assertEqual(payload["top_k"], 2)
        self.assertEqual(payload["where_column"], "bucket")
        self.assertEqual(payload["where_op"], "=")
        self.assertEqual(payload["where_value"], "1")
        self.assertEqual(payload["score_threshold"], 0.02)
        self.assertIn("mode=exact-scan-hybrid-search", payload["explain"])

    def test_embedding_build_cli_delegates_to_pipeline_helper(self):
        fake_table = pa.table(
            {
                "doc_id": ["doc-1"],
                "embedding": pa.array([[1.0, 0.0, 0.0]], type=pa.list_(pa.float32(), 3)),
            }
        )
        with tempfile.TemporaryDirectory(prefix="velaria-cli-embed-build-") as tmp:
            input_path = pathlib.Path(tmp) / "docs.csv"
            output_path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            input_path.write_text("doc_id,title,summary\n1,a,b\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=mock.Mock()):
                with mock.patch.object(velaria_cli, "build_file_embeddings", return_value=fake_table) as build_file:
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "embedding-build",
                                "--input-path",
                                str(input_path),
                                "--input-type",
                                "csv",
                                "--text-columns",
                                "title,summary",
                                "--provider",
                                "hash",
                                "--output-path",
                                str(output_path),
                            ]
                        )
        self.assertEqual(exit_code, 0)
        build_file.assert_called_once()
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["provider"], "hash")
        self.assertEqual(payload["text_columns"], ["title", "summary"])
        self.assertEqual(payload["row_count"], 1)

    def test_embedding_query_cli_delegates_to_pipeline_helper(self):
        fake_result = _FakeDataFrame()
        with tempfile.TemporaryDirectory(prefix="velaria-cli-embed-query-") as tmp:
            input_path = pathlib.Path(tmp) / "docs.csv"
            input_path.write_text("doc_id,title,summary\n1,a,b\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=mock.Mock()):
                with mock.patch.object(
                    velaria_cli,
                    "run_file_mixed_text_hybrid_search",
                    return_value=fake_result,
                ) as run_pipeline:
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "embedding-query",
                                "--input-path",
                                str(input_path),
                                "--input-type",
                                "csv",
                                "--text-columns",
                                "title,summary",
                                "--provider",
                                "hash",
                                "--query-text",
                                "find alpha",
                                "--top-k",
                                "2",
                                "--metric",
                                "cosine",
                            ]
                        )
        self.assertEqual(exit_code, 0)
        run_pipeline.assert_called_once()
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["provider"], "hash")
        self.assertEqual(payload["query_text"], "find alpha")
        self.assertEqual(payload["top_k"], 2)

    def test_embedding_query_cli_accepts_where_sql(self):
        fake_result = _FakeDataFrame()
        with tempfile.TemporaryDirectory(prefix="velaria-cli-embed-query-sql-") as tmp:
            dataset_path = pathlib.Path(tmp) / "docs_embeddings.parquet"
            dataset_path.write_text("placeholder", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=mock.Mock()):
                with mock.patch.object(
                    velaria_cli,
                    "query_file_embeddings",
                    return_value=fake_result,
                ) as query_embeddings:
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "embedding-query",
                                "--dataset-path",
                                str(dataset_path),
                                "--provider",
                                "hash",
                                "--query-text",
                                "find alpha",
                                "--where-sql",
                                "bucket = 1 AND region = 'apac'",
                                "--top-k",
                                "2",
                            ]
                        )
        self.assertEqual(exit_code, 0)
        query_embeddings.assert_called_once()
        self.assertEqual(query_embeddings.call_args.kwargs["where_sql"], "bucket = 1 AND region = 'apac'")
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["where_sql"], "bucket = 1 AND region = 'apac'")


if __name__ == "__main__":
    unittest.main()
