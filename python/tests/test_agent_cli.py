import asyncio
import io
import importlib
import json
import pathlib
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

try:
    velaria_cli = importlib.import_module("velaria_cli")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_cli = importlib.import_module("velaria_cli")


class _FakeTty(io.StringIO):
    def isatty(self):
        return True


class _FakeRuntime:
    def __init__(self, model="gpt-5.4-mini"):
        self.started = []
        self.messages = []
        self.closed = []
        self.status_calls = 0
        self.model = model
        self.shutdown_called = False

    async def start_thread(self, dataset_context=None):
        self.started.append(dataset_context or {})
        return "agent-session-1"

    async def resume_thread(self, session_id):
        return session_id == "agent-session-1"

    async def send_message(self, session_id, prompt):
        from velaria.ai_runtime.agent import AgentEvent

        self.messages.append((session_id, prompt))
        yield AgentEvent("assistant_text", "hello from velaria", session_id=session_id)
        yield AgentEvent("done", "", session_id=session_id)

    async def list_threads(self):
        return [
            {
                "session_id": "agent-session-1",
                "runtime_type": "codex",
                "status": "active",
                "last_active_at": "2026-06-13T00:00:00Z",
                "dataset_context": {},
            }
        ]

    async def close_thread(self, session_id):
        self.closed.append(session_id)

    def status(self, session_id=None):
        self.status_calls += 1
        return {
            "runtime": "codex",
            "model": self.model,
            "tools": ["velaria_sql", "velaria_cli_run"],
            "session": {"session_id": session_id},
        }

    def shutdown(self):
        self.shutdown_called = True


class AgentCliTest(unittest.TestCase):
    def test_no_args_enters_velaria_agent_tui(self):
        with mock.patch("velaria.cli.agent.entry._stdio_is_tty", return_value=True):
            with mock.patch("velaria.cli.agent.entry.run_agent_tui", return_value=0) as run_tui:
                exit_code = velaria_cli.main([])
        self.assertEqual(exit_code, 0)
        run_tui.assert_called_once()
        args = run_tui.call_args.args[0]
        self.assertIsNone(args.prompt)
        self.assertEqual(args.runtime, "")

    def test_interactive_flag_is_compat_alias_for_agent_tui(self):
        with mock.patch("velaria.cli.agent.entry._stdio_is_tty", return_value=True):
            with mock.patch("velaria.cli.agent.entry.run_agent_tui", return_value=0) as run_tui:
                exit_code = velaria_cli.main(["-i", "--runtime", "claude"])
        self.assertEqual(exit_code, 0)
        args = run_tui.call_args.args[0]
        self.assertEqual(args.runtime, "claude")

    def test_interactive_alias_only_applies_at_top_level(self):
        from velaria.cli.agent.entry import wants_agent_alias

        self.assertTrue(wants_agent_alias(["--interactive"]))
        self.assertFalse(wants_agent_alias(["file-sql", "--interactive"]))

    def test_no_args_in_non_tty_reports_structured_error(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.cli.agent.entry._stdio_is_tty", return_value=False):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = velaria_cli.main([])
        self.assertEqual(exit_code, 1)
        self.assertEqual(stderr.getvalue(), "")
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error_type"], "non_tty_agent_entry")
        self.assertIn("velaria-cli agent --print", payload["hint"])

    def test_help_does_not_start_agent_tui(self):
        stdout = io.StringIO()
        with mock.patch("velaria.cli.agent.entry.run_agent_tui") as run_tui:
            with redirect_stdout(stdout):
                exit_code = velaria_cli.main(["--help"])
        self.assertEqual(exit_code, 0)
        run_tui.assert_not_called()
        self.assertIn("usage:", stdout.getvalue())

    def test_agent_print_outputs_final_assistant_text_only(self):
        fake = _FakeRuntime()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("velaria.ai_runtime.create_runtime", return_value=fake):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = velaria_cli.main(["agent", "--print", "say hello"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(stdout.getvalue(), "hello from velaria\n")
        self.assertEqual(fake.messages, [("agent-session-1", "say hello")])

    def test_agent_model_argument_overrides_runtime_model(self):
        fake = _FakeRuntime()
        captured = {}

        def fake_create_runtime(config):
            captured["config"] = config
            return fake

        stdout = io.StringIO()
        with mock.patch("velaria.ai_runtime.create_runtime", side_effect=fake_create_runtime):
            with redirect_stdout(stdout):
                exit_code = velaria_cli.main(["agent", "--model", "gpt-custom", "--print", "say hello"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["config"]["model"], "gpt-custom")
        self.assertEqual(stdout.getvalue(), "hello from velaria\n")

    def test_agent_stream_json_outputs_turn_events(self):
        fake = _FakeRuntime()
        stdout = io.StringIO()
        with mock.patch("velaria.ai_runtime.create_runtime", return_value=fake):
            with redirect_stdout(stdout):
                exit_code = velaria_cli.main(["agent", "--stream-json", "say hello"])
        self.assertEqual(exit_code, 0)
        lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([line["type"] for line in lines], ["session.started", "assistant_text", "done"])
        self.assertEqual(lines[0]["session_id"], "agent-session-1")
        self.assertEqual(lines[1]["content"], "hello from velaria")

    def test_agent_tui_constructs_velaria_textual_app(self):
        from velaria.cli.agent.entry import AgentCommandArgs
        from velaria.cli.agent.tui_app import VelariaAgentApp, run_tui

        fake = _FakeRuntime()
        captured = {}

        def fake_run(app):
            captured["app"] = app

        with mock.patch("velaria.cli.agent.tui_app._controller_from_args", return_value=fake):
            with mock.patch.object(VelariaAgentApp, "run", autospec=True, side_effect=fake_run) as app_run:
                exit_code = run_tui(
                    AgentCommandArgs(
                        runtime="claude",
                        prompt="inspect data",
                        session="agent-session-1",
                        new=True,
                    )
                )
        self.assertEqual(exit_code, 0)
        app_run.assert_called_once()
        app = captured["app"]
        self.assertIs(app.controller, fake)
        self.assertEqual(app.initial_prompt, "inspect data")
        self.assertEqual(app.session_id, "agent-session-1")
        self.assertTrue(app.new_session)

    def test_agent_tui_set_model_rebuilds_controller(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.entry import AgentCommandArgs
        from velaria.cli.agent.tui_app import VelariaAgentApp

        old_runtime = _FakeRuntime("gpt-5.4-mini")
        created_models = []

        def controller_factory(args):
            created_models.append(args.model)
            return InteractiveController(_FakeRuntime(args.model))

        async def drive_app():
            app = VelariaAgentApp(
                InteractiveController(old_runtime),
                args=AgentCommandArgs(runtime="codex"),
                controller_factory=controller_factory,
            )
            async with app.run_test() as pilot:
                await pilot.pause()
                await app._set_model("gpt-custom")
                await pilot.pause()
                return app.controller.state.model, app.args.model

        model, args_model = asyncio.run(drive_app())
        self.assertTrue(old_runtime.shutdown_called)
        self.assertEqual(created_models, ["gpt-custom"])
        self.assertEqual(model, "gpt-custom")
        self.assertEqual(args_model, "gpt-custom")

    def test_agent_tui_ctrl_m_model_picker_selects_model(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.entry import AgentCommandArgs
        from velaria.cli.agent.tui_app import ModelSelectScreen, VelariaAgentApp

        old_runtime = _FakeRuntime("gpt-5.4-mini")
        created_models = []

        def controller_factory(args):
            created_models.append(args.model)
            return InteractiveController(_FakeRuntime(args.model))

        async def drive_app():
            app = VelariaAgentApp(
                InteractiveController(old_runtime),
                args=AgentCommandArgs(runtime="codex"),
                controller_factory=controller_factory,
            )
            async with app.run_test() as pilot:
                await pilot.pause()
                await pilot.press("ctrl+m")
                await pilot.pause()
                picker_open = isinstance(app.screen, ModelSelectScreen)
                await pilot.press("down")
                await pilot.press("enter")
                await pilot.pause(0.3)
                return picker_open, app.controller.state.model, app.args.model

        picker_open, model, args_model = asyncio.run(drive_app())
        self.assertTrue(picker_open)
        self.assertEqual(created_models, ["gpt-5.4"])
        self.assertEqual(model, "gpt-5.4")
        self.assertEqual(args_model, "gpt-5.4")

    def test_agent_tui_mount_handles_initial_prompt(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        async def drive_app():
            fake = _FakeRuntime()
            app = VelariaAgentApp(InteractiveController(fake), initial_prompt="say hello")
            async with app.run_test():
                pass
            return fake

        fake = asyncio.run(drive_app())
        self.assertEqual(fake.messages, [("agent-session-1", "say hello")])

    def test_agent_tui_focuses_composer_on_mount(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        async def drive_app():
            fake = _FakeRuntime()
            app = VelariaAgentApp(InteractiveController(fake))
            async with app.run_test() as pilot:
                await pilot.pause()
                return getattr(app.focused, "id", "")

        self.assertEqual(asyncio.run(drive_app()), "composer")

    def test_agent_tui_priority_shortcuts_work_with_composer_focused(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        async def drive_app():
            fake = _FakeRuntime()
            app = VelariaAgentApp(InteractiveController(fake))
            async with app.run_test() as pilot:
                await pilot.pause()
                self.assertEqual(getattr(app.focused, "id", ""), "composer")
                initial_status_calls = fake.status_calls
                await pilot.press("ctrl+n")
                await pilot.pause()
                started_after_new = len(fake.started)
                await pilot.press("ctrl+r")
                await pilot.pause()
                status_after_refresh = fake.status_calls
                await pilot.press("ctrl+c")
                await pilot.pause()
                return started_after_new, status_after_refresh, initial_status_calls, app.is_running

        started_after_new, status_after_refresh, initial_status_calls, is_running = asyncio.run(drive_app())
        self.assertEqual(started_after_new, 2)
        self.assertGreater(status_after_refresh, initial_status_calls)
        self.assertFalse(is_running)

    def test_agent_tui_history_scrolls_with_composer_focused(self):
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        async def drive_app():
            fake = _FakeRuntime()
            app = VelariaAgentApp(InteractiveController(fake))
            async with app.run_test(size=(80, 12)) as pilot:
                await pilot.pause()
                self.assertEqual(getattr(app.focused, "id", ""), "composer")
                transcript = app.query_one("#transcript")
                for index in range(80):
                    transcript.write(f"line {index}")
                await pilot.pause()
                transcript.scroll_end(animate=False, immediate=True)
                await pilot.pause()
                end_offset = transcript.scroll_offset.y
                await pilot.press("pageup")
                await pilot.pause()
                up_offset = transcript.scroll_offset.y
                await pilot.press("pagedown")
                await pilot.pause()
                down_offset = transcript.scroll_offset.y
                return end_offset, up_offset, down_offset

        end_offset, up_offset, down_offset = asyncio.run(drive_app())
        self.assertLess(up_offset, end_offset)
        self.assertGreaterEqual(down_offset, up_offset)

    def test_agent_tui_keeps_input_open_and_queues_prompts_while_running(self):
        from velaria.ai_runtime.agent import AgentEvent
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        class SlowRuntime(_FakeRuntime):
            def __init__(self):
                super().__init__()
                self.release = asyncio.Event()

            async def send_message(self, session_id, prompt):
                self.messages.append((session_id, prompt))
                await self.release.wait()
                yield AgentEvent("assistant_text", f"done {prompt}", session_id=session_id)
                yield AgentEvent("done", "", session_id=session_id)

        async def drive_app():
            fake = SlowRuntime()
            app = VelariaAgentApp(InteractiveController(fake))
            async with app.run_test() as pilot:
                await pilot.pause()
                composer = app.query_one("#composer")
                composer.value = "first"
                await pilot.press("enter")
                await pilot.pause(0.05)
                first_input_disabled = bool(composer.disabled)
                composer.value = "second"
                await pilot.press("enter")
                await pilot.pause(0.3)
                queued_count = len(app._pending_prompts)
                activity = str(app.query_one("#activity").content)
                fake.release.set()
                await pilot.pause(0.5)
                return first_input_disabled, queued_count, activity, fake.messages

        first_input_disabled, queued_count, activity, messages = asyncio.run(drive_app())
        self.assertFalse(first_input_disabled)
        self.assertEqual(queued_count, 1)
        self.assertIn("running", activity)
        self.assertIn("queued 1", activity)
        self.assertEqual(messages, [("agent-session-1", "first"), ("agent-session-1", "second")])

    def test_agent_tui_streams_assistant_markdown_before_turn_finishes(self):
        from rich.console import Group
        from rich.markdown import Markdown
        from velaria.ai_runtime.agent import AgentEvent
        from velaria.cli.agent.controller import InteractiveController
        from velaria.cli.agent.tui_app import VelariaAgentApp

        class StreamingRuntime(_FakeRuntime):
            def __init__(self):
                super().__init__()
                self.release = asyncio.Event()

            async def send_message(self, session_id, prompt):
                self.messages.append((session_id, prompt))
                yield AgentEvent("assistant_text", "**first**", session_id=session_id)
                await self.release.wait()
                yield AgentEvent("assistant_text", "\n- second", session_id=session_id)
                yield AgentEvent("done", "", session_id=session_id)

        async def drive_app():
            fake = StreamingRuntime()
            app = VelariaAgentApp(InteractiveController(fake))
            async with app.run_test() as pilot:
                await pilot.pause()
                composer = app.query_one("#composer")
                composer.value = "stream"
                await pilot.press("enter")
                await pilot.pause(0.1)
                live_content = app.query_one("#live_response").content
                fake.release.set()
                await pilot.pause(0.3)
                cleared_content = str(app.query_one("#live_response").content)
                return live_content, cleared_content, fake.messages

        live_content, cleared_content, messages = asyncio.run(drive_app())
        self.assertIsInstance(live_content, Group)
        self.assertIsInstance(live_content.renderables[1], Markdown)
        self.assertEqual(live_content.renderables[1].markup, "**first**")
        self.assertEqual(cleared_content, "")
        self.assertEqual(messages, [("agent-session-1", "stream")])

    def test_agent_tui_turn_render_buffer_coalesces_assistant_chunks(self):
        from rich.markdown import Markdown
        from velaria.cli.agent.events import VelariaAgentEvent
        from velaria.cli.agent.tui_app import TurnRenderBuffer

        buffer = TurnRenderBuffer()
        self.assertIsNone(buffer.observe(VelariaAgentEvent("assistant_text", "**hello** ")))
        self.assertIsNone(buffer.observe(VelariaAgentEvent("assistant_text", "- from velaria")))
        rendered = buffer.finish()
        self.assertEqual(rendered[0], "[b]Velaria[/b]")
        self.assertIsInstance(rendered[1], Markdown)
        self.assertEqual(rendered[1].markup, "**hello** - from velaria")

    def test_agent_tui_turn_render_buffer_reports_empty_assistant_turn(self):
        from velaria.cli.agent.events import VelariaAgentEvent
        from velaria.cli.agent.tui_app import TurnRenderBuffer

        buffer = TurnRenderBuffer()
        self.assertIsNone(buffer.observe(VelariaAgentEvent("done")))
        rendered = buffer.finish()
        self.assertEqual(len(rendered), 1)
        self.assertIn("no visible answer", rendered[0])
        self.assertIn("agent --stream-json", rendered[0])

    def test_agent_tui_turn_render_buffer_reports_tool_without_final_text(self):
        from velaria.cli.agent.events import VelariaAgentEvent
        from velaria.cli.agent.tui_app import TurnRenderBuffer

        buffer = TurnRenderBuffer()
        line = buffer.observe(VelariaAgentEvent("tool_result", data={"function": "velaria_sql"}))
        self.assertEqual(line, "[green]tool[/green] velaria_sql")
        rendered = buffer.finish()
        self.assertEqual(len(rendered), 1)
        self.assertIn("Tool calls completed", rendered[0])

    def test_agent_controller_normalizes_runtime_events_and_state(self):
        from velaria.ai_runtime.agent import AgentEvent
        from velaria.cli.agent.controller import InteractiveController

        class Runtime(_FakeRuntime):
            async def send_message(self, session_id, prompt):
                yield AgentEvent(
                    "tool_result",
                    "",
                    session_id=session_id,
                    data={
                        "function": "velaria_sql",
                        "row_count": 2,
                        "schema": ["region", "amount"],
                        "run_id": "run_1",
                        "artifact_id": "artifact_1",
                    },
                )
                yield AgentEvent("done", "", session_id=session_id)

        controller = InteractiveController(Runtime())
        events = asyncio.run(_collect(controller.send_turn("query data")))
        self.assertEqual([event.type for event in events], ["session.started", "tool_result", "done"])
        self.assertEqual(controller.state.last_run_id, "run_1")
        self.assertEqual(controller.state.last_artifact_id, "artifact_1")
        self.assertEqual(controller.state.result_schema, ["region", "amount"])
        self.assertEqual(controller.state.result_row_count, 2)

    def test_agent_controller_new_session_does_not_resume_requested_session(self):
        from velaria.cli.agent.controller import InteractiveController

        fake = _FakeRuntime()
        controller = InteractiveController(fake)
        event = asyncio.run(controller.ensure_session(session_id="agent-session-1", new_session=True))
        self.assertEqual(event.type, "session.started")
        self.assertEqual(fake.started, [{}])


async def _collect(source):
    return [event async for event in source]


if __name__ == "__main__":
    unittest.main()
