from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from rich.console import Group
from rich.markdown import Markdown
from rich.markup import escape
from rich.text import Text

from .controller import InteractiveController
from .events import VelariaAgentEvent
from .entry import AgentCommandArgs, _controller_from_args

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.screen import ModalScreen
    from textual.timer import Timer
    from textual.widgets import Footer, Header, Input, OptionList, RichLog, Static
    from textual.widgets.option_list import Option

    _TEXTUAL_IMPORT_ERROR: ModuleNotFoundError | None = None
except ModuleNotFoundError as exc:
    if exc.name != "textual":
        raise
    _TEXTUAL_IMPORT_ERROR = exc

    class _MissingApp:
        pass

    class _MissingWidget:
        pass

    class _MissingBinding:
        def __init__(self, *_args: Any, **_kwargs: Any):
            pass

    App = _MissingApp
    Binding = _MissingBinding
    ComposeResult = Any
    Footer = Header = Input = Option = OptionList = RichLog = Static = Vertical = _MissingWidget
    ModalScreen = _MissingWidget
    Timer = Any


CODEX_MODEL_CANDIDATES = (
    "gpt-5.4-mini",
    "gpt-5.4",
    "gpt-5.4-codex",
)
CLAUDE_MODEL_CANDIDATES = (
    "claude-sonnet-4-20250514",
    "claude-opus-4-20250514",
)


class Transcript(RichLog):
    pass


class StatusBar(Static):
    pass


class LiveResponse(Static):
    pass


class ModelSelectScreen(ModalScreen[str | None]):
    CSS = """
    ModelSelectScreen {
        align: center middle;
    }
    #model_picker {
        width: 72;
        max-height: 70%;
        border: solid $accent;
        background: $surface;
        padding: 1 2;
    }
    #model_options {
        height: auto;
        max-height: 16;
    }
    """
    BINDINGS = [
        Binding("escape", "cancel", "Cancel", priority=True),
    ]

    def __init__(self, *, runtime: str, current_model: str, candidates: list[str]):
        super().__init__()
        self.runtime = runtime or "-"
        self.current_model = current_model or "-"
        self.candidates = candidates

    def compose(self) -> ComposeResult:
        with Vertical(id="model_picker"):
            yield Static(f"[b]Model[/b] {escape(self.runtime)} | current: {escape(self.current_model)}")
            yield OptionList(
                *[
                    Option(_model_option_label(model, self.current_model), id=model)
                    for model in self.candidates
                ],
                Option("[dim]Custom model: type /model <model-name>[/dim]", id="__custom__", disabled=True),
                id="model_options",
            )
            yield Static("[dim]Enter selects. Esc cancels.[/dim]")

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option.id and event.option.id != "__custom__":
            self.dismiss(str(event.option.id))

    def action_cancel(self) -> None:
        self.dismiss(None)


@dataclass
class TurnRenderBuffer:
    assistant_chunks: list[str] = field(default_factory=list)
    saw_error: bool = False
    saw_tool_activity: bool = False

    def observe(self, event: VelariaAgentEvent) -> str | None:
        if event.type == "assistant_text":
            if event.content:
                self.assistant_chunks.append(event.content)
            return None
        if event.type == "tool_call":
            self.saw_tool_activity = True
            return f"[cyan]tool[/cyan] {_event_label(event) or 'running'}"
        if event.type == "tool_result":
            self.saw_tool_activity = True
            label = _event_label(event) or "completed"
            return f"[green]tool[/green] {label}"
        if event.type in {"error", "turn.failed"}:
            self.saw_error = True
            message = event.content or _error_message(event.data) or "Agent turn failed."
            return f"[red]error[/red] {escape(message)}"
        return None

    def finish(self) -> list[Any]:
        text = self.markdown_text()
        if text:
            return ["[b]Velaria[/b]", Markdown(text)]
        if self.saw_error:
            return []
        if self.saw_tool_activity:
            detail = "Tool calls completed, but the runtime returned no assistant text."
        else:
            detail = "The runtime returned no assistant text for this turn."
        return [
            "[yellow]no visible answer[/yellow] "
            f"{detail} Try again or inspect `agent --stream-json` for raw events."
        ]

    def markdown_text(self) -> str:
        return "".join(self.assistant_chunks).strip()


class VelariaAgentApp(App):
    TITLE = "Velaria Agent"
    SUB_TITLE = ""
    ACTIVITY_FRAMES = ("running.  ", "running.. ", "running...")
    CSS = """
    Screen {
        layout: vertical;
    }
    #main {
        width: 1fr;
        height: 1fr;
    }
    #transcript {
        height: 1fr;
        border-bottom: solid $surface;
        padding: 0 1;
        overflow-y: scroll;
    }
    #activity {
        height: 1;
        color: $text-muted;
        padding: 0 1;
    }
    #live_response {
        height: auto;
        max-height: 45%;
        overflow-y: scroll;
        padding: 0 1;
    }
    #composer {
        height: 3;
    }
    #status {
        height: 1;
        background: $boost;
        color: $text;
        padding: 0 1;
    }
    """
    BINDINGS = [
        Binding("ctrl+c", "interrupt_or_quit", "Cancel", priority=True),
        Binding("ctrl+m", "select_model", "Model", priority=True),
        Binding("ctrl+n", "new_session", "New", priority=True),
        Binding("ctrl+r", "refresh_status", "Refresh", priority=True),
        Binding("pageup", "scroll_history_up", "History up", show=False, priority=True),
        Binding("pagedown", "scroll_history_down", "History down", show=False, priority=True),
    ]

    def __init__(
        self,
        controller: InteractiveController,
        *,
        args: AgentCommandArgs | None = None,
        initial_prompt: str | None = None,
        session_id: str | None = None,
        new_session: bool = False,
        controller_factory: Any = _controller_from_args,
    ):
        super().__init__()
        self.controller = controller
        self.args = args or AgentCommandArgs()
        self.initial_prompt = initial_prompt
        self.session_id = session_id
        self.new_session = new_session
        self._controller_factory = controller_factory
        self._pending_prompts: deque[str] = deque()
        self._turn_task: asyncio.Task[None] | None = None
        self._activity_timer: Timer | None = None
        self._activity_frame = 0
        self._activity_detail = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical(id="main"):
            yield Transcript(id="transcript", wrap=True, highlight=True, markup=True)
            yield LiveResponse("", id="live_response")
            yield Static("", id="activity")
            yield Input(placeholder="Ask Velaria...", id="composer")
        yield StatusBar("", id="status")
        yield Footer()

    async def on_mount(self) -> None:
        self.query_one("#transcript", Transcript).write("[b]Velaria Agent[/b]")
        self._activity_timer = self.set_interval(0.25, self._tick_activity, pause=True)
        await self._ensure_session()
        self._render_state()
        if self.initial_prompt:
            self._enqueue_prompt(self.initial_prompt)
        self.query_one("#composer", Input).focus()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        prompt = event.value.strip()
        event.input.value = ""
        if not prompt:
            return
        if prompt in {"/exit", "/quit", "exit", "quit"}:
            self.exit()
            return
        if prompt == "/status":
            self.controller.refresh_status()
            self._render_state()
            self.query_one("#transcript", Transcript).write(_format_status(self.controller.state))
            return
        if prompt == "/model":
            await self.action_select_model()
            return
        if prompt.startswith("/model "):
            await self._set_model(prompt.removeprefix("/model ").strip())
            return
        if prompt == "/new":
            await self._ensure_session(new_session=True)
            self._render_state()
            return
        self._enqueue_prompt(prompt)

    async def action_new_session(self) -> None:
        await self._ensure_session(new_session=True)
        self._render_state()

    async def action_refresh_status(self) -> None:
        self.controller.refresh_status()
        self._render_state()

    async def action_select_model(self) -> None:
        if self._is_turn_running() or self._pending_prompts:
            self.query_one("#transcript", Transcript).write(
                "[yellow]model switch unavailable[/yellow] wait for the running turn and queue to finish."
            )
            return
        state = self.controller.state
        def on_model_selected(model: str | None) -> None:
            if model:
                asyncio.create_task(self._set_model(model))

        self.push_screen(
            ModelSelectScreen(
                runtime=state.runtime,
                current_model=state.model,
                candidates=_model_candidates(state.runtime, state.model),
            ),
            callback=on_model_selected,
        )

    def action_interrupt_or_quit(self) -> None:
        self.exit()

    def action_scroll_history_up(self) -> None:
        self.query_one("#transcript", Transcript).scroll_page_up(animate=False, force=True)

    def action_scroll_history_down(self) -> None:
        self.query_one("#transcript", Transcript).scroll_page_down(animate=False, force=True)

    async def _ensure_session(self, *, new_session: bool = False) -> None:
        event = await self.controller.ensure_session(
            session_id=self.session_id,
            new_session=new_session or self.new_session,
        )
        self.query_one("#transcript", Transcript).write(_format_session_event(event))

    async def _set_model(self, model: str) -> None:
        model = model.strip()
        transcript = self.query_one("#transcript", Transcript)
        if not model:
            transcript.write("[yellow]model required[/yellow] use /model <model-name>.")
            return
        if self._is_turn_running() or self._pending_prompts:
            transcript.write("[yellow]model switch unavailable[/yellow] wait for the running turn and queue to finish.")
            return
        current = self.controller.state.model
        if model == current:
            transcript.write(f"[dim]model unchanged[/dim] {escape(model)}")
            return
        old_controller = self.controller
        old_model = self.args.model
        self.args.model = model
        self.session_id = None
        new_controller: InteractiveController | None = None
        try:
            new_controller = self._controller_factory(self.args)
            event = await new_controller.ensure_session(new_session=True)
        except Exception as exc:
            if new_controller is not None:
                new_controller.shutdown()
            self.args.model = old_model
            self.controller.refresh_status()
            transcript.write(f"[red]model switch failed[/red] {escape(str(exc))}")
            self._render_state()
            return
        old_controller.shutdown()
        self.controller = new_controller
        transcript.write(f"[green]model switched[/green] {escape(model)}")
        transcript.write(_format_session_event(event))
        self._render_state()

    def _enqueue_prompt(self, prompt: str) -> None:
        was_running = self._is_turn_running()
        self._pending_prompts.append(prompt)
        if was_running:
            position = len(self._pending_prompts)
            self.query_one("#transcript", Transcript).write(
                f"[dim]queued {position}[/dim]\n{escape(prompt)}"
            )
        self._render_state(activity="queued" if was_running else "running")
        if not was_running:
            self._turn_task = asyncio.create_task(self._drain_prompt_queue())

    async def _drain_prompt_queue(self) -> None:
        self._start_activity("starting")
        try:
            while self._pending_prompts:
                prompt = self._pending_prompts.popleft()
                await self._send_prompt(prompt)
        except asyncio.CancelledError:
            self._render_state(activity="cancelled")
            raise
        finally:
            self._turn_task = None
            self._clear_live_response()
            self._stop_activity()
            with suppress(Exception):
                self.query_one("#composer", Input).focus()
            self._render_state()

    async def _send_prompt(self, prompt: str) -> None:
        transcript = self.query_one("#transcript", Transcript)
        self._clear_live_response()
        transcript.write(f"[b]You[/b]\n{escape(prompt)}")
        buffer = TurnRenderBuffer()
        self._start_activity("running")
        async for event in self.controller.send_turn(prompt):
            line = buffer.observe(event)
            if line:
                transcript.write(line)
            if event.type == "assistant_text":
                self._update_live_response(buffer.markdown_text())
            self._start_activity(_event_activity(event))
            await asyncio.sleep(0)
        for line in buffer.finish():
            transcript.write(line)
        self._clear_live_response()

    def _render_state(self, *, activity: str = "") -> None:
        state = self.controller.state
        queue_suffix = f" | queued {len(self._pending_prompts)}" if self._pending_prompts else ""
        status = (
            f"{state.runtime or '-'} | {state.model or '-'} | session {_short_id(state.session_id)} | "
            f"{activity or state.turn_state}{queue_suffix}"
        )
        with suppress(Exception):
            self.query_one("#status", StatusBar).update(status)

    def _update_live_response(self, markdown_text: str) -> None:
        if not markdown_text:
            return
        with suppress(Exception):
            self.query_one("#live_response", LiveResponse).update(
                Group(Text.from_markup("[b]Velaria[/b]"), Markdown(markdown_text))
            )

    def _clear_live_response(self) -> None:
        with suppress(Exception):
            self.query_one("#live_response", LiveResponse).update("")

    def _is_turn_running(self) -> bool:
        return self._turn_task is not None and not self._turn_task.done()

    def _start_activity(self, detail: str) -> None:
        self._activity_detail = detail
        if self._activity_timer is not None:
            self._activity_timer.resume()
        self._tick_activity()

    def _stop_activity(self) -> None:
        self._activity_detail = ""
        if self._activity_timer is not None:
            self._activity_timer.pause()
        with suppress(Exception):
            self.query_one("#activity", Static).update("")
        self._render_state()

    def _tick_activity(self) -> None:
        if not self._activity_detail and not self._pending_prompts:
            return
        frame = self.ACTIVITY_FRAMES[self._activity_frame % len(self.ACTIVITY_FRAMES)]
        self._activity_frame += 1
        queue = len(self._pending_prompts)
        queue_suffix = f" | queued {queue}" if queue else ""
        detail = f" | {self._activity_detail}" if self._activity_detail else ""
        with suppress(Exception):
            self.query_one("#activity", Static).update(f"{frame}{detail}{queue_suffix}")
        self._render_state(activity=frame.strip())

    async def on_unmount(self) -> None:
        if self._activity_timer is not None:
            self._activity_timer.stop()
        if self._turn_task is not None and not self._turn_task.done():
            self._turn_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._turn_task
        self.controller.shutdown()


def run_tui(args: AgentCommandArgs) -> int:
    if _TEXTUAL_IMPORT_ERROR is not None:
        print(
            "Velaria Agent TUI requires Textual. Run `uv sync --project python` before starting `velaria-cli`.",
            flush=True,
        )
        return 1

    controller = _controller_from_args(args)
    app = VelariaAgentApp(
        controller,
        args=args,
        initial_prompt=args.prompt,
        session_id=args.session or None,
        new_session=args.new,
    )
    app.run()
    return 0


def _format_session_event(event: VelariaAgentEvent) -> str:
    label = {
        "session.started": "session ready",
        "session.resumed": "session resumed",
        "session.current": "session ready",
    }.get(event.type, event.type)
    return f"[dim]{label} {_short_id(event.session_id)}[/dim]"


def _format_status(state: Any) -> str:
    rows = state.row_count if state.row_count is not None else "-"
    return (
        "[b]Status[/b]\n"
        f"runtime: {escape(state.runtime or '-')}\n"
        f"model: {escape(state.model or '-')}\n"
        f"session: {escape(_short_id(state.session_id))}\n"
        f"dataset: {escape(state.dataset_name or '-')}\n"
        f"rows: {rows}\n"
        f"last tool: {escape(state.last_tool or '-')}"
    )


def _model_candidates(runtime: str, current_model: str) -> list[str]:
    runtime_key = (runtime or "").strip().lower()
    defaults = CLAUDE_MODEL_CANDIDATES if runtime_key == "claude" else CODEX_MODEL_CANDIDATES
    candidates: list[str] = []
    for model in (current_model, *defaults):
        if model and model not in candidates:
            candidates.append(model)
    return candidates


def _model_option_label(model: str, current_model: str) -> str:
    suffix = " [green](current)[/green]" if model == current_model else ""
    return f"{escape(model)}{suffix}"


def _event_activity(event: VelariaAgentEvent) -> str:
    if event.type in {"tool_call", "tool_result"}:
        label = _event_label(event)
        return f"tool {label}" if label else "tool"
    if event.type == "assistant_text":
        return "answering"
    if event.type in {"error", "turn.failed"}:
        return "failed"
    return "running"


def _event_label(event: VelariaAgentEvent) -> str:
    return escape(_tool_label(event.data) or event.content or "")


def _error_message(data: dict[str, Any]) -> str:
    for key in ("message", "error", "hint"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _short_id(value: str) -> str:
    return value[:8] if value else "-"


def _tool_label(data: dict[str, Any]) -> str:
    for key in ("function", "tool_name", "name", "tool"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return ""
