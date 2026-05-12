"""Panel-based display system inspired by Claude Code's terminal UI.

Provides three-panel layout (header / scrollable content / status bar),
bordered panels for events/tools/results/errors, and minimal layout mode.
"""

import os
import sys
import time
from typing import Any


# ── ANSI helpers ────────────────────────────────────────────────────────────

def _supports_color():
    return (
        hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
        and not os.environ.get("NO_COLOR")
    )


def _sgr(code):
    if not _supports_color():
        return ""
    return "\033[%sm" % code


def _sgr_rgb(r, g, b, bg=False):
    if not _supports_color():
        return ""
    prefix = 48 if bg else 38
    return "\033[%d;2;%d;%d;%dm" % (prefix, r, g, b)


_RESET = "\033[0m"


def _styled(text, *codes):
    parts = "".join(_sgr(c) for c in codes)
    return "%s%s%s" % (parts, text, _RESET) if parts else text


def _bold(text):
    return _styled(text, 1)


# ── Color palette (24-bit) ──────────────────────────────────────────────────

class Palette:
    # Panel backgrounds are transparent ("") so terminal background shows through.
    # Only semantic panels (error, tool_call, tool_result) get subtle colored fills.
    bg_root       = ""
    bg_panel      = ""
    bg_border     = ""
    bg_header     = _sgr_rgb(22, 22, 35, bg=True)
    bg_status     = _sgr_rgb(20, 20, 32, bg=True)
    bg_selected   = ""

    bg_error      = _sgr_rgb(55, 20, 20, bg=True)
    bg_tool_call  = _sgr_rgb(20, 32, 48, bg=True)
    bg_tool_result= _sgr_rgb(20, 40, 24, bg=True)

    fg_dim        = _sgr_rgb(140, 140, 160)
    fg_muted      = _sgr_rgb(170, 170, 190)
    fg_normal     = _sgr_rgb(225, 225, 235)
    fg_bright     = _sgr_rgb(248, 248, 252)
    fg_accent     = _sgr_rgb(180, 160, 245)
    fg_cyan       = _sgr_rgb(130, 225, 245)
    fg_green      = _sgr_rgb(150, 240, 180)
    fg_yellow     = _sgr_rgb(240, 220, 130)
    fg_red        = _sgr_rgb(245, 150, 150)
    fg_orange     = _sgr_rgb(245, 190, 130)
    fg_blue       = _sgr_rgb(140, 200, 250)
    fg_pink       = _sgr_rgb(245, 170, 225)

    border        = _sgr_rgb(120, 120, 160)
    border_dim    = _sgr_rgb(95, 95, 135)

    @classmethod
    def level_fg(cls, level):
        return {
            "info": cls.fg_cyan,
            "warn": cls.fg_yellow,
            "error": cls.fg_red,
            "success": cls.fg_green,
            "debug": cls.fg_dim,
        }.get(level, cls.fg_normal)


class Box:
    H   = "─"
    V   = "│"
    TL  = "┌"
    TR  = "┐"
    BL  = "└"
    BR  = "┘"
    LT  = "├"
    RT  = "┤"


# ── Style constants ─────────────────────────────────────────────────────────

STYLE_DEFAULT     = "default"
STYLE_TOOL_CALL   = "tool_call"
STYLE_TOOL_RESULT = "tool_result"
STYLE_ERROR       = "error"
STYLE_SECTION     = "section"


def _style_attrs(style_name):
    m = {
        STYLE_DEFAULT:     (Palette.border,     Palette.fg_accent,  Palette.bg_panel),
        STYLE_TOOL_CALL:   (Palette.fg_blue,    Palette.fg_blue,    Palette.bg_tool_call),
        STYLE_TOOL_RESULT: (Palette.fg_green,   Palette.fg_green,   Palette.bg_tool_result),
        STYLE_ERROR:       (Palette.fg_red,     Palette.fg_red,     Palette.bg_error),
        STYLE_SECTION:     (Palette.border,     Palette.fg_bright,  Palette.bg_panel),
    }
    return m.get(style_name, m[STYLE_DEFAULT])


class Panel:
    """A single panel in the display system."""
    def __init__(self, id, title="", body="", style=STYLE_DEFAULT):
        self.id = id
        self.title = title
        self.body = body
        self.style = style


class LayoutMode:
    FULL = "full"
    COMPACT = "compact"
    MINIMAL = "minimal"


class PanelSystem:
    """Three-panel display system.

    Layout:
      Header bar (1 line, fixed)
      Content panels (scrollable)
      Status bar (1 line, fixed)
    """

    def __init__(self):
        self.panels = []
        self._layout_mode = LayoutMode.FULL
        self._max_panel_history = 200
        self._term_width = 80
        self._has_rendered = False

    @property
    def layout_mode(self):
        return self._layout_mode

    def set_layout_mode(self, mode):
        if mode in (LayoutMode.FULL, LayoutMode.COMPACT, LayoutMode.MINIMAL):
            self._layout_mode = mode

    def cycle_layout_mode(self):
        modes = [LayoutMode.FULL, LayoutMode.COMPACT, LayoutMode.MINIMAL]
        idx = modes.index(self._layout_mode) if self._layout_mode in modes else 0
        self._layout_mode = modes[(idx + 1) % len(modes)]
        return self._layout_mode

    def add_panel(self, panel):
        self.panels.append(panel)
        if len(self.panels) > self._max_panel_history:
            self.panels = self.panels[-self._max_panel_history:]

    def clear_panels(self):
        self.panels.clear()

    def get_panel(self, panel_id):
        for p in self.panels:
            if p.id == panel_id:
                return p
        return None

    def remove_panel(self, panel_id):
        before = len(self.panels)
        self.panels = [p for p in self.panels if p.id != panel_id]
        return len(self.panels) < before

    # ── Render methods ──────────────────────────────────────────────────

    def render_all(self, *, header_text="", status_text="",
                   status_spinner=""):
        """Refresh only the status bar line during live animation.
        First call draws header + content + status. Subsequent calls
        overwrite only the status bar line to avoid terminal flicker."""
        self._term_width = self._get_term_width()

        status_line = self._render_status_bar(
            status_text, spinner=status_spinner)

        if not self._has_rendered:
            lines = []
            lines.extend(self._render_header(header_text))
            if self._layout_mode != LayoutMode.MINIMAL:
                lines.extend(self._render_content())
            lines.append(status_line)
            self._has_rendered = True
            sys.stdout.write("\n".join(lines))
        else:
            sys.stdout.write("\r" + status_line + "\x1b[K")
        sys.stdout.flush()

    def render_static(self, header_text=""):
        """Render a static view for after turn ends."""
        old_mode = self._layout_mode
        self._layout_mode = LayoutMode.FULL
        self._term_width = self._get_term_width()

        lines = []
        lines.extend(self._render_header(header_text))
        lines.extend(self._render_content())
        lines.append("")

        sys.stdout.write("\n".join(lines))
        sys.stdout.flush()
        self._layout_mode = old_mode

    def clear_render(self):
        """Clear the last rendered status bar line."""
        if self._has_rendered:
            sys.stdout.write("\033[2K\r")
            sys.stdout.flush()
        self._has_rendered = False

    # ── Internal helpers ───────────────────────────────────────────────

    def _get_term_width(self):
        try:
            return max(40, os.get_terminal_size().columns)
        except OSError:
            return 100

    def _render_header(self, text):
        w = self._term_width
        bg = Palette.bg_header
        fg = Palette.fg_dim
        reset_bg = Palette.bg_root

        line = text[:w-2] if text else ""
        padded = line.ljust(w - 2)
        bar = (reset_bg + Box.TL + bg + Box.H + fg + " " + padded + " "
               + Palette.fg_dim + Box.H * 2 + _RESET + reset_bg + Box.TR + _RESET)
        return [bar]

    def _render_status_bar(self, text, *, spinner=""):
        w = self._term_width
        bg = Palette.bg_status
        fg = Palette.fg_muted
        reset_bg = Palette.bg_root

        content = text or ""
        if spinner:
            content = Palette.fg_cyan + spinner + _RESET + bg + " " + content
        padded = (content + " ").ljust(w - 4)

        return (reset_bg + Box.BL + bg + Box.H + " " + padded + Box.H + _RESET
                + reset_bg + Box.BR + _RESET)

    def _render_content(self):
        lines = []
        for panel in self.panels:
            p_lines = self._render_panel(panel)
            lines.extend(p_lines)
            if self._layout_mode == LayoutMode.COMPACT:
                lines.append("")
        return lines

    def _render_panel(self, panel):
        if self._layout_mode == LayoutMode.MINIMAL:
            return []
        bdr, title_fg_sgr, bg = _style_attrs(panel.style)
        w = self._term_width
        bdr_dim = Palette.border_dim
        reset_bg = Palette.bg_root

        title = panel.title or panel.id
        body = panel.body
        inner_w = max(2, w - 4)

        lines = []

        # Top border with title
        title_text = (" %s " % title) if title else ""
        title_part = (reset_bg + bdr + Box.TL + bdr + Box.H + _RESET + bg + " "
                      + title_fg_sgr + _bold(title_text) + _RESET + bg)
        remaining = inner_w - len(title_text) - 2
        if remaining > 0:
            title_part += bdr_dim + Box.H * remaining + _RESET + bg
        title_part += bdr + Box.TR + _RESET
        lines.append(title_part)

        # Body
        for bline in body.split("\n"):
            while len(bline) > inner_w:
                chunk = bline[:inner_w]
                lines.append(reset_bg + bdr + Box.V + _RESET + bg + " " + chunk + " "
                             + bdr + Box.V + _RESET)
                bline = bline[inner_w:]
            padded = bline.ljust(inner_w)
            lines.append(reset_bg + bdr + Box.V + _RESET + bg + " " + padded + " "
                         + bdr + Box.V + _RESET)

        # Bottom border
        lines.append(reset_bg + bdr + Box.BL + bdr_dim + Box.H * (inner_w + 2) + _RESET
                     + bdr + Box.BR + _RESET)
        return lines

    # ── Panel builders ──────────────────────────────────────────────────────────

_next_id = 0


def _gen_id():
    global _next_id
    _next_id += 1
    return "p%d" % _next_id


def make_text_panel(title, body, style=STYLE_DEFAULT):
    return Panel(_gen_id(), title=title, body=body, style=style)


def make_tool_call_panel(name, args_summary, status=""):
    title = "tool_call: " + name
    if status and status not in ("completed", "complete", "success"):
        title += " [" + status + "]"
    return Panel(_gen_id(), title=title, body=args_summary, style=STYLE_TOOL_CALL)


def make_tool_result_panel(label, summary):
    return Panel(_gen_id(), title=label, body=summary, style=STYLE_TOOL_RESULT)


def make_error_panel(message, details=""):
    body = details if details else message
    return Panel(_gen_id(), title="error", body=body, style=STYLE_ERROR)


def make_note_panel(label, message, level="info"):
    title = ("%s: %s" % (level, label)) if level != "info" else label
    style = STYLE_ERROR if level == "error" else STYLE_DEFAULT
    return Panel(_gen_id(), title=title, body=message, style=style)


def make_section_panel(title, rows):
    if not rows:
        return Panel(_gen_id(), title=title, body="-", style=STYLE_SECTION)
    kw = max(len(k) for k, _ in rows)
    lines = ["  " + k.ljust(kw) + "  " + v for k, v in rows]
    return Panel(_gen_id(), title=title, body="\n".join(lines), style=STYLE_SECTION)


def make_table_panel(title, headers, rows):
    if not rows:
        return Panel(_gen_id(), title=title, body="  -", style=STYLE_SECTION)
    widths = [max(len(headers[i]), *(len(r[i]) for r in rows))
              for i in range(len(headers))]
    header_line = "  " + "  ".join(headers[i].ljust(widths[i])
                                   for i in range(len(headers)))
    lines = [header_line]
    for row in rows:
        lines.append("  " + "  ".join(row[i].ljust(widths[i])
                                      for i in range(len(row))))
    return Panel(_gen_id(), title=title, body="\n".join(lines), style=STYLE_SECTION)


def make_assistant_panel(text):
    return Panel(_gen_id(), title="assistant", body=text, style=STYLE_DEFAULT)


# ── Global singleton ────────────────────────────────────────────────────────

_system = None


def get_system():
    global _system
    if _system is None:
        _system = PanelSystem()
    return _system


def reset_system():
    global _system, _next_id
    _system = None
    _next_id = 0
