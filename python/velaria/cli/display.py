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
    bg_root       = _sgr_rgb(18, 18, 28, bg=True)
    bg_panel      = _sgr_rgb(24, 24, 36, bg=True)
    bg_border     = _sgr_rgb(30, 30, 46, bg=True)
    bg_header     = _sgr_rgb(22, 22, 35, bg=True)
    bg_status     = _sgr_rgb(20, 20, 32, bg=True)
    bg_selected   = _sgr_rgb(36, 36, 52, bg=True)
    bg_error      = _sgr_rgb(40, 20, 20, bg=True)
    bg_tool_call  = _sgr_rgb(20, 30, 40, bg=True)
    bg_tool_result= _sgr_rgb(20, 35, 25, bg=True)
    bg_diff_add   = _sgr_rgb(22, 40, 26, bg=True)
    bg_diff_del   = _sgr_rgb(40, 22, 22, bg=True)

    fg_dim        = _sgr_rgb(100, 100, 120)
    fg_muted      = _sgr_rgb(130, 130, 150)
    fg_normal     = _sgr_rgb(200, 200, 210)
    fg_bright     = _sgr_rgb(230, 230, 240)
    fg_accent     = _sgr_rgb(150, 130, 220)
    fg_cyan       = _sgr_rgb(100, 200, 220)
    fg_green      = _sgr_rgb(120, 220, 150)
    fg_yellow     = _sgr_rgb(220, 200, 100)
    fg_red        = _sgr_rgb(220, 120, 120)
    fg_orange     = _sgr_rgb(220, 160, 100)
    fg_blue       = _sgr_rgb(110, 170, 230)
    fg_pink       = _sgr_rgb(220, 140, 200)

    border        = _sgr_rgb(60, 60, 85)
    border_dim    = _sgr_rgb(45, 45, 65)

    @classmethod
    def level_fg(cls, level):
        return {
            "info": cls.fg_cyan,
            "warn": cls.fg_yellow,
            "error": cls.fg_red,
            "success": cls.fg_green,
            "debug": cls.fg_dim,
        }.get(level, cls.fg_normal)

    @classmethod
    def level_bg(cls, level):
        return {
            "error": cls.bg_error,
        }.get(level, cls.bg_panel)


class Box:
    H   = "─"
    V   = "│"
    TL  = "┌"
    TR  = "┐"
    BL  = "└"
    BR  = "┘"
    LT  = "├"
    RT  = "┤"
    TB  = "┬"
    BB  = "┴"
    CR  = "┼"
    H2  = "╌"


# ── Style constants ─────────────────────────────────────────────────────────

STYLE_DEFAULT     = "default"
STYLE_TOOL_CALL   = "tool_call"
STYLE_TOOL_RESULT = "tool_result"
STYLE_ERROR       = "error"
STYLE_SECTION     = "section"
STYLE_DIFF        = "diff"


def _style_attrs(style_name):
    m = {
        STYLE_DEFAULT:     (Palette.border,     Palette.fg_accent,  Palette.bg_panel),
        STYLE_TOOL_CALL:   (Palette.fg_blue,    Palette.fg_blue,    Palette.bg_tool_call),
        STYLE_TOOL_RESULT: (Palette.fg_green,   Palette.fg_green,   Palette.bg_tool_result),
        STYLE_ERROR:       (Palette.fg_red,     Palette.fg_red,     Palette.bg_error),
        STYLE_SECTION:     (Palette.border,     Palette.fg_bright,  Palette.bg_panel),
        STYLE_DIFF:        (Palette.fg_yellow,  Palette.fg_yellow,  Palette.bg_panel),
    }
    return m.get(style_name, m[STYLE_DEFAULT])


class Panel:
    """A single panel in the display system."""
    def __init__(self, id, title="", body="", style=STYLE_DEFAULT,
                 collapsible=True, collapsed=False, timestamp=None):
        self.id = id
        self.title = title
        self.body = body
        self.style = style
        self.collapsible = collapsible
        self.collapsed = collapsed
        self.timestamp = timestamp if timestamp is not None else time.time()

    def set_body(self, text):
        self.body = text
        self.timestamp = time.time()


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
        self._header_pinned = True
        self._status_pinned = True
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
                   status_spinner="", status_elapsed=""):
        """Render the full three-panel layout (refresh mode)."""
        self._term_width = self._get_term_width()

        lines = []
        if self._header_pinned:
            lines.extend(self._render_header(header_text))
        if self._layout_mode != LayoutMode.MINIMAL:
            lines.extend(self._render_content())
        if self._status_pinned:
            lines.append(self._render_status_bar(
                status_text, spinner=status_spinner, elapsed=status_elapsed))

        output = "\n".join(lines)
        if self._has_rendered:
            sys.stdout.write("\033[%dA" % len(lines))
        else:
            self._has_rendered = True
        sys.stdout.write(output)
        sys.stdout.flush()

    def render_static(self, header_text=""):
        """Render a static view for after turn ends."""
        old_mode = self._layout_mode
        self._layout_mode = LayoutMode.FULL
        self._term_width = self._get_term_width()

        lines = []
        if self._header_pinned:
            lines.extend(self._render_header(header_text))
        lines.extend(self._render_content())
        lines.append("")

        sys.stdout.write("\n".join(lines))
        sys.stdout.flush()
        self._layout_mode = old_mode

    def clear_render(self):
        h = self._estimate_total_lines()
        if h > 0 and self._has_rendered:
            sys.stdout.write("\033[%dA\033[J" % h)
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

    def _render_status_bar(self, text, *, spinner="", elapsed=""):
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
        if panel.collapsed and panel.collapsible:
            return self._render_collapsed_panel(panel)

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

    def _render_collapsed_panel(self, panel):
        w = self._term_width
        inner_w = max(2, w - 4)
        bdr = Palette.border_dim
        bg = Palette.bg_panel
        reset_bg = Palette.bg_root
        title = panel.title or panel.id

        line = (reset_bg + bdr + Box.LT + bdr + Box.H + " "
                + Palette.fg_dim + title + _RESET + bdr + " "
                + Box.H * (inner_w - len(title) - 3) + Box.RT + _RESET)
        return [line]

    def _estimate_total_lines(self):
        count = 0
        if self._header_pinned:
            count += 1
        if self._layout_mode != LayoutMode.MINIMAL:
            for panel in self.panels:
                if panel.collapsed and panel.collapsible:
                    count += 1
                else:
                    count += panel.body.count("\n") + 3  # +3: top+bottom border +1 safe
        if self._status_pinned:
            count += 1
        return count + 1


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
    global _system
    _system = None
