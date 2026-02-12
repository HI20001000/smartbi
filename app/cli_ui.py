# cli_ui.py
import os
import sys
import shutil
import platform
import re
import unicodedata
from datetime import datetime


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def _display_width(s: str) -> int:
    """Terminal display width, roughly handling wide chars (emoji/CJK)."""
    s = _strip_ansi(s)
    w = 0
    for ch in s:
        if unicodedata.combining(ch):
            continue
        eaw = unicodedata.east_asian_width(ch)
        # W/F are usually 2 columns in terminals (CJK + many emoji)
        w += 2 if eaw in ("W", "F") else 1
    return w


def _pad_to_width(s: str, width: int) -> str:
    pad = width - _display_width(s)
    if pad <= 0:
        return s
    return s + (" " * pad)


def _center_to_width(s: str, width: int) -> str:
    # center by display width (not len)
    cur = _display_width(s)
    if cur >= width:
        return s
    total = width - cur
    left = total // 2
    right = total - left
    return (" " * left) + s + (" " * right)


def _wrap_display(s: str, width: int) -> list[str]:
    """Wrap text by display width; for URLs/no-spaces, hard-wrap."""
    if width <= 0:
        return [s]

    # Try word wrap first
    parts = s.split(" ")
    lines: list[str] = []
    line = ""
    for token in parts:
        if not line:
            line = token
            continue
        if _display_width(line) + 1 + _display_width(token) <= width:
            line += " " + token
        else:
            lines.append(line)
            line = token
    if line:
        lines.append(line)

    # Hard wrap any line still too wide (e.g., URLs)
    hard: list[str] = []
    for ln in lines:
        if _display_width(ln) <= width:
            hard.append(ln)
            continue

        # Hard cut by characters, respecting display width
        buf = ""
        buf_w = 0
        for ch in ln:
            ch_w = 0 if unicodedata.combining(ch) else (2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1)
            if buf_w + ch_w > width:
                hard.append(buf)
                buf = ch
                buf_w = ch_w
            else:
                buf += ch
                buf_w += ch_w
        if buf:
            hard.append(buf)
    return hard or [""]


def _supports_color() -> bool:
    """Better ANSI color detection; allow FORCE_COLOR override."""
    if os.getenv("NO_COLOR"):
        return False
    if os.getenv("FORCE_COLOR") and os.getenv("FORCE_COLOR") != "0":
        return True
    if not sys.stdout.isatty():
        return False

    # On Windows, try to enable ANSI if colorama exists; otherwise assume modern terminals OK.
    if os.name == "nt":
        try:
            import colorama  # type: ignore

            # Newer colorama has this helper
            if hasattr(colorama, "just_fix_windows_console"):
                colorama.just_fix_windows_console()
            else:
                colorama.init()
        except Exception:
            pass
        return True

    return True


def _clear_screen() -> None:
    if sys.stdout.isatty():
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
    else:
        os.system("cls" if os.name == "nt" else "clear")


def print_startup_ui(
    model: str,
    base_url: str,
    *,
    app_name: str = "SmartBI Chat CLI",
    framework: str = "LangChain",
    version: str | None = None,
    clear_screen: bool = True,
    show_system: bool = False,
    prefer_rich: bool = True,
) -> None:
    if clear_screen:
        _clear_screen()

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    term_w = shutil.get_terminal_size((88, 20)).columns
    w = min(max(70, min(term_w, 100)), 100)
    inner = w - 2

    # 1) Rich path
    if prefer_rich:
        try:
            from rich.console import Console
            from rich.panel import Panel
            from rich.table import Table
            from rich.text import Text
            from rich.align import Align
            from rich import box

            console = Console()

            title = Text.assemble(
                ("🤖 ", "bold cyan"),
                (app_name, "bold cyan"),
                ("  ", ""),
                (f"({framework})", "dim"),
            )
            if version:
                title.append(f"  v{version}", style="dim")

            table = Table.grid(padding=(0, 1))
            table.add_column(justify="right", style="bold magenta", width=10)
            table.add_column(style="white")
            table.add_row("Model", f"[bold green]{model}[/bold green]")
            table.add_row("Base URL", f"[bold blue]{base_url}[/bold blue]")
            table.add_row("Time", f"[dim]{now}[/dim]")

            if show_system:
                sys_info = f"{platform.system()} {platform.release()} · Python {platform.python_version()}"
                table.add_row("System", f"[dim]{sys_info}[/dim]")

            help_lines = Text.assemble(
                ("• ", "bold"),
                ("Type a message and press Enter\n", ""),
                ("• ", "bold"),
                ("Type ", ""),
                ("exit", "bold red"),
                (" or ", ""),
                ("quit", "bold red"),
                (" to stop\n", ""),
                ("• ", "bold"),
                ("Tip: set ", "dim"),
                ("NO_COLOR=1", "dim bold"),
                (" to disable colors", "dim"),
            )

            panel = Panel(
                Align.center(Text.assemble(title, "\n\n", table, "\n", help_lines)),
                box=box.ROUNDED,
                border_style="cyan",
                padding=(1, 2),
                width=min(w, 96),
            )
            console.print(panel)
            return
        except Exception:
            pass  # fall back

    # 2) ANSI fallback (fixed)
    use_color = _supports_color()

    def C(s: str, code: str) -> str:
        return f"\033[{code}m{s}\033[0m" if use_color else s

    def pad_line(s: str) -> str:
        # Never slice ANSI strings; pad by display width
        s = _pad_to_width(s, inner)
        return "│" + s + "│"

    top = "┌" + "─" * inner + "┐"
    mid = "├" + "─" * inner + "┤"
    bot = "└" + "─" * inner + "┘"

    header_plain = f"🤖 {app_name} ({framework})" + (f" v{version}" if version else "")
    header_line = _center_to_width(header_plain, inner)
    header_line = C(header_line, "1;36")  # whole header cyan to avoid ANSI width issues

    def kv(label: str, value: str, value_color: str) -> list[str]:
        left_plain = f"{label:<10}: "
        left = C(f"{label:<10}", "1;35") + ": "
        max_v = inner - _display_width(left_plain)
        wrapped = _wrap_display(value, max_v)
        out = []
        for i, ln in enumerate(wrapped):
            if i == 0:
                out.append(left + C(ln, value_color))
            else:
                out.append(" " * _display_width(left_plain) + C(ln, value_color))
        return out

    lines = [top, pad_line(header_line), pad_line("")]

    for ln in kv("Model", model, "1;32"):
        lines.append(pad_line(ln))
    for ln in kv("Base URL", base_url, "1;34"):
        lines.append(pad_line(ln))
    for ln in kv("Time", now, "2"):
        lines.append(pad_line(ln))

    if show_system:
        sys_info = f"{platform.system()} {platform.release()} · Python {platform.python_version()}"
        for ln in kv("System", sys_info, "2"):
            lines.append(pad_line(ln))

    lines += [
        mid,
        pad_line(f"{C('•', '1;37')} Type a message and press Enter"),
        pad_line(f"{C('•', '1;37')} Type {C('exit', '1;31')} or {C('quit', '1;31')} to stop"),
        bot,
    ]
    print("\n".join(lines))
