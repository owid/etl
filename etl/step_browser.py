#
#  step_browser.py
#  Interactive step browser for etlr
#

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.styles import Style

from etl.dag_helpers import graph_nodes

# Simple type alias - avoids importing heavy etl.steps module
DAG = Dict[str, Set[str]]

# OWID-styled colors (reused from harmonize.py and apps/pr/cli.py)
OWID_YELLOW = "#fac800"
OWID_GREEN = "#54cc90"
OWID_GRAY = "#888888"

# Style for the step browser
BROWSER_STYLE = Style.from_dict(
    {
        "prompt": f"fg:{OWID_YELLOW} bold",
        "input": "bold",
        "match-count": f"fg:{OWID_GRAY}",
        "separator": f"fg:{OWID_GRAY}",
        "step": f"fg:{OWID_GRAY}",
        "step.selected": f"bg:{OWID_YELLOW} fg:#000000 bold",
        "hint": f"fg:{OWID_GRAY} italic",
    }
)

# Maximum number of steps to display
MAX_DISPLAY_STEPS = 15


class BrowserState:
    """State container for the step browser application."""

    def __init__(self) -> None:
        self.selected_index: int = -1  # -1 means no selection (run all matches)
        self.matches: List[str] = []
        self.result: Optional[str] = None
        self.is_exact: bool = False
        self.cancelled: bool = False


def filter_steps(pattern: str, all_steps: List[str]) -> List[str]:
    """Filter steps by pattern using regex matching.

    Matches steps that contain the pattern anywhere in their name.
    """
    if not pattern:
        return []

    try:
        compiled = re.compile(pattern, re.IGNORECASE)
        matches = [s for s in all_steps if compiled.search(s)]
        # Sort by: exact matches first, then by step length (shorter = more specific)
        matches.sort(key=lambda s: (pattern.lower() not in s.lower(), len(s), s))
        return matches
    except re.error:
        # If the pattern is not a valid regex, fall back to simple substring match
        pattern_lower = pattern.lower()
        matches = [s for s in all_steps if pattern_lower in s.lower()]
        matches.sort(key=lambda s: (len(s), s))
        return matches


def get_all_steps(dag: DAG, private: bool = False) -> List[str]:
    """Get all step URIs from the DAG.

    If private=False, filter out private steps.
    """
    all_steps = sorted(graph_nodes(dag))

    # Filter based on private flag
    if not private:
        all_steps = [s for s in all_steps if "private://" not in s]

    # Filter out steps that are not relevant for etlr (snapshots, github, etag)
    # Focus on data and grapher steps
    relevant_prefixes = ("data://", "data-private://", "grapher://", "export://")
    all_steps = [s for s in all_steps if s.startswith(relevant_prefixes)]

    return all_steps


def browse_steps(dag: DAG, private: bool = False) -> Tuple[Optional[str], bool]:
    """Interactive step browser using prompt_toolkit.

    Returns:
        Tuple of (pattern_or_step, is_exact_match):
        - If user presses Enter: (current_text, False) to run all matches
        - If user selects a step: (step_uri, True) to run just that step
        - If user cancels: (None, False)
    """
    all_steps = get_all_steps(dag, private=private)

    if not all_steps:
        print("No steps found in DAG.")
        return None, False

    # State for the application
    state = BrowserState()

    # Create the input buffer
    def on_text_changed(buf: Buffer) -> None:
        text = buf.text.strip()
        state.matches = filter_steps(text, all_steps)
        state.selected_index = -1  # Reset selection when text changes

    input_buffer = Buffer(
        multiline=False,
        on_text_changed=on_text_changed,
    )

    # Key bindings
    kb = KeyBindings()

    @kb.add("enter")
    def handle_enter(event: Any) -> None:
        text = input_buffer.text.strip()
        if state.selected_index >= 0 and state.matches:
            # User has selected a specific step
            state.result = state.matches[state.selected_index]
            state.is_exact = True
        elif text:
            # User pressed Enter with text - run all matches
            state.result = text
            state.is_exact = False
        else:
            # Empty input - do nothing
            return
        event.app.exit()

    @kb.add("c-c")
    @kb.add("escape")
    def handle_cancel(event: Any) -> None:
        state.cancelled = True
        event.app.exit()

    @kb.add("down")
    @kb.add("tab")
    def handle_down(event: Any) -> None:
        if state.matches:
            max_idx = min(len(state.matches), MAX_DISPLAY_STEPS) - 1
            state.selected_index = min(state.selected_index + 1, max_idx)

    @kb.add("up")
    @kb.add("s-tab")
    def handle_up(event: Any) -> None:
        if state.selected_index > -1:
            state.selected_index -= 1

    # Layout components
    def get_prompt_text() -> List[Tuple[str, str]]:
        return [("class:prompt", "etlr> ")]

    def get_matches_text() -> List[Tuple[str, str]]:
        text = input_buffer.text.strip()
        matches = state.matches

        lines: List[Tuple[str, str]] = []

        # Match count and hint line
        if text:
            count = len(matches)
            if count == 0:
                lines.append(("class:match-count", "  No matches"))
            else:
                hint = "Enter=run all" if state.selected_index < 0 else "Enter=run selected"
                if count == 1:
                    lines.append(("class:match-count", f"  1 step matches ({hint}, Esc=cancel)"))
                else:
                    lines.append(("class:match-count", f"  {count} steps match ({hint}, Esc=cancel)"))
        else:
            lines.append(("class:hint", f"  Type to filter {len(all_steps)} steps..."))

        lines.append(("", "\n"))

        # Separator
        lines.append(("class:separator", "  " + "-" * 50 + "\n"))

        # Display matches
        display_matches = matches[:MAX_DISPLAY_STEPS]
        for i, step in enumerate(display_matches):
            if i == state.selected_index:
                lines.append(("class:step.selected", f"  > {step}"))
            else:
                lines.append(("class:step", f"    {step}"))
            lines.append(("", "\n"))

        if len(matches) > MAX_DISPLAY_STEPS:
            lines.append(("class:hint", f"    ... and {len(matches) - MAX_DISPLAY_STEPS} more"))
            lines.append(("", "\n"))

        return lines

    # Create layout
    prompt_window = Window(
        content=FormattedTextControl(get_prompt_text), height=1, dont_extend_height=True, dont_extend_width=True
    )
    input_window = Window(content=BufferControl(buffer=input_buffer), height=1, dont_extend_height=True)
    matches_window = Window(content=FormattedTextControl(get_matches_text), wrap_lines=False)

    # Combine prompt and input on the same line
    input_row = VSplit([prompt_window, input_window])

    root_container = HSplit([input_row, matches_window])

    layout = Layout(root_container)
    layout.focus(input_window)

    # Create and run application
    app: Application[None] = Application(
        layout=layout,
        key_bindings=kb,
        style=BROWSER_STYLE,
        full_screen=False,
        mouse_support=False,
    )

    try:
        app.run()
    except EOFError:
        state.cancelled = True

    if state.cancelled:
        return None, False

    return state.result, state.is_exact
