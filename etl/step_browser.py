#
#  step_browser.py
#  Interactive step browser for etlr
#

import json
import re
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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
        self.loading: bool = False  # True while DAG is loading in background
        self.all_steps: List[str] = []  # Populated when loading completes
        self.app: Optional[Application[None]] = None  # Reference to app for invalidation


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


def get_dag_max_mtime(dag_path: Path) -> float:
    """Get the max mtime of all DAG YAML files (excluding archive/).

    The DAG uses includes, so we need to check all .yml files, not just main.yml.
    """
    dag_dir = dag_path.parent
    max_mtime = 0.0

    for yml_file in dag_dir.glob("*.yml"):
        max_mtime = max(max_mtime, yml_file.stat().st_mtime)

    return max_mtime


def load_cached_steps(dag_path: Path, private: bool) -> Optional[List[str]]:
    """Load steps from cache if valid (no DAG files have changed).

    Returns cached steps list, or None if cache is invalid/missing.
    """
    from etl import paths

    cache_file = paths.STEP_CACHE_FILE
    if not cache_file.exists():
        return None

    try:
        with open(cache_file) as f:
            cache = json.load(f)

        # Check if cache matches current settings
        if cache.get("dag_path") != str(dag_path):
            return None
        if cache.get("private") != private:
            return None

        # Check if any DAG file has been modified (main.yml + includes)
        dag_max_mtime = get_dag_max_mtime(dag_path)
        if cache.get("dag_max_mtime") != dag_max_mtime:
            return None

        return cache.get("steps", [])
    except (json.JSONDecodeError, OSError, KeyError):
        return None


def save_step_cache(dag_path: Path, private: bool, steps: List[str]) -> None:
    """Save steps to cache for instant startup next time."""
    from etl import paths

    cache_file = paths.STEP_CACHE_FILE
    try:
        # Ensure cache directory exists
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        dag_max_mtime = get_dag_max_mtime(dag_path)
        cache = {
            "dag_path": str(dag_path),
            "dag_max_mtime": dag_max_mtime,
            "private": private,
            "steps": steps,
        }
        with open(cache_file, "w") as f:
            json.dump(cache, f)
    except OSError:
        pass  # Silently fail - cache is optional


def browse_steps(
    dag: Optional[DAG] = None,
    private: bool = False,
    dag_loader: Optional[Callable[[], DAG]] = None,
    dag_path: Optional[Path] = None,
) -> Tuple[Optional[str], bool]:
    """Interactive step browser using prompt_toolkit.

    Args:
        dag: Pre-loaded DAG (if available)
        private: Whether to include private steps
        dag_loader: Callable that returns DAG (for async loading)
        dag_path: Path to DAG file (for caching)

    Returns:
        Tuple of (pattern_or_step, is_exact_match):
        - If user presses Enter: (current_text, False) to run all matches
        - If user selects a step: (step_uri, True) to run just that step
        - If user cancels: (None, False)
    """
    # State for the application
    state = BrowserState()

    # If dag is provided directly, use it immediately
    if dag is not None:
        state.all_steps = get_all_steps(dag, private=private)
        state.loading = False
    elif dag_loader is not None:
        # Try loading from cache first (instant startup)
        cached_steps = load_cached_steps(dag_path, private) if dag_path else None

        if cached_steps is not None:
            # Cache hit: mtime matched, cache is fresh - use it directly
            # (load_cached_steps already validated mtime)
            state.all_steps = cached_steps
            state.loading = False
        else:
            # Cache miss: show loading state
            state.loading = True

            def load_in_background() -> None:
                loaded_dag = dag_loader()
                state.all_steps = get_all_steps(loaded_dag, private=private)
                state.loading = False

                # Save to cache for next time
                if dag_path:
                    save_step_cache(dag_path, private, state.all_steps)

                # Trigger UI refresh from background thread
                if state.app is not None:
                    state.app.invalidate()

            thread = threading.Thread(target=load_in_background, daemon=True)
            thread.start()
    else:
        raise ValueError("Either dag or dag_loader must be provided")

    # Create the input buffer
    def on_text_changed(buf: Buffer) -> None:
        text = buf.text.strip()
        state.matches = filter_steps(text, state.all_steps)
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

        # Show loading state
        if state.loading:
            lines.append(("class:hint", "  Loading steps..."))
            lines.append(("", "\n"))
            lines.append(("class:separator", "  " + "-" * 50 + "\n"))
            return lines

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
            lines.append(("class:hint", f"  Type to filter {len(state.all_steps)} steps..."))

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

    # Store app reference so background thread can trigger refresh
    state.app = app

    try:
        app.run()
    except EOFError:
        state.cancelled = True

    if state.cancelled:
        return None, False

    # Check if no steps were loaded (empty DAG)
    if not state.all_steps and not state.loading:
        print("No steps found in DAG.")
        return None, False

    return state.result, state.is_exact
