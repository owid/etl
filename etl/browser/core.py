#
#  core.py
#  Generic browser UI components for interactive selection
#

import re
from typing import Any, Callable, List, Optional, Tuple

from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.styles import Style

# OWID-styled colors (reused from harmonize.py and apps/pr/cli.py)
OWID_YELLOW = "#fac800"
OWID_GREEN = "#54cc90"
OWID_GRAY = "#888888"

# Style for the browser
BROWSER_STYLE = Style.from_dict(
    {
        "prompt": f"fg:{OWID_YELLOW} bold",
        "input": "bold",
        "match-count": f"fg:{OWID_GRAY}",
        "separator": f"fg:{OWID_GRAY}",
        "item": f"fg:{OWID_GRAY}",
        "item.highlight": f"fg:{OWID_GREEN} bold",
        "item.selected": f"bg:{OWID_YELLOW} fg:#000000 bold",
        "item.selected.highlight": f"bg:{OWID_YELLOW} fg:#000000 bold underline",
        "hint": f"fg:{OWID_GRAY} italic",
        "shortcut-key": f"bg:{OWID_GRAY} fg:#000000 bold",
        "shortcut-desc": f"fg:{OWID_GRAY}",
    }
)

# Maximum number of items to display
MAX_DISPLAY_ITEMS = 15


def highlight_matches(item: str, pattern: str, is_selected: bool) -> List[Tuple[str, str]]:
    """Create styled text segments with matching terms highlighted.

    Returns a list of (style, text) tuples for prompt_toolkit.
    """
    if not pattern:
        style = "class:item.selected" if is_selected else "class:item"
        return [(style, item)]

    base_style = "class:item.selected" if is_selected else "class:item"
    highlight_style = "class:item.selected.highlight" if is_selected else "class:item.highlight"

    terms = pattern.split()
    item_lower = item.lower()

    # Find all match positions
    matches: List[Tuple[int, int]] = []  # (start, end) positions
    for term in terms:
        term_lower = term.lower()
        start = 0
        while True:
            pos = item_lower.find(term_lower, start)
            if pos == -1:
                break
            matches.append((pos, pos + len(term)))
            start = pos + 1

    if not matches:
        return [(base_style, item)]

    # Merge overlapping matches and sort by position
    matches.sort()
    merged: List[Tuple[int, int]] = []
    for start, end in matches:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    # Build styled segments
    segments: List[Tuple[str, str]] = []
    pos = 0
    for start, end in merged:
        if pos < start:
            segments.append((base_style, item[pos:start]))
        segments.append((highlight_style, item[start:end]))
        pos = end
    if pos < len(item):
        segments.append((base_style, item[pos:]))

    return segments


class BrowserState:
    """State container for the browser application."""

    def __init__(self) -> None:
        self.selected_index: int = -1  # -1 means no selection (run all matches)
        self.scroll_offset: int = 0  # First visible item index
        self.matches: List[str] = []
        self.result: Optional[str] = None
        self.is_exact: bool = False
        self.cancelled: bool = False
        self.loading: bool = False  # True while items are loading in background
        self.all_items: List[str] = []  # Populated when loading completes
        self.app: Optional[Application[None]] = None  # Reference to app for invalidation


def filter_items(pattern: str, all_items: List[str]) -> List[str]:
    """Filter items by pattern using segment matching.

    Supports two modes:
    1. Space-separated terms: "energy 2024" matches items containing BOTH terms
    2. Single term: treated as regex (or substring if invalid regex)

    Matches are case-insensitive and sorted by relevance.
    """
    if not pattern:
        return []

    terms = pattern.split()

    # Multiple terms: AND matching (all terms must be present)
    if len(terms) > 1:
        terms_lower = [t.lower() for t in terms]
        matches = [s for s in all_items if all(term in s.lower() for term in terms_lower)]

        # Sort by: number of terms matched at word boundaries, then length
        def score(s: str) -> Tuple[int, int, str]:
            s_lower = s.lower()
            # Count terms that match at segment boundaries (after / or -)
            boundary_matches = sum(
                1 for term in terms_lower if f"/{term}" in s_lower or f"-{term}" in s_lower or s_lower.startswith(term)
            )
            return (-boundary_matches, len(s), s)

        matches.sort(key=score)
        return matches

    # Single term: regex matching with substring fallback
    try:
        compiled = re.compile(pattern, re.IGNORECASE)
        matches = [s for s in all_items if compiled.search(s)]
        # Sort by: exact matches first, then by item length (shorter = more specific)
        matches.sort(key=lambda s: (pattern.lower() not in s.lower(), len(s), s))
        return matches
    except re.error:
        # If the pattern is not a valid regex, fall back to simple substring match
        pattern_lower = pattern.lower()
        matches = [s for s in all_items if pattern_lower in s.lower()]
        matches.sort(key=lambda s: (len(s), s))
        return matches


def browse_items(
    items_loader: Callable[[], List[str]],
    prompt: str = "> ",
    loading_message: str = "Loading...",
    empty_message: str = "No items found.",
    item_noun: str = "item",
    item_noun_plural: str = "items",
    cached_items: Optional[List[str]] = None,
    on_items_loaded: Optional[Callable[[List[str]], None]] = None,
) -> Tuple[Optional[str], bool]:
    """Interactive item browser using prompt_toolkit.

    Args:
        items_loader: Callable that returns list of items to browse
        prompt: The prompt text to display (e.g., "etlr> ")
        loading_message: Message shown while items load
        empty_message: Message shown when no items exist
        item_noun: Singular noun for items (e.g., "step")
        item_noun_plural: Plural noun for items (e.g., "steps")
        cached_items: Pre-loaded items (skips loading if provided)
        on_items_loaded: Callback when items finish loading (for caching)

    Returns:
        Tuple of (pattern_or_item, is_exact_match):
        - If user presses Enter: (current_text, False) to run all matches
        - If user selects an item: (item, True) to run just that item
        - If user cancels: (None, False)
    """
    import threading

    # State for the application
    state = BrowserState()

    # If cached_items provided, use them immediately
    if cached_items is not None:
        state.all_items = cached_items
        state.loading = False
    else:
        # Show loading state and load in background
        state.loading = True

        def load_in_background() -> None:
            state.all_items = items_loader()
            state.loading = False

            # Call the on_items_loaded callback for caching
            if on_items_loaded is not None:
                on_items_loaded(state.all_items)

            # Trigger UI refresh from background thread
            if state.app is not None:
                state.app.invalidate()

        thread = threading.Thread(target=load_in_background, daemon=True)
        thread.start()

    # Create the input buffer
    def on_text_changed(buf: Buffer) -> None:
        text = buf.text.strip()
        state.matches = filter_items(text, state.all_items)
        state.selected_index = -1  # Reset selection when text changes
        state.scroll_offset = 0  # Reset scroll when text changes

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
            # User has selected a specific item
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
            max_idx = len(state.matches) - 1
            if state.selected_index < max_idx:
                state.selected_index += 1
                # Scroll down if selection goes past visible window
                if state.selected_index >= state.scroll_offset + MAX_DISPLAY_ITEMS:
                    state.scroll_offset = state.selected_index - MAX_DISPLAY_ITEMS + 1

    @kb.add("up")
    @kb.add("s-tab")
    def handle_up(event: Any) -> None:
        if state.selected_index > -1:
            state.selected_index -= 1
            # Scroll up if selection goes above visible window
            if state.selected_index >= 0 and state.selected_index < state.scroll_offset:
                state.scroll_offset = state.selected_index

    # Layout components
    def get_prompt_text() -> List[Tuple[str, str]]:
        return [("class:prompt", prompt)]

    def get_matches_text() -> List[Tuple[str, str]]:
        text = input_buffer.text.strip()
        matches = state.matches

        lines: List[Tuple[str, str]] = []

        # Show loading state
        if state.loading:
            lines.append(("class:hint", f"  {loading_message}"))
            lines.append(("", "\n"))
            lines.append(("class:separator", "  " + "-" * 50 + "\n"))
            return lines

        # Match count and hint line with styled shortcuts
        def add_shortcuts(shortcuts: List[Tuple[str, str]]) -> None:
            """Add nano-style shortcuts: [(key, description), ...]"""
            for i, (key, desc) in enumerate(shortcuts):
                lines.append(("", "  "))
                lines.append(("class:shortcut-key", f" {key} "))
                lines.append(("class:shortcut-desc", f" {desc}"))

        if text:
            count = len(matches)
            if count == 0:
                lines.append(("class:match-count", "  No matches"))
                add_shortcuts([("^C", "Exit")])
            else:
                count_text = f"1 {item_noun}" if count == 1 else f"{count} {item_noun_plural}"
                lines.append(("class:match-count", f"  {count_text}"))
                run_desc = "Run all" if state.selected_index < 0 else "Run selected"
                shortcuts = [("↑↓", "Select"), ("Enter", run_desc), ("^C", "Exit")]
                add_shortcuts(shortcuts)
        else:
            lines.append(("class:hint", f"  Type to filter {len(state.all_items)} {item_noun_plural}"))
            add_shortcuts([("^C", "Exit")])

        lines.append(("", "\n"))

        # Separator
        lines.append(("class:separator", "  " + "-" * 50 + "\n"))

        # Display matches with highlighting and scrolling
        start = state.scroll_offset
        end = min(start + MAX_DISPLAY_ITEMS, len(matches))
        display_matches = matches[start:end]

        # Show "more above" indicator
        if start > 0:
            lines.append(("class:hint", f"    ... {start} more above"))
            lines.append(("", "\n"))

        for i, item in enumerate(display_matches):
            absolute_idx = start + i
            is_selected = absolute_idx == state.selected_index
            prefix_style = "class:item.selected" if is_selected else "class:item"
            prefix = "  > " if is_selected else "    "
            lines.append((prefix_style, prefix))
            lines.extend(highlight_matches(item, text, is_selected))
            lines.append(("", "\n"))

        # Show "more below" indicator
        remaining = len(matches) - end
        if remaining > 0:
            lines.append(("class:hint", f"    ... {remaining} more below"))
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

    # Check if no items were loaded
    if not state.all_items and not state.loading:
        print(empty_message)
        return None, False

    return state.result, state.is_exact
