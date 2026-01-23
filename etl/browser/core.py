"""Generic browser UI using prompt_toolkit for interactive selection."""

import re
from typing import TYPE_CHECKING, Any, Callable, Literal, Protocol

from prompt_toolkit import Application
from prompt_toolkit.document import Document
from prompt_toolkit.formatted_text import StyleAndTextTuples
from prompt_toolkit.lexers import Lexer

if TYPE_CHECKING:
    from etl.browser.commands import Command
    from etl.browser.filters import FilterOptions, ParsedInput
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
from prompt_toolkit.layout.containers import Container
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.styles import Style

# OWID-styled colors (reused from harmonize.py and apps/pr/cli.py)
OWID_YELLOW = "#fac800"
OWID_GREEN = "#54cc90"
OWID_GRAY = "#888888"
OWID_DIM_GRAY = "#666666"  # Even dimmer for navigation hints

# Style for the browser
BROWSER_STYLE = Style.from_dict(
    {
        "prompt": f"fg:{OWID_YELLOW} bold",
        "input": "",  # Regular weight for search terms
        "match-count": f"fg:{OWID_DIM_GRAY}",
        "item": f"fg:{OWID_GRAY}",  # Dim gray for results
        "item.highlight": f"fg:{OWID_GREEN} bold",  # Bold green for search matches
        "item.filter-match": f"fg:{OWID_GREEN} italic",  # Italic green for filter-matched segments
        "item.selected": f"bg:{OWID_YELLOW} fg:#000000 bold",
        "item.selected.highlight": f"bg:{OWID_YELLOW} fg:#000000 bold underline",
        "item.selected.filter-match": f"bg:{OWID_YELLOW} fg:#000000 bold italic",
        "hint": f"fg:{OWID_DIM_GRAY} italic",  # Even dimmer for hints
        "shortcut-key": f"bg:{OWID_DIM_GRAY} fg:#000000 bold",
        "shortcut-desc": f"fg:{OWID_DIM_GRAY}",
        "frame.border": f"fg:{OWID_GRAY}",  # Subtle gray border
        "filter": f"fg:{OWID_GREEN} italic",  # Italic green for filter tokens in input
    }
)

# Maximum number of items to display
MAX_DISPLAY_ITEMS = 15

# Dashed box-drawing characters for OWID-styled frame
ROUNDED_TOP_LEFT = "╭"
ROUNDED_TOP_RIGHT = "╮"
ROUNDED_BOTTOM_LEFT = "╰"
ROUNDED_BOTTOM_RIGHT = "╯"
HORIZONTAL = "┄"  # Light triple dash horizontal
VERTICAL = "┆"  # Light triple dash vertical


def create_rounded_frame(body: Container) -> Container:
    """Create a frame with rounded corners around the given content.

    Args:
        body: The container to wrap in a frame

    Returns:
        A container with rounded border around the body
    """
    # Corner windows (fixed width)
    top_left = Window(
        content=FormattedTextControl([("class:frame.border", ROUNDED_TOP_LEFT)]),
        width=1,
        height=1,
        dont_extend_width=True,
        dont_extend_height=True,
    )
    top_right = Window(
        content=FormattedTextControl([("class:frame.border", ROUNDED_TOP_RIGHT)]),
        width=1,
        height=1,
        dont_extend_width=True,
        dont_extend_height=True,
    )
    bottom_left = Window(
        content=FormattedTextControl([("class:frame.border", ROUNDED_BOTTOM_LEFT)]),
        width=1,
        height=1,
        dont_extend_width=True,
        dont_extend_height=True,
    )
    bottom_right = Window(
        content=FormattedTextControl([("class:frame.border", ROUNDED_BOTTOM_RIGHT)]),
        width=1,
        height=1,
        dont_extend_width=True,
        dont_extend_height=True,
    )

    # Horizontal lines (extend to fill width)
    top_line = Window(char=HORIZONTAL, height=1, dont_extend_height=True, style="class:frame.border")
    bottom_line = Window(char=HORIZONTAL, height=1, dont_extend_height=True, style="class:frame.border")

    # Vertical borders (fixed width)
    left_border = Window(
        content=FormattedTextControl([("class:frame.border", VERTICAL)]),
        width=1,
        dont_extend_width=True,
    )
    right_border = Window(
        content=FormattedTextControl([("class:frame.border", VERTICAL)]),
        width=1,
        dont_extend_width=True,
    )

    # Build rows
    top_row = VSplit([top_left, top_line, top_right])
    middle_row = VSplit([left_border, body, right_border])
    bottom_row = VSplit([bottom_left, bottom_line, bottom_right])

    return HSplit([top_row, middle_row, bottom_row])


class Ranker(Protocol):
    """Protocol for ranking match results.

    Rankers take a search pattern and list of matched items, and return
    them reordered by relevance. Called after filtering, before display.
    """

    def __call__(self, pattern: str, matches: list[str]) -> list[str]:
        """Rank matches by relevance to pattern.

        Args:
            pattern: The search pattern used for filtering
            matches: List of items that matched the pattern

        Returns:
            Same items, reordered by relevance (most relevant first)
        """
        ...


def highlight_matches(
    item: str,
    pattern: str,
    is_selected: bool,
    filter_spans: list[tuple[int, int]] | None = None,
) -> list[tuple[str, str]]:
    """Create styled text segments with matching terms and filter matches highlighted.

    Args:
        item: The item text to highlight
        pattern: Search pattern (space-separated terms)
        is_selected: Whether this item is currently selected
        filter_spans: Optional list of (start, end) positions for filter-matched segments

    Returns:
        List of (style, text) tuples for prompt_toolkit.
    """
    base_style = "class:item.selected" if is_selected else "class:item"
    search_style = "class:item.selected.highlight" if is_selected else "class:item.highlight"
    filter_style = "class:item.selected.filter-match" if is_selected else "class:item.filter-match"

    # Collect all highlight spans with their types
    # Type 1 = search match (higher priority), Type 2 = filter match
    all_spans: list[tuple[int, int, int]] = []  # (start, end, type)

    # Add filter match spans
    if filter_spans:
        for start, end in filter_spans:
            all_spans.append((start, end, 2))

    # Find search term matches
    if pattern:
        terms = pattern.split()
        item_lower = item.lower()

        for term in terms:
            term_lower = term.lower()
            start = 0
            while True:
                pos = item_lower.find(term_lower, start)
                if pos == -1:
                    break
                all_spans.append((pos, pos + len(term), 1))
                start = pos + 1

    # If no highlights at all, return plain text
    if not all_spans:
        return [(base_style, item)]

    # Sort spans by position, then by type (search matches first for overlap handling)
    all_spans.sort(key=lambda x: (x[0], x[2]))

    # Build a character-level style map
    # 0 = base, 1 = search highlight, 2 = filter highlight
    char_styles = [0] * len(item)

    for start, end, span_type in all_spans:
        for i in range(start, min(end, len(item))):
            # Search matches (type 1) override filter matches (type 2)
            if span_type == 1 or char_styles[i] == 0:
                char_styles[i] = span_type

    # Build segments from character styles
    segments: list[tuple[str, str]] = []
    if not item:
        return segments

    current_style = char_styles[0]
    segment_start = 0

    style_map = {0: base_style, 1: search_style, 2: filter_style}

    for i in range(1, len(item)):
        if char_styles[i] != current_style:
            segments.append((style_map[current_style], item[segment_start:i]))
            current_style = char_styles[i]
            segment_start = i

    # Add final segment
    segments.append((style_map[current_style], item[segment_start:]))

    return segments


class FilterLexer(Lexer):
    """Lexer that highlights filter tokens (n:value, c:value, etc.) in the input."""

    def __init__(self, state: "BrowserState") -> None:
        self.state = state

    def lex_document(self, document: Document) -> Callable[[int], StyleAndTextTuples]:
        """Return a function that returns styled text for a given line."""

        def get_line(lineno: int) -> StyleAndTextTuples:
            text = document.lines[lineno] if lineno < len(document.lines) else ""
            parsed = self.state.parsed_input

            # No filters or not in search mode - return plain text
            if not parsed or not parsed.filter_spans or self.state.mode != "search":
                return [("class:input", text)]

            # Build styled fragments with filter tokens highlighted
            fragments: StyleAndTextTuples = []
            pos = 0
            for start, end in sorted(parsed.filter_spans):
                # Add text before this filter token
                if pos < start:
                    fragments.append(("class:input", text[pos:start]))
                # Add the filter token with highlight
                fragments.append(("class:filter", text[start:end]))
                pos = end
            # Add remaining text after last filter
            if pos < len(text):
                fragments.append(("class:input", text[pos:]))

            return fragments

        return get_line


class BrowserConfig:
    """Mutable configuration for browser display.

    This allows dynamic updates (e.g., mode switching) without recreating the browser.
    """

    def __init__(
        self,
        prompt: str = "> ",
        loading_message: str = "Loading...",
        empty_message: str = "No items found.",
        item_noun: str = "item",
        item_noun_plural: str = "items",
    ) -> None:
        self.prompt = prompt
        self.loading_message = loading_message
        self.empty_message = empty_message
        self.item_noun = item_noun
        self.item_noun_plural = item_noun_plural


# Type for mode switch callback (receives target mode name and state to update)
ModeSwitchCallback = Callable[[str, "BrowserState"], None]


class BrowserState:
    """State container for the browser application."""

    def __init__(self, history: list[str] | None = None) -> None:
        self.selected_index: int = -1  # -1 means no selection (run all matches)
        self.scroll_offset: int = 0  # First visible item index
        self.matches: list[str] = []
        self.result: str | None = None
        self.is_exact: bool = False
        self.cancelled: bool = False
        self.loading: bool = False  # True while items are loading in background
        self.all_items: list[str] = []  # Populated when loading completes
        self.app: Application[None] | None = None  # Reference to app for invalidation
        # Command mode state
        self.mode: Literal["search", "command"] = "search"
        self.command_matches: list["Command"] = []
        self.available_commands: list["Command"] = []
        # Refresh callback for reload functionality
        self.items_loader: Callable[[], list[str]] | None = None
        self.on_items_loaded: Callable[[list[str]], None] | None = None
        # Filter parsing state
        self.parsed_input: "ParsedInput" | None = None
        # Filter autocomplete options (cached)
        self.filter_options: "FilterOptions" | None = None
        # History state (shared across browser sessions)
        self.history: list[str] = history if history is not None else []
        self.history_index: int = -1  # -1 means not browsing history
        self.history_temp: str = ""  # Stores current input when entering history mode
        self._navigating_history: bool = False  # Flag to prevent resetting history on programmatic text changes
        # Mode switch state (for unified browser)
        self.switch_mode_target: str | None = None  # Target mode name when switching
        # Mutable display configuration
        self.config: BrowserConfig = BrowserConfig()
        # Mode switch callback (for in-place mode switching)
        self.on_mode_switch: ModeSwitchCallback | None = None
        # Ranker function (can be updated on mode switch)
        self.rank_matches: Ranker | None = None


def filter_items(pattern: str, all_items: list[str]) -> list[str]:
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
        def score(s: str) -> tuple[int, int, str]:
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
    items_loader: Callable[[], list[str]],
    prompt: str = "> ",
    loading_message: str = "Loading...",
    empty_message: str = "No items found.",
    item_noun: str = "item",
    item_noun_plural: str = "items",
    cached_items: list[str] | None = None,
    on_items_loaded: Callable[[list[str]], None] | None = None,
    rank_matches: Ranker | None = None,
    commands: list["Command"] | None = None,
    history: list[str] | None = None,
    on_mode_switch: ModeSwitchCallback | None = None,
) -> tuple[str | None, bool, list[str], str | None]:
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
        rank_matches: Optional ranker to reorder matches by relevance.
            If provided, called after filtering to sort results.
            If None, uses default sort (filter_items ordering).
        commands: Optional list of commands available via / prefix.
            If provided, typing / enters command mode.
        history: Optional list of previous search queries (most recent last).
            Use Up/Down when input is empty to navigate history.
        on_mode_switch: Optional callback for in-place mode switching.
            If provided, mode switches happen without exiting the browser.
            Callback receives target mode name and should update state.

    Returns:
        Tuple of (pattern_or_item, is_exact_match, updated_history, switch_mode_target):
        - If user presses Enter: (current_text, False, history, None) to run all matches
        - If user selects an item: (item, True, history, None) to run just that item
        - If user cancels: (None, False, history, None)
        - If mode switch (without callback): (None, False, history, target_mode_name)
    """
    import threading

    from etl.browser.commands import filter_commands
    from etl.browser.filters import (
        apply_filters,
        extract_filter_options,
        find_filter_match_spans,
        get_active_filter,
        parse_filters,
    )

    # State for the application
    state = BrowserState(history=history)
    state.available_commands = commands or []
    state.items_loader = items_loader
    state.on_items_loaded = on_items_loaded
    state.on_mode_switch = on_mode_switch
    state.rank_matches = rank_matches

    # Initialize mutable config
    state.config.prompt = prompt
    state.config.loading_message = loading_message
    state.config.empty_message = empty_message
    state.config.item_noun = item_noun
    state.config.item_noun_plural = item_noun_plural

    # If cached_items provided, use them immediately
    if cached_items is not None:
        state.all_items = cached_items
        state.filter_options = extract_filter_options(cached_items)
        state.loading = False
    else:
        # Show loading state and load in background
        state.loading = True

        def load_in_background() -> None:
            state.all_items = items_loader()
            state.filter_options = extract_filter_options(state.all_items)
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

        # Reset history navigation when user types (not when we set text programmatically)
        if state._navigating_history:
            state._navigating_history = False
        else:
            state.history_index = -1
            state.history_temp = ""

        # Check for command mode (input starts with /)
        if text.startswith("/") and state.available_commands:
            state.mode = "command"
            state.parsed_input = None  # Clear filter state in command mode
            pattern = text[1:]  # Strip leading /
            state.command_matches = filter_commands(pattern, state.available_commands)
            state.selected_index = 0 if state.command_matches else -1
        else:
            # Search mode with filter support
            state.mode = "search"
            state.command_matches = []

            # Parse filter prefixes from input
            parsed = parse_filters(text)
            state.parsed_input = parsed

            # Build search pattern from remaining terms
            search_pattern = " ".join(parsed.search_terms)

            # Filter by search terms first
            if search_pattern:
                matches = filter_items(search_pattern, state.all_items)
            else:
                # No search terms - start with all items if filters are present
                matches = state.all_items[:] if parsed.filters else []

            # Apply attribute filters
            matches = apply_filters(matches, parsed.filters)

            # Apply custom ranking
            if state.rank_matches is not None and matches:
                matches = state.rank_matches(search_pattern, matches)

            state.matches = matches
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
        # Handle command mode
        if state.mode == "command" and state.selected_index >= 0 and state.command_matches:
            cmd = state.command_matches[state.selected_index]
            result = cmd.handler(state)

            if result.action == "exit":
                state.cancelled = True
                event.app.exit()
            elif result.action == "switch_mode":
                if state.on_mode_switch is not None and result.target_mode:
                    # In-place mode switch - call callback and stay in browser
                    state.on_mode_switch(result.target_mode, state)
                    # Reload items for new mode
                    if state.items_loader is not None:
                        state.loading = True
                        state.all_items = []
                        state.matches = []

                        def reload_for_mode_switch() -> None:
                            state.all_items = state.items_loader()  # type: ignore[misc]
                            state.filter_options = extract_filter_options(state.all_items)
                            state.loading = False
                            if state.on_items_loaded is not None:
                                state.on_items_loaded(state.all_items)
                            if state.app is not None:
                                state.app.invalidate()

                        thread = threading.Thread(target=reload_for_mode_switch, daemon=True)
                        thread.start()
                    # Clear input and go back to search mode
                    input_buffer.text = ""
                else:
                    # No callback - store target and exit (legacy behavior)
                    state.switch_mode_target = result.target_mode
                    event.app.exit()
            elif result.action == "refresh":
                # Reload items
                if state.items_loader is not None:
                    state.loading = True
                    state.all_items = []
                    state.matches = []

                    def reload_items() -> None:
                        state.all_items = state.items_loader()  # type: ignore[misc]
                        state.loading = False
                        if state.on_items_loaded is not None:
                            state.on_items_loaded(state.all_items)
                        if state.app is not None:
                            state.app.invalidate()

                    thread = threading.Thread(target=reload_items, daemon=True)
                    thread.start()

                # Clear input and go back to search mode
                input_buffer.text = ""
            # "continue" action - stay in browser with input cleared
            return

        # Handle search mode
        text = input_buffer.text.strip()
        if state.selected_index >= 0 and state.matches:
            # User has selected a specific item
            selected_item = state.matches[state.selected_index]
            state.result = selected_item
            state.is_exact = True
            # Add selected item to history (the actual step that was executed)
            if not state.history or state.history[-1] != selected_item:
                state.history.append(selected_item)
        elif text and state.matches:
            # User pressed Enter with text AND matches exist - run all matches
            state.result = text
            state.is_exact = False
            # Add to history (if not duplicate of last entry)
            if not state.history or state.history[-1] != text:
                state.history.append(text)
        else:
            # Empty input OR no matches - do nothing, stay in browser
            return
        event.app.exit()

    @kb.add("c-c")
    @kb.add("escape")
    def handle_cancel(event: Any) -> None:
        state.cancelled = True
        event.app.exit()

    @kb.add("down")
    def handle_down(event: Any) -> None:
        if state.mode == "command":
            if state.command_matches:
                max_idx = len(state.command_matches) - 1
                if state.selected_index < max_idx:
                    state.selected_index += 1
        elif state.history_index >= 0:
            # Navigate history forward (toward more recent)
            if state.history_index < len(state.history) - 1:
                state.history_index += 1
                state._navigating_history = True
                input_buffer.text = state.history[state.history_index]
            else:
                # At the end of history, restore original input
                state.history_index = -1
                state._navigating_history = True
                input_buffer.text = state.history_temp
        elif state.matches:
            max_idx = len(state.matches) - 1
            if state.selected_index < max_idx:
                state.selected_index += 1
                # Scroll down if selection goes past visible window
                if state.selected_index >= state.scroll_offset + MAX_DISPLAY_ITEMS:
                    state.scroll_offset = state.selected_index - MAX_DISPLAY_ITEMS + 1

    @kb.add("tab")
    def handle_tab(event: Any) -> None:
        if state.mode == "command":
            if state.command_matches:
                max_idx = len(state.command_matches) - 1
                if state.selected_index < max_idx:
                    state.selected_index += 1
        elif state.history_index >= 0:
            # Exit history mode, keep current text, enable match selection
            state.history_index = -1
            # Start selection at first match if matches exist
            if state.matches:
                state.selected_index = 0
                state.scroll_offset = 0
        elif state.matches:
            max_idx = len(state.matches) - 1
            if state.selected_index < max_idx:
                state.selected_index += 1
                # Scroll down if selection goes past visible window
                if state.selected_index >= state.scroll_offset + MAX_DISPLAY_ITEMS:
                    state.scroll_offset = state.selected_index - MAX_DISPLAY_ITEMS + 1

    @kb.add("up")
    @kb.add("s-tab")
    def handle_up(event: Any) -> None:
        if state.mode == "command":
            if state.selected_index > 0:
                state.selected_index -= 1
        elif not input_buffer.text.strip() and state.history and state.history_index == -1:
            # Empty input and not in history mode - enter history mode from the end
            state.history_temp = input_buffer.text
            state.history_index = len(state.history) - 1
            state._navigating_history = True
            input_buffer.text = state.history[state.history_index]
        elif state.history_index > 0:
            # Navigate history backward (toward older)
            state.history_index -= 1
            state._navigating_history = True
            input_buffer.text = state.history[state.history_index]
        elif state.selected_index > -1:
            state.selected_index -= 1
            # Scroll up if selection goes above visible window
            if state.selected_index >= 0 and state.selected_index < state.scroll_offset:
                state.scroll_offset = state.selected_index

    # Layout components
    def get_prompt_text() -> list[tuple[str, str]]:
        return [("class:prompt", state.config.prompt)]

    def get_matches_text() -> list[tuple[str, str]]:
        text = input_buffer.text.strip()

        lines: list[tuple[str, str]] = []

        # Show loading state
        if state.loading:
            lines.append(("class:hint", f"  {state.config.loading_message}"))
            lines.append(("", "\n"))
            return lines

        # Helper to add nano-style shortcuts
        def add_shortcuts(shortcuts: list[tuple[str, str]]) -> None:
            """Add nano-style shortcuts: [(key, description), ...]"""
            for key, desc in shortcuts:
                lines.append(("", "  "))
                lines.append(("class:shortcut-key", f" {key} "))
                lines.append(("class:shortcut-desc", f" {desc}"))

        # Command mode rendering
        if state.mode == "command":
            cmd_matches = state.command_matches
            count = len(cmd_matches)

            if count == 0:
                lines.append(("class:match-count", "  No commands"))
                add_shortcuts([("^C", "Exit")])
            else:
                count_text = "1 command" if count == 1 else f"{count} commands"
                lines.append(("class:match-count", f"  {count_text}"))
                add_shortcuts([("↑↓", "Select"), ("Enter", "Run"), ("^C", "Exit")])

            lines.append(("", "\n"))

            # Display commands
            pattern = text[1:] if text.startswith("/") else ""  # Strip leading /
            for i, cmd in enumerate(cmd_matches):
                is_selected = i == state.selected_index
                prefix_style = "class:item.selected" if is_selected else "class:item"
                prefix = "  > " if is_selected else "    "
                lines.append((prefix_style, prefix))

                # Highlight matching portion of command name
                cmd_name = f"/{cmd.name}"
                lines.extend(highlight_matches(cmd_name, pattern, is_selected))

                # Add description in dimmer style
                desc_style = "class:item.selected" if is_selected else "class:hint"
                lines.append((desc_style, f"  {cmd.description}"))
                lines.append(("", "\n"))

            return lines

        # Search mode rendering (existing behavior)
        matches = state.matches

        # History mode: show matches but indicate history navigation is active
        if state.history_index >= 0:
            count = len(matches)
            if count == 0:
                lines.append(("class:match-count", "  No matches"))
            else:
                count_text = f"1 {state.config.item_noun}" if count == 1 else f"{count} {state.config.item_noun_plural}"
                lines.append(("class:match-count", f"  {count_text}"))
            lines.append(("class:hint", f"  History ({state.history_index + 1}/{len(state.history)})"))
            add_shortcuts([("Tab", "Select"), ("Enter", "Run"), ("^C", "Exit")])
        elif text:
            count = len(matches)
            if count == 0:
                lines.append(("class:match-count", "  No matches"))
                add_shortcuts([("^C", "Exit")])
            else:
                count_text = f"1 {state.config.item_noun}" if count == 1 else f"{count} {state.config.item_noun_plural}"
                lines.append(("class:match-count", f"  {count_text}"))
                run_desc = "Run all" if state.selected_index < 0 else "Run selected"
                shortcuts = [("↑↓", "Select"), ("Enter", run_desc), ("^C", "Exit")]
                add_shortcuts(shortcuts)
        else:
            # Show hint with filter prefixes for discoverability
            filter_hint = " (n: c: v: d:)" if state.all_items else ""
            lines.append(
                ("class:hint", f"  Type to filter {len(state.all_items)} {state.config.item_noun_plural}{filter_hint}")
            )
            shortcuts = [("^C", "Exit")]
            if state.history:
                shortcuts.insert(0, ("↑", "History"))
            add_shortcuts(shortcuts)

        lines.append(("", "\n"))

        # Show filter suggestions when user is typing a filter
        active_filter = get_active_filter(text)
        if active_filter and state.filter_options:
            options = state.filter_options.get(active_filter.attr)
            if options:
                # Filter options by what user has typed so far
                if active_filter.value:
                    matching = [o for o in options if o.lower().startswith(active_filter.value.lower())]
                else:
                    matching = options

                if matching:
                    # Show up to 8 suggestions
                    display_options = matching[:8]
                    more = len(matching) - 8 if len(matching) > 8 else 0

                    attr_label = active_filter.attr + "s"  # e.g., "namespaces"
                    suggestions = ", ".join(display_options)
                    if more > 0:
                        suggestions += f", +{more} more"

                    lines.append(("class:hint", f"  {attr_label}: "))
                    lines.append(("class:filter", suggestions))
                    lines.append(("", "\n"))

        # Display matches with highlighting and scrolling
        start = state.scroll_offset
        end = min(start + MAX_DISPLAY_ITEMS, len(matches))
        display_matches = matches[start:end]

        # Show "more above" indicator
        if start > 0:
            lines.append(("class:hint", f"    ... {start} more above"))
            lines.append(("", "\n"))

        # Use only search terms for highlighting (not filter tokens)
        highlight_pattern = " ".join(state.parsed_input.search_terms) if state.parsed_input else text
        # Get filters for highlighting matched segments
        filters = state.parsed_input.filters if state.parsed_input else {}

        for i, item in enumerate(display_matches):
            absolute_idx = start + i
            is_selected = absolute_idx == state.selected_index
            prefix_style = "class:item.selected" if is_selected else "class:item"
            prefix = "  > " if is_selected else "    "
            lines.append((prefix_style, prefix))
            # Compute filter match spans for this item
            filter_spans = find_filter_match_spans(item, filters) if filters else None
            lines.extend(highlight_matches(item, highlight_pattern, is_selected, filter_spans))
            lines.append(("", "\n"))

        # Show "more below" indicator
        remaining = len(matches) - end
        if remaining > 0:
            lines.append(("class:hint", f"    ... {remaining} more below"))
            lines.append(("", "\n"))

        return lines

    # Create layout with filter-highlighting lexer
    filter_lexer = FilterLexer(state)
    prompt_window = Window(
        content=FormattedTextControl(get_prompt_text), height=1, dont_extend_height=True, dont_extend_width=True
    )
    input_window = Window(
        content=BufferControl(buffer=input_buffer, lexer=filter_lexer), height=1, dont_extend_height=True
    )
    matches_window = Window(content=FormattedTextControl(get_matches_text), wrap_lines=False)

    # Combine prompt and input on the same line, wrapped in a rounded frame
    input_row = VSplit([prompt_window, input_window])
    input_frame = create_rounded_frame(input_row)

    root_container = HSplit([input_frame, matches_window])

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
        return None, False, state.history, None

    # Check for mode switch (unified browser feature)
    if state.switch_mode_target:
        return None, False, state.history, state.switch_mode_target

    # Check if no items were loaded
    if not state.all_items and not state.loading:
        print(state.config.empty_message)
        return None, False, state.history, None

    return state.result, state.is_exact, state.history, None
