"""Unified browser orchestrator for multi-mode ETL browser."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from etl.browser.core import OWID_YELLOW, BrowserState, browse_items
from etl.browser.modes import BrowserMode, ModeRegistry, get_registry

if TYPE_CHECKING:
    from etl.browser.commands import Command

# ASCII art title for the browser
BROWSER_TITLE = r"""
  ___
 | _ ) _ _ _____ __ _____ ___ _ _
 | _ \| '_/ _ \ V  V (_-</ -_) '_|
 |___/|_| \___/\_/\_//__/\___|_|
"""


def _get_time_greeting() -> str:
    """Get a casual greeting based on time of day."""
    hour = datetime.now().hour
    if hour < 5:
        return "🌙 Burning the midnight oil?"
    elif hour < 7:
        return "🌅 Early bird gets the worm!"
    elif hour < 12:
        return "☀️  Good morning!"
    elif hour < 14:
        return "🍴 Lunchtime hacking?"
    elif hour < 17:
        return "☀️  Good afternoon!"
    elif hour < 21:
        return "🌆 Good evening!"
    else:
        return "🦉 Night owl mode activated!"


def print_browser_intro() -> None:
    """Print the browser intro/title with greeting."""
    # ANSI escape for OWID yellow
    yellow = f"\033[38;2;{int(OWID_YELLOW[1:3], 16)};{int(OWID_YELLOW[3:5], 16)};{int(OWID_YELLOW[5:7], 16)}m"
    reset = "\033[0m"
    dim = "\033[2m"

    print(f"{yellow}{BROWSER_TITLE}{reset}")
    print(f"  {_get_time_greeting()}")
    print(f"{dim}  Type to search · Tab/arrows to select · /commands for actions{reset}")
    print()


@dataclass
class UnifiedBrowserResult:
    """Result from a unified browser session."""

    mode: str  # Mode that produced the result
    action: str  # "run", "exit"
    value: str | None = None  # Selected item or pattern
    is_exact: bool = False  # True if specific item selected


class UnifiedBrowser:
    """Orchestrator for multi-mode browser sessions.

    Manages mode switching and delegates to individual modes for
    item loading, ranking, and selection handling.

    Usage:
        browser = UnifiedBrowser()
        browser.register_mode(StepMode(...), default=True)
        browser.register_mode(SnapshotMode())

        while True:
            result = browser.run()
            if result.action == "exit":
                break
            if result.action == "run":
                # Execute the selected item
                ...
    """

    def __init__(self, registry: ModeRegistry | None = None) -> None:
        self._registry = registry or get_registry()
        self._current_mode_name: str | None = None
        # Per-mode history (in-memory, persisted by modes)
        self._histories: dict[str, list[str]] = {}
        # Track if intro has been shown
        self._intro_shown: bool = False

    def register_mode(self, mode: BrowserMode, default: bool = False) -> None:
        """Register a browser mode."""
        self._registry.register(mode, default=default)

    @property
    def current_mode(self) -> BrowserMode | None:
        """Get the current mode."""
        if self._current_mode_name:
            return self._registry.get(self._current_mode_name)
        return self._registry.get_default()

    def _get_mode_history(self, mode: BrowserMode) -> list[str]:
        """Get history for a mode, loading from cache if needed."""
        mode_name = mode.config.name
        if mode_name not in self._histories:
            self._histories[mode_name] = mode.get_history()
        return self._histories[mode_name]

    def _save_mode_history(self, mode: BrowserMode, history: list[str]) -> None:
        """Save history for a mode."""
        mode_name = mode.config.name
        self._histories[mode_name] = history
        mode.save_history(history)

    def _get_all_commands(self, mode: BrowserMode) -> list["Command"]:
        """Get all commands for a mode including mode-switch commands."""
        # Start with mode-specific commands
        commands = mode.get_commands()

        # Add mode-switch commands (only for modes other than current)
        current_name = mode.config.name
        for switch_cmd in self._registry.get_mode_switch_commands():
            if switch_cmd.name != current_name:
                commands.append(switch_cmd)

        return commands

    def _create_mode_switch_callback(self) -> Callable[[str, BrowserState], None]:
        """Create a callback for in-place mode switching.

        The callback updates BrowserState with new mode's configuration.
        """

        def on_mode_switch(target_mode_name: str, state: BrowserState) -> None:
            target_mode = self._registry.get(target_mode_name)
            if target_mode is None:
                return  # Unknown mode, ignore

            # Save current mode's history before switching
            if self._current_mode_name:
                current_mode = self._registry.get(self._current_mode_name)
                if current_mode:
                    self._save_mode_history(current_mode, state.history)

            # Switch to new mode
            self._current_mode_name = target_mode_name
            config = target_mode.config

            # Update state configuration
            state.config.prompt = config.prompt
            state.config.loading_message = config.loading_message
            state.config.empty_message = config.empty_message
            state.config.item_noun = config.item_noun
            state.config.item_noun_plural = config.item_noun_plural

            # Update items loader and callbacks
            state.items_loader = target_mode.get_items_loader()
            state.on_items_loaded = target_mode.on_items_loaded
            state.rank_matches = target_mode.get_ranker()

            # Update commands (include new mode-switch commands)
            state.available_commands = self._get_all_commands(target_mode)

            # Load history for new mode
            state.history = self._get_mode_history(target_mode)
            state.history_index = -1
            state.history_temp = ""

        return on_mode_switch

    def run(self, initial_mode: str | None = None) -> UnifiedBrowserResult:
        """Run the browser, returning when user selects an item or exits.

        Args:
            initial_mode: Optional mode name to start in. Uses default if not specified.

        Returns:
            UnifiedBrowserResult with the outcome of the session.
        """
        # Show intro on first run
        if not self._intro_shown:
            print_browser_intro()
            self._intro_shown = True

        # Set initial mode
        if initial_mode:
            self._current_mode_name = initial_mode
        elif not self._current_mode_name:
            self._current_mode_name = self._registry.default_mode_name

        mode = self.current_mode
        if mode is None:
            return UnifiedBrowserResult(mode="unknown", action="exit", value=None, is_exact=False)

        config = mode.config
        history = self._get_mode_history(mode)
        commands = self._get_all_commands(mode)

        # Create mode switch callback for in-place switching
        on_mode_switch = self._create_mode_switch_callback()

        # Run the browser with in-place mode switching
        result, is_exact, updated_history, _switch_mode = browse_items(
            items_loader=mode.get_items_loader(),
            prompt=config.prompt,
            loading_message=config.loading_message,
            empty_message=config.empty_message,
            item_noun=config.item_noun,
            item_noun_plural=config.item_noun_plural,
            cached_items=mode.get_cached_items(),
            on_items_loaded=mode.on_items_loaded,
            rank_matches=mode.get_ranker(),
            commands=commands,
            history=history,
            on_mode_switch=on_mode_switch,
        )

        # Save history for final mode
        final_mode = self.current_mode
        if final_mode:
            self._save_mode_history(final_mode, updated_history)
            final_mode_name = final_mode.config.name
        else:
            final_mode_name = "unknown"

        # Handle cancellation
        if result is None:
            return UnifiedBrowserResult(mode=final_mode_name, action="exit", value=None, is_exact=False)

        # Handle selection - delegate to mode
        if final_mode:
            mode_result = final_mode.on_select(result, is_exact)
            return UnifiedBrowserResult(
                mode=final_mode_name,
                action=mode_result.action,
                value=mode_result.value,
                is_exact=mode_result.is_exact,
            )

        return UnifiedBrowserResult(
            mode=final_mode_name,
            action="run",
            value=result,
            is_exact=is_exact,
        )


def create_default_browser(
    private: bool = False,
    dag_path: Path | None = None,
    dag_loader: Callable[[], dict[str, set[str]]] | None = None,
) -> UnifiedBrowser:
    """Create a unified browser with default modes (steps and snapshots).

    Args:
        private: Whether to include private steps
        dag_path: Path to DAG file (for step caching)
        dag_loader: Callable that returns DAG

    Returns:
        Configured UnifiedBrowser instance
    """
    from etl.browser.modes.snapshots import SnapshotMode
    from etl.browser.modes.steps import StepMode

    browser = UnifiedBrowser()

    # Register steps mode (default)
    step_mode = StepMode(
        private=private,
        dag_path=dag_path,
        dag_loader=dag_loader,
    )
    browser.register_mode(step_mode, default=True)

    # Register snapshots mode
    snapshot_mode = SnapshotMode()
    browser.register_mode(snapshot_mode)

    return browser


def browse_unified(
    private: bool = False,
    dag_path: Path | None = None,
    dag_loader: Callable[[], dict[str, set[str]]] | None = None,
    initial_mode: str = "steps",
) -> tuple[str | None, bool, str]:
    """Run unified browser session.

    Convenience function for running the unified browser.

    Args:
        private: Whether to include private steps
        dag_path: Path to DAG file
        dag_loader: Callable that returns DAG
        initial_mode: Mode to start in ("steps" or "snapshots")

    Returns:
        Tuple of (selected_item, is_exact, mode_name):
        - selected_item: The selected item or pattern, None if cancelled
        - is_exact: True if specific item selected
        - mode_name: Name of mode that produced the result
    """
    browser = create_default_browser(
        private=private,
        dag_path=dag_path,
        dag_loader=dag_loader,
    )

    result = browser.run(initial_mode=initial_mode)

    if result.action == "exit":
        return None, False, result.mode

    return result.value, result.is_exact, result.mode
