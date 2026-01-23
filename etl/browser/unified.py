#
#  unified.py
#  Unified browser orchestrator for multi-mode ETL browser
#

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

from etl.browser.core import browse_items
from etl.browser.modes import BrowserMode, ModeRegistry, get_registry


@dataclass
class UnifiedBrowserResult:
    """Result from a unified browser session."""

    mode: str  # Mode that produced the result
    action: str  # "run", "exit"
    value: Optional[str] = None  # Selected item or pattern
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

    def __init__(self, registry: Optional[ModeRegistry] = None) -> None:
        self._registry = registry or get_registry()
        self._current_mode_name: Optional[str] = None
        # Per-mode history (in-memory, persisted by modes)
        self._histories: Dict[str, List[str]] = {}

    def register_mode(self, mode: BrowserMode, default: bool = False) -> None:
        """Register a browser mode."""
        self._registry.register(mode, default=default)

    @property
    def current_mode(self) -> Optional[BrowserMode]:
        """Get the current mode."""
        if self._current_mode_name:
            return self._registry.get(self._current_mode_name)
        return self._registry.get_default()

    def _get_mode_history(self, mode: BrowserMode) -> List[str]:
        """Get history for a mode, loading from cache if needed."""
        mode_name = mode.config.name
        if mode_name not in self._histories:
            self._histories[mode_name] = mode.get_history()
        return self._histories[mode_name]

    def _save_mode_history(self, mode: BrowserMode, history: List[str]) -> None:
        """Save history for a mode."""
        mode_name = mode.config.name
        self._histories[mode_name] = history
        mode.save_history(history)

    def _get_all_commands(self, mode: BrowserMode) -> List:
        """Get all commands for a mode including mode-switch commands."""

        # Start with mode-specific commands
        commands = mode.get_commands()

        # Add mode-switch commands (only for modes other than current)
        current_name = mode.config.name
        for switch_cmd in self._registry.get_mode_switch_commands():
            if switch_cmd.name != current_name:
                commands.append(switch_cmd)

        return commands

    def run(self, initial_mode: Optional[str] = None) -> UnifiedBrowserResult:
        """Run the browser, returning when user selects an item or exits.

        Args:
            initial_mode: Optional mode name to start in. Uses default if not specified.

        Returns:
            UnifiedBrowserResult with the outcome of the session.
        """
        # Set initial mode
        if initial_mode:
            self._current_mode_name = initial_mode
        elif not self._current_mode_name:
            self._current_mode_name = self._registry.default_mode_name

        while True:
            mode = self.current_mode
            if mode is None:
                return UnifiedBrowserResult(mode="unknown", action="exit", value=None, is_exact=False)

            config = mode.config
            history = self._get_mode_history(mode)
            commands = self._get_all_commands(mode)

            # Run the browser for current mode
            result, is_exact, updated_history, switch_mode = browse_items(
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
            )

            # Save updated history
            self._save_mode_history(mode, updated_history)

            # Handle mode switch
            if switch_mode:
                target_mode = self._registry.get(switch_mode)
                if target_mode:
                    self._current_mode_name = switch_mode
                    # Continue loop with new mode
                    continue
                # Unknown mode - ignore and stay in current mode
                continue

            # Handle cancellation
            if result is None:
                return UnifiedBrowserResult(mode=config.name, action="exit", value=None, is_exact=False)

            # Handle selection - delegate to mode
            mode_result = mode.on_select(result, is_exact)

            if mode_result.action == "switch_mode" and mode_result.value:
                # Mode requested a switch
                target_mode = self._registry.get(mode_result.value)
                if target_mode:
                    self._current_mode_name = mode_result.value
                    continue

            return UnifiedBrowserResult(
                mode=config.name,
                action=mode_result.action,
                value=mode_result.value,
                is_exact=mode_result.is_exact,
            )


def create_default_browser(
    private: bool = False,
    dag_path: Optional[Path] = None,
    dag_loader: Optional[Callable[[], Dict[str, Set[str]]]] = None,
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
    dag_path: Optional[Path] = None,
    dag_loader: Optional[Callable[[], Dict[str, Set[str]]]] = None,
    initial_mode: str = "steps",
) -> Tuple[Optional[str], bool, str]:
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
