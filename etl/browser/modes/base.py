"""Base class for browser modes with history and command defaults."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable

from etl.browser.commands import DEFAULT_COMMANDS, Command
from etl.browser.modes import ModeConfig, ModeResult

if TYPE_CHECKING:
    from etl.browser.core import Ranker

# Maximum number of history entries to cache per mode
MAX_HISTORY_ENTRIES = 10


class BaseBrowserMode:
    """Base class for browser modes with shared functionality.

    Provides:
    - History loading/saving with per-mode cache files
    - Default commands (refresh, exit)
    - Common configuration patterns
    """

    def __init__(self, config: ModeConfig) -> None:
        self._config = config
        self._cached_items: list[str] | None = None

    @property
    def config(self) -> ModeConfig:
        """Mode configuration."""
        return self._config

    def get_items_loader(self) -> Callable[[], list[str]]:
        """Return a callable that loads items for this mode.

        Override in subclasses.
        """
        raise NotImplementedError

    def get_cached_items(self) -> list[str] | None:
        """Return cached items if available."""
        return self._cached_items

    def get_ranker(self) -> "Ranker" | None:
        """Return optional ranker for match ordering.

        Override in subclasses if ranking is needed.
        """
        return None

    def get_commands(self) -> list[Command]:
        """Return commands available in this mode.

        Default includes refresh and exit. Override to add more.
        """
        return DEFAULT_COMMANDS.copy()

    def on_items_loaded(self, items: list[str]) -> None:
        """Called when items finish loading (for caching).

        Override in subclasses for custom caching.
        """
        self._cached_items = items

    def on_select(self, item: str, is_exact: bool) -> ModeResult:
        """Handle item selection.

        Default behavior: return a "run" action with the selected item.
        Override in subclasses for custom behavior.
        """
        return ModeResult(action="run", value=item, is_exact=is_exact)

    def get_history(self) -> list[str]:
        """Get browsing history for this mode."""
        from etl import paths

        cache_file = paths.get_mode_history_file(self._config.name)
        if not cache_file.exists():
            return []

        try:
            with open(cache_file) as f:
                cache = json.load(f)
            history = cache.get("history", [])
            if isinstance(history, list):
                return [h for h in history if isinstance(h, str)][-MAX_HISTORY_ENTRIES:]
            return []
        except (json.JSONDecodeError, OSError):
            return []

    def save_history(self, history: list[str]) -> None:
        """Save browsing history for this mode."""
        from etl import paths

        cache_file = paths.get_mode_history_file(self._config.name)
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            trimmed_history = history[-MAX_HISTORY_ENTRIES:]
            cache = {"history": trimmed_history}
            with open(cache_file, "w") as f:
                json.dump(cache, f)
        except OSError:
            pass  # Silently fail - cache is optional
