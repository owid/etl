"""SnapshotMode: Snapshot browsing with version-based ranking."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from apps.browser.commands import DEFAULT_COMMANDS, Command
from apps.browser.modes import ModeConfig, ModeResult
from apps.browser.modes.base import BaseBrowserMode
from apps.browser.options import BrowserOption

if TYPE_CHECKING:
    from apps.browser.core import Ranker


class SnapshotMode(BaseBrowserMode):
    """Browser mode for snapshots.

    Wraps existing snapshot browser functionality with:
    - Snapshot directory scanning
    - Version-based ranking
    """

    def __init__(self) -> None:
        config = ModeConfig(
            name="snapshots",
            prompt="snapshot> ",
            item_noun="snapshot",
            item_noun_plural="snapshots",
            description="Browse snapshot files for data ingestion",
            loading_message="Loading snapshots...",
            empty_message="No snapshots found.",
        )
        super().__init__(config)

    def get_items_loader(self) -> Callable[[], list[str]]:
        """Return callable that loads snapshots from directory."""
        from apps.browser.snapshots import get_all_snapshots

        return get_all_snapshots

    def get_cached_items(self) -> list[str] | None:
        """Return cached snapshots if available."""
        if self._cached_items is not None:
            return self._cached_items

        from apps.browser.snapshots import load_cached_snapshots

        cached = load_cached_snapshots()
        if cached is not None:
            self._cached_items = cached
            return cached

        return None

    def get_ranker(self) -> "Ranker" | None:
        """Return version-based ranker for snapshots."""
        from apps.browser.snapshots import create_snapshot_ranker

        return create_snapshot_ranker()

    def get_commands(self) -> list[Command]:
        """Return commands available in snapshot mode."""
        return DEFAULT_COMMANDS.copy()

    def on_items_loaded(self, items: list[str]) -> None:
        """Cache loaded items and save to disk."""
        super().on_items_loaded(items)

        from apps.browser.snapshots import save_snapshot_cache

        save_snapshot_cache(items)

    def on_select(self, item: str, is_exact: bool) -> ModeResult:
        """Handle snapshot selection."""
        return ModeResult(action="run", value=item, is_exact=is_exact)

    def get_options(self) -> list[BrowserOption]:
        """Get CLI options available for snapshot execution.

        Returns key options from `etl snapshot` command that are useful in browser context.
        """
        return [
            BrowserOption(
                name="dry_run",
                flag_name="dry-run",
                is_flag=True,
                default=False,
                help="Preview without creating snapshot",
            ),
            BrowserOption(
                name="upload",
                flag_name="upload",
                is_flag=True,
                default=True,
                help="Upload to snapshot storage",
            ),
        ]
