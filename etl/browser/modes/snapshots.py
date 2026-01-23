#
#  modes/snapshots.py
#  Snapshot browser mode for unified browser
#

from typing import Callable, List, Optional

from etl.browser.commands import DEFAULT_COMMANDS, Command
from etl.browser.modes import ModeConfig, ModeResult
from etl.browser.modes.base import BaseBrowserMode

# Type alias for ranker function
RankerFunc = Callable[[str, List[str]], List[str]]


class SnapshotMode(BaseBrowserMode):
    """Browser mode for snapshots.

    Wraps existing snapshot browser functionality with:
    - Snapshot directory scanning
    - Version-based ranking
    """

    def __init__(self) -> None:
        config = ModeConfig(
            name="snapshots",
            prompt="etls> ",
            item_noun="snapshot",
            item_noun_plural="snapshots",
            loading_message="Loading snapshots...",
            empty_message="No snapshots found.",
        )
        super().__init__(config)

    def get_items_loader(self) -> Callable[[], List[str]]:
        """Return callable that loads snapshots from directory."""
        from etl.browser.snapshots import get_all_snapshots

        return get_all_snapshots

    def get_cached_items(self) -> Optional[List[str]]:
        """Return cached snapshots if available."""
        if self._cached_items is not None:
            return self._cached_items

        from etl.browser.snapshots import load_cached_snapshots

        cached = load_cached_snapshots()
        if cached is not None:
            self._cached_items = cached
            return cached

        return None

    def get_ranker(self) -> Optional[RankerFunc]:
        """Return version-based ranker for snapshots."""
        from etl.browser.snapshots import create_snapshot_ranker

        return create_snapshot_ranker()

    def get_commands(self) -> List[Command]:
        """Return commands available in snapshot mode."""
        return DEFAULT_COMMANDS.copy()

    def on_items_loaded(self, items: List[str]) -> None:
        """Cache loaded items and save to disk."""
        super().on_items_loaded(items)

        from etl.browser.snapshots import save_snapshot_cache

        save_snapshot_cache(items)

    def on_select(self, item: str, is_exact: bool) -> ModeResult:
        """Handle snapshot selection."""
        return ModeResult(action="run", value=item, is_exact=is_exact)
