#
#  snapshots.py
#  Snapshot browser for etls command
#

import json
from typing import TYPE_CHECKING

from etl.browser.commands import DEFAULT_COMMANDS
from etl.browser.core import browse_items
from etl.browser.scoring import create_ranker, extract_version_from_snapshot

if TYPE_CHECKING:
    from etl.browser.core import Ranker


def get_all_snapshots() -> list[str]:
    """Get all snapshot URIs from the snapshots directory.

    Scans snapshots/**/*.dvc and returns URIs in the format:
    namespace/version/short_name (without extension)
    """
    from etl import paths

    snapshots = []
    for dvc_file in paths.SNAPSHOTS_DIR.glob("**/*.dvc"):
        # Get relative path from snapshots dir
        rel_path = dvc_file.relative_to(paths.SNAPSHOTS_DIR)

        # Convert to snapshot URI: namespace/version/short_name
        # The .dvc file is like: namespace/version/short_name.ext.dvc
        # We want: namespace/version/short_name.ext (without .dvc)
        # But the URI format is: namespace/version/short_name (remove both .dvc and file extension)
        uri = str(rel_path.with_suffix(""))  # Remove .dvc
        snapshots.append(uri)

    return sorted(snapshots)


def load_cached_snapshots() -> list[str] | None:
    """Load snapshots from cache if valid.

    Uses snapshot count as cache key - fast O(1) check.
    Returns cached snapshots list, or None if cache is invalid/missing.
    """
    from etl import paths

    cache_file = paths.SNAPSHOT_CACHE_FILE
    if not cache_file.exists():
        return None

    try:
        with open(cache_file) as f:
            cache = json.load(f)

        # Quick validation: just return cached data
        # The count check happens in browse_snapshots after loading
        return cache.get("snapshots", [])
    except (json.JSONDecodeError, OSError, KeyError):
        return None


def save_snapshot_cache(snapshots: list[str]) -> None:
    """Save snapshots to cache for instant startup next time."""
    from etl import paths

    cache_file = paths.SNAPSHOT_CACHE_FILE
    try:
        # Ensure cache directory exists
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        cache = {
            "snapshots_dir": str(paths.SNAPSHOTS_DIR),
            "count": len(snapshots),
            "snapshots": snapshots,
        }
        with open(cache_file, "w") as f:
            json.dump(cache, f)
    except OSError:
        pass  # Silently fail - cache is optional


def create_snapshot_ranker() -> "Ranker":
    """Create a ranker for snapshot browser results.

    Uses lexicographic sorting:
    1. Match quality (better matches first)
    2. Version recency (newer versions first, as tiebreaker)

    No popularity data for snapshots.

    Returns:
        Ranker function for use with browse_items
    """
    return create_ranker(
        popularity_data=None,
        slug_extractor=None,
        version_extractor=extract_version_from_snapshot,
    )


def browse_snapshots(history: list[str] | None = None) -> tuple[str | None, bool, list[str], str | None]:
    """Interactive snapshot browser using prompt_toolkit.

    Args:
        history: Optional list of previous search queries (most recent last).
            Use Up/Down when input is empty to navigate history.

    Returns:
        Tuple of (pattern_or_snapshot, is_exact_match, updated_history, switch_mode_target):
        - If user presses Enter: (current_text, False, history, None) to run all matches
        - If user selects a snapshot: (snapshot_uri, True, history, None) to run just that snapshot
        - If user cancels: (None, False, history, None)
        - If mode switch: (None, False, history, target_mode_name)
    """
    # Try loading from cache first
    cached_items = load_cached_snapshots()

    # Create ranker for improved match ordering
    ranker = create_snapshot_ranker()

    return browse_items(
        items_loader=get_all_snapshots,
        prompt="etls> ",
        loading_message="Loading snapshots...",
        empty_message="No snapshots found.",
        item_noun="snapshot",
        item_noun_plural="snapshots",
        cached_items=cached_items,
        on_items_loaded=save_snapshot_cache,
        rank_matches=ranker,
        commands=DEFAULT_COMMANDS,
        history=history,
    )
