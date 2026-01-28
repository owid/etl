"""Step browser with popularity ranking and caching."""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from apps.browser.core import filter_items
from apps.browser.scoring import create_ranker, extract_version_from_uri
from etl.dag_helpers import graph_nodes

if TYPE_CHECKING:
    from apps.browser.core import Ranker

# Simple type alias - avoids importing heavy etl.steps module
DAG = dict[str, set[str]]

# Popularity cache TTL in seconds (1 hour)
POPULARITY_CACHE_TTL = 3600

# Maximum number of history entries to cache
MAX_HISTORY_ENTRIES = 10


def get_all_steps(dag: DAG, private: bool = False) -> list[str]:
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


def load_cached_steps(dag_path: Path, private: bool) -> list[str] | None:
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


def save_step_cache(dag_path: Path, private: bool, steps: list[str]) -> None:
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


def extract_dataset_slug(uri: str) -> str | None:
    """Extract dataset slug from step URI for popularity lookup.

    Converts URI like 'data://grapher/who/2024-01-15/gho' to 'who/2024-01-15/gho'.

    Args:
        uri: Step URI

    Returns:
        Dataset slug (namespace/version/dataset) or None
    """
    # Strip protocol prefix
    if "://" in uri:
        _, path = uri.split("://", 1)
    else:
        path = uri

    parts = path.split("/")

    # Format: channel/namespace/version/dataset[/...]
    if len(parts) >= 4:
        # Return namespace/version/dataset
        return f"{parts[1]}/{parts[2]}/{parts[3]}"

    return None


def load_popularity_cache() -> tuple[dict[str, float], bool]:
    """Load popularity data from cache.

    Returns:
        Tuple of (popularity_dict, is_stale):
        - popularity_dict: Maps dataset slug to popularity score (0.0-1.0)
        - is_stale: True if cache is older than POPULARITY_CACHE_TTL
    """
    from etl import paths

    cache_file = paths.POPULARITY_CACHE_FILE
    if not cache_file.exists():
        return {}, True

    try:
        with open(cache_file) as f:
            cache = json.load(f)

        # Check if cache is stale
        fetched_at = cache.get("fetched_at", "")
        is_stale = True
        if fetched_at:
            try:
                cache_time = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - cache_time).total_seconds()
                is_stale = age > POPULARITY_CACHE_TTL
            except (ValueError, TypeError):
                pass

        return cache.get("data", {}), is_stale
    except (json.JSONDecodeError, OSError, KeyError):
        return {}, True


def save_popularity_cache(data: dict[str, float]) -> None:
    """Save popularity data to cache."""
    from etl import paths

    cache_file = paths.POPULARITY_CACHE_FILE
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        cache = {
            "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "data": data,
        }
        with open(cache_file, "w") as f:
            json.dump(cache, f)
    except OSError:
        pass  # Silently fail - cache is optional


def load_history_cache() -> list[str]:
    """Load browser history from cache.

    Returns:
        List of previous search queries (most recent last), limited to MAX_HISTORY_ENTRIES.
    """
    from etl import paths

    cache_file = paths.HISTORY_CACHE_FILE
    if not cache_file.exists():
        return []

    try:
        with open(cache_file) as f:
            cache = json.load(f)
        history = cache.get("history", [])
        # Ensure it's a list of strings and limit size
        if isinstance(history, list):
            return [h for h in history if isinstance(h, str)][-MAX_HISTORY_ENTRIES:]
        return []
    except (json.JSONDecodeError, OSError):
        return []


def save_history_cache(history: list[str]) -> None:
    """Save browser history to cache.

    Args:
        history: List of search queries (most recent last).
    """
    from etl import paths

    cache_file = paths.HISTORY_CACHE_FILE
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Keep only the last MAX_HISTORY_ENTRIES
        trimmed_history = history[-MAX_HISTORY_ENTRIES:]

        cache = {"history": trimmed_history}
        with open(cache_file, "w") as f:
            json.dump(cache, f)
    except OSError:
        pass  # Silently fail - cache is optional


def fetch_popularity_data(steps: list[str]) -> dict[str, float]:
    """Fetch popularity data from Datasette for given steps.

    Args:
        steps: List of step URIs (unused, we fetch all dataset popularity)

    Returns:
        Dict mapping dataset slug to popularity score (0.0-1.0)
    """
    _ = steps  # Unused - we fetch all dataset popularity in one query
    try:
        from owid.catalog.api.datasette import DatasetteAPI

        api = DatasetteAPI()
        # Filter to slugs with namespace/version/dataset format (at least 2 slashes)
        # This excludes invalid entries like "dataset/1234"
        df = api.query("SELECT slug, popularity FROM analytics_popularity WHERE type = 'dataset' AND slug LIKE '%/%/%'")

        if df.empty:
            return {}

        return dict(zip(df["slug"], df["popularity"].astype(float)))
    except Exception:
        return {}


def refresh_popularity_cache_async(
    steps: list[str],
    live_data: dict[str, float] | None = None,
) -> None:
    """Refresh popularity cache in background thread.

    Args:
        steps: List of step URIs to fetch popularity for
        live_data: Optional mutable dict to update with fetched data.
            If provided, the dict will be updated in-place when fetch completes,
            allowing live updates to ranking without restart.
    """

    def _refresh() -> None:
        data = fetch_popularity_data(steps)
        if data:
            save_popularity_cache(data)
            # Update live data dict if provided (for immediate use in current session)
            if live_data is not None:
                live_data.clear()
                live_data.update(data)

    thread = threading.Thread(target=_refresh, daemon=True)
    thread.start()


def create_step_ranker(popularity_data: dict[str, float]) -> "Ranker":
    """Create a ranker for step browser results.

    Uses lexicographic sorting:
    1. Match quality (better matches first)
    2. Popularity (more popular datasets first)
    3. Version recency (newer versions first, as tiebreaker)

    Args:
        popularity_data: Dict mapping dataset slug to popularity (0.0-1.0)

    Returns:
        Ranker function for use with browse_items
    """
    return create_ranker(
        popularity_data=popularity_data,
        slug_extractor=extract_dataset_slug,
        version_extractor=extract_version_from_uri,
    )


# Re-export filter_items as filter_steps for backwards compatibility
filter_steps = filter_items
