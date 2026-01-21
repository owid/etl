#
#  steps.py
#  Step browser for etlr command
#

import json
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

from etl.browser.core import browse_items, filter_items
from etl.dag_helpers import graph_nodes

# Simple type alias - avoids importing heavy etl.steps module
DAG = Dict[str, Set[str]]


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
    # If dag is provided directly, use it immediately
    if dag is not None:
        cached_items = get_all_steps(dag, private=private)
        items_loader = lambda: cached_items  # noqa: E731
        on_items_loaded = None
    elif dag_loader is not None:
        # Try loading from cache first (instant startup)
        cached_items = load_cached_steps(dag_path, private) if dag_path else None

        def items_loader() -> List[str]:
            loaded_dag = dag_loader()
            return get_all_steps(loaded_dag, private=private)

        def on_items_loaded(items: List[str]) -> None:
            if dag_path:
                save_step_cache(dag_path, private, items)
    else:
        raise ValueError("Either dag or dag_loader must be provided")

    return browse_items(
        items_loader=items_loader,
        prompt="etlr> ",
        loading_message="Loading steps...",
        empty_message="No steps found in DAG.",
        item_noun="step",
        item_noun_plural="steps",
        cached_items=cached_items,
        on_items_loaded=on_items_loaded,
    )


# Re-export filter_items as filter_steps for backwards compatibility
filter_steps = filter_items
