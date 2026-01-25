"""StepMode: ETL step browsing with popularity ranking."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Set

from etl.browser.commands import DEFAULT_COMMANDS, Command
from etl.browser.modes import ModeConfig, ModeResult
from etl.browser.modes.base import BaseBrowserMode

if TYPE_CHECKING:
    from etl.browser.core import Ranker

# Simple type alias - avoids importing heavy etl.steps module
DAG = dict[str, Set[str]]


class StepMode(BaseBrowserMode):
    """Browser mode for ETL steps.

    Wraps existing step browser functionality with:
    - DAG-based step loading with caching
    - Popularity-based ranking
    - Private step filtering
    """

    def __init__(
        self,
        dag: DAG | None = None,
        private: bool = False,
        dag_loader: Callable[[], DAG] | None = None,
        dag_path: Path | None = None,
    ) -> None:
        config = ModeConfig(
            name="steps",
            prompt="steps> ",
            item_noun="step",
            item_noun_plural="steps",
            description="Browse and run ETL pipeline steps (data://, grapher://, export://)",
            loading_message="Loading steps...",
            empty_message="No steps found in DAG.",
        )
        super().__init__(config)

        self._dag = dag
        self._private = private
        self._dag_loader = dag_loader
        self._dag_path = dag_path

        # Popularity data for ranking (mutable dict for live updates)
        self._popularity_data: dict[str, float] = {}
        self._popularity_refresh_started = False

    def get_items_loader(self) -> Callable[[], list[str]]:
        """Return callable that loads steps from DAG."""
        from etl.browser.steps import get_all_steps

        if self._dag is not None:
            dag = self._dag
            private = self._private
            return lambda: get_all_steps(dag, private=private)

        if self._dag_loader is not None:
            dag_loader = self._dag_loader
            private = self._private
            return lambda: get_all_steps(dag_loader(), private=private)

        raise ValueError("Either dag or dag_loader must be provided")

    def get_cached_items(self) -> list[str] | None:
        """Return cached steps if available."""
        if self._cached_items is not None:
            return self._cached_items

        if self._dag_path:
            from etl.browser.steps import load_cached_steps

            cached = load_cached_steps(self._dag_path, self._private)
            if cached is not None:
                self._cached_items = cached
                self._start_popularity_refresh(cached)
                return cached

        if self._dag is not None:
            from etl.browser.steps import get_all_steps

            self._cached_items = get_all_steps(self._dag, private=self._private)
            self._start_popularity_refresh(self._cached_items)
            return self._cached_items

        return None

    def _start_popularity_refresh(self, steps: list[str]) -> None:
        """Start background popularity data refresh if needed."""
        if self._popularity_refresh_started:
            return

        from etl.browser.steps import load_popularity_cache, refresh_popularity_cache_async

        # Load cached popularity data
        cached_popularity, is_stale = load_popularity_cache()
        self._popularity_data.update(cached_popularity)

        # Refresh in background if stale
        if is_stale:
            refresh_popularity_cache_async(steps, live_data=self._popularity_data)
            self._popularity_refresh_started = True

    def get_ranker(self) -> "Ranker" | None:
        """Return popularity-based ranker for steps."""
        from etl.browser.steps import create_step_ranker

        return create_step_ranker(self._popularity_data)

    def get_commands(self) -> list[Command]:
        """Return commands available in step mode."""
        return DEFAULT_COMMANDS.copy()

    def on_items_loaded(self, items: list[str]) -> None:
        """Cache loaded items and save to disk."""
        super().on_items_loaded(items)

        if self._dag_path:
            from etl.browser.steps import save_step_cache

            save_step_cache(self._dag_path, self._private, items)

        self._start_popularity_refresh(items)

    def on_select(self, item: str, is_exact: bool) -> ModeResult:
        """Handle step selection."""
        return ModeResult(action="run", value=item, is_exact=is_exact)
