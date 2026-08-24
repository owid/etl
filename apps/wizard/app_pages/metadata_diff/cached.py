"""Cached wrappers around discovery and the diff computation.

Streamlit reruns the whole script on every widget interaction, so anything that talks to two databases
has to be cached or the page re-queries on each click. The TTL is deliberately long (`CACHE_TTL`): what
these read only changes when somebody rebuilds a step, and a timer expiring mid-review just costs a cold
load. Staleness is handled by the **Re-read** button in the section bar, which calls
`clear_discovery_caches`.

Engine arguments are prefixed with `_` so Streamlit skips them when hashing (they aren't hashable, and
a session only ever has one pair). `cache_key` is the deliberate part of the key — pass something that
changes when the underlying data should be re-read.
"""

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.utils import TARGET
from apps.wizard.app_pages.metadata_diff import discovery
from apps.wizard.app_pages.metadata_diff.core import ViewDiff
from apps.wizard.app_pages.metadata_diff.usage import charts_using_indicators, mdims_using_indicators
from etl.config import OWIDEnv

log = get_logger()


def _in_parallel(*thunks: Callable[[], Any]) -> list[Any]:
    """Run thunks concurrently, carrying this script run's context into each thread.

    They call `st.cache_data`-wrapped functions, which need the script run context for their spinners and
    their cache bookkeeping; a bare thread has none, and Streamlit then warns on every call.
    """
    ctx = get_script_run_ctx()

    def with_ctx(thunk: Callable[[], Any]) -> Any:
        add_script_run_ctx(threading.current_thread(), ctx)
        return thunk()

    with ThreadPoolExecutor(max_workers=len(thunks)) as pool:
        return list(pool.map(with_ctx, thunks))


# How long a reading of the two servers stays good. Long, because it only goes stale when somebody
# rebuilds a step, and the page carries a refresh button for exactly that: a timer that expires mid-review
# costs a cold load and explains nothing, whereas a button is asked for.
CACHE_TTL = 1800


@st.cache_resource
def master_engine() -> Engine | None:
    """Master's own staging server, or None when it cannot be reached.

    It answers the one question dataset timestamps cannot: is this text the branch's, or an edit master
    made that the baseline has not rebuilt yet? Cached as a resource because it is a connection pool, and
    optional because a review must still work when that server is down.
    """
    try:
        env = OWIDEnv.from_staging("master")
        if env.name == TARGET.name:
            # The baseline already *is* master's server, so cross-checking against it answers trivially.
            return None
        return env.get_engine()
    except Exception as e:  # noqa: BLE001 — no master server means "unknown", not a broken page
        log.warning("metadata_diff.master_engine_unavailable", error=str(e))
        return None


@st.cache_data(ttl=CACHE_TTL, show_spinner="Reading this staging server…")
def shared_facts(_source_engine: Engine, cache_key: str = "") -> tuple[Any, set[str]]:
    """(git scope, datasets rebuilt here) — read once per page rather than once per surface."""
    return discovery.shared_facts(_source_engine)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Looking for metadata changes on this staging server…")
def summary(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> discovery.Summary:
    """Counts behind the section badges (and the same numbers owidbot reports).

    Built from the same cache entries the three sections read, so a cold page computes each surface once
    instead of twice — the sections' own calls are hits afterwards. A surface that fails is passed as
    None, which leaves `summarize` to hit the same failure inside its own try/except and report it as the
    warning it always did.
    """
    scope_and_built = shared_facts(_source_engine, cache_key=cache_key)

    def read(fn):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — summarize re-raises it in the block that owns the surface
            log.warning("metadata_diff.prefetch_failed", fn=getattr(fn, "__name__", "?"), error=str(e))
            return None

    charts, mdims, explorers = _in_parallel(
        lambda: read(lambda: indicator_changes(_source_engine, _target_engine, cache_key=cache_key)),
        lambda: read(lambda: mdim_changes(_source_engine, _target_engine, cache_key=cache_key)),
        lambda: read(lambda: explorer_changes(_source_engine, _target_engine, cache_key=cache_key)),
    )
    # Attribution needs the changed paths, so it follows the three reads rather than joining them — but
    # it is the same cache entry the Charts section captions from, so it is computed once either way.
    origins = (
        read(lambda: indicator_attribution(_source_engine, _target_engine, tuple(charts.paths), cache_key=cache_key))
        if charts is not None
        else None
    )
    return discovery.summarize(
        _source_engine,
        _target_engine,
        master_engine(),
        changed=charts,
        df_mdims=mdims,
        explorers=explorers,
        facts=scope_and_built,
        attribution=origins,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Checking which MDims changed…")
def mdim_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> pd.DataFrame:
    """MDim list + change flags, indexed by catalogPath."""
    scope, built = shared_facts(_source_engine, cache_key=cache_key)
    return discovery.mdim_changes_df(_source_engine, _target_engine, scope, built)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Computing metadata diff for all views…")
def mdim_view_diffs(
    catalog_path: str,
    _source_engine: Engine,
    _target_engine: Engine,
    cache_key: str = "",
) -> tuple[list[dict[str, Any]], list[ViewDiff]]:
    """(dimensions, per-view diffs) for one MDim. `cache_key` should carry its config hashes."""
    config = discovery.load_mdim_config(_source_engine, catalog_path)
    assert config is not None, f"MDim {catalog_path} not found in staging."
    return config.get("dimensions") or [], discovery.mdim_text_changes(_source_engine, _target_engine, catalog_path)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Finding indicators whose text changed…")
def indicator_changes(
    _source_engine: Engine, _target_engine: Engine, cache_key: str = ""
) -> discovery.IndicatorChanges:
    """Indicators used by published charts whose user-visible text this branch changed."""
    scope, _ = shared_facts(_source_engine, cache_key=cache_key)
    return discovery.changed_indicators(_source_engine, _target_engine, None, scope)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Checking which side changed each dataset…")
def indicator_attribution(
    _source_engine: Engine,
    _target_engine: Engine,
    catalog_paths: tuple[str, ...],
    cache_key: str = "",
) -> dict[str, str]:
    """Per changed indicator: is the difference this branch's, master's, or a stale build here?"""
    return discovery.attribute_indicator_changes(_source_engine, _target_engine, list(catalog_paths), master_engine())


@st.cache_data(ttl=CACHE_TTL, show_spinner="Checking the charts' own config text…")
def chart_text_changes(
    _source_engine: Engine, _target_engine: Engine, cache_key: str = ""
) -> discovery.ChartTextChanges:
    """Published charts whose own config text (title / subtitle / footnote) this branch changed.

    A `presentation.grapher_config` edit in a garden step lands in the chart's resolved config and never
    touches the `variables` row, so it needs its own comparison — the indicator-layer one cannot see it.
    """
    scope, built = shared_facts(_source_engine, cache_key=cache_key)
    return discovery.changed_chart_texts(_source_engine, _target_engine, scope, built)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Finding explorer views whose text changed…")
def explorer_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> discovery.ExplorerChanges:
    """Published explorers whose view text changed, split into this branch's and baseline lag."""
    scope, built = shared_facts(_source_engine, cache_key=cache_key)
    return discovery.changed_explorer_views(_source_engine, _target_engine, scope, built)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Finding affected charts and MDims…")
def usage_for_indicators(
    indicator_ids: tuple[int, ...],
    catalog_path: str,
    _source_engine: Engine,
    cache_key: str = "",
) -> dict[int, dict[str, list[dict[str, Any]]]]:
    """For each changed indicator, the charts and *other* MDims on this server that use it.

    Published and draft charts come back under separate keys. Every caller that counts reach reads
    `charts`, so a draft cannot inflate a number by being in the same list — it has to be asked for.
    """
    ids = list(indicator_ids)
    if not ids:
        return {}
    charts = charts_using_indicators(_source_engine, ids)
    mdims = mdims_using_indicators(_source_engine, ids, exclude_catalog_path=catalog_path)
    return {
        i: {
            "charts": [c for c in charts.get(i, []) if c.get("is_published", True)],
            "draft_charts": [c for c in charts.get(i, []) if not c.get("is_published", True)],
            "mdims": mdims.get(i, []),
        }
        for i in ids
    }


def clear_discovery_caches() -> None:
    """Drop this page's readings of the two servers, so the next run re-reads them.

    Only this page's caches: `st.cache_data.clear()` would also throw away Chart Diff's and the producer
    analytics', which no reviewer asked for by pressing refresh here.
    """
    for cached_fn in (
        summary,
        chart_text_changes,
        mdim_changes,
        mdim_view_diffs,
        indicator_changes,
        indicator_attribution,
        explorer_changes,
        usage_for_indicators,
    ):
        cached_fn.clear()
