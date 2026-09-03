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
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import data, discovery
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    LAYOUT_QUERY_KEY,
    ViewDiff,
    dims_str,
    field_label,
    group_changes,
    group_usage,
    item_identity,
    surface_key,
    view_label,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.usage import charts_using_indicators, mdims_using_indicators
from etl.analytics.data import get_chart_views_last_n_days
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
) -> tuple[str, list[dict[str, Any]], list[ViewDiff]]:
    """(title, dimensions, per-view diffs) for one MDim. `cache_key` should carry its config hashes.

    The title is the config's own human-readable one ("Poverty", not `wb/latest/poverty_pip#poverty_pip`),
    falling back to the catalogPath where the config carries none.
    """
    config = discovery.load_mdim_config(_source_engine, catalog_path)
    assert config is not None, f"MDim {catalog_path} not found in staging."
    raw_title = config.get("title")
    title = str((raw_title.get("title") if isinstance(raw_title, dict) else raw_title) or catalog_path)
    return (
        title,
        config.get("dimensions") or [],
        discovery.mdim_text_changes(_source_engine, _target_engine, catalog_path),
    )


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


@st.cache_data(ttl=CACHE_TTL, show_spinner="Listing the items to review…")
def item_index(
    _source_engine: Engine, _target_engine: Engine, cache_key: str = ""
) -> tuple[dict[str, dict[str, str]], dict[str, int]]:
    """change key -> {name, url, hash} for every reviewable item, plus how many each surface holds.

    Shared by the Review tab, which needs the names, and the section bar, which needs the totals. Built by
    enumerating the items the sections show and hashing each one the way its tick was hashed — a stored row
    carries a hash, not a name, because the slot has to survive an edit to the text.

    `hash` is that slot's *current* content hash — what `resolve_item_mark` compares a stored row against
    to decide whether a verdict still describes the text it was made on. The Review tab needs it for the
    same reason the section lists do: a stored row keeps its status when the wording moves under it, and
    counting it as decided would put a rejection of text nobody read into the hand-off document. Charts
    included: their fields are not enumerable the way a view's are, so `chart_diff_fields` builds them in
    bulk from the same comparison their own page renders.

    An enumeration that fails is skipped rather than guessed at: a total that quietly omits a surface is
    worse than one that is conservative, and the sections report their own ceilings.
    """
    index: dict[str, dict[str, str]] = {}
    totals: dict[str, int] = {}

    # --- MDim views ---
    try:
        df = mdim_changes(_source_engine, _target_engine, cache_key=cache_key)
        flagged = [str(cp) for cp in df.index[df["in_branch"] & df["has_changes"]]]
    except Exception:  # noqa: BLE001
        flagged, df = [], None
    for catalog_path in flagged:
        assert df is not None
        row = df.loc[catalog_path]
        surface = surface_key("item", f"mdim:{catalog_path}")
        try:
            title, dimensions, view_diffs = mdim_view_diffs(
                catalog_path,
                _source_engine,
                _target_engine,
                cache_key=f"{row['configMd5_source']}::{row['configMd5_target']}",
            )
        except Exception:  # noqa: BLE001
            continue
        slug = str(row["slug_source"]) if row.get("slug_source") else ""
        changed = [v for v in view_diffs if v.changed]
        totals[surface] = len(changed)
        for view in changed:
            key, content_hash = item_identity(surface, dims_str(view.dimensions), view.fields)
            index[key] = {
                "name": f"{title or catalog_path} — {view_label(view, dimensions)}",
                "url": view_url(SOURCE, catalog_path, None if row["is_draft"] else slug, view.dimensions),
                "hash": content_hash,
            }

    # --- Explorer views ---
    try:
        branch = explorer_changes(_source_engine, _target_engine, cache_key=cache_key).branch_views()
    except Exception:  # noqa: BLE001
        branch = {}
    for explorer_slug, diffs in branch.items():
        surface = surface_key("item", f"explorer:{explorer_slug}")
        changed = [d for d in diffs if d.changed]
        totals[surface] = len(changed)
        for view in changed:
            key, content_hash = item_identity(surface, dims_str(view.dimensions), view.fields)
            label = " · ".join(str(v) for v in view.dimensions.values()) or "(view)"
            index[key] = {
                "name": f"{explorer_slug} — {label}",
                "url": f"{SOURCE.site}/explorers/{explorer_slug}?{urlencode(view.dimensions)}",
                "hash": content_hash,
            }

    # --- Charts ---
    surface = surface_key("item", "chart")
    try:
        counts = changed_charts(_source_engine, _target_engine, cache_key=cache_key)
    except Exception:  # noqa: BLE001
        counts = {}
    totals[surface] = len(counts)
    # The same fields the chart's own page hashes its verdict on, so a chart verdict goes stale exactly
    # as an MDim or explorer one does. An enumeration that fails leaves the hash out rather than guessing
    # at it: without a hash a verdict is reported as recorded, which is what it was before; with a wrong
    # one every chart verdict would read as reopened forever.
    try:
        chart_fields = chart_diff_fields(_source_engine, _target_engine, cache_key=cache_key)
    except Exception:  # noqa: BLE001
        chart_fields = {}
    for chart_slug, n_changes in counts.items():
        fields = chart_fields.get(chart_slug)
        key, content_hash = item_identity(surface, chart_slug, fields or {})
        index[key] = {
            "name": f"{chart_slug} ({n_changes} change{'s' if n_changes != 1 else ''})",
            "url": f"{SOURCE.site}/grapher/{chart_slug}",
            **({"hash": content_hash} if fields is not None else {}),
        }

    # --- Edits, per section: what the By-edit layouts tick ---
    try:
        facts = summary(_source_engine, _target_engine, cache_key=cache_key)
    except Exception:  # noqa: BLE001
        facts = None
    if facts is not None:
        wizard = SOURCE.wizard_url.rstrip("/")
        for section in ("charts", "mdims", "explorers"):
            surface = surface_key("item", f"edit:{section}")
            edits = discovery.edits_for(facts, section)
            totals[surface] = len(edits)
            for edit in edits:
                key, content_hash = item_identity(surface, discovery.edit_key(edit), discovery.edit_fields(edit))
                words = " ".join((edit.inserted or edit.deleted or "").split())
                words = words if len(words) <= 60 else words[:57].rstrip() + "…"
                index[key] = {
                    "name": f"{field_label(edit.field)} — “{words}” ({edit.n_texts} text{'s' if edit.n_texts != 1 else ''})",
                    "url": f"{wizard}/metadata-diff?diff-type={section}&{LAYOUT_QUERY_KEY}=changes",
                    "hash": content_hash,
                }
    return index, totals


@st.cache_data(ttl=CACHE_TTL, show_spinner="Reading what changed on each chart…")
def chart_diff_fields(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> dict[str, dict]:
    """slug -> the changed fields of that chart: exactly what its own page hashes its verdict on.

    Every other surface enumerates its items and hashes each one, so a stored verdict reopens when the
    wording moves under it. A chart's fields are assembled by comparing two bundles rather than listed,
    which is why they were missing here and chart verdicts alone never reopened — a tick survived any
    number of rewrites of the text it was supposed to certify.

    Bulk, and cached, because that is what makes it affordable: `compare_charts` reads every changed chart
    in a fixed number of queries per environment, where doing it chart by chart was three queries each per
    side and turned this into hundreds of round trips on a branch touching a few dozen charts.
    """
    slugs = sorted(changed_charts(_source_engine, _target_engine, cache_key=cache_key))
    if not slugs:
        return {}
    changed = indicator_changes(_source_engine, _target_engine, cache_key=cache_key)
    comparisons = data.compare_charts(_source_engine, _target_engine, slugs, changed_paths=changed.diffs)
    return {ref: cmp.diff.fields for ref, cmp in comparisons.items()}


@st.cache_data(ttl=CACHE_TTL, show_spinner="Listing the charts this branch changed…")
def changed_charts(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> dict[str, int]:
    """Every published chart this branch changed, and how many distinct changes each carries.

    Shared by the Charts section's picker and the Review tab: both need the same list, and computing it
    twice would let them disagree about how many charts there are.
    """
    changed = indicator_changes(_source_engine, _target_engine, cache_key=cache_key)
    chart_text = chart_text_changes(_source_engine, _target_engine, cache_key=cache_key)
    groups = group_changes(changed.view_diffs()) + group_changes(chart_text.view_diffs())
    usage = usage_for_indicators(tuple(changed.ids_list), "", _source_engine, cache_key=cache_key)

    counts: dict[str, int] = {}
    for group in groups:
        if group.field.startswith(CHART_FIELD_PREFIX):
            charts = [chart_text.charts[d["chart"]] for d in group.view_dims if d.get("chart") in chart_text.charts]
        else:
            charts = group_usage(group, usage).get("charts", [])
        for chart in charts:
            slug = str(chart.get("slug") or "")
            if slug:
                counts[slug] = counts.get(slug, 0) + 1
    return counts


@st.cache_data(ttl=CACHE_TTL, show_spinner="Finding explorer views whose text changed…")
def explorer_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str = "") -> discovery.ExplorerChanges:
    """Published explorers whose view text changed, split into this branch's and baseline lag."""
    scope, built = shared_facts(_source_engine, cache_key=cache_key)
    return discovery.changed_explorer_views(_source_engine, _target_engine, scope, built)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Reading how much these charts are viewed…")
def chart_views(chart_ids: tuple[int, ...], n_days: int = 365, cache_key: str = "") -> dict[int, int]:
    """chart id -> page views over the last `n_days`, for ordering a list of affected charts.

    A year rather than a month: what an author wants from this order is which of these charts matters,
    and a month of traffic on a seasonal topic answers a different question.

    Empty on any failure, and the caller falls back to name order. This is the one reading in the tool
    that leaves OWID's databases for the analytics warehouse, so it is also the one most likely to be
    unavailable — no credentials on this server, or the warehouse down — and a review must not stop for
    it. Measured at ~3s for 76 charts, which is why it is cached and fetched only when the branch that
    needs it is actually drawn.
    """
    if not chart_ids:
        return {}
    try:
        df = get_chart_views_last_n_days(chart_ids=list(chart_ids), n_days=n_days)
    except Exception as e:  # noqa: BLE001 — an unreachable warehouse means "unknown", not a broken page
        log.warning("metadata_diff.chart_views_unavailable", error=str(e))
        return {}
    return {int(row["chart_id"]): int(row["views"]) for row in df.to_dict("records")}


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def mdim_redirected_charts(_source_engine: Engine, cache_key: str = "") -> dict[str, str]:
    """Chart slug -> the MDim its URL redirects to. Empty when the lookup fails, which lists them all."""
    try:
        return data.fetch_mdim_redirected_charts(_source_engine)
    except Exception as e:  # noqa: BLE001 — an older server may not have the table; list the charts then
        log.warning("metadata_diff.mdim_redirects_unavailable", error=str(e))
        return {}


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def explorer_titles(_source_engine: Engine, cache_key: str = "") -> dict[str, str]:
    """Published explorers' reader-facing names, by slug."""
    return data.fetch_explorer_titles(_source_engine)


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
        explorer_titles,
        usage_for_indicators,
        changed_charts,
        item_index,
    ):
        cached_fn.clear()
