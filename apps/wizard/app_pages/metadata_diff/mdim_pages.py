"""The single-chart lookup: is this chart's text different on this staging server, and how.

Reached from the Charts list — a button on a change, or the "Look up any chart" box, which takes any
published chart whether this branch touched it or not. That second use is the point: confirming a chart
you were worried about is untouched.

The module is named for the three per-MDim pages it used to hold (Blast radius, View diff, Review & PR
brief). Those are gone: they formed a closed loop nothing read at merge, and what they contributed lives
on the MDim cards and in the Blast radius section. Renaming the module is a follow-up.

One baseline throughout (see render.py): production where this server has production credentials,
`staging-site-master` otherwise — the same baseline chart-diff uses.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import (
    ViewDiff,
    diff_views,
    field_label,
    group_changes,
)
from apps.wizard.app_pages.metadata_diff.data import (
    build_chart_bundle,
    fetch_chart_indicator_paths,
    load_reviews,
)
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    chart_datapage_url,
    render_impact,
    render_text_html,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    resolve_item_mark,
    st_review_strip,
    surface_key,
)

# URL-parameter prefix for the MDim view selectors (`?d_<dimension>=<choice>`).
DIM_PARAM_PREFIX = "d_"


def _clear_view_params() -> None:
    """Drop the previous MDim's view-selector params when another MDim is selected."""
    for key in list(st.query_params.keys()):
        if key.startswith(DIM_PARAM_PREFIX):
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)


def render_chart_review(
    chart: dict[str, Any],
    diff: ViewDiff,
    source_engine: Engine,
    baseline_url: str,
    staging_url: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    recorded: dict | None = None,
    page: str = "data page",
) -> None:
    """One looked-up chart's changed fields, side by side, in data-page order.

    `page` names what the two links open — a data page for a single-indicator chart, the chart itself for
    one that has none. The layout below is still the data page's slot order, which is what makes two
    charts comparable at a glance; where a chart has no data page its readers meet the same texts in the
    sources drawer, in this order, under each indicator.

    Diff only. It used to carry an Approve/Flag sign-off keyed on `chart:<slug>` — a second review record
    beside the Charts list's ticks, read by nothing and consulted at no point in the merge — and a PR
    brief of its own. Both went with the rest of that workflow; the ticks on the Charts list are the one
    review state, and this is the lookup that answers "did this branch touch this chart's text".
    """
    groups = group_changes([diff])
    if not groups:
        return

    # The two pages, once, with the chart's own tick beside them. They were repeated on every field's two
    # columns — the same pair of links four or five times down a chart with four changed fields — and the
    # tick was below the last of them, so on a chart with several fields you scrolled past the answer to
    # reach the control.
    st.markdown(
        f":gray[**{BASELINE_NAME.capitalize()}**] [{page} ↗]({baseline_url}) · "
        f":green[**This staging server**] [{page} ↗]({staging_url})"
    )
    if diff.fields:
        surface = surface_key("item", "chart")
        mark = resolve_item_mark(
            # The picker has already read every chart's recorded state to mark its options.
            recorded if recorded is not None else load_reviews(source_engine, surface),
            surface,
            str(chart.get("slug") or chart["chartId"]),
            diff.fields,
        )
        st_review_strip(source_engine, surface, mark)
    for g in groups:
        with st.expander(f"{field_label(g.field)}", expanded=True):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{BASELINE_NAME.capitalize()}**]")
                st.markdown(render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(":green[**This staging server**]")
                st.markdown(render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)


def _changed_indicator_of(source_engine: Engine, target_engine: Engine, chart_id: int) -> str | None:
    """One indicator of this chart whose own text this branch changed, or None.

    `changed.diffs`, not `changed.ids`: the latter says an indicator was compared, which every indicator
    of a rebuilt dataset was. Deterministic on ties — sorted — so the chart reviews the same series on
    every rerun and the verdict recorded against it keeps meaning the same thing.
    """
    changed = cached.indicator_changes(source_engine, target_engine)
    for path in sorted(fetch_chart_indicator_paths(source_engine, chart_id)):
        if path in changed.diffs:
            return path
    return None


def render_chart_by_ref(source_engine: Engine, target_engine: Engine, ref: str, recorded: dict | None = None) -> None:
    """Review one chart's inherited text against the baseline — every field of it this branch changed.

    Takes the chart as an argument rather than reading a text box: the box was the only way to reach this
    page, so it sat above the answer on every visit, asking for something the caller already knows.
    """
    src = build_chart_bundle(source_engine, ref)
    if src is None:
        st.warning(f"No published chart found for “{ref}”. Check the slug/id.")
        return
    src_bundle, chart = src

    # Grapher renders a data page only for single-indicator charts — say so when it doesn't.
    if not chart.get("has_data_page", True):
        st.warning(
            f"**{chart.get('title') or chart['slug']}** is a **multi-indicator chart** "
            f"({chart['n_indicators']} indicators), so it has **no data page**: its readers reach this text "
            "through *Learn more about this data*, under each indicator, rather than on the page itself."
        )
    tgt = build_chart_bundle(target_engine, str(chart["slug"]))
    target_bundle = tgt[0] if tgt is not None else None

    diff = diff_views([src_bundle], [target_bundle] if target_bundle is not None else [])[0]

    # A multi-series chart is in the changed list because *some* indicator of it moved, and the primary y
    # — whose metadata a data page renders, and which every comparison above is built from — is not always
    # that one. Reviewing it then compared an indicator nothing had happened to and said "No changes" on a
    # chart that does carry an edit, with no verdict control on it and the section's total out of reach.
    #
    # Asked only when the default comparison finds nothing, so the ordinary chart costs no extra query.
    # It reviews one changed indicator, not all of them: a chart carrying edits to two of its series still
    # shows only the first, which is worth knowing rather than worth implying.
    if not diff.fields and int(chart.get("n_indicators") or 0) > 1:
        pinned = _changed_indicator_of(source_engine, target_engine, int(chart["chartId"]))
        rebuilt = build_chart_bundle(source_engine, ref, catalog_path=pinned) if pinned else None
        if rebuilt is not None:
            src_bundle, chart = rebuilt
            other = build_chart_bundle(target_engine, str(chart["slug"]), catalog_path=pinned)
            target_bundle = other[0] if other is not None else None
            diff = diff_views([src_bundle], [target_bundle] if target_bundle is not None else [])[0]

    # Blast radius on the chart's indicator — but exclude the chart itself from its own affected list.
    usage: dict[int, dict[str, list[dict[str, Any]]]] = {}
    if diff.affects_indicator and diff.indicator_id is not None:
        raw = cached.usage_for_indicators(
            (diff.indicator_id,),
            f"chart:{chart['slug']}",
            source_engine,
            cache_key=f"chart-{chart['slug']}",
        )
        cur = int(chart["chartId"])
        usage = {
            vid: {"charts": [c for c in e.get("charts", []) if c.get("chartId") != cur], "mdims": e.get("mdims", [])}
            for vid, e in raw.items()
        }

    st.markdown(DIFF_CSS, unsafe_allow_html=True)  # same diff styling as the MDim view page
    st.markdown(f"#### {chart.get('title') or chart['slug']}")
    # Single-indicator chart data pages don't render on a staging server by default (they come up
    # blank); the admin chart preview with `forceDatapage=true` forces the data page, so WYSK /
    # description_key edits are actually visible. Use it for both envs (works on production too).
    cid = chart["chartId"]
    # `forceDatapage=true` only where there is a data page to force. A multi-indicator chart has none —
    # this function says so a few lines up — and forcing one there opened a page the chart does not have,
    # under a link calling it the data page. Those charts open as the chart, which is where their readers
    # meet this text, in the sources drawer.
    has_data_page = bool(chart.get("has_data_page", True))
    slug = str(chart.get("slug") or "")
    if has_data_page:
        baseline_url = chart_datapage_url(TARGET, cid)
        staging_url = chart_datapage_url(SOURCE, cid)
        page = "data page"
    else:
        baseline_url = f"{TARGET.site}/grapher/{slug}"
        staging_url = f"{SOURCE.site}/grapher/{slug}"
        page = "chart"
    links = f"[{BASELINE_NAME} ({page})]({baseline_url}) · [this staging server ({page})]({staging_url})"

    if diff.is_new:
        st.info(f"This chart is **new** — it does not exist in {BASELINE_NAME}. " + links)
        return
    if not diff.changed:
        st.success("No changes to this chart's data-page text. " + links)
        return

    nf = len(diff.fields)
    st.markdown(f"**{nf} field{'s' if nf != 1 else ''} changed** in this chart.")
    render_impact(diff, usage, unit="chart")
    # Each changed field in its own collapsible, baseline on the left and this server on the right.
    render_chart_review(chart, diff, source_engine, baseline_url, staging_url, usage, recorded, page)
