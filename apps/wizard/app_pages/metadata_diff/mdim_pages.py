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
    CHART_FIELD_PREFIX,
    ViewDiff,
    diff_views,
    field_label,
    group_changes,
)
from apps.wizard.app_pages.metadata_diff.data import (
    build_chart_bundle,
)
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    chart_datapage_url,
    render_impact,
    render_text_html,
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
) -> None:
    """One looked-up chart's changed fields, side by side, in data-page order.

    Diff only. It used to carry an Approve/Flag sign-off keyed on `chart:<slug>` — a second review record
    beside the Charts list's ticks, read by nothing and consulted at no point in the merge — and a PR
    brief of its own. Both went with the rest of that workflow; the ticks on the Charts list are the one
    review state, and this is the lookup that answers "did this branch touch this chart's text".
    """
    groups = group_changes([diff])
    if not groups:
        return

    for g in groups:
        link_kind = "chart ↗" if g.field.startswith(CHART_FIELD_PREFIX) else "data page ↗"
        with st.expander(f"{field_label(g.field)}", expanded=True):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{BASELINE_NAME.capitalize()}**] · [{link_kind}]({baseline_url})")
                st.markdown(render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(f":green[**This staging server**] · [{link_kind}]({staging_url})")
                st.markdown(render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)


def chart_flow(source_engine: Engine, target_engine: Engine) -> None:
    """Review a standalone chart's data-page WYSK (the indicator metadata it inherits), vs the baseline."""
    ref = st.text_input(
        "Chart",
        key="chart",
        placeholder="Chart slug, id, or grapher URL (e.g. daily-mean-income)",
        help="Select a chart to see changes to its metadata.",
    )
    if not ref:
        st.info("Select a chart to see changes to its metadata.")
        return

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
    baseline_url = chart_datapage_url(TARGET, cid)
    staging_url = chart_datapage_url(SOURCE, cid)
    links = f"[{BASELINE_NAME} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"

    if diff.is_new:
        st.info(f"This chart is **new** — it does not exist in {BASELINE_NAME}. " + links)
        return
    if not diff.changed:
        st.success("No changes to this chart's data-page text. " + links)
        return

    nf = len(diff.fields)
    st.warning(f"**{nf} field{'s' if nf != 1 else ''} changed** in this chart.")
    render_impact(diff, usage, unit="chart")
    # Each changed field in its own collapsible, baseline on the left and this server on the right.
    render_chart_review(chart, diff, source_engine, baseline_url, staging_url, usage)
