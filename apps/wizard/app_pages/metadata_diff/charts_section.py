"""Charts section: the indicator texts this branch changed, and the charts that render them.

Organised by *change*, not by chart. One reworded WYSK bullet can reach hundreds of charts; listing it
once with its charts underneath is both shorter and truer to what the author has to decide, and it keeps
the page usable on a big data update where a per-chart list would run to thousands of entries.

What this section adds is the layer Chart Diff cannot see: text a chart inherits from ETL metadata.
That is more than the indicator's own fields. A garden step can also set the chart's FAUST through
`presentation.grapher_config`, and those edits reach readers as title, subtitle and footnote while leaving
the `variables` row untouched — so they are compared here too, alongside WYSK. What belongs to Chart Diff
is the other origin: text typed into a chart in the admin.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff import cached, edits_view, mdim_pages
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    ChangeGroup,
    group_changes,
    group_usage,
    requested_chart,
    version_pairs,
)
from apps.wizard.app_pages.metadata_diff.data import load_reviews
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    st_layout_switcher,
    st_note,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    item_marker,
    surface_key,
)


def st_show_chart_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the Charts section: every indicator text change, with the charts it lands on.

    Unless one chart is named. `?chart=<slug>` is a route, not a filter: it shows that chart's own review
    and nothing else. It used to render inside an expander below the whole list, which is why following a
    link to it looked like nothing had happened.
    """
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    changed = cached.indicator_changes(source_engine, target_engine)
    chart_text = cached.chart_text_changes(source_engine, target_engine)
    # Indicator-layer changes first, then the charts' own config text. Grouped separately because the two
    # carry their affected charts differently, then reviewed as one list.
    groups = group_changes(changed.view_diffs()) + group_changes(chart_text.view_diffs())
    usage = cached.usage_for_indicators(tuple(changed.ids_list), "", source_engine)

    if not changed.narrowed:
        st.warning(
            "Could not read this branch's changed files from git, so the list is **not narrowed to your "
            "branch** — it may include metadata that master has moved on since this server was created."
        )

    if not groups:
        all_clear, message = _empty_diff_notice(changed)
        (st.success if all_clear else st.info)(message)
        _extra_notes(changed)
        return

    reached = {c["chartId"] for g in groups for c in _group_charts(g, usage, chart_text)}
    n_charts = len(reached)
    authored = f"**{len(changed.diffs)} indicator{'s' if len(changed.diffs) != 1 else ''}**"
    if chart_text.diffs:
        # Said separately because it is a different edit to make: one is the indicator's metadata, the
        # other is the chart's own config (`presentation.grapher_config` in the garden step).
        authored += f" and the config of **{len(chart_text.diffs)} chart{'s' if len(chart_text.diffs) != 1 else ''}**"
    st.markdown(
        f"**{len(groups)} text change{'s' if len(groups) != 1 else ''}** on "
        f"{authored}, reaching "
        f"**{n_charts} published chart{'s' if n_charts != 1 else ''}**",
    )
    _extra_notes(changed)
    col_layout, col_reject = st.columns([4, 1], vertical_alignment="center")
    with col_layout:
        layout = st_layout_switcher(
            "🔍 Chart by chart",
            "**Chart by chart** steps through the charts this branch changed, one full review at a time",
        )
    with col_reject:
        edits_view.st_reject_all(source_engine, cached.summary(source_engine, target_engine), "charts")

    if layout == "items":
        _chart_browser(source_engine, target_engine, groups, usage, chart_text)
        return

    # One card per authored edit, every chart it reaches counted once. The per-text cards this replaces
    # showed a shared edit once per wording it rendered into.
    edits_view.st_edit_cards(source_engine, target_engine, cached.summary(source_engine, target_engine), "charts")


def _chart_browser(source_engine: Engine, target_engine: Engine, groups, usage: dict, chart_text) -> None:
    """One chart at a time: pick it or step to it, and read its whole review.

    Ordered by how much changed, so stepping goes from the chart carrying four edits down to the ones
    carrying one. The picker and **Next ▶** write the same `?chart=` the change-grouped cards link to, so
    all three ways of arriving here agree and any of them can be pasted to somebody else.
    """
    # The same enumeration the Review tab counts against, so the two can never disagree about how many
    # charts this branch changed.
    counts = cached.changed_charts(source_engine, target_engine)
    if not counts:
        st.caption("No published chart renders these changes.")
        return

    slugs = sorted(counts, key=lambda slug: (-counts[slug], slug))
    # One query for every chart's recorded state, so the picker can mark what you have already done.
    item_surface = surface_key("item", "chart")
    recorded = load_reviews(source_engine, item_surface)
    current = requested_chart(st.session_state.get("chart"), st.query_params.get("chart"))
    if current not in slugs:
        # Nothing chosen yet (or a slug from another branch): open the most-changed chart rather than an
        # empty page asking to be told what to show.
        current = slugs[0]
    st.session_state["chart"] = current
    position = slugs.index(current)

    col_pick, col_next = st.columns([4, 1], vertical_alignment="bottom")
    with col_pick:
        st.selectbox(
            f"Chart {position + 1} of {len(slugs)} changed by this branch",
            options=slugs,
            index=position,
            format_func=lambda slug: (
                item_marker(recorded, item_surface, slug)
                + f"{slug} · {counts[slug]} change{'s' if counts[slug] != 1 else ''}"
            ),
            key="mdd-chart-picker",
            on_change=_pick_chart,
            help="Type to search. Every chart here has at least one text this branch changed.",
        )
    with col_next:
        st.button(
            "Next change ▶",
            key="mdd-chart-next",
            on_click=_step_chart,
            args=(slugs, position + 1),
            width="stretch",
            help="The next changed chart, wrapping round at the end.",
        )

    mdim_pages.render_chart_by_ref(source_engine, target_engine, current, recorded)


def _pick_chart() -> None:
    """The picker's choice becomes the URL, so the page and the address agree."""
    slug = str(st.session_state.get("mdd-chart-picker") or "").strip()
    if slug:
        st.session_state["chart"] = slug
        st.query_params["chart"] = slug


def _step_chart(slugs: list[str], index: int) -> None:
    """Step to the next chart, wrapping — the list is finite and stepping off the end should not dead-end."""
    slug = slugs[index % len(slugs)]
    st.session_state["chart"] = slug
    st.session_state["mdd-chart-picker"] = slug
    st.query_params["chart"] = slug


def _empty_diff_notice(changed) -> tuple[bool, str]:
    """Whether an empty diff is genuinely all clear, and what to say about it.

    Green means "nothing here needs your eyes", and an empty comparison does not establish that. A version
    bump replaces every catalog path, so nothing has a baseline counterpart and the diff comes back empty
    while a whole dataset's worth of reader-facing text has never been read. `Summary.has_changes` counts
    new indicators for that reason; this section has to agree, or the page says all clear right above a
    caption admitting a hundred indicators went unreviewed.
    """
    if changed.new_paths:
        n = len(changed.new_paths)
        return False, (
            f"**Nothing to diff, and {n} indicator{'s' if n != 1 else ''} unreviewed** — no indicator's "
            f"text *differs* from {BASELINE_NAME}, but {n} {'are' if n != 1 else 'is'} new on this server, "
            "so there is no old text to compare against. These are indicators the baseline has under no "
            "version at all: a re-versioned one is now matched by indicator and diffed."
        )
    return True, (
        f"**No indicator text changes** against {BASELINE_NAME} — no chart's inherited title, subtitle "
        "or *What you should know* text differs here."
    )


def _extra_notes(changed) -> None:
    """New indicators and the section's scope — stated, so an empty list is never read as 'all clear'."""
    pairs = version_pairs(getattr(changed, "across_versions", {}))
    if pairs:
        # A dataset update re-versions every catalogPath, so these diffs were matched by indicator rather
        # than by path. Worth saying: a difference here can be an edit somebody made or simply how the new
        # release words the same thing, and only the reviewer can tell those apart.
        moves = ", ".join(f"`{was}` → `{now}`" for was, now in pairs[:3])
        n = len(getattr(changed, "across_versions", {}))
        st_note(
            f"🔄 {n} of these {'indicator was' if n == 1 else 'indicators were'} compared <b>across a "
            f"version bump</b> ({moves}) — matched by indicator, since a new version renames every "
            "catalogPath. A difference may be an edit, or may be how the new release words it."
        )
    if changed.new_paths:
        n = len(changed.new_paths)
        st_note(
            f"➕ {n} indicator{'s' if n != 1 else ''} on this server {'do' if n != 1 else 'does'} not exist in "
            f"{BASELINE_NAME} yet, so there is no old text to diff. New indicators are not listed here."
        )
    st_note(
        "This section covers the text a chart <b>inherits from ETL metadata</b>: the indicator's own "
        "fields, and the title, subtitle or footnote a garden step sets for the chart. Text typed "
        "directly into a chart in the admin is not from ETL — review that in <b>Chart Diff</b>."
    )


def _group_charts(g: ChangeGroup, usage: dict, chart_text) -> list[dict[str, Any]]:
    """The published charts one change lands on, from wherever that change knows them.

    An indicator-layer change reaches whatever renders the indicator, so its charts come from `usage`. A
    chart-level change *is* a set of charts — each one a view keyed by its slug — so it carries its own.
    """
    if g.field.startswith(CHART_FIELD_PREFIX):
        return [chart_text.charts[d["chart"]] for d in g.view_dims if d.get("chart") in chart_text.charts]
    return group_usage(g, usage)["charts"]
