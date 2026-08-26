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

from apps.wizard.app_pages.metadata_diff import cached, datapage, mdim_pages
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    ChangeGroup,
    affected_drafts,
    distinct_garden_datasets,
    distinct_indicator_short_names,
    field_label,
    group_changes,
    group_usage,
    parse_catalog_path,
    requested_chart,
)
from apps.wizard.app_pages.metadata_diff.data import load_reviews
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    render_chart_list,
    st_layout_switcher,
    st_note,
    st_origin_caption,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    item_marker,
    resolve_marks,
    surface_key,
)
from apps.wizard.utils.components import Pagination

# All chart-side changes share one reviewed-state surface: a change is a change to an indicator's text,
# wherever it surfaces.
SURFACE = surface_key("charts", "indicators")

CHANGES_PER_PAGE = 5
CHARTS_PER_PAGE = 25


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
    attribution = cached.indicator_attribution(source_engine, target_engine, tuple(changed.paths))

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

    marks = resolve_marks(source_engine, SURFACE, groups)
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
    layout = st_layout_switcher(
        "🔍 Chart by chart",
        "**Chart by chart** steps through the charts this branch changed, one full review at a time",
    )

    if layout == "items":
        _chart_browser(source_engine, target_engine, groups, usage, chart_text)
        return

    pagination = Pagination(marks, items_per_page=CHANGES_PER_PAGE, pagination_key="mdd-charts-pagination")
    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls()

    for mark in pagination.get_page_items():
        _render_change(source_engine, mark, usage, attribution, chart_text)

    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls(position="bottom")


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

    mdim_pages.render_chart_by_ref(source_engine, target_engine, current)


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
            "so there is no old text to compare against. A version bump lands exactly here."
        )
    return True, (
        f"**No indicator text changes** against {BASELINE_NAME} — no chart's inherited title, subtitle "
        "or *What you should know* text differs here."
    )


def _extra_notes(changed) -> None:
    """New indicators and the section's scope — stated, so an empty list is never read as 'all clear'."""
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


def _render_change(
    source_engine: Engine,
    mark,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    attribution: dict[str, str],
    chart_text=None,
) -> None:
    """One distinct text change: what changed, where it appears, and what it reaches."""
    g: ChangeGroup = mark.group
    # Charts only, here. An MDim rendering the same indicator appears in the MDims section on its own
    # card — its indicator text changed, which is what puts it there — so naming it in both places
    # doubles every shared change and leaves neither section answering its own question.
    # Every chart using the indicator counts: its readers see the new text either on the chart's data page
    # or through "Learn more about this data". The list below groups them; neither group is deducted.
    charts = _group_charts(g, usage, chart_text) if chart_text is not None else group_usage(g, usage)["charts"]
    drafts = [] if g.field.startswith(CHART_FIELD_PREFIX) else affected_drafts(g, usage)

    plural = "s" if len(charts) != 1 else ""
    # Drafts sit outside the reach count and are named in the label, so "10 charts" keeps meaning
    # "10 charts a reader can open" wherever it appears.
    draft_note = f" · {len(drafts)} draft{'s' if len(drafts) != 1 else ''}" if drafts else ""
    with st.container(border=True):
        # The header carries the reach and the list behind it: "10 charts" is a claim, and the charts are
        # what lets an author check it, so the count itself opens them rather than sending the reader to
        # the foot of the card. Review sits alongside, the way the MDim cards lead with their actions.
        col_head, col_review = st.columns([3, 1], vertical_alignment="center")
        with col_head:
            with st.container(border=False, horizontal=True, vertical_alignment="center"):
                st.markdown(f"**{field_label(g.field)}** ·")
                with st.popover(f"📊 {len(charts)} chart{plural}{draft_note}"):
                    render_chart_list(charts, fields={g.field}, drafts=drafts)

        st_note(_where_line(g))
        st_origin_caption(_group_paths(g), attribution)

        datapage.st_datapage_diff(
            {g.field: {"old": g.old, "new": g.new}},
            baseline_label=BASELINE_NAME.capitalize(),
            staging_label="This staging server",
            show_unchanged_slots=False,
        )


def _group_paths(g: ChangeGroup) -> set[str]:
    """The indicator catalogPaths carrying this change (a shared definition reaches several)."""
    return g.catalog_paths or ({g.catalog_path} if g.catalog_path else set())


def _where_line(g: ChangeGroup) -> str:
    """Why this card is one change and not several: the same text, written once, shared.

    Returns HTML for `st_note`. It says what is true — these indicators carry the identical text, all of
    it — rather than which YAML construct produced it. Naming `definitions.*` or a variable key here was
    both jargon and a trap: on a dimensional indicator the variable's own field holds a template
    reference, so pointing at it sends the author to edit the wrong line. The exact field to edit is the
    PR brief's job, and it still does it.

    One case is not sharing at all: the group is keyed on the text, so the same wording edited in two
    garden datasets arrives as one card. Nothing is shared across datasets, so that is as many edits as
    there are datasets, and saying otherwise would send someone to fix half of it.
    """
    garden_dirs = distinct_garden_datasets(g.catalog_paths)
    if len(garden_dirs) > 1:
        files = ", ".join(f"<code>{d}.meta.yml</code>" for d in garden_dirs[:4]) + (
            " …" if len(garden_dirs) > 4 else ""
        )
        return (
            f"✂️ Grouped by their text, but <b>edited in {len(garden_dirs)} separate garden datasets</b> "
            f"({files}) — nothing is shared between datasets, so this is {len(garden_dirs)} edits, not one. "
            "Each one has to be changed on its own."
        )
    label = field_label(g.field)
    shared_names = distinct_indicator_short_names(g.catalog_paths)
    if len(shared_names) > 1:
        preview = ", ".join(f"<code>{n}</code>" for n in shared_names[:5]) + (" …" if len(shared_names) > 5 else "")
        return (
            f"🔗 Grouped because these {len(shared_names)} indicators ({preview}) have <b>exactly the same "
            f"{label}</b> — the whole text, word for word. It is written once and shared between them, so "
            "this is one edit."
        )
    parsed = parse_catalog_path(g.catalog_path)
    if len(g.catalog_paths) > 1:
        name = f"<code>{parsed[2]}</code>" if parsed else "this indicator"
        return (
            f"🔗 Grouped because {len(g.catalog_paths)} versions of {name} have <b>exactly the same "
            f"{label}</b> — the whole text, word for word. It is written once and shared between them, so "
            "this is one edit."
        )
    return f"This {label} belongs to one indicator only — nothing else shares it."


def _clear_chart() -> None:
    """Leave the per-chart page: clear the widget and the URL it is routed by."""
    st.session_state["chart"] = ""
    st.query_params.pop("chart", None)
