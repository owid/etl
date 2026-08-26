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
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    chart_review_url,
    render_chart_list,
    st_layout_switcher,
    st_note,
    st_origin_caption,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
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

    requested = requested_chart(st.session_state.get("chart"), st.query_params.get("chart"))
    if requested:
        # Seed the widget from the link before it renders, or `url_persist` writes its blank back and the
        # navigation undoes itself.
        st.session_state["chart"] = requested
        _single_chart_page(source_engine, target_engine)
        return

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
        _lookup_expander(source_engine, target_engine)
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
        "**Chart by chart** lists every chart this branch changed, with what changed on each",
    )
    _chart_picker(groups, usage, chart_text)

    if layout == "items":
        _render_chart_list(groups, usage, chart_text)
        _lookup_expander(source_engine, target_engine)
        return

    pagination = Pagination(marks, items_per_page=CHANGES_PER_PAGE, pagination_key="mdd-charts-pagination")
    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls()

    for mark in pagination.get_page_items():
        _render_change(source_engine, mark, usage, attribution, chart_text)

    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls(position="bottom")

    _lookup_expander(source_engine, target_engine)


def _render_chart_list(groups: list[ChangeGroup], usage: dict, chart_text) -> None:
    """Every changed chart, with the fields that changed on it and a link to its own review.

    A list, not sixty-seven diffs: the fields name what happened, and the review page has the text. Ordered
    by how much changed, because a chart carrying four edits is the one to look at first.
    """
    per_chart: dict[str, dict[str, Any]] = {}
    for g in groups:
        for chart in _group_charts(g, usage, chart_text):
            slug = str(chart.get("slug") or "")
            if not slug:
                continue
            row = per_chart.setdefault(slug, {"fields": [], "has_data_page": chart.get("has_data_page", True)})
            row["fields"].append(field_label(g.field))

    if not per_chart:
        st.caption("No published chart renders these changes.")
        return

    rows = sorted(per_chart.items(), key=lambda kv: (-len(kv[1]["fields"]), kv[0]))
    st.markdown(f"**{len(rows)} chart{'s' if len(rows) != 1 else ''}** changed by this branch:")
    pagination = Pagination(rows, items_per_page=CHARTS_PER_PAGE, pagination_key="mdd-chart-items-pagination")
    if len(rows) > CHARTS_PER_PAGE:
        pagination.show_controls()
    for slug, row in pagination.get_page_items():
        fields = ", ".join(sorted(set(row["fields"])))
        n = len(row["fields"])
        where = "" if row["has_data_page"] else " · behind *Learn more about this data*"
        st_note(
            f'<a href="{chart_review_url(slug)}" target="_self"><code>{slug}</code></a> — '
            f"<b>{n} change{'s' if n != 1 else ''}</b>: {fields}{where}"
        )
    if len(rows) > CHARTS_PER_PAGE:
        pagination.show_controls(position="bottom")


def _chart_picker(groups: list[ChangeGroup], usage: dict, chart_text) -> None:
    """Pick one of the charts this branch changed, and go to its own review.

    The list below is organised by *change*, which is the right shape for judging an edit and the wrong one
    for "what happened to the chart I care about" — that chart's changes are spread across however many
    cards mention it. This is the other question, answered directly: every changed chart, searchable by
    slug (a selectbox filters as you type), opening the per-chart page.

    Charts only — an edit reaching an MDim view or an explorer view has no chart page to open. Counting is
    per chart, not per change, so a chart appearing under three cards is one row saying "3 changes".
    """
    counts: dict[str, int] = {}
    for g in groups:
        for chart in _group_charts(g, usage, chart_text):
            slug = str(chart.get("slug") or "")
            if slug:
                counts[slug] = counts.get(slug, 0) + 1
    if not counts:
        return

    # Most-changed first, then alphabetical: a chart carrying four edits is the one worth opening first.
    slugs = sorted(counts, key=lambda s: (-counts[s], s))
    st.selectbox(
        f"Open one of the {len(slugs)} changed charts",
        options=slugs,
        index=None,
        format_func=lambda s: f"{s} · {counts[s]} change{'s' if counts[s] != 1 else ''}",
        placeholder="Type to search the changed charts…",
        key="mdd-chart-picker",
        on_change=_pick_chart,
        help="Opens that chart's own review: every field of it this branch changed, in data-page order.",
    )


def _pick_chart() -> None:
    """Route to the picked chart's review, the same way its link in a chart list does."""
    slug = str(st.session_state.get("mdd-chart-picker") or "").strip()
    if not slug:
        return
    st.session_state["chart"] = slug
    st.query_params["chart"] = slug
    # Leave the picker empty, or coming back from the review re-opens the chart you just left.
    st.session_state["mdd-chart-picker"] = None


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


def _single_chart_page(source_engine: Engine, target_engine: Engine) -> None:
    """One chart's review, alone on the page, with the way back to the list.

    The box stays: it is how you get to a different chart from here, and it is the same URL-persisted
    widget the list's lookup renders, so the two can never disagree about which chart is open.
    """
    st.button(
        "← All chart changes",
        key="mdd-clear-chart",
        on_click=_clear_chart,
        help="Back to every text change on this branch.",
    )
    mdim_pages.chart_flow(source_engine, target_engine)


def _clear_chart() -> None:
    """Leave the per-chart page: clear the widget and the URL it is routed by."""
    st.session_state["chart"] = ""
    st.query_params.pop("chart", None)


def _lookup_expander(source_engine: Engine, target_engine: Engine) -> None:
    """Any chart, changed or not — for checking that a chart you expected to change didn't, or vice versa.

    Opens itself when a slug is already in the box, which is how it survives the rerun that a lookup
    causes. It used to be opened remotely by a button on every card, and that button was the problem: it
    worked, silently, a page and a half below where it was pressed.
    """
    with st.expander("🔎 Look up any chart", expanded=bool(st.session_state.get("chart"))):
        st.caption(
            "The lists above only show what this branch changed. Use this to inspect any published chart's "
            "inherited metadata — including confirming that a chart you were worried about is untouched."
        )
        mdim_pages.chart_flow(source_engine, target_engine)
