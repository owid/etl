"""Charts section: the indicator texts this branch changed, and the charts that render them.

Organised by *change*, not by chart. One reworded WYSK bullet can reach hundreds of charts; listing it
once with its charts underneath is both shorter and truer to what the author has to decide, and it keeps
the page usable on a big data update where a per-chart list would run to thousands of entries.

A chart's own title/subtitle/footnote is Chart Diff's territory (it is chart config). What this section
adds is the layer Chart Diff cannot see: the text a chart *inherits* from indicator metadata, authored
in the garden step — WYSK above all, which no config diff shows.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff import cached, datapage, mdim_pages
from apps.wizard.app_pages.metadata_diff.core import (
    ChangeGroup,
    distinct_garden_datasets,
    distinct_indicator_short_names,
    field_label,
    group_changes,
    group_usage,
    parse_catalog_path,
    renders_change,
)
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    render_chart_list,
    st_origin_caption,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    n_reviewed,
    resolve_marks,
    st_reviewed_toggle,
    surface_key,
)
from apps.wizard.utils.components import Pagination

# All chart-side changes share one reviewed-state surface: a change is a change to an indicator's text,
# wherever it surfaces.
SURFACE = surface_key("charts", "indicators")

CHANGES_PER_PAGE = 5


def st_show_chart_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the Charts section: every indicator text change, with the charts it lands on."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    changed = cached.indicator_changes(source_engine, target_engine)
    groups = group_changes(changed.view_diffs())
    usage = cached.usage_for_indicators(tuple(changed.ids_list), "", source_engine)
    attribution = cached.indicator_attribution(source_engine, target_engine, tuple(changed.paths))

    if not changed.narrowed:
        st.warning(
            "Could not read this branch's changed files from git, so the list is **not narrowed to your "
            "branch** — it may include metadata that master has moved on since this server was created."
        )

    if not groups:
        st.success(
            f"**No indicator text changes** against {BASELINE_NAME} — no chart's inherited title, subtitle "
            "or *What you should know* text differs here."
        )
        _extra_notes(changed)
        _lookup_expander(source_engine, target_engine)
        return

    marks = resolve_marks(source_engine, SURFACE, groups)
    # Charts that *show* the changed field, not merely charts using the indicator: a WYSK edit is
    # invisible on a multi-indicator chart, which has no data page.
    n_charts = len({c["chartId"] for g in groups for c in group_usage(g, usage)["charts"] if renders_change(g, c)})
    st.markdown(
        f"**{len(groups)} text change{'s' if len(groups) != 1 else ''}** on "
        f"**{len(changed.diffs)} indicator{'s' if len(changed.diffs) != 1 else ''}**, reaching "
        f"**{n_charts} published chart{'s' if n_charts != 1 else ''}** · "
        f"{n_reviewed(marks)}/{len(marks)} reviewed",
        help="Reviewed is your own progress marker — it is stored on this staging server, resets if the "
        "text is edited again, and is never synced to production.",
    )
    _extra_notes(changed)

    pagination = Pagination(marks, items_per_page=CHANGES_PER_PAGE, pagination_key="mdd-charts-pagination")
    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls()

    for mark in pagination.get_page_items():
        _render_change(source_engine, mark, usage, attribution)

    if len(marks) > CHANGES_PER_PAGE:
        pagination.show_controls(position="bottom")

    _lookup_expander(source_engine, target_engine)


def _extra_notes(changed) -> None:
    """New indicators and the section's scope — stated, so an empty list is never read as 'all clear'."""
    if changed.new_paths:
        n = len(changed.new_paths)
        st.caption(
            f"➕ {n} indicator{'s' if n != 1 else ''} on this server {'do' if n != 1 else 'does'} not exist in "
            f"{BASELINE_NAME} yet, so there is no old text to diff. New indicators are not listed here."
        )
    st.caption(
        "This section covers text a chart **inherits from indicator metadata** (garden `.meta.yml`). A "
        "chart's own title/subtitle/footnote lives in its config — review those in **Chart Diff**."
    )


def _render_change(
    source_engine: Engine,
    mark,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    attribution: dict[str, str],
) -> None:
    """One distinct text change: what changed, where it appears, and what it reaches."""
    g: ChangeGroup = mark.group
    imp = group_usage(g, usage)
    mdims = imp["mdims"]
    # Same split as the section total: every chart using the indicator is affected, but a data-page-only
    # field (WYSK, processing note, producer description) is invisible on a multi-indicator chart. Counting
    # those here would contradict the total and claim an audience that cannot see the edit; they are still
    # named below, because a chart the author might expect in the list must not vanish without a word.
    charts = [c for c in imp["charts"] if renders_change(g, c)]
    no_data_page = [c for c in imp["charts"] if not renders_change(g, c)]

    with st.container(border=True):
        head = f"{mark.icon} **{field_label(g.field)}** · {len(charts)} chart{'s' if len(charts) != 1 else ''}"
        if mdims:
            head += f" · {len(mdims)} MDim{'s' if len(mdims) != 1 else ''}"
        st.markdown(head)
        st.caption(_where_line(g))
        st_origin_caption(_group_paths(g), attribution)

        datapage.st_datapage_diff(
            {g.field: {"old": g.old, "new": g.new}},
            baseline_label=BASELINE_NAME.capitalize(),
            staging_label="This staging server",
            show_unchanged_slots=False,
        )

        col_charts, col_review = st.columns([3, 1])
        with col_charts:
            with st.popover(f"📊 {len(charts)} affected chart{'s' if len(charts) != 1 else ''}", width="stretch"):
                render_chart_list(charts)
                _no_data_page_note(no_data_page)
                if mdims:
                    verb = "use" if len(mdims) != 1 else "uses"
                    st.markdown(f"**{len(mdims)} MDim{'s' if len(mdims) != 1 else ''}** also {verb} these indicators:")
                    st.markdown("\n".join(f"- `{m.get('slug') or m.get('catalogPath')}`" for m in mdims))
                _open_chart_buttons(charts, mark.change_key)
        with col_review:
            st_reviewed_toggle(source_engine, SURFACE, mark)


def _no_data_page_note(charts: list[dict[str, Any]]) -> None:
    """Name the charts left out of the count — they use the indicator but cannot show this field.

    Left out of the reach count, not hidden: an author looking for a chart they know uses the indicator
    has to find it here, with the reason it is not counted.
    """
    if not charts:
        return
    n = len(charts)
    slugs = ", ".join(
        f"`{c.get('slug') or c.get('chartId')}`" for c in sorted(charts, key=lambda c: str(c.get("slug") or ""))
    )
    st.caption(
        f"⚠️ {n} further chart{'s' if n != 1 else ''} use{'' if n != 1 else 's'} these indicators but "
        f"ha{'ve' if n != 1 else 's'} no data page (multi-indicator charts), so this text is **not shown to "
        f"readers** there and is not counted above: {slugs}"
    )


def _group_paths(g: ChangeGroup) -> set[str]:
    """The indicator catalogPaths carrying this change (a shared definition reaches several)."""
    return g.catalog_paths or ({g.catalog_path} if g.catalog_path else set())


def _where_line(g: ChangeGroup) -> str:
    """Where to edit this text — never presenting a variable key as the target when a definition is likelier.

    Two shapes of sharing, and only one used to be caught. Several *differently named* indicators carrying
    the identical text can only have got it from a shared `definitions.*` entry. But the same text landing
    on many dimensional variants of ONE indicator (`thr__welfare_type_income…`, ×8) is equally a template,
    and naming `tables.<t>.variables.thr` as the target there sends the author to edit a field whose value
    is a `{definitions.*}` reference — the exact mistake this tool exists to prevent.

    Both shapes assume one garden dataset. When the identical text was edited in several, it is that many
    separate edits — no definition is shared across datasets — so those are named instead of one file.
    """
    garden_dirs = distinct_garden_datasets(g.catalog_paths)
    if len(garden_dirs) > 1:
        files = ", ".join(f"`{d}.meta.yml`" for d in garden_dirs[:4]) + (" …" if len(garden_dirs) > 4 else "")
        return (
            f"✂️ The identical text was edited in {len(garden_dirs)} separate garden datasets ({files}) — no "
            "`definitions.*` block is shared across datasets, so this is that many edits. Edit each file."
        )
    shared_names = distinct_indicator_short_names(g.catalog_paths)
    if len(shared_names) > 1:
        preview = ", ".join(f"`{n}`" for n in shared_names[:5]) + (" …" if len(shared_names) > 5 else "")
        return (
            f"🔗 The identical text renders on {len(shared_names)} indicators ({preview}) — that is a shared "
            "`definitions.*` / `shared.meta.yml` entry, not a per-variable field. Edit the definition."
        )
    parsed = parse_catalog_path(g.catalog_path)
    if len(g.catalog_paths) > 1:
        where = f"`{parsed[0]}.meta.yml`" if parsed else "the indicator's garden `.meta.yml`"
        return (
            f"🔗 The identical text renders on {len(g.catalog_paths)} dimensional variants of "
            f"`{parsed[2] if parsed else 'this indicator'}` — so it comes from a template, not a literal "
            f"value. Grep {where} for the changed text and edit the `definitions.*` entry it resolves to."
        )
    if parsed:
        return (
            f"Authored in `{parsed[0]}.meta.yml` → `tables.{parsed[1]}.variables.{parsed[2]}` — if that "
            "field holds a `{definitions.*}` reference, edit the definition rather than the field."
        )
    return "Authored in the indicator's garden `.meta.yml`"


def _open_chart_buttons(charts: list[dict[str, Any]], change_key: str) -> None:
    """Jump into the per-chart review (the deep view with sign-off and a PR brief) for one chart.

    The same chart can render several distinct changes, so the widget key carries the change too.
    """
    if not charts:
        return
    st.caption("Open one of them in the full per-chart review:")
    for c in sorted(charts, key=lambda c: str(c.get("slug") or ""))[:8]:
        slug = str(c.get("slug") or "")
        if not slug:
            continue
        st.button(
            f"🔍 {slug}",
            key=f"mdd-open-chart-{change_key[:12]}-{slug}",
            on_click=_select_chart,
            args=(slug,),
            help="Opens the chart's own review below, with its blast radius and PR brief.",
        )


def _select_chart(slug: str) -> None:
    st.session_state["chart"] = slug


def _lookup_expander(source_engine: Engine, target_engine: Engine) -> None:
    """Any chart, changed or not — for checking that a chart you expected to change didn't, or vice versa."""
    selected = st.session_state.get("chart")
    with st.expander("🔎 Look up any chart", expanded=bool(selected)):
        st.caption(
            "The lists above only show what this branch changed. Use this to inspect any published chart's "
            "inherited metadata — including confirming that a chart you were worried about is untouched."
        )
        mdim_pages.chart_flow(source_engine, target_engine)
