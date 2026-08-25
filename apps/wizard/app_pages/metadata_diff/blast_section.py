"""Blast radius: everywhere this branch's metadata edits land, across all three surfaces.

The three review sections each keep to their own surface — a chart change is not repeated on an MDim card
and vice versa, because a reviewer working through one surface should not be counting the same edit twice.
This section is the deliberate exception: it is the one place where crossing surfaces is the point, so an
author can see what one edit costs before deciding how careful to be with it.

Three levels, which is the point: an **edit** somebody authored, the **texts** it renders into, and the
**pages** each of those texts lands on. A sentence added to a shared `definitions.*` entry is one edit,
eleven texts and seventy-odd pages, and reporting any one of those numbers alone misleads — as reporting
the middle one did.

Two readings of the same data:

- **dimension tree** — every affected MDim's views on their own dimension grids, with the charts beside
  them. Not how many views changed but *which*, and what sits next to them.
- **by edit** — edit → text → page. How far does one authored edit actually go?

By-edit reads the cached summary the section badges already use, so it costs no queries. The grid diffs
each MDim's views, which is why it stops at `MAX_TREE_MDIMS`.
"""

import html
from typing import Any
from urllib.parse import quote, urlencode

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import diff_window_html, field_label
from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, EditGroup, group_by_edit, reach_by_surface
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    impact_counts,
    render_chart_list,
    view_impact,
)
from apps.wizard.app_pages.metadata_diff.tree import render_multi_tree_html
from apps.wizard.utils.components import url_persist

GROUP_KEY = "blast-group"
_CONTEXT_CSS = """
<style>
.mdd-context {{ color: #555; }}
.mdd-context-label {{ color: #999; font-size: 12px; text-transform: uppercase;
  letter-spacing: .04em; margin-right: 6px; }}
</style>
""".replace("{{", "{").replace("}}", "}")
MAX_ROWS = 60
# MDims drawn on the grid at once. Each costs a view diff against the baseline, and past a handful the
# canvas is unreadable anyway; the rest are named rather than silently dropped.
MAX_TREE_MDIMS = 6


def st_show_blast_radius(source_engine: Engine, target_engine: Engine) -> None:
    """Everywhere the branch's metadata changes land: by edit, by surface, or on an MDim's dimension grid."""
    st.markdown(DIFF_CSS + _CONTEXT_CSS, unsafe_allow_html=True)
    summary = cached.summary(source_engine, target_engine)
    reach = summary.reach

    if not reach:
        st.success(f"**Nothing to show:** no metadata text on this server differs from `{BASELINE_NAME}`.")
        return

    edits = group_by_edit(reach)
    # Pages are counted from the inverted view, so a page rendering two of these texts counts once.
    # Summing each text's reach gave a larger number for the same data, which is what made "11 changes"
    # read as eleven things to review when one sentence had been written.
    rows = reach_by_surface(reach)
    pages = sum(1 for r in rows if r["published"])
    hidden_pages = len(rows) - pages

    head = (
        f"**{len(edits)} edit{'s' if len(edits) != 1 else ''}** authored here, rendering "
        f"**{len(reach)} distinct text{'s' if len(reach) != 1 else ''}**, on "
        f"**{pages} page{'s' if pages != 1 else ''}** a reader can reach"
    )
    if hidden_pages:
        head += f" · {hidden_pages} unpublished"
    st.markdown(head)
    st.caption(
        "Three different numbers, on purpose. One sentence added to a shared `definitions.*` entry is "
        "**one edit** to judge; it splices into every description referencing it, so the site renders "
        "**several texts**; and each of those is read on **many pages**. Sign-off lives in the three "
        "sections — this view is for seeing the spread before you decide how careful to be."
    )

    if not summary.mdims_resolved:
        st.warning(
            "Too many changed MDims to diff view by view, so the MDim rows below are incomplete — the "
            "same ceiling the MDims badge reports."
        )

    # The grid leads, but only when there is one to draw: on a branch that changes no MDim it would
    # open on "nothing to draw" while the real changes sat one click away. Both names are unchanged, so
    # existing `?blast-group=` links still resolve.
    has_mdim = any(r.mdims for r in reach)
    options = ["dimensions", "change"]
    # A link written before "By surface" was dropped would otherwise reach url_persist's strict check and
    # raise on every load. Sanitize it here rather than let a stale bookmark break the page.
    if st.query_params.get(GROUP_KEY) not in options:
        st.query_params.pop(GROUP_KEY, None)
    if st.session_state.get(GROUP_KEY) not in options:
        st.session_state.pop(GROUP_KEY, None)

    grouping = url_persist(st.segmented_control)(
        label="Group by",
        # "change" keeps its name so existing links still resolve; it groups by authored edit now.
        options=options,
        format_func=lambda g: {"dimensions": "🌳 Dimension tree", "change": "🧬 By edit"}[g],
        key=GROUP_KEY,
        value="dimensions" if has_mdim else "change",
        label_visibility="collapsed",
    )

    if grouping == "dimensions":
        # Last thing on the page, deliberately: the component resizes its own iframe to fit its content,
        # and Streamlit-rendered siblings below it overlap while that happens.
        _dimension_tree(source_engine, target_engine, reach)
    else:
        _tree(edits)


def _tree(edits: list[EditGroup]) -> None:
    """Edit → text → page, one card per authored edit.

    The texts sit inside an expander: with a shared definition there are ten or more of them, and the
    thing worth seeing first is that they are one edit, not ten.
    """
    for group in edits[:MAX_ROWS]:
        with st.container(border=True):
            st.markdown(
                f"**{field_label(group.field)}** · **1 edit** → "
                f"**{group.n_texts} rendered text{'s' if group.n_texts != 1 else ''}** → "
                f"**{group.n_reader_facing} page{'s' if group.n_reader_facing != 1 else ''}** a reader can reach"
            )
            _edit_body(group)
            st.markdown(f"_{_surface_summary(group)}_")

            label = f"🌳 {group.n_texts} rendered text{'s' if group.n_texts != 1 else ''}, and where each lands"
            with st.expander(label, expanded=group.n_texts == 1):
                for i, r in enumerate(group.changes, start=1):
                    st.markdown(
                        f'<div class="mdd-diff"><b>Text {i} of {group.n_texts}</b> — {_preview(r)}</div>',
                        unsafe_allow_html=True,
                    )
                    for line in _reach_lines(r):
                        st.markdown(f"  {line}")
    _truncation_note(len(edits))


def _edit_body(group: EditGroup) -> None:
    """The edit itself: the words as one diff line, then the same edit in context.

    A rewording is one change, so it reads as one line — `old` struck through, `new` highlighted — rather
    than as an "added" statement and a "removed" statement that the reader has to pair up. The context
    line below shows where it lands, windowed on the change inside the first text carrying it: every text
    in the group shares this edit by construction, so one is a fair exemplar, labelled as one.
    """
    deleted = f'<del class="mdd-del">{html.escape(group.deleted)}</del>' if group.deleted else ""
    inserted = f'<ins class="mdd-ins">{html.escape(group.inserted)}</ins>' if group.inserted else ""

    if deleted and inserted:
        st.markdown(f'<div class="mdd-diff">{deleted} &#8594; {inserted}</div>', unsafe_allow_html=True)
    elif inserted:
        st.markdown(f'<div class="mdd-diff">added {inserted}</div>', unsafe_allow_html=True)
    elif deleted:
        st.markdown(f'<div class="mdd-diff">removed {deleted}</div>', unsafe_allow_html=True)
    else:
        # No words either way — a whitespace-only edit. A reordered list is not this case: its moved
        # bullets do read as an insertion and a deletion.
        st.caption("No words added or removed — whitespace only. The texts below show both sides.")

    if group.changes:
        exemplar = group.changes[0]
        where = "in context" if group.n_texts == 1 else f"in context, in the first of {group.n_texts} texts"
        st.markdown(
            f'<div class="mdd-diff mdd-context"><span class="mdd-context-label">{where}</span> '
            f"{diff_window_html(exemplar.old, exemplar.new, max_chars=300)}</div>",
            unsafe_allow_html=True,
        )


def _surface_summary(group: EditGroup) -> str:
    """One line naming the kinds of page this edit reaches, deduped across its texts."""
    s = group.surfaces()
    bits = []
    if s["charts"]:
        bits.append(f"{len(s['charts'])} chart(s)")
    if s["mdims"]:
        bits.append(f"{len(s['mdims'])} MDim(s)")
    if s["explorers"]:
        bits.append(f"{len(s['explorers'])} explorer(s)")
    if s["draft_charts"]:
        bits.append(f"{len(s['draft_charts'])} draft chart(s)")
    return "Reaches " + ", ".join(bits) if bits else "Reaches nothing a reader or an editor can open yet"


def _reach_lines(r: ChangeReach) -> list[str]:
    """The surfaces one change lands on, most prominent first, each naming what it is."""
    lines: list[str] = []
    on_page = [c for c in r.charts if c.get("has_data_page", True)]
    drawer = [c for c in r.charts if not c.get("has_data_page", True)]
    if on_page:
        lines.append(f"- 📈 **{len(on_page)}** data page(s): {_names(on_page)}")
    if drawer:
        lines.append(f"- 🔍 **{len(drawer)}** chart(s) via *Learn more about this data*: {_names(drawer)}")
    for m in sorted(r.mdims, key=lambda m: str(m["catalogPath"])):
        draft = " :orange-badge[unpublished]" if m["is_draft"] else ""
        # Straight into that MDim's dimension tree, which is the one thing this view cannot show: which
        # views exist, not just how many changed.
        lines.append(f"- 🧩 `{m['catalogPath']}` — [{m['n_views']} view(s)]({_mdim_tree_url(m['catalogPath'])}){draft}")
    for e in sorted(r.explorers, key=lambda e: str(e["slug"])):
        lines.append(f"- 🧭 explorer `{e['slug']}` — {e['n_views']} view(s)")
    if r.draft_charts:
        lines.append(f"- 📝 **{len(r.draft_charts)}** unpublished chart(s): {_names(r.draft_charts)}")
    if not lines:
        # Worth saying rather than leaving blank: a real change nobody can currently see is a finding.
        lines.append("- Nothing renders this text yet — no published chart, MDim view or explorer view.")
    return lines


def _mdim_tree_url(catalog_path: str) -> str:
    """Link to one MDim's dimension tree.

    The catalogPath is percent-encoded: it always carries a `#`, which a browser would otherwise read as
    the start of the fragment, dropping `&mode=tree` and truncating the path.
    """
    base = SOURCE.wizard_url.rstrip("/")
    return f"{base}/metadata-diff?diff-type=mdims&mdim={quote(catalog_path, safe='/')}&mode=tree"


def _names(charts: list[dict[str, Any]], limit: int = 6) -> str:
    slugs = sorted(str(c.get("slug") or f"chart {c.get('chartId')}") for c in charts)
    shown = ", ".join(f"`{s}`" for s in slugs[:limit])
    return shown if len(slugs) <= limit else f"{shown} … +{len(slugs) - limit}"


def _dimension_tree(source_engine: Engine, target_engine: Engine, reach: list[ChangeReach]) -> None:
    """Every affected MDim on its dimension grid, with the charts as one more branch.

    One component holding all of them rather than one component each: a component sizes its own iframe and
    would overlap whatever Streamlit renders after it, so they cannot be stacked. Its cost is one view
    diff per MDim, which is why it stops at MAX_TREE_MDIMS and says so instead of drawing forever.
    """
    affected = sorted({str(m["catalogPath"]) for r in reach for m in r.mdims})
    if not affected:
        st.caption("No MDim renders any of these changes, so there is no dimension grid to draw.")
        # Nothing to badge, so every affected chart is one the grid says nothing about.
        _chart_reach(reach, badged=set())
        return

    df = cached.mdim_changes(source_engine, target_engine)
    shown, dropped = affected[:MAX_TREE_MDIMS], affected[MAX_TREE_MDIMS:]

    sections, badged = [], set()
    for catalog_path in shown:
        row = df.loc[catalog_path] if catalog_path in df.index else None
        cache_key = f"{row.get('configMd5_source')}-{row.get('configMd5_target')}" if row is not None else ""
        title, dimensions, view_diffs = cached.mdim_view_diffs(
            catalog_path, source_engine, target_engine, cache_key=cache_key
        )
        if not view_diffs:
            continue

        ids = sorted({v.indicator_id for v in view_diffs if v.affects_indicator and v.indicator_id is not None})
        usage = cached.usage_for_indicators(tuple(ids), catalog_path, source_engine, cache_key=cache_key)
        # What this MDim's `↗ N charts` badges account for, union across every MDim drawn.
        badged |= {c["chartId"] for v in view_diffs for c in view_impact(v, usage)[0]}

        # Each leaf opens that view on this staging server — the view as a reader gets it, which is the
        # question you have when you click one view out of fifty.
        slug = str(row["slug_source"]) if row is not None and row.get("slug_source") else ""
        draft = " · unpublished" if row is not None and bool(row.get("is_draft")) else ""
        sections.append(
            {
                "title": f"{title}{draft}",
                "subtitle": catalog_path,
                "catalog_path": catalog_path,
                "dimensions": dimensions,
                "view_diffs": view_diffs,
                "leaf_hrefs": [
                    f"{SOURCE.site}/grapher/{slug}?{urlencode(v.dimensions)}" if slug else "" for v in view_diffs
                ],
                "external_impacts": [impact_counts(v, usage) for v in view_diffs],
            }
        )

    if not sections:
        st.warning("The affected MDims have no views to draw.")
        _chart_reach(reach, badged=set())
        return

    if dropped:
        st.caption(
            f"Drawing {len(sections)} of {len(affected)} affected MDims — each one costs a view diff. "
            f"Not drawn: {', '.join(f'`{cp}`' for cp in dropped)}."
        )

    tree_html, height = render_multi_tree_html(
        sections,
        chart_branch=_chart_branch(reach, badged),
        self_url=f"{SOURCE.wizard_url.rstrip('/')}/metadata-diff",
    )
    # NOTE: nothing may be rendered below this — the component resizes itself to its content, and
    # Streamlit-rendered siblings would overlap during the resize.
    # scrolling=False: the component resizes its frame to its content, so an iframe scrollbar could only
    # ever nest a second vertical scroll inside the page's.
    components.html(tree_html, height=height, scrolling=False)


def _chart_branch(reach: list[ChangeReach], badged: set) -> dict[str, Any]:
    """The charts these edits reach, as a branch of the grid: grouped by how a reader meets the text.

    Grouped the way the chart lists elsewhere group: a data page lays the text out, a multi-indicator
    chart keeps it behind "Learn more about this data", and a draft shows nobody anything. Charts the
    grid already accounts for through a view badge are marked rather than hidden, so the branch's total
    and the badges' totals can be reconciled by eye.
    """
    charts, drafts = {}, {}
    for r in reach:
        for c in r.charts:
            charts.setdefault(c["chartId"], (c, r))
        for c in r.draft_charts:
            drafts.setdefault(c["chartId"], (c, r))

    def leaf(chart: dict[str, Any], change: ChangeReach, published: bool = True) -> dict[str, Any]:
        slug = str(chart.get("slug") or f"chart {chart.get('chartId')}")
        href = (
            f"{SOURCE.site}/grapher/{slug}"
            if published and chart.get("slug")
            else f"{SOURCE.admin_site}/admin/charts/{chart.get('chartId')}/edit"
        )
        return {
            "label": slug,
            "href": href,
            "badged": chart.get("chartId") in badged,
            "preview": (
                f'<p class="mdd-impact-line">{html.escape(field_label(change.field))} changed</p>'
                f'<div class="mdd-diff">{_preview(change)}</div>'
            ),
        }

    on_page = [leaf(c, r) for c, r in charts.values() if c.get("has_data_page", True)]
    drawer = [leaf(c, r) for c, r in charts.values() if not c.get("has_data_page", True)]
    draft_leaves = [leaf(c, r, published=False) for c, r in drafts.values()]
    return {
        "label": "Charts",
        "groups": [
            {"name": "Data pages", "note": "The text is laid out on the chart's data page.", "charts": on_page},
            {
                "name": "Via Learn more about this data",
                "note": "Multi-indicator charts: their readers reach the text in the sources drawer.",
                "charts": drawer,
            },
            {"name": "Unpublished drafts", "note": "No reader can open these.", "charts": draft_leaves},
        ],
    }


def _chart_reach(reach: list[ChangeReach], badged: set) -> None:
    """Every chart these edits reach — the fallback for when there is no grid to hang them off.

    With a grid on screen the charts are a branch of it (`_chart_branch`); this is what a branch that
    changes no MDim gets instead.

    A grapher chart is never a view of an MDim, so the grid can only mention one indirectly, through a
    view's `↗ N charts` badge — which fires when that view's change is in the shared indicator layer.
    Anything else the branch touched (its chart-config edits, and indicators this MDim does not render)
    appears nowhere in the grid, and that is the part worth naming: measured on this branch, 23 of 66
    affected charts for one MDim and 47 of 66 for the other.
    """
    charts, drafts = {}, {}
    for r in reach:
        for c in r.charts:
            charts.setdefault(c["chartId"], c)
        for c in r.draft_charts:
            drafts.setdefault(c["chartId"], c)
    if not charts and not drafts:
        st.caption("These edits reach no published chart.")
        return

    elsewhere = [c for cid, c in charts.items() if cid not in badged]
    in_grid = [c for cid, c in charts.items() if cid in badged]

    label = f"📈 {len(charts)} chart{'s' if len(charts) != 1 else ''} affected"
    if elsewhere:
        label += f" · {len(elsewhere)} the grid does not mention"
    if drafts:
        label += f" · {len(drafts)} draft{'s' if len(drafts) != 1 else ''}"
    with st.expander(label):
        if elsewhere:
            st.markdown(f"**{len(elsewhere)} chart(s) the grid below says nothing about**")
            st.caption(
                "Either their text came from a chart-config edit, which no MDim view carries, or they "
                "render an indicator this MDim does not."
            )
            render_chart_list(elsewhere, verb="render this branch's edited text", drafts=list(drafts.values()))
        if in_grid:
            st.markdown(f"**{len(in_grid)} chart(s) the grid accounts for**")
            st.caption(
                "Reachable in the grid through a view's `↗ N charts` badge: they use the same indicator as "
                "a view whose shared metadata changed."
            )
            render_chart_list(in_grid, verb="share an indicator with a changed view")


def _truncation_note(total: int) -> None:
    if total > MAX_ROWS:
        st.caption(f"Showing the first {MAX_ROWS} of {total}. The three sections list them all, per surface.")


def _preview(r: ChangeReach) -> str:
    """A one-line preview of the edit, windowed on what changed — two rows can share a field name."""
    return diff_window_html(r.old, r.new, max_chars=260)
