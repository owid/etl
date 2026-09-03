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

- **dimension tree** — every affected MDim's views on their own dimension grids, with the charts and
  explorer views beside them. Not how many views changed but *which*, and what sits next to them.
- **by edit** — edit → text → page. How far does one authored edit actually go?

By-edit reads the cached summary the section badges already use, so it costs no queries. The grid diffs
each MDim's views, which is why it stops at `MAX_TREE_MDIMS`.
"""

import html
from dataclasses import replace
from typing import Any
from urllib.parse import quote, urlencode

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached, view_nav
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, diff_window_html, field_label, view_url
from apps.wizard.app_pages.metadata_diff.discovery import (
    ChangeReach,
    EditGroup,
    edit_slot,
    group_by_edit,
    reach_by_surface,
)
from apps.wizard.app_pages.metadata_diff.edits_view import CONTEXT_CSS, st_edit_body
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    impact_counts,
    render_chart_list,
)
from apps.wizard.app_pages.metadata_diff.tree import render_multi_tree_html
from apps.wizard.utils.components import url_persist

GROUP_KEY = view_nav.BLAST_GROUP_KEY
# One edit to focus on, set by a By-edit card's grid link: its views are highlighted and every other view
# is drawn as unchanged, so the grid answers "where does *this* land" rather than "where does the branch".
EDIT_KEY = view_nav.BLAST_EDIT_KEY
# The MDim a reader arrived here to see — set by the MDim cards' "🌳 Dimension tree" button and
# carried by `_mdim_tree_url`. Read from the URL so the link is shareable, and drawn first so
# MAX_TREE_MDIMS can never be what drops the one MDim somebody asked for.
TREE_MDIM_KEY = "blast-tree-mdim"
# Which surfaces the grid draws. A filter rather than three separate views: the whole point of the grid is
# what sits *beside* an edit, so everything is the default and narrowing is the deliberate act. Done in
# Python rather than by hiding nodes in the component, because each MDim grid costs a view diff against the
# baseline — filtering them out skips that work rather than drawing it and covering it up.
SURFACE_KEY = "blast-surface"
SURFACE_LABELS = {
    "all": "Everything",
    "mdims": "MDims only",
    "explorers": "Explorers only",
    "charts": "Charts only",
}
MAX_ROWS = 60
# MDims drawn on the grid at once. Each costs a view diff against the baseline, and past a handful the
# canvas is unreadable anyway; the rest are named rather than silently dropped.
MAX_TREE_MDIMS = 6
# Explorer grids drawn at once. Their views come from one comparison already made for the badges, so the
# cost is layout rather than queries — but a page of folded grids nobody opens is still a page.
MAX_TREE_EXPLORERS = 4


def st_show_blast_radius(source_engine: Engine, target_engine: Engine) -> None:
    """Everywhere the branch's metadata changes land: by edit, by surface, or on an MDim's dimension grid."""
    st.markdown(DIFF_CSS + CONTEXT_CSS, unsafe_allow_html=True)
    summary = cached.summary(source_engine, target_engine)
    reach = summary.reach

    if not reach:
        # The page has already said there are no changes; repeating it teaches nobody what this view does.
        st.success(f"**No metadata text changes** on this server against `{BASELINE_NAME}`.")
        st.caption(
            "This view traces where an edit lands — every affected MDim view on its dimension grid, with "
            "the charts and explorer views beside it. With no changed text there is nothing to trace, and "
            "the three review sections are empty for the same reason."
        )
        return

    edits = group_by_edit(reach)
    focus = _requested_edit(edits)
    if focus is not None:
        # One edit only, at the request of a card elsewhere: its texts, its pages, its views highlighted.
        edits, reach = [focus], focus.changes
        _st_focus_banner(focus)
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
        "Why three numbers: one edit can render into several texts, and each text lands on many pages. "
        "A sentence added to a shared definition across multiple indicators is one thing to judge, even "
        "when it turns up in eighty places."
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
        _dimension_tree(source_engine, target_engine, reach, focused=focus is not None)
    else:
        _tree(edits)


def _tree(edits: list[EditGroup]) -> None:
    """Edit → text → page, one card per authored edit.

    The texts sit inside an expander: with a shared definition there are ten or more of them, and the
    thing worth seeing first is that they are one edit, not ten.
    """
    for group in edits[:MAX_ROWS]:
        datasets = group.authored_in
        with st.container(border=True):
            st.markdown(
                f"**{field_label(group.field)}** · **{group.n_edits} edit{'s' if group.n_edits != 1 else ''}** → "
                f"**{group.n_texts} rendered text{'s' if group.n_texts != 1 else ''}** → "
                f"**{group.n_reader_facing} page{'s' if group.n_reader_facing != 1 else ''}** a reader can reach"
            )
            st_edit_body(group)
            if len(datasets) > 1:
                files = ", ".join(f"`{d}.meta.yml`" for d in datasets)
                st.caption(
                    f"Authored in {len(datasets)} separate garden datasets — {files}. No `definitions.*` "
                    "block spans files, so each one has to be edited and rebuilt on its own."
                )
            st.markdown(f"_{_surface_summary(group)}_")

            with st.expander("Everywhere this edit lands", expanded=False):
                _edit_detail(group)
    _truncation_note(len(edits))


def _edit_detail(group: EditGroup) -> None:
    """Where one edit lands, aggregated over its texts: what a reader can see, then what nobody can yet.

    Counts and names, not links. One edit reaches a hundred views, the dimension grid already enumerates
    every one of them, and listing them again here buried the two numbers this expander exists to give.

    Published first, and unpublished kept apart, because "can a reader see this today" is the question
    that decides how much care the edit needs — not how many places it touches in total.

    Deduped across the edit's texts: a view or chart carrying two of them is one place, not two.
    """
    mdims = _mdim_totals(group)
    charts = {c["chartId"]: c for change in group.changes for c in change.charts}
    drafts = {c["chartId"]: c for change in group.changes for c in change.draft_charts}
    on_page = [c for c in charts.values() if c.get("has_data_page", True)]
    drawer = [c for c in charts.values() if not c.get("has_data_page", True)]

    explorers = _explorer_totals(group)
    live_mdims = [e for e in mdims if not e["is_draft"]]
    draft_mdims = [e for e in mdims if e["is_draft"]]

    if live_mdims or charts or explorers:
        lines = _mdim_lines(live_mdims)
        for slug, n_views in explorers:
            # Explorer views have no data page, so a WYSK edit reaches the view and shows nobody anything.
            invisible = (
                " — but not in the view itself, which has no data page" if group.field == "descriptionKey" else ""
            )
            lines.append(f"- **{n_views} view{'s' if n_views != 1 else ''}** in the explorer *{slug}*{invisible}")
        if charts:
            where = []
            if on_page:
                where.append(f"{len(on_page)} on their data page")
            if drawer:
                where.append(f"{len(drawer)} via *Learn more about this data*")
            lines.append(f"- **{len(charts)} chart{'s' if len(charts) != 1 else ''}** — {', '.join(where)}")
        st.markdown("**A reader can see this**")
        st.markdown("\n".join(lines))

    if draft_mdims or drafts:
        lines = _mdim_lines(draft_mdims)
        if drafts:
            lines.append(f"- **{len(drafts)} draft chart{'s' if len(drafts) != 1 else ''}**")
        st.markdown("**Not published, so no reader can see it yet**")
        st.markdown("\n".join(lines))

    if not mdims and not charts and not drafts and not explorers:
        st.caption("Nothing renders this text yet — no MDim view, chart or explorer view.")


def _explorer_totals(group: EditGroup) -> list[tuple[str, int]]:
    """One row per explorer this edit reaches, widest first.

    Explorer reach carries a view count and no view identities, so texts landing on the same explorer are
    reconciled by taking the largest count rather than a union — the same lower bound `_mdim_totals` falls
    back to, and exact whenever the texts land on the same views. Only published explorers are compared
    upstream, so there is no unpublished split to make here.
    """
    views: dict[str, set] = {}
    counted: dict[str, int] = {}
    for change in group.changes:
        for explorer in change.explorers:
            slug = str(explorer["slug"])
            views.setdefault(slug, set()).update(tuple(sorted(v.items())) for v in explorer.get("views") or [])
            counted[slug] = max(counted.get(slug, 0), int(explorer.get("n_views") or 0))
    totals = [(slug, len(seen) or counted[slug]) for slug, seen in views.items()]
    return sorted(totals, key=lambda kv: (-kv[1], kv[0]))


def _mdim_totals(group: EditGroup) -> list[dict[str, Any]]:
    """One row per MDim this edit reaches: what to call it, whether it is published, how many views.

    Views are counted as a set of dimension tuples, so an MDim reached by several of the edit's texts is
    one row with its distinct views — not the same view counted twice. Where a reach entry carries only a
    count and no dimensions, the largest count any single text reported is used: it is the tightest lower
    bound available, and it is exact whenever the texts land on the same views.
    """
    by_path: dict[str, dict[str, Any]] = {}
    for change in group.changes:
        for mdim in change.mdims:
            entry = by_path.setdefault(
                str(mdim["catalogPath"]),
                {
                    "title": str(mdim.get("title") or mdim["catalogPath"]),
                    "is_draft": bool(mdim.get("is_draft")),
                    "views": set(),
                    "counted": 0,
                },
            )
            entry["views"] |= {tuple(sorted(v.items())) for v in mdim.get("views") or []}
            entry["counted"] = max(entry["counted"], int(mdim.get("n_views") or 0))
    for entry in by_path.values():
        entry["n_views"] = len(entry["views"]) or entry["counted"]
    return sorted(by_path.values(), key=lambda e: (-e["n_views"], e["title"]))


def _mdim_lines(entries: list[dict[str, Any]]) -> list[str]:
    """One bullet per MDim, named as the dimension grid names it, widest reach first.

    The surface is named on every line. A bullet reading "30 views in Incomes across the distribution"
    leaves the reader to infer that the italicised thing is an MDim, and beside a bullet counting charts
    that inference is exactly what should not be left to them.
    """
    return [f"- **{e['n_views']} view{'s' if e['n_views'] != 1 else ''}** in the MDim *{e['title']}*" for e in entries]


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


def _mdim_tree_url(catalog_path: str) -> str:
    """Link to one MDim's dimension tree — which lives in this section.

    `?diff-type=mdims&mdim=...&mode=tree` was the removed deep page's route, and the MDims list drops both
    of those parameters on load, so a link built that way opened the plain list instead. This carries the
    state `_open_dimension_tree` sets, as a URL somebody can paste.

    The catalogPath is percent-encoded: it always carries a `#`, which a browser would otherwise read as
    the start of the fragment, dropping every parameter after it and truncating the path.
    """
    base = SOURCE.wizard_url.rstrip("/")
    return (
        f"{base}/metadata-diff?diff-type=blast&{GROUP_KEY}=dimensions&{TREE_MDIM_KEY}={quote(catalog_path, safe='/')}"
    )


def _requested_mdim() -> str | None:
    """The MDim somebody clicked through to see, from the URL or from the button that set it."""
    return st.query_params.get(TREE_MDIM_KEY) or st.session_state.get(TREE_MDIM_KEY) or None


def _requested_first(affected: list[str]) -> list[str]:
    """Draw the requested MDim first, so the cap cannot drop the one the reader asked for.

    Ordering, not filtering: the rest of the grid is what makes the section worth arriving at.
    """
    requested = _requested_mdim()
    if requested is None or requested not in affected:
        return affected
    return [requested] + [cp for cp in affected if cp != requested]


def _dimension_tree(
    source_engine: Engine, target_engine: Engine, reach: list[ChangeReach], focused: bool = False
) -> None:
    """Every affected MDim on its dimension grid, with the charts as one more branch.

    One component holding all of them rather than one component each: a component sizes its own iframe and
    would overlap whatever Streamlit renders after it, so they cannot be stacked. Its cost is one view
    diff per MDim, which is why it stops at MAX_TREE_MDIMS and says so instead of drawing forever.

    `focused` means `reach` is one edit's texts: only the views those land on are drawn as changed. The
    other changed views of the same MDim are still on the grid, greyed, because what sits beside an edit is
    half of what the grid is for.

    The surface filter above it narrows to one kind of surface. Everything downstream is built behind it, so
    choosing "Charts only" on a branch touching six MDims skips six view diffs rather than computing them
    and hiding the result.
    """
    surface = _st_surface_filter(reach)
    focus_mdims = _view_keys(reach, "mdims", "catalogPath") if focused else None
    explorer_hierarchy = (
        _explorer_hierarchy(source_engine, target_engine, _view_keys(reach, "explorers", "slug") if focused else None)
        if _wants(surface, "explorers")
        else None
    )
    affected = (
        _requested_first(sorted({str(m["catalogPath"]) for r in reach for m in r.mdims}))
        if _wants(surface, "mdims")
        else []
    )
    if not affected:
        if _wants(surface, "mdims"):
            st.caption("No MDim renders any of these changes, so there is no MDim grid to draw.")
        hierarchies = [explorer_hierarchy] if explorer_hierarchy else []
        branches = _chart_branches(source_engine, reach, surface)
        if not hierarchies and not branches:
            # Filtered to a surface these edits do not reach at all. Said plainly, rather than drawn as an
            # empty shell — and only reachable by choosing it, since a surface with nothing on it is not
            # offered in the first place.
            st.caption(f"Nothing on this surface renders any of these changes ({SURFACE_LABELS[surface]}).")
            return
        if surface == "all" and not hierarchies:
            # No grid anywhere on this branch: the charts are all there is, and they read better as the
            # grouped list than as a lone branch of a tree that is not being drawn.
            _chart_reach(reach, badged=set())
            _explorer_reach(reach)
            return
        # Explorer views have dimensions of their own, so there is still a grid worth drawing.
        tree_html, height = render_multi_tree_html(
            [],
            branches=branches,
            hierarchies=hierarchies,
            self_url=f"{SOURCE.wizard_url.rstrip('/')}/metadata-diff",
        )
        components.html(tree_html, height=height, scrolling=False)
        return

    df = cached.mdim_changes(source_engine, target_engine)
    shown, dropped = affected[:MAX_TREE_MDIMS], affected[MAX_TREE_MDIMS:]

    sections = []
    for catalog_path in shown:
        row = df.loc[catalog_path] if catalog_path in df.index else None
        cache_key = f"{row.get('configMd5_source')}-{row.get('configMd5_target')}" if row is not None else ""
        title, dimensions, view_diffs = cached.mdim_view_diffs(
            catalog_path, source_engine, target_engine, cache_key=cache_key
        )
        if not view_diffs:
            continue
        if focus_mdims is not None:
            view_diffs = _only_these_views(view_diffs, focus_mdims.get(catalog_path, set()))

        ids = sorted({v.indicator_id for v in view_diffs if v.affects_indicator and v.indicator_id is not None})
        usage = cached.usage_for_indicators(tuple(ids), catalog_path, source_engine, cache_key=cache_key)

        # Each leaf opens that view on this staging server — the view as a reader gets it, which is the
        # question you have when you click one view out of fifty.
        slug = str(row["slug_source"]) if row is not None and row.get("slug_source") else ""
        is_draft = row is not None and bool(row.get("is_draft"))
        sections.append(
            {
                "title": f"{title}{' · unpublished' if is_draft else ''}",
                "subtitle": catalog_path,
                "catalog_path": catalog_path,
                "dimensions": dimensions,
                "view_diffs": view_diffs,
                # An unpublished MDim has no reader-facing page — `/grapher/<slug>` 404s even when the
                # slug is already set — so its views open in the admin preview, which applies dimensions.
                "leaf_hrefs": [
                    view_url(SOURCE, catalog_path, None if is_draft else slug, v.dimensions) for v in view_diffs
                ],
                "external_impacts": [impact_counts(v, usage) for v in view_diffs],
            }
        )

    if not sections:
        st.warning("The affected MDims have no views to draw.")
        if surface == "all":
            _chart_reach(reach, badged=set())
            _explorer_reach(reach)
        return

    if dropped:
        st.caption(
            f"Drawing {len(sections)} of {len(affected)} affected MDims — each one costs a view diff. "
            f"Not drawn: {', '.join(f'`{cp}`' for cp in dropped)}."
        )

    tree_html, height = render_multi_tree_html(
        [],
        branches=_chart_branches(source_engine, reach, surface),
        # The explorers are a hierarchy of grids now, not a flat branch: their views have dimensions.
        hierarchies=[h for h in ({"id": "mdims", "label": "MDims", "sections": sections}, explorer_hierarchy) if h],
        self_url=f"{SOURCE.wizard_url.rstrip('/')}/metadata-diff",
    )
    # NOTE: nothing may be rendered below this — the component resizes itself to its content, and
    # Streamlit-rendered siblings would overlap during the resize.
    # scrolling=False: the component resizes its frame to its content, so an iframe scrollbar could only
    # ever nest a second vertical scroll inside the page's.
    components.html(tree_html, height=height, scrolling=False)


def _wants(surface: str, kind: str) -> bool:
    """Whether the chosen filter draws this kind of surface."""
    return surface in ("all", kind)


def _chart_branches(source_engine: Engine, reach: list[ChangeReach], surface: str) -> list[dict[str, Any]]:
    """The charts branch, when the filter draws charts and these edits reach one.

    The lookups happen here rather than inside `_chart_branch` so they are paid only when the branch is
    actually drawn — under "MDims only" it is not, and the views one leaves OWID's own databases.

    An empty list when every chart it would have drawn is redirected away: the caller then says the
    surface holds nothing rather than drawing a heading over no leaves.
    """
    if not _wants(surface, "charts") or not any(r.charts or r.draft_charts for r in reach):
        return []
    ids = sorted({int(c["chartId"]) for r in reach for c in (*r.charts, *r.draft_charts)})
    branch = _chart_branch(
        reach,
        cached.chart_views(tuple(ids)),
        cached.mdim_redirected_charts(source_engine),
    )
    return [branch] if any(group["leaves"] for group in branch["groups"]) else []


def surface_options(reach: list[ChangeReach]) -> list[str]:
    """ "all", plus every surface these edits actually reach.

    An option for a surface with nothing on it would filter the grid to an empty page and read as a bug in
    the tool rather than as a fact about the branch — the section bar greys its empty sections for the same
    reason. With one surface reached there is nothing to choose between, so the caller draws no control.
    """
    options = ["all"]
    if any(r.mdims for r in reach):
        options.append("mdims")
    if any(r.explorers for r in reach):
        options.append("explorers")
    if any(r.charts or r.draft_charts for r in reach):
        options.append("charts")
    return options


def _st_surface_filter(reach: list[ChangeReach]) -> str:
    """Draw everything, or one surface alone. Kept in the URL, so a narrowed grid is a link.

    Sanitized before the widget exists, the way the grouping control above it is: `url_persist` checks its
    held value against the options strictly, so a link written when the branch reached an explorer would
    otherwise raise on a branch that reaches none. A deselecting click returns None, which is not a
    third state — it means everything.
    """
    options = surface_options(reach)
    if len(options) < 3:
        # One surface, or none: nothing to filter. Any held value is stale, and left in the URL it would
        # be checked against options this control never rendered.
        st.query_params.pop(SURFACE_KEY, None)
        st.session_state.pop(SURFACE_KEY, None)
        return "all"

    for store in (st.query_params, st.session_state):
        if store.get(SURFACE_KEY) not in options:
            store.pop(SURFACE_KEY, None)

    picked = url_persist(st.segmented_control)(
        label="Surfaces",
        options=options,
        format_func=lambda s: SURFACE_LABELS[s],
        key=SURFACE_KEY,
        value="all",
        label_visibility="collapsed",
        help="Narrow the grid to one kind of surface. Only the surfaces this branch reaches are offered.",
    )
    return picked if picked in options else "all"


def _explorer_hierarchy(
    source_engine: Engine, target_engine: Engine, focus: dict[str, set[tuple]] | None = None
) -> dict[str, Any] | None:
    """The affected explorers as dimension grids, a hierarchy beside the MDims — or nothing, when none are.

    An explorer view is addressed by dimensions exactly as an MDim view is, so the same grid answers the
    same question: not how many views changed but *which*, and what sits next to them.

    Drawn like the MDim grids in every respect — expanded, titled with the explorer's own name and its
    slug beneath — so the two hierarchies read the same way. That does mean an explorer whose every view
    changed draws every one of them: a shared subtitle edit reached 402 views of one LIS explorer. The
    section's own "Show all views" filter and its collapsible branches are what keep that navigable.
    """
    changes = cached.explorer_changes(source_engine, target_engine)
    branch = changes.branch_views()
    if not branch:
        return None

    titles = cached.explorer_titles(source_engine)
    sections = []
    # Focused on one edit, only the explorers it reaches are drawn, and only the views it lands on: an
    # explorer grid has no unchanged views to grey, so the views of other edits are left out instead.
    slugs = sorted(branch) if focus is None else sorted(s for s in branch if s in focus)
    for slug in slugs[:MAX_TREE_EXPLORERS]:
        diffs = [d for d in branch[slug] if d.changed] or branch[slug]
        if focus is not None:
            diffs = [d for d in diffs if _key(d.dimensions) in focus[slug]]
            if not diffs:
                continue
        dimensions = _explorer_dimensions(diffs)
        if not dimensions:
            continue
        sections.append(
            {
                # Titled the way an MDim section is: the name a reader sees, with the slug beneath it.
                "title": titles.get(slug, slug),
                "subtitle": slug,
                "dimensions": dimensions,
                "view_diffs": diffs,
                "leaf_hrefs": [f"{SOURCE.site}/explorers/{slug}?{urlencode(v.dimensions)}" for v in diffs],
            }
        )
    if not sections:
        return None

    dropped = slugs[MAX_TREE_EXPLORERS:]
    label = f"{len(sections)} explorer{'s' if len(sections) != 1 else ''}"
    if dropped:
        label += f" · not drawn: {', '.join(dropped)}"
    return {"id": "explorers", "label": "Explorers", "count_label": label, "sections": sections}


def _explorer_dimensions(diffs: list[ViewDiff]) -> list[dict[str, Any]]:
    """The grid's columns, inferred from the views themselves.

    An explorer publishes no dimension list the tool can read — there is no `dimensions` block like an
    MDim config's — so the columns are the keys its views carry, with each key's values in first-seen
    order. The slugs are the labels because that is all there is, and an explorer's are already written
    for people ("1-poorest", "After tax").

    Narrowest dimension first, widest last, which is a guess but a better one than the order the views
    happen to list. A leaf is named by the last dimension's value, so leaving a two-choice toggle there
    labels four hundred leaves "true" and "false"; putting the widest dimension last names them by the
    thing that actually distinguishes them, and the toggle becomes the branch you open to get there.
    """
    order: list[str] = []
    choices: dict[str, list[str]] = {}
    for diff in diffs:
        for key, value in diff.dimensions.items():
            if key not in choices:
                order.append(key)
                choices[key] = []
            if value not in choices[key]:
                choices[key].append(value)
    # Positions captured first: `list.sort` empties the list while it runs, so a key function calling
    # `order.index` raises ValueError on the very first comparison.
    seen_at = {key: i for i, key in enumerate(order)}
    order.sort(key=lambda key: (len(choices[key]), seen_at[key]))

    def pretty(text: str) -> str:
        # Tidied, but never tidied away: a dimension value of "-" (this view has no decile) turned into a
        # single space and left four hundred leaves labelled with nothing at all.
        return text.replace("-", " ").replace("_", " ").strip() or text

    return [
        {
            "slug": key,
            "name": pretty(key),
            "choices": [{"slug": value, "name": pretty(value)} for value in choices[key]],
        }
        for key in order
    ]


def _explorer_branch(reach: list[ChangeReach]) -> dict[str, Any] | None:
    """The explorers these edits reach, as a branch beside the charts — or nothing, when none are.

    One leaf per explorer rather than per view: the reach carries a view count, not view identities, and
    an explorer's views are not addressable the way an MDim's dimensions are. The count rides on the
    label so the branch's total and the header's page count can still be reconciled.
    """
    seen_views: dict[str, set] = {}
    counted: dict[str, int] = {}
    first_change: dict[str, ChangeReach] = {}
    for r in reach:
        for e in r.explorers:
            slug = str(e["slug"])
            seen_views.setdefault(slug, set()).update(tuple(sorted(v.items())) for v in e.get("views") or [])
            counted[slug] = max(counted.get(slug, 0), int(e.get("n_views") or 0))
            first_change.setdefault(slug, r)
    if not seen_views:
        return None

    leaves = []
    for slug in sorted(seen_views):
        # Distinct views across every text of every edit — a view carrying two of them is one view.
        n_views = len(seen_views[slug]) or counted[slug]
        change = first_change[slug]
        leaves.append(
            {
                "label": f"{slug} · {n_views} view{'s' if n_views != 1 else ''}",
                "href": f"{SOURCE.site}/explorers/{slug}",
                "preview": (
                    f'<p class="mdd-impact-line">{html.escape(field_label(change.field))} changed</p>'
                    f'<div class="mdd-diff">{_preview(change)}</div>'
                ),
            }
        )
    return {
        "id": "explorers",
        "label": "Explorers",
        "groups": [
            {
                "name": "Published explorers",
                "note": "An explorer view renders no data page, so a WYSK edit is not visible in one.",
                "leaves": leaves,
            }
        ],
    }


def _chart_branch(
    reach: list[ChangeReach],
    views: dict[int, int] | None = None,
    redirected: dict[str, str] | None = None,
) -> dict[str, Any]:
    """The charts these edits reach, as a branch of the grid: grouped by how a reader meets the text.

    `redirected` names the charts whose URL now serves an MDim view instead (see
    `data.fetch_mdim_redirected_charts`). They are left out, and nothing is said about it: the chart row
    is still published, so the usage lookup still calls them live charts, but nobody can open one —
    following the link lands on the MDim, which is already drawn on this grid with its own views. Listing
    them made the same reader-facing page appear twice, once as a chart nobody reaches. The count of them
    is not a finding a reviewer acts on, so it is not on the page either; the exclusion is a correctness
    fix, not something to report.

    Grouped the way the chart lists elsewhere group: a data page lays the text out, a multi-indicator
    chart keeps it behind "Learn more about this data", and a draft shows nobody anything.

    Each leaf is named by the chart's **title**, with its yearly views underneath in small grey type. The
    title is what a reviewer recognises — "Share of population living in extreme poverty" rather than
    `share-of-population-in-extreme-poverty` — and the slug it replaces is still one hover away, in the
    link the leaf already is.

    Ordered most-viewed first within each group, since which of seventy charts matters is the question an
    author has and the one a name cannot answer: on this branch the busiest carries 196,000 views a year
    and the median a few hundred. A chart with no views recorded sorts last rather than first — 16 of
    these 76 have none, drafts among them — and with no view data at all the order falls back to the name.
    """
    charts, drafts = {}, {}
    gone: dict[str, str] = {}
    for r in reach:
        for c in (*r.charts, *r.draft_charts):
            target = (redirected or {}).get(str(c.get("slug") or ""))
            if target:
                gone[str(c["slug"])] = target
        for c in r.charts:
            if str(c.get("slug") or "") not in gone:
                charts.setdefault(c["chartId"], (c, r))
        for c in r.draft_charts:
            if str(c.get("slug") or "") not in gone:
                drafts.setdefault(c["chartId"], (c, r))

    def leaf(chart: dict[str, Any], change: ChangeReach, published: bool = True) -> dict[str, Any]:
        slug = str(chart.get("slug") or f"chart {chart.get('chartId')}")
        href = (
            f"{SOURCE.site}/grapher/{slug}"
            if published and chart.get("slug")
            else SOURCE.chart_admin_site(chart.get("chartId"))  # ty: ignore
        )
        # A chart whose title is only set in its config carries one here; one that inherits its title from
        # indicator metadata may not, and then the slug is the only name there is.
        title = str(chart.get("title") or "").strip()
        seen = (views or {}).get(int(chart["chartId"]))
        # Views only under the name. Nothing said when nothing was recorded: absence of a row in the
        # warehouse is not a measured zero, and "0 views" beside a chart published last week would be a
        # claim rather than a fact.
        return {
            "label": title or slug,
            "sublabel": f"{seen:,} views/yr" if seen else "",
            "views": seen or 0,
            "href": href,
            "preview": (
                f'<p class="mdd-impact-line">{html.escape(field_label(change.field))} changed</p>'
                f'<div class="mdd-diff">{_preview(change)}</div>'
            ),
        }

    # Busiest first, then by the name on screen: the list runs to dozens of charts over several columns,
    # and both "which of these matters" and "is mine in here" have to be answerable by eye. Reach order is
    # the order the comparison happened to find them, which is no order at all to a reader.
    def by_reach(leaves: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(leaves, key=lambda leaf: (-int(leaf["views"]), str(leaf["label"]).casefold()))

    on_page = by_reach([leaf(c, r) for c, r in charts.values() if c.get("has_data_page", True)])
    drawer = by_reach([leaf(c, r) for c, r in charts.values() if not c.get("has_data_page", True)])
    draft_leaves = by_reach([leaf(c, r, published=False) for c, r in drafts.values()])
    measured = sum(1 for leaf in (*on_page, *drawer, *draft_leaves) if leaf["views"])
    # Which order this is, said rather than left to be inferred from the numbers — and the honest answer
    # when the warehouse could not be reached is that it is alphabetical.
    note = (
        f"Most viewed first — page views over the last year, known for {measured} of these charts."
        if measured
        else "In name order: this server could not read how much these charts are viewed."
    )
    return {
        "id": "charts",
        "label": "Charts",
        "note": note,
        "groups": [
            {"name": "Data pages", "note": "The text is laid out on the chart's data page.", "leaves": on_page},
            {
                "name": "Via Learn more about this data",
                "note": "Multi-indicator charts: their readers reach the text in the sources drawer.",
                "leaves": drawer,
            },
            {"name": "Unpublished drafts", "note": "No reader can open these.", "leaves": draft_leaves},
        ],
    }


def _explorer_reach(reach: list[ChangeReach]) -> None:
    """The explorer views these edits reach, for the paths with no grid to hang a branch off.

    Silent when nothing is reached: an "Explorers (0)" heading in a page about what changed is noise.
    """
    branch = _explorer_branch(reach)
    if branch is None:
        return
    leaves = branch["groups"][0]["leaves"]
    st.markdown(f"**{len(leaves)} explorer{'s' if len(leaves) != 1 else ''} affected**")
    st.caption(branch["groups"][0]["note"])
    st.markdown("\n".join(f"- [`{leaf['label']}`]({leaf['href']})" for leaf in leaves))


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


def _requested_edit(edits: list[EditGroup]) -> EditGroup | None:
    """The one edit a By-edit card sent the reader here to see, or None for the whole branch.

    A handle that matches nothing — the edit was reverted, or the link is from another branch — is dropped
    with a caption rather than left to show an empty grid under a focus banner.
    """
    slot = str(st.query_params.get(EDIT_KEY) or st.session_state.get(EDIT_KEY) or "").strip()
    if not slot:
        return None
    focus = next((e for e in edits if edit_slot(e) == slot), None)
    if focus is None:
        _clear_focus()
        st.caption("The edit this link pointed at is no longer in the diff, so this shows every edit.")
    return focus


def _st_focus_banner(edit: EditGroup) -> None:
    """Say that the page is about one edit, and offer the way back to all of them."""
    words = edit.inserted or edit.deleted or "whitespace only"
    words = words if len(words) <= 90 else words[:87].rstrip() + "…"
    col_text, col_button = st.columns([5, 1], vertical_alignment="center")
    with col_text:
        st.info(
            f"Showing **one edit**: **{field_label(edit.field)}** — “{words}”. Only the views it lands on count "
            "as changed; the other changed views of the same MDims are greyed, behind each grid's *Show all views*."
        )
    with col_button:
        st.button("Show every edit", key="mdd-blast-unfocus", on_click=_clear_focus, width="stretch")


def _clear_focus() -> None:
    st.query_params.pop(EDIT_KEY, None)
    st.session_state.pop(EDIT_KEY, None)


def _key(dims: dict[str, str]) -> tuple:
    return tuple(sorted(dims.items()))


def _view_keys(reach: list[ChangeReach], kind: str, name: str) -> dict[str, set[tuple]]:
    """Per MDim (or explorer), the dimension keys of every view these texts land on."""
    out: dict[str, set[tuple]] = {}
    for r in reach:
        for entry in getattr(r, kind):
            out.setdefault(str(entry[name]), set()).update(_key(v) for v in entry.get("views") or [])
    return out


def _only_these_views(view_diffs: list[ViewDiff], keep: set[tuple]) -> list[ViewDiff]:
    """The same grid, with only `keep` drawn as changed — the rest greyed, not dropped.

    A view outside the focus loses its fields for display, so the grid's counters and highlights count
    only the edit in focus, while the view itself stays on the grid where it belongs.
    """
    return [v if _key(v.dimensions) in keep else replace(v, fields={}, is_new=False) for v in view_diffs]


def _truncation_note(total: int) -> None:
    if total > MAX_ROWS:
        st.caption(f"Showing the first {MAX_ROWS} of {total}. The three sections list them all, per surface.")


def _preview(r: ChangeReach) -> str:
    """A one-line preview of the edit, windowed on what changed — two rows can share a field name."""
    return diff_window_html(r.old, r.new, max_chars=260)
