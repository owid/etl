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
from typing import Any
from urllib.parse import quote, urlencode

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, diff_window_html, field_label
from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, EditGroup, group_by_edit, reach_by_surface
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    impact_counts,
    render_chart_list,
    view_impact,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.tree import render_multi_tree_html
from apps.wizard.utils.components import url_persist

GROUP_KEY = "blast-group"
# The MDim a reader arrived here to see — set by the MDim cards' "🌳 Dimension tree" button and
# carried by `_mdim_tree_url`. Read from the URL so the link is shareable, and drawn first so
# MAX_TREE_MDIMS can never be what drops the one MDim somebody asked for.
TREE_MDIM_KEY = "blast-tree-mdim"
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
# Explorer grids drawn at once. Their views come from one comparison already made for the badges, so the
# cost is layout rather than queries — but a page of folded grids nobody opens is still a page.
MAX_TREE_EXPLORERS = 4


def st_show_blast_radius(source_engine: Engine, target_engine: Engine) -> None:
    """Everywhere the branch's metadata changes land: by edit, by surface, or on an MDim's dimension grid."""
    st.markdown(DIFF_CSS + _CONTEXT_CSS, unsafe_allow_html=True)
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


def _dimension_tree(source_engine: Engine, target_engine: Engine, reach: list[ChangeReach]) -> None:
    """Every affected MDim on its dimension grid, with the charts as one more branch.

    One component holding all of them rather than one component each: a component sizes its own iframe and
    would overlap whatever Streamlit renders after it, so they cannot be stacked. Its cost is one view
    diff per MDim, which is why it stops at MAX_TREE_MDIMS and says so instead of drawing forever.
    """
    explorer_hierarchy = _explorer_hierarchy(source_engine, target_engine)
    affected = _requested_first(sorted({str(m["catalogPath"]) for r in reach for m in r.mdims}))
    if not affected:
        st.caption("No MDim renders any of these changes, so there is no MDim grid to draw.")
        if explorer_hierarchy is None:
            # Nothing to badge, so every affected chart is one the grid says nothing about.
            _chart_reach(reach, badged=set())
            _explorer_reach(reach)
            return
        # Explorer views have dimensions of their own, so there is still a grid worth drawing.
        tree_html, height = render_multi_tree_html(
            [],
            branches=[_chart_branch(reach, set())],
            hierarchies=[explorer_hierarchy],
            self_url=f"{SOURCE.wizard_url.rstrip('/')}/metadata-diff",
        )
        components.html(tree_html, height=height, scrolling=False)
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
        branches=[_chart_branch(reach, badged)],
        # The explorers are a hierarchy of grids now, not a flat branch: their views have dimensions.
        hierarchies=[h for h in ({"id": "mdims", "label": "MDims", "sections": sections}, explorer_hierarchy) if h],
        self_url=f"{SOURCE.wizard_url.rstrip('/')}/metadata-diff",
    )
    # NOTE: nothing may be rendered below this — the component resizes itself to its content, and
    # Streamlit-rendered siblings would overlap during the resize.
    # scrolling=False: the component resizes its frame to its content, so an iframe scrollbar could only
    # ever nest a second vertical scroll inside the page's.
    components.html(tree_html, height=height, scrolling=False)


def _explorer_hierarchy(source_engine: Engine, target_engine: Engine) -> dict[str, Any] | None:
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
    for slug in sorted(branch)[:MAX_TREE_EXPLORERS]:
        diffs = [d for d in branch[slug] if d.changed] or branch[slug]
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

    dropped = sorted(branch)[MAX_TREE_EXPLORERS:]
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
                "badged": False,
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
            else SOURCE.chart_admin_site(chart.get("chartId"))  # ty: ignore
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
        "id": "charts",
        "label": "Charts",
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


def _truncation_note(total: int) -> None:
    if total > MAX_ROWS:
        st.caption(f"Showing the first {MAX_ROWS} of {total}. The three sections list them all, per surface.")


def _preview(r: ChangeReach) -> str:
    """A one-line preview of the edit, windowed on what changed — two rows can share a field name."""
    return diff_window_html(r.old, r.new, max_chars=260)
