"""Blast Radius tree: a self-contained interactive HTML component.

Renders the views of an MDIM as a horizontal tree following the order of the MDIM's
dimensions (first control = first fork). Nodes are colored by whether any view
underneath them changed; leaves are real links (supplied by the caller) and show a hover
preview of the change. All interactivity (collapse, filter, tooltips, self-resizing)
is inline JS with no external dependencies, so the component is fully deterministic.
"""

import html
import json
import urllib.parse
from typing import Any

from apps.wizard.app_pages.metadata_diff.core import ViewDiff, diff_preview_html

# The component iframe grows to fit its content up to this cap; beyond it, the tree
# scrolls internally.
# First-paint estimate only: fit() resizes the frame to its real content as soon as the script runs,
# so this caps nothing — it just keeps the initial placeholder frame from being absurd.
INITIAL_HEIGHT_CAP_PX = 4000


# CSS/JS for the affected-charts component kept as plain (non-f) strings so their literal
# braces don't need escaping.
_AC_CSS = """
    #ac-root { font-family: -apple-system, system-ui, sans-serif; font-size: 13px; color: #333; }
    #ac-root .ac-header { color: #555; margin: 0 0 8px; line-height: 1.45; }
    #ac-root .ac-header a { color: #1971c2; }
    #ac-root ol.ac-list { margin: 0; padding-left: 22px; }
    #ac-root li.ac-li { margin: 3px 0; }
    #ac-root a.ac-item { color: #1971c2; text-decoration: none; }
    #ac-root a.ac-item:hover { text-decoration: underline; }
    #ac-root .ac-flag { color: #e8590c; font-size: 11px; background: #fff4e6; border-radius: 6px; padding: 1px 5px; margin-left: 6px; white-space: nowrap; }
    #ac-root .ac-note { color: #888; font-size: 11px; margin: 10px 0 0; line-height: 1.45; }
    #ac-root .ac-pager { margin-top: 10px; display: flex; gap: 10px; align-items: center; color: #777; font-size: 12px; }
    #ac-root .ac-pager button { font-size: 12px; padding: 2px 9px; cursor: pointer; }
    #ac-root .ac-pager button:disabled { opacity: .4; cursor: default; }
    #ac-tooltip { position: fixed; display: none; z-index: 10; max-width: 460px; background: #fff;
                  border: 1px solid #bbb; border-radius: 6px; box-shadow: 0 3px 14px rgba(0,0,0,.2);
                  padding: 10px 12px; font-size: 12.5px; line-height: 1.45; white-space: normal; }
    #ac-tooltip .mdd-field { margin-bottom: 6px; }
    #ac-tooltip .mdd-field b { display: block; color: #555; margin-bottom: 2px; }
    #ac-tooltip ul.mdd-bullets { margin: 2px 0 2px 16px; padding: 0; }
    #ac-tooltip del.mdd-del { background: #ffe3e3; color: #c92a2a; text-decoration: line-through; }
    #ac-tooltip ins.mdd-ins { background: #d3f9d8; color: #2b8a3e; text-decoration: none; }
"""

_AC_JS = """
    const root = document.getElementById("ac-root");
    const tooltip = document.getElementById("ac-tooltip");
    const items = Array.from(root.querySelectorAll("li.ac-li"));
    const info = document.getElementById("ac-info");
    const prevBtn = document.getElementById("ac-prev");
    const nextBtn = document.getElementById("ac-next");
    let page = 0;
    const pages = Math.max(1, Math.ceil(items.length / PER));
    const fit = () => {
      const fe = window.frameElement;
      if (!fe) return;
      const h = document.documentElement.scrollHeight + 16;
      fe.style.height = h + "px"; fe.setAttribute("height", h);
    };
    const render = () => {
      items.forEach((li, i) => { li.style.display = (Math.floor(i / PER) === page) ? "" : "none"; });
      const start = page * PER + 1, end = Math.min((page + 1) * PER, items.length);
      if (info) info.textContent = start + "\\u2013" + end + " of " + items.length;
      if (prevBtn) prevBtn.disabled = page === 0;
      if (nextBtn) nextBtn.disabled = page >= pages - 1;
      fit();
    };
    if (prevBtn) prevBtn.addEventListener("click", () => { if (page > 0) { page--; render(); } });
    if (nextBtn) nextBtn.addEventListener("click", () => { if (page < pages - 1) { page++; render(); } });
    root.querySelectorAll("a.ac-item").forEach(a => {
      a.addEventListener("mousemove", (ev) => {
        tooltip.innerHTML = PREVIEW;
        tooltip.style.display = "block";
        const pad = 14; let x = ev.clientX + pad, y = ev.clientY + pad;
        const r = tooltip.getBoundingClientRect();
        if (x + r.width > window.innerWidth - 10) x = Math.max(10, ev.clientX - r.width - pad);
        if (y + r.height > window.innerHeight - 10) y = Math.max(10, ev.clientY - r.height - pad);
        tooltip.style.left = x + "px"; tooltip.style.top = y + "px";
      });
      a.addEventListener("mouseleave", () => { tooltip.style.display = "none"; });
    });
    window.addEventListener("load", render);
    render();
"""


def render_affected_charts_html(
    charts: list[dict[str, Any]],
    preview_html: str,
    staging_site: str,
    per_page: int = 10,
) -> tuple[str, int]:
    """Self-contained component: paginated affected-chart links with a hover preview of the change.

    All listed charts use the same indicator, so they show the same metadata change; `preview_html`
    is reused as every chart's hover tooltip. Links open each chart on this staging server, where
    the change is live. Returns (html, initial_height_px).
    """
    no_dp_title = "Multi-indicator chart — no data page, so readers reach this text through Learn more about this data"
    items = []
    for i, c in enumerate(charts):
        slug = c.get("slug")
        label = html.escape(str(c.get("title") or slug or f"chart {c.get('chartId')}"))
        href = f"{staging_site}/grapher/{slug}" if slug else "#"
        flag = (
            ""
            if c.get("has_data_page", True)
            else f' <span class="ac-flag" title="{no_dp_title}">via Learn more</span>'
        )
        items.append(
            f'<li class="ac-li" data-i="{i}"><a class="ac-item" href="{html.escape(href)}" '
            f'target="_blank" rel="noopener">{label}</a>{flag}</li>'
        )

    n = len(charts)
    n_no_dp = sum(1 for c in charts if not c.get("has_data_page", True))
    paged = n > per_page
    header = (
        f'<p class="ac-header">These <b>{n}</b> chart{"s" if n != 1 else ""} also use this indicator, so each '
        "inherits this same change. Hover a chart to preview the change, or click to open its data page on "
        "<b>this staging server</b>.</p>"
    )
    footnote = ""
    if n_no_dp:
        verb, plural = ("is a", "") if n_no_dp == 1 else ("are", "s")
        note = (
            f"<b>{n_no_dp}</b> of these {verb} multi-indicator chart{plural} with no data page, so readers "
            "reach the change through <b>Learn more about this data</b> rather than on the page."
        )
        footnote = f'<p class="ac-note">{note}</p>'
    pager_style = "" if paged else ' style="display:none"'
    pager = (
        f'<div class="ac-pager"{pager_style}>'
        '<button id="ac-prev">&#8249; Prev</button><span id="ac-info"></span>'
        '<button id="ac-next">Next &#8250;</button></div>'
    )
    script = (
        "<script>\n  const PREVIEW = "
        + json.dumps(preview_html)
        + ";\n  const PER = "
        + str(per_page)
        + ";\n"
        + _AC_JS
        + "\n</script>"
    )
    body = (
        '<div id="ac-root"><style>'
        + _AC_CSS
        + "</style>"
        + header
        + '<ol class="ac-list">'
        + "".join(items)
        + "</ol>"
        + pager
        + footnote
        + '<div id="ac-tooltip"></div>'
        + script
        + "</div>"
    )
    height = 130 + min(n, per_page) * 30 + (44 if paged else 0) + (28 if n_no_dp else 0)
    return body, min(height, 680)


def _build_tree(
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
) -> list[dict[str, Any]]:
    """Group views into a nested tree following the dimension order."""

    def choice_name(level: int, slug: str) -> str:
        for choice in dimensions[level].get("choices", []):
            if choice["slug"] == slug:
                return choice.get("name") or slug
        return slug

    def group(indices: list[int], level: int) -> list[dict[str, Any]]:
        dim_slug = dimensions[level]["slug"]
        # Preserve the choice order of the config's dimensions section.
        order = [c["slug"] for c in dimensions[level].get("choices", [])]
        buckets: dict[str, list[int]] = {}
        for i in indices:
            buckets.setdefault(view_diffs[i].dimensions.get(dim_slug, ""), []).append(i)
        sorted_slugs = sorted(buckets, key=lambda s: order.index(s) if s in order else len(order))

        nodes = []
        for slug in sorted_slugs:
            bucket = buckets[slug]
            node: dict[str, Any] = {
                "slug": slug,
                "name": choice_name(level, slug),
                "changed": sum(1 for i in bucket if view_diffs[i].changed),
                "total": len(bucket),
            }
            if level + 1 < len(dimensions):
                node["children"] = group(bucket, level + 1)
            else:
                # A leaf should be a single view; tolerate duplicates defensively.
                node["view_index"] = bucket[0]
            nodes.append(node)
        return nodes

    if not dimensions:
        return []
    return group(list(range(len(view_diffs))), 0)


def _impact_badge(impact: dict[str, int] | None) -> str:
    """Small '↗ affects N charts / M MDIMs' marker for a leaf whose change escapes the MDIM."""
    if not impact:
        return ""
    bits = []
    if impact.get("charts"):
        bits.append(f"{impact['charts']} chart{'s' if impact['charts'] != 1 else ''}")
    if impact.get("mdims"):
        bits.append(f"{impact['mdims']} MDim{'s' if impact['mdims'] != 1 else ''}")
    if not bits:
        return ""
    return f'<span class="mdd-impact" title="This change is in the shared indicator metadata">&#8599; {" · ".join(bits)}</span>'


def _render_node(
    node: dict[str, Any],
    view_diffs: list[ViewDiff],
    leaf_hrefs: list[str],
    leaf_badges: list[str],
    fold: bool = False,
) -> str:
    changed = node["changed"] > 0
    status = "changed" if changed else "unchanged"

    if "view_index" in node:
        i = node["view_index"]
        is_new = view_diffs[i].is_new
        cls = f"mdd-box mdd-leaf mdd-{status}" + (" mdd-newview" if is_new else "")
        badge = '<span class="mdd-badge-new">new</span>' if is_new else ""
        # A real link: srcdoc iframes resolve relative URLs against the parent page, so
        # "?query" points at the Wizard app itself. target=_blank because the component
        # sandbox only allows user-initiated popups, not top-page navigation — a plain
        # click on a _top link is silently blocked.
        return (
            f'<div class="mdd-node mdd-leafnode mdd-n-{status}">'
            f'<a class="{cls}" data-view="{i}" href="{html.escape(leaf_hrefs[i])}" target="_blank" rel="noopener">'
            f'{html.escape(node["name"])}{badge}{leaf_badges[i]}<span class="mdd-golink">&#8599;</span></a>'
            f"</div>"
        )

    counter = f'<span class="mdd-count">{node["changed"]}/{node["total"]}</span>' if changed else ""
    children = "".join(_render_node(child, view_diffs, leaf_hrefs, leaf_badges, fold) for child in node["children"])
    # A folded branch keeps its subtree out of the initial layout — the only way a grid of hundreds of
    # views is something you open rather than something you scroll past.
    folded = " mdd-collapsed" if fold else ""
    return (
        f'<div class="mdd-node mdd-n-{status}{folded}">'
        f'<div class="mdd-box mdd-branch mdd-{status}" role="button" title="Click to collapse/expand">'
        f'<span class="mdd-caret">&#9662;</span>{html.escape(node["name"])}{counter}</div>'
        f'<div class="mdd-children">{children}</div>'
        f"</div>"
    )


def _offset_view_indices(nodes: list[dict[str, Any]], offset: int) -> list[dict[str, Any]]:
    """Shift a section's leaf indices into the combined view list, non-destructively."""
    out = []
    for node in nodes:
        copy = dict(node)
        if "view_index" in copy:
            copy["view_index"] += offset
        if "children" in copy:
            copy["children"] = _offset_view_indices(copy["children"], offset)
        out.append(copy)
    return out


def _render_leaf_branch(branch: dict[str, Any], preview_offset: int) -> tuple[str, list[str]]:
    """(html, previews) for a branch of plain leaves — a root sibling of the dimension tree.

    Used for the surfaces that have no dimension grid of their own: charts, and explorers. `branch` is
    {"id": str, "label": str, "groups": [{"name": str, "note": str, "leaves": [leaf, ...]}]} where each
    leaf is {"label", "href", "preview", "badged"}. Leaves reuse `.mdd-leaf` and `data-view`, indexing
    into the shared previews array from `preview_offset`, so the existing hover and filter code applies
    unchanged. Everything here is marked "changed": a leaf in this list is affected by definition.
    """
    previews: list[str] = []
    group_html: list[str] = []
    total = 0

    for group in branch.get("groups") or []:
        items = group.get("leaves") or []
        if not items:
            continue
        total += len(items)
        leaves = []
        for chart in items:
            index = preview_offset + len(previews)
            previews.append(chart.get("preview") or "")
            # The same marker the grid uses on a view, so a chart the grid already accounts for is
            # recognisable as the same fact rather than a second one.
            mark = '<span class="mdd-impact" title="Also reachable from a view badge in the grid">&#8599;</span>'
            leaves.append(
                f'<div class="mdd-node mdd-leafnode mdd-n-changed">'
                f'<a class="mdd-box mdd-leaf mdd-changed" data-view="{index}" '
                f'href="{html.escape(chart.get("href") or "")}" target="_blank" rel="noopener">'
                f"{html.escape(chart.get('label') or '')}{mark if chart.get('badged') else ''}"
                f'<span class="mdd-golink">&#8599;</span></a>'
                f"</div>"
            )
        title = html.escape(group.get("name") or "")
        # Expanded by default. A flat branch *is* its list — "is my chart in here" is the question it
        # exists to answer, and a closed box answers it only after a click. The grids above stay folded
        # where they are folded because a dimension grid is readable as a shape with its leaves closed;
        # a list of chart names is not. The cost is a taller page, which the jump index and the
        # still-one-click collapse are there for, and which `initial_height` below now accounts for.
        group_html.append(
            f'<div class="mdd-node mdd-n-changed">'
            f'<div class="mdd-box mdd-branch mdd-changed" role="button" title="{html.escape(group.get("note") or "")}">'
            f'<span class="mdd-caret">&#9662;</span>{title}'
            f'<span class="mdd-count">{len(items)}</span></div>'
            f'<div class="mdd-children">{"".join(leaves)}</div>'
            f"</div>"
        )

    if not group_html:
        return "", []

    # A sibling of the "MDims" hierarchy: same header level, its groups indented the same way.
    label = html.escape(branch.get("label") or "Charts")
    body = (
        f'<div class="mdd-super-title">{label}<span class="mdd-count">{total} affected</span></div>'
        f'<div class="mdd-super-children">{"".join(group_html)}</div>'
    )
    return body, previews


def _impact_preview_line(impact: dict[str, int] | None) -> str:
    """A '↗ Also affects …' line appended to a leaf's hover preview."""
    if not impact:
        return ""
    bits = []
    if impact.get("charts"):
        bits.append(f"{impact['charts']} chart{'s' if impact['charts'] != 1 else ''}")
    if impact.get("mdims"):
        bits.append(f"{impact['mdims']} other MDim{'s' if impact['mdims'] != 1 else ''}")
    if not bits:
        return ""
    return f'<p class="mdd-impact-line">&#8599; Shared indicator change — also affects {" and ".join(bits)}.</p>'


def render_tree_html(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    leaf_hrefs: list[str] | None = None,
    external_impacts: list[dict[str, int]] | None = None,
    self_url: str = "",
    branches: list[dict[str, Any]] | None = None,
) -> tuple[str, int]:
    """One MDim's grid. Thin wrapper over `render_multi_tree_html`, which does the work."""
    return render_multi_tree_html(
        [
            {
                "catalog_path": catalog_path,
                "dimensions": dimensions,
                "view_diffs": view_diffs,
                "leaf_hrefs": leaf_hrefs,
                "external_impacts": external_impacts,
            }
        ],
        branches=branches,
        self_url=self_url,
    )


def _column_heads(dims: list[str]) -> str:
    """The title row over a section's columns.

    Only possible because the columns are real: inside an `mdd-cols` section every pill has the same
    fixed width, so depth d starts at d × pitch and a title can sit exactly over its column.
    """
    titles = dims
    spans = "".join(f'<span class="mdd-colhead">{html.escape(t)}</span>' for t in titles)
    return f'<div class="mdd-colheads">{spans}</div>'


def _legend_html(with_external: bool) -> str:
    items = [("#ff922b", "Changed"), ("#d9d9d9", "No change")]
    if with_external:
        items.append(("#9c36b5", "&#8599; Affects charts/other MDims"))
    return "".join(
        f'<span><span class="mdd-dot" style="background:{color}"></span>{label}</span>' for color, label in items
    )


def _render_section(
    node: dict[str, Any],
    anchor: str,
    all_views: list[ViewDiff],
    all_hrefs: list[str],
    all_badges: list[str],
) -> str:
    """One MDim's block: title, catalogPath subtitle, its own filter + legend, column heads, the tree.

    The filter is per section — each checkbox hides only this section's unchanged views, and defaults to
    hidden only where the section has changes (a fully-unchanged MDim with everything hidden would be an
    empty box). There is no root pill: the catalogPath lives in the subtitle, so the tree starts at the
    first dimension and the columns are exactly the dimensions the heads name.
    """
    checked = " checked" if node["changed"] == 0 else ""
    hide = "" if node["changed"] == 0 else " mdd-hide-unchanged"
    counter = f'<span class="mdd-count">{node["changed"]}/{node["total"]} views changed</span>'
    subtitle = f'<div class="mdd-sec-sub">{html.escape(node["subtitle"])}</div>' if node["subtitle"] else ""
    fold = bool(node.get("fold"))
    trees = "".join(_render_node(child, all_views, all_hrefs, all_badges, fold) for child in node["children"])
    folded = " mdd-sec-collapsed" if fold else ""
    return (
        f'<div id="{anchor}" class="mdd-section mdd-cols{hide}{folded}">'
        f'<div class="mdd-sec-title mdd-sec-toggle" role="button" title="Click to collapse/expand">'
        f'<span class="mdd-caret">&#9662;</span>{html.escape(node["title"])}{counter}</div>'
        f"{subtitle}"
        f'<div class="mdd-sec-body">'
        f'<div class="mdd-sec-toolbar">'
        f'<label><input type="checkbox" class="mdd-sec-check"{checked}> Show all views</label>'
        f'<span class="mdd-legend">{_legend_html(node["has_external"])}</span></div>'
        f"{_column_heads(node['dims'])}{trees}</div></div>"
    )


def _mdim_count_label(drawn: int) -> str:
    """How many MDims changed — the MDims section's own phrasing, without a denominator.

    "1 of 78" invited a comparison nobody is making: 78 is every MDim on the site, and a branch touching
    one of them has not covered a seventy-eighth of anything.
    """
    return f"{drawn} MDim{'s' if drawn != 1 else ''} changed by this branch"


def _section_nodes(
    sections: list[dict[str, Any]],
    all_views: list[ViewDiff],
    all_hrefs: list[str],
    all_badges: list[str],
    previews: list[str],
    self_url: str,
) -> list[dict[str, Any]]:
    """Turn one hierarchy's sections into render-ready nodes, appending their views to the shared lists.

    The lists are shared across every hierarchy on the page: leaf indices are offsets into one combined
    view list, which is what lets the hover previews, the badges and each section's "show all views"
    filter work the same whether the page holds one grid or ten.
    """
    nodes_out: list[dict[str, Any]] = []
    for section in sections:
        dimensions = section.get("dimensions") or []
        view_diffs: list[ViewDiff] = section.get("view_diffs") or []
        if not view_diffs:
            continue
        impacts = section.get("external_impacts") or [{} for _ in view_diffs]
        has_external = any((i.get("charts") or i.get("mdims")) for i in impacts)
        hrefs = section.get("leaf_hrefs")
        if hrefs is None:
            hrefs = [
                self_url + "?" + urllib.parse.urlencode({"mdim": section.get("catalog_path", ""), **v.dimensions})
                for v in view_diffs
            ]

        offset = len(all_views)
        nodes = _offset_view_indices(_build_tree(dimensions, view_diffs), offset)
        n_changed = sum(1 for v in view_diffs if v.changed)
        nodes_out.append(
            {
                # The human-readable title leads; the catalogPath is its subtitle, not a tree column.
                "title": str(section.get("title") or section.get("catalog_path") or "MDim"),
                "subtitle": str(section.get("subtitle") or ""),
                "dims": [str(d.get("name") or d["slug"]) for d in dimensions],
                "changed": n_changed,
                "total": len(view_diffs),
                "has_external": has_external,
                "fold": bool(section.get("fold")),
                "children": nodes,
            }
        )
        all_views.extend(view_diffs)
        all_hrefs.extend(hrefs)
        all_badges.extend(_impact_badge(impacts[i]) for i in range(len(view_diffs)))
        previews.extend(diff_preview_html(v) + _impact_preview_line(impacts[i]) for i, v in enumerate(view_diffs))
    return nodes_out


def render_multi_tree_html(
    sections: list[dict[str, Any]],
    branches: list[dict[str, Any]] | None = None,
    self_url: str = "",
    hierarchies: list[dict[str, Any]] | None = None,
) -> tuple[str, int]:
    """Render every affected surface as a root branch of one tree.

    Two kinds of root. A **hierarchy** is a group of dimension grids under one heading: the MDims, and the
    explorers, whose views are addressed by dimensions in the same way. A **branch** is a flat list of
    linked leaves, for a surface with no dimensions of its own — the charts. `hierarchies` names the groups
    (`{id, label, count_label, sections}`); `sections` is the shorthand for one group of MDims, which is
    what a single MDim's own page renders.

    One component, not one per grid: each component sizes its own iframe and would overlap whatever follows
    it, so stacking them is not available. A section is
    `{catalog_path, dimensions, view_diffs, leaf_hrefs, external_impacts, fold}`.

    Returns (html, initial_height_px). The component resizes itself to its content afterwards.
    """
    all_views: list[ViewDiff] = []
    all_hrefs: list[str] = []
    all_badges: list[str] = []
    previews: list[str] = []

    groups = list(hierarchies) if hierarchies else [{"id": "mdims", "label": "MDims", "sections": sections}]
    for group in groups:
        group["nodes"] = _section_nodes(
            group.get("sections") or [], all_views, all_hrefs, all_badges, previews, self_url
        )
    section_nodes = [node for group in groups for node in group["nodes"]]
    total_changed = sum(1 for v in all_views if v.changed)

    # (anchor id, label, count, group label) — a row with a group label is indented under that heading.
    index_entries: list[tuple[str, str, str, str]] = []
    body = ""
    drawn_groups = 0
    for group in groups:
        nodes = group["nodes"]
        if not nodes:
            continue
        parts = []
        for node in nodes:
            anchor = f"mdd-section-{len(index_entries)}"
            index_entries.append(
                (anchor, node["title"], f"{node['changed']}/{node['total']} views changed", str(group["label"]))
            )
            parts.append(_render_section(node, anchor, all_views, all_hrefs, all_badges))
        # A heading over the group's grids. It counts surfaces, not views: every section header and index
        # row below already carries its own view count, and two denominators side by side read as a
        # discrepancy.
        count_label = group.get("count_label")
        if count_label is None and group["id"] == "mdims":
            count_label = _mdim_count_label(len(nodes))
        head = (
            f'<div class="mdd-super-title">{html.escape(str(group["label"]))}'
            f'<span class="mdd-count">{html.escape(str(count_label or ""))}</span></div>'
        )
        body += f'<div class="mdd-super">{head}<div class="mdd-super-children">{"".join(parts)}</div></div>'
        drawn_groups += 1

    # Each flat surface that has anything in it becomes a branch beside the MDims, in the order given.
    # An empty one is not drawn at all: a "Explorers — 0 affected" heading is a heading about nothing.
    drawn_branches = 0
    branch_leaves = 0
    for branch in branches or []:
        branch_html, branch_previews = _render_leaf_branch(branch, len(previews))
        if branch_html:
            anchor = f"mdd-section-{branch.get('id') or 'charts'}"
            n_leaves = sum(len(g.get("leaves") or []) for g in branch.get("groups") or [])
            index_entries.append((anchor, str(branch.get("label") or "Charts"), f"{n_leaves} affected", False))
            body += f'<div id="{anchor}" class="mdd-section">{branch_html}</div>'
            drawn_branches += 1
            branch_leaves += n_leaves
        previews = previews + branch_previews

    # The index only earns its place when there is more than one place to jump to. A panel of rows, one
    # per section, rather than an inline "a · b · c" line that vanished into the toolbar text around it.
    index_html = ""
    if len(index_entries) > 1:
        rows = []
        current_group = None
        for anchor, label, count, group_label in index_entries:
            if group_label and group_label != current_group:
                rows.append(f'<div class="mdd-index-group">{html.escape(group_label)}</div>')
            current_group = group_label
            indent = " mdd-index-indent" if group_label else ""
            # The bullet lives in the label span, not a ::before: the row is a two-item flex with the
            # count pushed right, and a third flex item would strand the label in the middle.
            bullet = '<span class="mdd-index-bullet">•</span> ' if group_label else ""
            rows.append(
                f'<a class="mdd-index-link{indent}" href="#" data-target="{anchor}">'
                f"<span>{bullet}{html.escape(label)}</span>"
                f'<span class="mdd-index-count">{html.escape(count)}</span></a>'
            )
        index_html = f'<div class="mdd-index"><div class="mdd-index-title">Jump to a section</div>{"".join(rows)}</div>'

    visible_leaves = total_changed if total_changed else len(all_views)
    # One row per section header, one per hierarchy heading, one per flat-branch heading — plus every leaf
    # a flat branch draws, since those render expanded and are in the initial layout. The frame does not
    # scroll, so an under-estimate shows a cut-off tree until `fit()` runs; the cap keeps the overshoot
    # bounded on a branch reaching hundreds of charts.
    extra_rows = len(section_nodes) + drawn_groups + drawn_branches + branch_leaves
    initial_height = min(INITIAL_HEIGHT_CAP_PX, 170 + (visible_leaves + extra_rows) * 38)
    return (
        f"""
<div id="mdd-root">
  <style>
    #mdd-root {{ font-family: -apple-system, system-ui, sans-serif; font-size: 13px; color: #333; }}
    #mdd-root .mdd-toolbar {{ margin-bottom: 10px; display: flex; flex-direction: column; gap: 6px; }}
    #mdd-root .mdd-row-top {{ display: flex; gap: 18px; align-items: center; flex-wrap: wrap; }}
    #mdd-root .mdd-summary {{ color: #333; }}
    #mdd-root .mdd-dims {{ color: #777; }}
    #mdd-root .mdd-hint {{ color: #999; font-size: 12px; margin-bottom: 8px; }}
    #mdd-root .mdd-legend span {{ margin-right: 12px; }}
    #mdd-root .mdd-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 5px; margin-right: 4px; }}
    #mdd-root .mdd-index {{ border: 1px solid #dee2e6; border-radius: 8px; padding: 8px 12px;
      margin: 4px 0 2px; max-width: 560px; background: #fafafa; }}
    #mdd-root .mdd-index-title {{ font-weight: 600; color: #333; margin-bottom: 4px; }}
    #mdd-root .mdd-index-count {{ color: #868e96; font-size: 12px; }}
    /* Inside an MDim section every pill takes the same width, so each depth is a true column and the
       title row above can point at it. Pitch = 190 (border-box pill) + 26 (children indent). */
    #mdd-root .mdd-cols .mdd-box {{ width: 190px; box-sizing: border-box; white-space: normal; }}
    #mdd-root .mdd-super-title {{ font-size: 17px; font-weight: 800; color: #111; margin: 12px 0 2px; }}
    #mdd-root .mdd-super-title .mdd-count {{ font-weight: 400; font-size: 12px; }}
    #mdd-root .mdd-super-children {{ margin-left: 12px; border-left: 3px solid #ececec; padding-left: 14px; }}
    #mdd-root .mdd-index-group {{ color: #333; font-weight: 600; padding: 4px 0 1px; }}
    #mdd-root .mdd-index-indent {{ padding-left: 20px; }}
    #mdd-root .mdd-index-bullet {{ color: #adb5bd; }}
    #mdd-root .mdd-sec-title {{ font-size: 15px; font-weight: 700; color: #222; }}
    #mdd-root .mdd-sec-toggle {{ cursor: pointer; user-select: none; }}
    #mdd-root .mdd-sec-collapsed > .mdd-sec-title .mdd-caret {{ transform: rotate(-90deg); }}
    #mdd-root .mdd-sec-collapsed > .mdd-sec-body {{ display: none; }}
    #mdd-root .mdd-sec-title .mdd-count {{ font-weight: 400; font-size: 12px; }}
    #mdd-root .mdd-sec-sub {{ color: #868e96; font-size: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin-top: 1px; }}
    #mdd-root .mdd-sec-toolbar {{ display: flex; gap: 18px; align-items: center; color: #555;
      margin: 6px 0 2px; flex-wrap: wrap; }}
    #mdd-root .mdd-colheads {{ display: flex; margin: 8px 0 4px; }}
    #mdd-root .mdd-colhead {{ width: 216px; flex-shrink: 0; color: #868e96; font-size: 11px;
      font-weight: 600; text-transform: uppercase; letter-spacing: .04em; }}
    #mdd-root .mdd-section {{ margin: 4px 0 16px; padding-top: 14px; border-top: 1px solid #e3e3e3; }}
    #mdd-root .mdd-section:first-child {{ border-top: none; padding-top: 0; margin-top: 0; }}
    #mdd-root .mdd-index-link {{ color: #1c7ed6; text-decoration: none; display: flex;
      justify-content: space-between; gap: 16px; padding: 3px 0; }}
    #mdd-root .mdd-index-link:hover span:first-child {{ text-decoration: underline; }}
    #mdd-root .mdd-tree {{ overflow-x: auto; overflow-y: visible; padding: 4px; }}
    #mdd-root .mdd-node {{ display: flex; align-items: flex-start; margin: 2px 0; }}
    #mdd-root .mdd-children {{ display: flex; flex-direction: column; border-left: 2px solid #e3e3e3;
                               margin-left: 10px; padding-left: 14px; }}
    #mdd-root .mdd-box {{ border: 1.5px solid; border-radius: 6px; padding: 4px 10px; margin: 2px 0;
                          white-space: nowrap; cursor: pointer; text-decoration: none; color: inherit;
                          background: #fff; flex-shrink: 0; display: inline-block; }}
    #mdd-root .mdd-unchanged {{ border-color: #d9d9d9; color: #999; background: #fafafa; }}
    #mdd-root .mdd-changed {{ border-color: #e8590c; border-width: 2px; background: #fff4e6; font-weight: 600; }}
    #mdd-root .mdd-newview {{ border-color: #1971c2; background: #e7f5ff; }}
    #mdd-root .mdd-count {{ margin-left: 7px; font-weight: 400; font-size: 11px; color: #e8590c;
                            background: #ffe8cc; border-radius: 8px; padding: 1px 6px; }}
    #mdd-root .mdd-badge-new {{ margin-left: 7px; font-size: 10px; color: #1971c2; background: #d0ebff;
                                border-radius: 8px; padding: 1px 6px; }}
    #mdd-root .mdd-impact {{ margin-left: 7px; font-size: 10px; color: #9c36b5; background: #f3d9fa;
                             border-radius: 8px; padding: 1px 6px; font-weight: 600; }}
    #mdd-tooltip .mdd-impact-line {{ margin: 6px 0 0; color: #9c36b5; font-weight: 600; }}
    #mdd-root .mdd-caret {{ display: inline-block; margin-right: 6px; transition: transform .15s; }}
    #mdd-root .mdd-golink {{ margin-left: 7px; color: #1971c2; font-size: 12px; }}
    #mdd-root .mdd-leaf {{ text-decoration: none; }}
    #mdd-root .mdd-leaf:hover {{ box-shadow: 0 1px 5px rgba(0,0,0,.25); text-decoration: underline; }}
    #mdd-root .mdd-collapsed > .mdd-box .mdd-caret {{ transform: rotate(-90deg); }}
    #mdd-root .mdd-collapsed > .mdd-children {{ display: none; }}
    #mdd-root .mdd-hide-unchanged .mdd-n-unchanged {{ display: none; }}
    #mdd-tooltip {{ position: fixed; display: none; z-index: 10; max-width: 460px; background: #fff;
                    border: 1px solid #bbb; border-radius: 6px; box-shadow: 0 3px 14px rgba(0,0,0,.2);
                    padding: 10px 12px; font-size: 12.5px; line-height: 1.45; white-space: normal; }}
    #mdd-tooltip .mdd-field {{ margin-bottom: 6px; }}
    #mdd-tooltip .mdd-field b {{ display: block; color: #555; margin-bottom: 2px; }}
    #mdd-tooltip ul.mdd-bullets {{ margin: 2px 0 2px 16px; padding: 0; }}
    #mdd-root del.mdd-del, #mdd-tooltip del.mdd-del {{ background: #ffe3e3; color: #c92a2a; text-decoration: line-through; }}
    #mdd-root ins.mdd-ins, #mdd-tooltip ins.mdd-ins {{ background: #d3f9d8; color: #2b8a3e; text-decoration: none; }}
  </style>
  <div class="mdd-toolbar">
    {index_html}
  </div>
  <div class="mdd-hint">Hover over a view to preview its changes; click it to open the View diff in a
    new tab. Click a branch to collapse/expand it.</div>
  <div class="mdd-tree">{body}</div>
  <div id="mdd-tooltip"></div>
  <script>
    const PREVIEWS = {json.dumps(previews)};
    const root = document.getElementById("mdd-root");
    const tooltip = document.getElementById("mdd-tooltip");
    const treeEl = root.querySelector(".mdd-tree");

    // Resize the component's iframe to fit the content (Streamlit renders this in a
    // same-origin iframe, so we can set our own frame height). Beyond MAX_HEIGHT the
    // tree scrolls internally.
    const fit = () => {{
      const fe = window.frameElement;
      if (!fe) return;
      // The frame always grows to its content, so the page is the only vertical scrollbar — a capped
      // frame put a second scrollbar inside the tree, nested in the page's. Set both the style and the
      // height attribute so Streamlit's layout pushes the content below the component down.
      const wanted = document.documentElement.scrollHeight + 24;
      fe.style.height = wanted + "px";
      fe.setAttribute("height", wanted);
    }};

    // Each section filters itself: its checkbox hides only its own unchanged views.
    root.querySelectorAll(".mdd-sec-check").forEach(cb => {{
      const section = cb.closest(".mdd-section");
      cb.addEventListener("change", () => {{
        section.classList.toggle("mdd-hide-unchanged", !cb.checked);
        fit();
      }});
    }});

    root.querySelectorAll(".mdd-index-link").forEach(link => {{
      link.addEventListener("click", (ev) => {{
        ev.preventDefault();
        const target = document.getElementById(link.dataset.target);
        if (!target) return;
        // Jumping to a section whose top-level groups are collapsed would land on closed boxes.
        // A folded section, or a section whose top-level groups are folded, would be nothing to land on.
        target.classList.remove("mdd-sec-collapsed");
        target.querySelectorAll(
          ":scope > .mdd-node.mdd-collapsed, :scope > .mdd-super-children > .mdd-node.mdd-collapsed"
        ).forEach(n => n.classList.remove("mdd-collapsed"));
        fit();
        // This frame is sized to its content, so the *page* scrolls, not this document —
        // scrollIntoView here moves nothing. Same-origin, so scroll the parent's main
        // container to the target's position.
        try {{
          const fe = window.frameElement;
          const scroller = window.parent.document.querySelector("section.stMain")
            || window.parent.document.scrollingElement;
          const HEADER = 130;  // the app header plus the sticky section bar
          const top = fe.getBoundingClientRect().top + target.getBoundingClientRect().top;
          scroller.scrollBy({{top: top - HEADER, behavior: "smooth"}});
        }} catch (err) {{
          target.scrollIntoView({{behavior: "smooth", block: "start"}});
        }}
      }});
    }});

    root.querySelectorAll(".mdd-sec-toggle").forEach(title => {{
      title.addEventListener("click", () => {{
        title.closest(".mdd-section").classList.toggle("mdd-sec-collapsed");
        fit();
      }});
    }});

    root.querySelectorAll(".mdd-branch").forEach(box => {{
      box.addEventListener("click", () => {{
        box.parentElement.classList.toggle("mdd-collapsed");
        fit();
      }});
    }});

    root.querySelectorAll(".mdd-leaf").forEach(leaf => {{
      const i = parseInt(leaf.dataset.view);
      leaf.addEventListener("mousemove", (ev) => {{
        tooltip.innerHTML = PREVIEWS[i];
        tooltip.style.display = "block";
        const pad = 14;
        let x = ev.clientX + pad, y = ev.clientY + pad;
        const r = tooltip.getBoundingClientRect();
        if (x + r.width > window.innerWidth - 10) x = Math.max(10, ev.clientX - r.width - pad);
        if (y + r.height > window.innerHeight - 10) y = Math.max(10, ev.clientY - r.height - pad);
        tooltip.style.left = x + "px";
        tooltip.style.top = y + "px";
      }});
      leaf.addEventListener("mouseleave", () => {{ tooltip.style.display = "none"; }});
    }});

    window.addEventListener("load", fit);
    fit();
  </script>
</div>
""",
        initial_height,
    )
