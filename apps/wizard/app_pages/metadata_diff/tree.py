"""Blast Radius tree: a self-contained interactive HTML component.

Renders the views of an MDIM as a horizontal tree following the order of the MDIM's
dimensions (first control = first fork). Nodes are colored by whether any view
underneath them changed; leaves are real links to the View diff page and show a hover
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
MAX_HEIGHT_PX = 4000


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
    chart_diff_url: str,
    per_page: int = 10,
) -> tuple[str, int]:
    """Self-contained component: paginated affected-chart links with a hover preview of the change.

    All listed charts use the same indicator, so they show the same metadata change; `preview_html`
    is reused as every chart's hover tooltip. Links open each chart on this staging server, where
    the change is live. Returns (html, initial_height_px).
    """
    no_dp_title = "Multi-indicator chart — no data page, so this text is not shown to readers here"
    items = []
    for i, c in enumerate(charts):
        slug = c.get("slug")
        label = html.escape(str(c.get("title") or slug or f"chart {c.get('chartId')}"))
        href = f"{staging_site}/grapher/{slug}" if slug else "#"
        flag = (
            ""
            if c.get("wysk_shown", True)
            else f' <span class="ac-flag" title="{no_dp_title}">&#9888; no data page</span>'
        )
        items.append(
            f'<li class="ac-li" data-i="{i}"><a class="ac-item" href="{html.escape(href)}" '
            f'target="_blank" rel="noopener">{label}</a>{flag}</li>'
        )

    n = len(charts)
    n_no_dp = sum(1 for c in charts if not c.get("wysk_shown", True))
    paged = n > per_page
    header = (
        f'<p class="ac-header">These <b>{n}</b> chart{"s" if n != 1 else ""} also use this indicator, so each '
        "inherits this same change. Hover a chart to preview it, or click to open it on <b>this staging "
        "server</b>. &nbsp;"
        f'<a href="{html.escape(chart_diff_url)}" target="_blank" rel="noopener">Open all in Chart Diff &#8599;</a></p>'
    )
    footnote = ""
    if n_no_dp:
        verb, plural = ("is a", "") if n_no_dp == 1 else ("are", "s")
        note = (
            f"<b>&#9888; {n_no_dp}</b> of these {verb} multi-indicator chart{plural} with no data page, so "
            "the change is <b>not shown to readers</b> there."
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
    children = "".join(_render_node(child, view_diffs, leaf_hrefs, leaf_badges) for child in node["children"])
    return (
        f'<div class="mdd-node mdd-n-{status}">'
        f'<div class="mdd-box mdd-branch mdd-{status}" role="button" title="Click to collapse/expand">'
        f'<span class="mdd-caret">&#9662;</span>{html.escape(node["name"])}{counter}</div>'
        f'<div class="mdd-children">{children}</div>'
        f"</div>"
    )


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
    dim_param_prefix: str = "d_",
    external_impacts: list[dict[str, int]] | None = None,
) -> tuple[str, int]:
    """Render the Blast Radius component. Returns (html, initial_height_px).

    The component resizes its own iframe to fit its content (collapse, filter), so the
    returned height is only the initial estimate.

    `external_impacts` (per view index) carries the count of charts / other MDIMs that share the
    view's changed indicator, surfaced as a leaf marker and a preview line.
    """
    tree = _build_tree(dimensions, view_diffs)
    n_changed = sum(1 for v in view_diffs if v.changed)
    impacts = external_impacts or [{} for _ in view_diffs]

    # Legend: only show states that actually occur here (e.g. drop "New view" when nothing is new).
    legend_items = [("#e8590c", "Changed")]
    if any(v.is_new for v in view_diffs):
        legend_items.append(("#1971c2", "New view"))
    legend_items.append(("#d9d9d9", "No change"))
    if any((imp.get("charts") or imp.get("mdims")) for imp in impacts):
        legend_items.append(("#9c36b5", "&#8599; Affects charts/other MDims"))
    legend_html = "".join(
        f'<span><span class="mdd-dot" style="background:{color}"></span>{label}</span>' for color, label in legend_items
    )

    previews = [diff_preview_html(v) + _impact_preview_line(impacts[i]) for i, v in enumerate(view_diffs)]
    leaf_badges = [_impact_badge(impacts[i]) for i in range(len(view_diffs))]
    leaf_hrefs = [
        "?"
        + urllib.parse.urlencode(
            {"mdim": catalog_path, "mode": "view", **{(dim_param_prefix + s): c for s, c in v.dimensions.items()}}
        )
        for v in view_diffs
    ]

    dim_names = " &#8594; ".join(html.escape(d.get("name") or d["slug"]) for d in dimensions)
    body = "".join(_render_node(node, view_diffs, leaf_hrefs, leaf_badges) for node in tree)

    show_unchanged_default = "false" if n_changed else "true"
    visible_leaves = n_changed if n_changed else len(view_diffs)
    initial_height = min(MAX_HEIGHT_PX, 170 + visible_leaves * 38)

    return (
        f"""
<div id="mdd-root" class="mdd-hide-unchanged">
  <style>
    #mdd-root {{ font-family: -apple-system, system-ui, sans-serif; font-size: 13px; color: #333; }}
    #mdd-root .mdd-toolbar {{ margin-bottom: 10px; display: flex; flex-direction: column; gap: 6px; }}
    #mdd-root .mdd-row-top {{ display: flex; gap: 18px; align-items: center; flex-wrap: wrap; }}
    #mdd-root .mdd-summary {{ color: #333; }}
    #mdd-root .mdd-dims {{ color: #777; }}
    #mdd-root .mdd-hint {{ color: #999; font-size: 12px; margin-bottom: 8px; }}
    #mdd-root .mdd-legend span {{ margin-right: 12px; }}
    #mdd-root .mdd-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 5px; margin-right: 4px; }}
    #mdd-root .mdd-tree {{ overflow: auto; padding: 4px; }}
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
    #mdd-root.mdd-hide-unchanged .mdd-n-unchanged {{ display: none; }}
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
    <div class="mdd-row-top">
      <label><input type="checkbox" id="mdd-show-unchanged"> Show all views</label>
      <span class="mdd-legend">{legend_html}</span>
    </div>
    <div class="mdd-dims">Controls: {dim_names}</div>
    <div class="mdd-summary"><b>{n_changed}</b> of {len(view_diffs)} views changed</div>
  </div>
  <div class="mdd-hint">Hover over a view to preview its changes; click it to open the View diff in a
    new tab. Click a branch to collapse/expand it.</div>
  <div class="mdd-tree">{body}</div>
  <div id="mdd-tooltip"></div>
  <script>
    const PREVIEWS = {json.dumps(previews)};
    const MAX_HEIGHT = {MAX_HEIGHT_PX};
    const root = document.getElementById("mdd-root");
    const tooltip = document.getElementById("mdd-tooltip");
    const treeEl = root.querySelector(".mdd-tree");

    // Resize the component's iframe to fit the content (Streamlit renders this in a
    // same-origin iframe, so we can set our own frame height). Beyond MAX_HEIGHT the
    // tree scrolls internally.
    const fit = () => {{
      const fe = window.frameElement;
      if (!fe) return;
      treeEl.style.maxHeight = "none";
      const wanted = document.documentElement.scrollHeight + 24;
      const h = Math.min(wanted, MAX_HEIGHT);
      if (wanted > MAX_HEIGHT) treeEl.style.maxHeight = (MAX_HEIGHT - 100) + "px";
      // Set both the style and the height attribute so Streamlit's layout pushes the
      // content below the component down instead of letting it overlap the tree.
      fe.style.height = h + "px";
      fe.setAttribute("height", h);
    }};

    const checkbox = document.getElementById("mdd-show-unchanged");
    checkbox.checked = {show_unchanged_default};
    const applyFilter = () => {{
      root.classList.toggle("mdd-hide-unchanged", !checkbox.checked);
      fit();
    }};
    checkbox.addEventListener("change", applyFilter);
    applyFilter();

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
