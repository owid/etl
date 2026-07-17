"""Blast Radius tree: a self-contained interactive HTML component.

Renders the views of an MDIM as a horizontal tree following the order of the MDIM's
dimensions (first control = first fork). Nodes are colored by whether any view
underneath them changed; leaves link to the View diff page and show a hover preview
of the change. All interactivity (collapse, filter, tooltips) is inline JS with no
external dependencies, so the component is fully deterministic.
"""

import html
import json
from typing import Any

from apps.wizard.app_pages.metadata_diff.core import ViewDiff, diff_preview_html

# Layout constants used to estimate the component height.
LEAF_HEIGHT_PX = 34
MAX_HEIGHT_PX = 1000


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


def _render_node(node: dict[str, Any], view_diffs: list[ViewDiff]) -> str:
    changed = node["changed"] > 0
    status = "changed" if changed else "unchanged"

    if "view_index" in node:
        i = node["view_index"]
        is_new = view_diffs[i].is_new
        cls = f"mdd-box mdd-leaf mdd-{status}" + (" mdd-newview" if is_new else "")
        badge = '<span class="mdd-badge-new">new</span>' if is_new else ""
        return (
            f'<div class="mdd-node mdd-leafnode mdd-n-{status}">'
            f'<a class="{cls}" data-view="{i}" href="javascript:void(0)">{html.escape(node["name"])}{badge}</a>'
            f"</div>"
        )

    counter = f'<span class="mdd-count">{node["changed"]}/{node["total"]}</span>' if changed else ""
    children = "".join(_render_node(child, view_diffs) for child in node["children"])
    return (
        f'<div class="mdd-node mdd-n-{status}">'
        f'<div class="mdd-box mdd-branch mdd-{status}" role="button" title="Click to collapse/expand">'
        f'<span class="mdd-caret">&#9662;</span>{html.escape(node["name"])}{counter}</div>'
        f'<div class="mdd-children">{children}</div>'
        f"</div>"
    )


def render_tree_html(
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    dim_param_prefix: str = "d_",
) -> tuple[str, int]:
    """Render the Blast Radius component. Returns (html, suggested_height_px)."""
    tree = _build_tree(dimensions, view_diffs)
    n_changed = sum(1 for v in view_diffs if v.changed)

    previews = [diff_preview_html(v) for v in view_diffs]
    view_params = [
        {(dim_param_prefix + slug): choice for slug, choice in v.dimensions.items()} for v in view_diffs
    ]

    dim_names = " &#8594; ".join(html.escape(d.get("name") or d["slug"]) for d in dimensions)
    body = "".join(_render_node(node, view_diffs) for node in tree)

    show_unchanged_default = "false" if n_changed else "true"
    visible_leaves = n_changed if n_changed else len(view_diffs)
    height = min(MAX_HEIGHT_PX, 170 + visible_leaves * LEAF_HEIGHT_PX)

    return (
        f"""
<div id="mdd-root" class="mdd-hide-unchanged">
  <style>
    #mdd-root {{ font-family: -apple-system, system-ui, sans-serif; font-size: 13px; color: #333; }}
    #mdd-root .mdd-toolbar {{ margin-bottom: 10px; display: flex; gap: 18px; align-items: center; flex-wrap: wrap; }}
    #mdd-root .mdd-dims {{ color: #777; }}
    #mdd-root .mdd-legend span {{ margin-right: 12px; }}
    #mdd-root .mdd-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 5px; margin-right: 4px; }}
    #mdd-root .mdd-tree {{ overflow: auto; max-height: {MAX_HEIGHT_PX - 120}px; padding: 4px; }}
    #mdd-root .mdd-node {{ display: flex; align-items: flex-start; margin: 2px 0; }}
    #mdd-root .mdd-children {{ display: flex; flex-direction: column; border-left: 2px solid #e3e3e3;
                               margin-left: 10px; padding-left: 14px; }}
    #mdd-root .mdd-box {{ border: 1.5px solid; border-radius: 6px; padding: 4px 10px; margin: 2px 0;
                          white-space: nowrap; cursor: pointer; text-decoration: none; color: inherit;
                          background: #fff; flex-shrink: 0; }}
    #mdd-root .mdd-unchanged {{ border-color: #d9d9d9; color: #999; background: #fafafa; }}
    #mdd-root .mdd-changed {{ border-color: #e8590c; border-width: 2px; background: #fff4e6; font-weight: 600; }}
    #mdd-root .mdd-newview {{ border-color: #1971c2; background: #e7f5ff; }}
    #mdd-root .mdd-count {{ margin-left: 7px; font-weight: 400; font-size: 11px; color: #e8590c;
                            background: #ffe8cc; border-radius: 8px; padding: 1px 6px; }}
    #mdd-root .mdd-badge-new {{ margin-left: 7px; font-size: 10px; color: #1971c2; background: #d0ebff;
                                border-radius: 8px; padding: 1px 6px; }}
    #mdd-root .mdd-caret {{ display: inline-block; margin-right: 6px; transition: transform .15s; }}
    #mdd-root .mdd-collapsed > .mdd-box .mdd-caret {{ transform: rotate(-90deg); }}
    #mdd-root .mdd-collapsed > .mdd-children {{ display: none; }}
    #mdd-root.mdd-hide-unchanged .mdd-n-unchanged {{ display: none; }}
    #mdd-root .mdd-leaf:hover {{ box-shadow: 0 1px 5px rgba(0,0,0,.25); }}
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
    <label><input type="checkbox" id="mdd-show-unchanged"> Show unchanged views</label>
    <span class="mdd-legend">
      <span><span class="mdd-dot" style="background:#e8590c"></span>Changed</span>
      <span><span class="mdd-dot" style="background:#1971c2"></span>New view</span>
      <span><span class="mdd-dot" style="background:#d9d9d9"></span>No change</span>
    </span>
    <span class="mdd-dims">Controls: {dim_names}</span>
    <span><b>{n_changed}</b> of {len(view_diffs)} views changed</span>
  </div>
  <div class="mdd-tree">{body}</div>
  <div id="mdd-tooltip"></div>
  <script>
    const PREVIEWS = {json.dumps(previews)};
    const VIEW_PARAMS = {json.dumps(view_params)};
    const DIM_PREFIX = {json.dumps(dim_param_prefix)};
    const root = document.getElementById("mdd-root");
    const tooltip = document.getElementById("mdd-tooltip");

    const checkbox = document.getElementById("mdd-show-unchanged");
    checkbox.checked = {show_unchanged_default};
    const applyFilter = () => root.classList.toggle("mdd-hide-unchanged", !checkbox.checked);
    checkbox.addEventListener("change", applyFilter);
    applyFilter();

    root.querySelectorAll(".mdd-branch").forEach(box => {{
      box.addEventListener("click", () => box.parentElement.classList.toggle("mdd-collapsed"));
    }});

    root.querySelectorAll(".mdd-leaf").forEach(leaf => {{
      const i = parseInt(leaf.dataset.view);
      leaf.addEventListener("click", () => {{
        try {{
          const url = new URL(window.parent.location.href);
          // Drop the previous view selection, keep everything else (mdim, filters).
          [...url.searchParams.keys()].filter(k => k.startsWith(DIM_PREFIX)).forEach(k => url.searchParams.delete(k));
          Object.entries(VIEW_PARAMS[i]).forEach(([k, v]) => url.searchParams.set(k, v));
          url.searchParams.set("mode", "view");
          window.parent.location.href = url.toString();
        }} catch (e) {{ console.error(e); }}
      }});
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
  </script>
</div>
""",
        height,
    )
