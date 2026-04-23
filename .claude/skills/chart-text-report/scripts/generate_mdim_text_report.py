"""Generate Markdown reports of user-facing chart text for OWID MDim collections.

For each view in the MDim's `.config.json`, resolves Title / Subtitle / Footnote /
description_short / description_key, tagging each field as [override], [inherited],
or [missing]. Supports per-MDim dimension collapsing with placeholder parametrization.

Usage (edit the MDIMS list below or invoke with a JSON config):

    # Use the built-in MDIMS list:
    .venv/bin/python .claude/skills/chart-text-report/scripts/generate_mdim_text_report.py

    # Or pass a JSON file with one object per MDim:
    .venv/bin/python .claude/skills/chart-text-report/scripts/generate_mdim_text_report.py --config ai/mdim_list.json

JSON config shape (list of objects):
    [
      {
        "name": "incomes_pip",
        "config_path": "export/multidim/wb/latest/incomes_pip/incomes_pip.config.json",
        "collapse_dims": ["period"]
      },
      ...
    ]
Paths are resolved relative to etl.paths.BASE_DIR.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from etl.paths import BASE_DIR

# Allow `python <path-to-script>` to import _common from the same folder.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from urllib.parse import quote  # noqa: E402

from _common import (  # noqa: E402
    ADMIN_BASE,
    BulletLibrary,
    collect_used_tags,
    get_indicator_meta,
    how_to_read_block,
    inherited_note,
    inherited_subtitle,
    inherited_title,
    md_escape,
    preview_url,
    render_value,
    resolve_field,
)

# Default list — edit to your needs, or pass --config <json>.
MDIMS: list[dict[str, Any]] = [
    {
        "name": "incomes_pip",
        "config_path": BASE_DIR / "export/multidim/wb/latest/incomes_pip/incomes_pip.config.json",
        "collapse_dims": ["period"],
    },
    {
        "name": "gini_pip",
        "config_path": BASE_DIR / "export/multidim/wb/latest/gini_pip/gini_pip.config.json",
        "collapse_dims": [],
    },
    {
        "name": "gini_lis",
        "config_path": BASE_DIR / "export/multidim/lis/latest/gini_lis/gini_lis.config.json",
        "collapse_dims": [],
    },
    {
        "name": "incomes_wid",
        "config_path": BASE_DIR / "export/multidim/wid/latest/incomes_wid/incomes_wid.config.json",
        "collapse_dims": [],
    },
]

OUT_DIR = BASE_DIR / "ai"


# ---------------------------------------------------------------------------
# MDim-specific logic
# ---------------------------------------------------------------------------


def primary_catalog_path(view: dict[str, Any]) -> str | None:
    indicators = view.get("indicators", {})
    y = indicators.get("y") or []
    if not y:
        return None
    first = y[0]
    if isinstance(first, str):
        return first
    return first.get("catalogPath")


def format_view_header(view: dict[str, Any]) -> str:
    dims = view.get("dimensions", {})
    parts = [f"{k}={v}" for k, v in dims.items()]
    return ", ".join(parts) if parts else "(no dimensions)"


def build_dim_lookup(cfg: dict[str, Any]) -> dict[str, tuple[str, dict[str, str]]]:
    """Map dim slug → (human name, {choice slug → choice name})."""
    lookup: dict[str, tuple[str, dict[str, str]]] = {}
    for dim in cfg.get("dimensions", []) or []:
        slug = dim.get("slug")
        if not slug:
            continue
        name = dim.get("name", slug)
        choice_map = {
            c.get("slug"): c.get("name", c.get("slug")) for c in (dim.get("choices", []) or []) if c.get("slug")
        }
        lookup[slug] = (name, choice_map)
    return lookup


def human_selections(
    variants: list[dict[str, Any]],
    collapse_dims: list[str],
    dim_lookup: dict[str, tuple[str, dict[str, str]]],
) -> str:
    """Single-line summary of dim selections using dimension/choice human names."""
    parts: list[str] = []
    if not variants:
        return ""
    dims_order = list((variants[0].get("dimensions") or {}).keys())
    for slug in dims_order:
        if slug not in dim_lookup:
            continue
        dim_name, choice_map = dim_lookup[slug]
        if slug in collapse_dims:
            seen: list[str] = []
            for v in variants:
                dv = (v.get("dimensions") or {}).get(slug)
                if dv and dv != "nan" and dv not in seen:
                    seen.append(dv)
            if not seen:
                continue
            rendered = [choice_map.get(s, s) for s in seen]
            parts.append(f"**{dim_name}:** {', '.join(rendered)}")
        else:
            dv = (variants[0].get("dimensions") or {}).get(slug)
            if dv is None or dv == "nan":
                continue
            parts.append(f"**{dim_name}:** {choice_map.get(dv, dv)}")
    return " · ".join(parts)


def resolve_view_fields(view: dict[str, Any]) -> dict[str, tuple[str, Any]]:
    cp = primary_catalog_path(view)
    meta = get_indicator_meta(cp) if cp else None
    vc = view.get("config") or {}
    vm = view.get("metadata") or {}
    return {
        "Title": resolve_field(vc.get("title"), inherited_title(meta)),
        "Subtitle": resolve_field(vc.get("subtitle"), inherited_subtitle(meta)),
        "Footnote": resolve_field(vc.get("note"), inherited_note(meta)),
        "description_short": resolve_field(
            vm.get("description_short"),
            getattr(meta, "description_short", None) if meta else None,
        ),
        "description_key": resolve_field(
            vm.get("description_key"),
            getattr(meta, "description_key", None) if meta else None,
        ),
        "__primary_y": ("override" if cp else "missing", cp),
    }


# ---------------------------------------------------------------------------
# Dimension collapse + placeholder parametrization
# ---------------------------------------------------------------------------


def _value_transforms(v: str) -> list[str]:
    """Candidate literal forms of a dim value — slugs are snake_case but prose uses spaces/hyphens."""
    return [v, v.replace("_", " "), v.replace("_", "-")]


def _try_parametrize_string(values: list[tuple[str, str]], dim_name: str) -> str | None:
    if not values:
        return None
    placeholder = "{" + dim_name + "}"
    if any(not isinstance(text, str) for _, text in values):
        return None
    num_strategies = len(_value_transforms(values[0][0]))
    for strategy_idx in range(num_strategies):
        results = []
        for dim_value, text in values:
            pattern_src = _value_transforms(dim_value)[strategy_idx]
            if not pattern_src:
                results.append(text)
                continue
            results.append(re.sub(re.escape(pattern_src), placeholder, text, flags=re.IGNORECASE))
        if len(set(results)) == 1 and placeholder in results[0]:
            return results[0]
    return None


def try_parametrize_field(
    variants: list[dict[str, Any]],
    entries: list[tuple[str, Any]],
    collapse_dims: list[str],
) -> tuple[str, Any] | None:
    sources = {src for src, _ in entries}
    if len(sources) != 1:
        return None
    (source,) = sources
    values = [val for _, val in entries]
    if any(v is None for v in values):
        return None

    if all(isinstance(v, str) for v in values):
        result = values[0]
        for dim in collapse_dims:
            dim_values = [v.get("dimensions", {}).get(dim) for v in variants]
            if any(dv is None for dv in dim_values):
                continue
            texts_now = [
                re.sub(re.escape(_value_transforms(dv)[0]), "{" + dim + "}", t, flags=re.IGNORECASE)
                for dv, t in zip(dim_values, values)
            ]
            if len(set(texts_now)) == 1 and "{" + dim + "}" in texts_now[0]:
                result = texts_now[0]
                values = [result] * len(values)
                continue
            candidate = _try_parametrize_string(list(zip(dim_values, values)), dim)
            if candidate is None:
                continue
            values = [candidate] * len(values)
            result = candidate
        if len({v for v in values}) == 1 and any("{" + d + "}" in result for d in collapse_dims):
            return source, result
        return None

    if all(isinstance(v, list) for v in values):
        lengths = {len(v) for v in values}
        if len(lengths) != 1:
            return None
        parametrized_bullets: list[str] = []
        for bullet_idx in range(lengths.pop()):
            bullet_values = [v[bullet_idx] for v in values]
            if len(set(bullet_values)) == 1:
                parametrized_bullets.append(bullet_values[0])
                continue
            resolved = None
            for dim in collapse_dims:
                dim_values = [v.get("dimensions", {}).get(dim) for v in variants]
                if any(dv is None for dv in dim_values):
                    continue
                resolved = _try_parametrize_string(list(zip(dim_values, bullet_values)), dim)
                if resolved is not None:
                    break
            if resolved is None:
                return None
            parametrized_bullets.append(resolved)
        return source, parametrized_bullets

    return None


def _compute_heading_disambiguators(
    groups: list[tuple[dict[str, str], list[dict[str, Any]]]],
    raw_headings: list[str],
    collapse_dims: list[str],
    dim_lookup: dict[str, tuple[str, dict[str, str]]],
    config_dim_order: list[str],
) -> dict[int, str]:
    """For each heading shared by >1 groups, return a `{group_idx: suffix}` map.

    The suffix is something like ` (Income measure: After taxes)` built from the
    non-collapsed dim(s) whose values differ across the colliding groups. The goal
    is to keep duplicate headings distinguishable in the rendered outline.
    """
    from collections import defaultdict

    heading_to_idxs: dict[str, list[int]] = defaultdict(list)
    for i, h in enumerate(raw_headings):
        heading_to_idxs[h].append(i)

    out: dict[int, str] = {}
    for heading, idxs in heading_to_idxs.items():
        if len(idxs) <= 1:
            continue
        # For each colliding group, take the non-collapsed dims of its first variant
        # (all variants in a group share the non-collapsed dim values by construction).
        dim_values_per_group: list[dict[str, str]] = []
        for idx in idxs:
            variants = groups[idx][1]
            dims = variants[0].get("dimensions", {}) or {}
            dim_values_per_group.append({k: v for k, v in dims.items() if k not in collapse_dims})

        # Find dims whose values actually differ across these groups.
        all_slugs = {s for d in dim_values_per_group for s in d.keys()}
        discriminating_slugs = [
            s for s in all_slugs if len({d.get(s) for d in dim_values_per_group}) > 1
        ]
        # Preserve dim order from the MDim config for a predictable suffix.
        discriminating_slugs.sort(
            key=lambda s: config_dim_order.index(s) if s in config_dim_order else 999
        )

        for idx, dims in zip(idxs, dim_values_per_group):
            parts: list[str] = []
            for slug in discriminating_slugs:
                dv = dims.get(slug)
                if dv is None or dv == "nan":
                    continue
                name, choice_map = dim_lookup.get(slug, (slug, {}))
                parts.append(f"{name}: {choice_map.get(dv, dv)}")
            if parts:
                out[idx] = f" ({', '.join(parts)})"
    return out


def group_views(
    views: list[dict[str, Any]], collapse_dims: list[str]
) -> list[tuple[dict[str, str], list[dict[str, Any]]]]:
    groups: list[tuple[dict[str, str], list[dict[str, Any]]]] = []
    index: dict[tuple, int] = {}
    for view in views:
        dims = view.get("dimensions", {}) or {}
        remaining = {k: v for k, v in dims.items() if k not in collapse_dims}
        key = tuple(sorted(remaining.items()))
        if key in index:
            groups[index[key]][1].append(view)
        else:
            index[key] = len(groups)
            groups.append((remaining, [view]))
    return groups


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render_mdim(mdim: dict[str, Any]) -> str:
    config_path = Path(mdim["config_path"])
    collapse_dims = mdim.get("collapse_dims", []) or []
    with open(config_path) as f:
        cfg = json.load(f)

    catalog_path = cfg.get("catalog_path", "")
    title_block = cfg.get("title", {})
    top_title = title_block.get("title", mdim["name"])
    title_variant = title_block.get("title_variant", "")

    header_lines: list[str] = []
    header_lines.append(f"# {mdim['name']} — {top_title}")
    if title_variant:
        header_lines.append(f"*Variant:* {title_variant}")
    header_lines.append("")
    if catalog_path:
        main_url = f"{ADMIN_BASE}/{quote(catalog_path, safe='')}"
        header_lines.append(f"**Preview:** [{catalog_path}]({main_url})")
    header_lines.append("")
    header_lines.append(f"Total views: **{len(cfg.get('views', []))}**")

    # Collect full set of values per collapsed dim (filter out `nan` sentinels).
    global_dim_values: dict[str, list[str]] = {}
    for dim in collapse_dims:
        seen: list[str] = []
        for v in cfg.get("views", []):
            dv = (v.get("dimensions") or {}).get(dim)
            if dv is None or dv == "nan":
                continue
            if dv not in seen:
                seen.append(dv)
        if seen:
            global_dim_values[dim] = seen

    if collapse_dims:
        header_lines.append("")
        header_lines.append(
            f"Collapsed dimensions: **{', '.join(f'`{d}`' for d in collapse_dims)}** — "
            "views that differ only in these dimensions are merged into one section. "
            "Fields that vary across the collapsed values are shown with a `{dim}` placeholder "
            "when the variation is a simple substitution; otherwise they're split into sub-bullets."
        )
        for dim, vals in global_dim_values.items():
            header_lines.append(f"- `{{{dim}}}` ∈ {{{', '.join(vals)}}}")
    header_lines.append("")
    # `how_to_read_block` is emitted AFTER the body is built, so we can tailor
    # the legend to only describe tags that actually appear.

    library = BulletLibrary()
    view_lines: list[str] = []
    groups = group_views(cfg.get("views", []), collapse_dims)
    dim_lookup = build_dim_lookup(cfg)

    # First pass: resolve every group's fields and compute each group's raw heading.
    # We need the headings up front so that when multiple groups collapse to the same
    # heading we can append a differentiating dimension selection to each.
    resolved_per_group: list[list[tuple[dict[str, Any], dict[str, tuple[str, Any]]]]] = []
    raw_headings: list[str] = []
    for _remaining_dims, variants in groups:
        resolved = [(v, resolve_view_fields(v)) for v in variants]
        resolved_per_group.append(resolved)

        title_entries = [r["Title"] for _, r in resolved]
        unique_titles = list(dict.fromkeys(v for _, v in title_entries))
        if len(unique_titles) > 1 and len(variants) > 1:
            parametrized_heading = try_parametrize_field(variants, title_entries, collapse_dims)
        else:
            parametrized_heading = None
        if parametrized_heading is not None:
            raw_headings.append(parametrized_heading[1])
        else:
            first_title_value = title_entries[0][1]
            raw_headings.append(
                first_title_value if first_title_value else format_view_header(resolved[0][0])
            )

    # For each heading used by more than one group, find the non-collapsed dim(s) whose
    # values differ across those groups and build a suffix like " (Dim: Choice)" to
    # append to each colliding heading.
    disambiguators = _compute_heading_disambiguators(
        groups=groups,
        raw_headings=raw_headings,
        collapse_dims=collapse_dims,
        dim_lookup=dim_lookup,
        config_dim_order=[d.get("slug") for d in cfg.get("dimensions", []) or [] if d.get("slug")],
    )

    for group_idx, (_remaining_dims, variants) in enumerate(groups):
        resolved = resolved_per_group[group_idx]
        heading = raw_headings[group_idx] + disambiguators.get(group_idx, "")
        view_lines.append(f"## {heading}")
        view_lines.append("")

        selections = human_selections(variants, collapse_dims, dim_lookup)
        if selections:
            view_lines.append(selections)
            view_lines.append("")

        if catalog_path:
            if len(variants) == 1:
                url = preview_url(catalog_path, variants[0].get("dimensions", {}) or {})
                view_lines.append(f"**Preview:** [{heading}]({url})")
            else:
                labels = []
                for v in variants:
                    dims = v.get("dimensions", {}) or {}
                    label_parts = [f"{k}={dims[k]}" for k in collapse_dims if k in dims]
                    label = ", ".join(label_parts) if label_parts else "variant"
                    labels.append(f"[{label}]({preview_url(catalog_path, dims)})")
                view_lines.append(f"**Previews:** {' · '.join(labels)}")
            view_lines.append("")

        for field_label in ["Title", "Subtitle", "Footnote", "description_short", "description_key"]:
            entries = [r[field_label] for _, r in resolved]

            def normalize(entry):
                src, val = entry
                if isinstance(val, list):
                    return (src, tuple(md_escape(str(x)) for x in val))
                return (src, val)

            unique_entries = list(dict.fromkeys(normalize(e) for e in entries))
            identical = len(unique_entries) == 1

            parametrized = None
            if not identical and len(variants) > 1:
                parametrized = try_parametrize_field(variants, entries, collapse_dims)

            if field_label == "description_key":
                if identical:
                    source, value = entries[0]
                    if isinstance(value, list) and value:
                        ids = library.register(value)
                        view_lines.append(f"- **description_key** [{source}]")
                        for slug in ids:
                            view_lines.append(f"  - {slug}")
                    else:
                        view_lines.append(f"- **description_key** {render_value(source, value)}")
                elif parametrized is not None:
                    source, value = parametrized
                    ids = library.register(value)
                    view_lines.append(f"- **description_key** [{source}]")
                    for slug in ids:
                        view_lines.append(f"  - {slug}")
                else:
                    view_lines.append("- **description_key** _varies:_")
                    for v, (source, value) in zip(variants, entries):
                        dims = v.get("dimensions", {}) or {}
                        label_parts = [f"{k}={dims[k]}" for k in collapse_dims if k in dims]
                        label = ", ".join(label_parts) if label_parts else "variant"
                        if isinstance(value, list) and value:
                            ids = library.register(value)
                            view_lines.append(f"  - {label}: [{source}]")
                            for slug in ids:
                                view_lines.append(f"    - {slug}")
                        else:
                            view_lines.append(f"  - {label}: {render_value(source, value)}")
                continue

            if identical:
                source, value = entries[0]
                view_lines.append(f"- **{field_label}** {render_value(source, value)}")
            elif parametrized is not None:
                source, value = parametrized
                view_lines.append(f"- **{field_label}** {render_value(source, value)}")
            else:
                view_lines.append(f"- **{field_label}** _varies:_")
                for v, (source, value) in zip(variants, entries):
                    dims = v.get("dimensions", {}) or {}
                    label_parts = [f"{k}={dims[k]}" for k in collapse_dims if k in dims]
                    label = ", ".join(label_parts) if label_parts else "variant"
                    view_lines.append(f"  - {label}: {render_value(source, value)}")

        view_lines.append("")
        view_lines.append("---")
        view_lines.append("")

    used_tags = collect_used_tags(view_lines)
    header_lines.extend(how_to_read_block(used_tags))
    return "\n".join(header_lines + library.legend_lines() + view_lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a JSON list of MDim entries. If omitted, uses the built-in MDIMS.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help=f"Output directory for the generated Markdown files (default: {OUT_DIR}).",
    )
    args = parser.parse_args()

    if args.config:
        raw = json.loads(Path(args.config).read_text())
        mdims = [
            {
                **m,
                "config_path": BASE_DIR / m["config_path"]
                if not str(m["config_path"]).startswith("/")
                else Path(m["config_path"]),
            }
            for m in raw
        ]
    else:
        mdims = MDIMS

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for mdim in mdims:
        print(f"\n=== {mdim['name']} ===")
        md = render_mdim(mdim)
        out_path = args.out_dir / f"{mdim['name']}.md"
        out_path.write_text(md, encoding="utf-8")
        rel = out_path.relative_to(BASE_DIR) if out_path.is_relative_to(BASE_DIR) else out_path
        print(f"Wrote {rel} ({len(md.splitlines())} lines)")


if __name__ == "__main__":
    main()
