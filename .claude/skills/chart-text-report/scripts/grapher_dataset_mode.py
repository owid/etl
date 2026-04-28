"""Generate a Markdown chart-text report for a grapher dataset or a hand-picked list of indicators.

For each indicator (column), resolves the five fields that the chart-text-report
skill cares about and tags each one as [inherited] (from the column's VariableMeta)
or [missing]. There is no [override] tag in this mode because there are no views —
each indicator is reported standalone.

Two input modes:

1. Whole dataset:
       .venv/bin/python .claude/skills/chart-text-report/scripts/grapher_dataset_mode.py \\
           --dataset data/grapher/wb/2026-03-24/world_bank_pip

   Iterates every column of every table in the dataset.

2. Hand-picked indicators (by catalogPath, one per line in a file or passed directly):
       .venv/bin/python .claude/skills/chart-text-report/scripts/grapher_dataset_mode.py \\
           --indicators grapher/wb/2026-03-24/world_bank_pip/incomes#thr__... \\
                        grapher/wb/2026-03-24/world_bank_pip/incomes#share__...

Output: one Markdown file per input, written under `ai/` by default.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from owid.catalog import Dataset

from etl.paths import BASE_DIR, DATA_DIR

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
    parse_catalog_path,
    render_value,
    resolve_field,
)

OUT_DIR = BASE_DIR / "ai"


def resolve_indicator_fields(catalog_path: str) -> dict[str, tuple[str, Any]]:
    """Resolve the five reported fields for a single indicator catalogPath.

    No override source exists in dataset mode — every tag is [inherited] or [missing]."""
    meta = get_indicator_meta(catalog_path)
    return {
        "Title": resolve_field(None, inherited_title(meta)),
        "Subtitle": resolve_field(None, inherited_subtitle(meta)),
        "Footnote": resolve_field(None, inherited_note(meta)),
        "description_short": resolve_field(None, getattr(meta, "description_short", None) if meta else None),
        "description_key": resolve_field(None, getattr(meta, "description_key", None) if meta else None),
    }


def dataset_catalog_paths(dataset_path: Path) -> list[str]:
    """Return the catalogPath of every column across every table in a grapher dataset.

    Catalog paths are formatted like `grapher/<ns>/<ver>/<ds>/<table>#<col>`."""
    ds = Dataset(dataset_path)
    # Derive ns/ver/ds from the filesystem path.
    try:
        rel = dataset_path.resolve().relative_to(DATA_DIR.resolve())
    except ValueError as e:
        raise ValueError(f"Dataset must live under {DATA_DIR}; got {dataset_path}") from e
    parts = rel.parts
    if len(parts) < 4 or parts[0] != "grapher":
        raise ValueError(f"Expected path like 'data/grapher/<ns>/<ver>/<ds>'; got {dataset_path}")
    channel, ns, ver, ds_name = parts[0], parts[1], parts[2], parts[3]
    paths: list[str] = []
    for table_name in ds.table_names:
        tb = ds.read(table_name, safe_types=False)
        # Skip the index columns (typically `country` / `year`).
        for col in tb.columns:
            if col in ("country", "year", "entity", "date"):
                continue
            paths.append(f"{channel}/{ns}/{ver}/{ds_name}/{table_name}#{col}")
    return paths


def render_report(
    title: str,
    catalog_paths: list[str],
    subheading: str = "",
) -> str:
    header_lines: list[str] = []
    header_lines.append(f"# {title}")
    if subheading:
        header_lines.append(f"*{subheading}*")
    header_lines.append("")
    header_lines.append(f"Total indicators: **{len(catalog_paths)}**")
    header_lines.append("")
    # `how_to_read_block` is appended below after the body has been built,
    # so we only document the source tags that actually occur.

    library = BulletLibrary()
    body: list[str] = []
    for catalog_path in catalog_paths:
        resolved = resolve_indicator_fields(catalog_path)

        title_source, title_value = resolved["Title"]
        heading = title_value if title_value else catalog_path.split("#")[-1]
        body.append(f"## {heading}")
        body.append("")

        url = f"{ADMIN_BASE}/{quote(catalog_path, safe='')}"
        body.append(f"**Preview:** [{catalog_path}]({url})")
        body.append("")

        for field_label in ["Title", "Subtitle", "Footnote", "description_short", "description_key"]:
            source, value = resolved[field_label]
            if field_label == "description_key":
                if isinstance(value, list) and value:
                    ids = library.register(value)
                    body.append(f"- **description_key** [{source}]")
                    for slug in ids:
                        body.append(f"  - {slug}")
                else:
                    body.append(f"- **description_key** {render_value(source, value)}")
            else:
                body.append(f"- **{field_label}** {render_value(source, value)}")

        body.append("")
        body.append("---")
        body.append("")

    used_tags = collect_used_tags(body)
    header_lines.extend(how_to_read_block(used_tags))
    return "\n".join(header_lines + library.legend_lines() + body).rstrip() + "\n"


def default_output_name_for_dataset(dataset_path: Path) -> str:
    """e.g. data/grapher/wb/2026-03-24/world_bank_pip → wb_world_bank_pip_indicators.md"""
    parts = dataset_path.resolve().relative_to(DATA_DIR.resolve()).parts
    if len(parts) >= 4:
        _, ns, _ver, ds_name = parts[0], parts[1], parts[2], parts[3]
        return f"{ns}_{ds_name}_indicators.md"
    return f"{dataset_path.name}_indicators.md"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--dataset",
        type=Path,
        help="Path to a grapher dataset folder (e.g. data/grapher/wb/2026-03-24/world_bank_pip).",
    )
    src.add_argument(
        "--indicators",
        nargs="+",
        help="Space-separated grapher catalogPath strings.",
    )
    src.add_argument(
        "--indicators-file",
        type=Path,
        help="File with one catalogPath per line.",
    )
    parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Override the top-level title in the rendered Markdown.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output Markdown path (default: ai/<inferred>.md).",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.dataset:
        dataset_path = args.dataset if args.dataset.is_absolute() else BASE_DIR / args.dataset
        catalog_paths = dataset_catalog_paths(dataset_path)
        try:
            parts = dataset_path.resolve().relative_to(DATA_DIR.resolve()).parts
            title = args.title or f"{parts[3]} — {parts[1]} @ {parts[2]}"
            subheading = (
                str(dataset_path.relative_to(BASE_DIR)) if dataset_path.is_relative_to(BASE_DIR) else str(dataset_path)
            )
        except ValueError:
            title = args.title or dataset_path.name
            subheading = str(dataset_path)
        out_path = args.output or OUT_DIR / default_output_name_for_dataset(dataset_path)

    else:
        if args.indicators_file:
            lines = args.indicators_file.read_text().splitlines()
            catalog_paths = [ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")]
        else:
            catalog_paths = list(args.indicators)
        # Validate shape.
        for cp in catalog_paths:
            parse_catalog_path(cp)  # raises ValueError on malformed paths
        title = args.title or "Indicator chart-text report"
        subheading = f"{len(catalog_paths)} catalogPaths"
        out_path = args.output or OUT_DIR / "indicators_chart_text_report.md"

    md = render_report(title=title, catalog_paths=catalog_paths, subheading=subheading)
    out_path.write_text(md, encoding="utf-8")
    rel = out_path.relative_to(BASE_DIR) if out_path.is_relative_to(BASE_DIR) else out_path
    print(f"Wrote {rel} ({len(md.splitlines())} lines, {len(catalog_paths)} indicators)")


if __name__ == "__main__":
    main()
