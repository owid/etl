"""Extract explorer views and MDIM views into CSVs, plus a mapping scaffold.

Reads from the grapher DB via ``OWID_ENV`` (run on a staging branch, or against
production with ``ENV_FILE=.env.prod DATA_API_ENV=production``):

- Explorer views come from the ``tsv`` column of the ``explorers`` table — the
  ``graphers`` block, one row per view; dimension columns are those whose header
  ends in " Dropdown" / " Radio" / " Checkbox".
- MDIM views come from ``multi_dim_data_pages.config`` (the published, fully
  expanded ``views[].dimensions``, slug -> slug).

Outputs into ``--out``:
- ``explorer_views.csv``        — id (1..N) + dimension_1..M (explorer display values)
- ``multidim_<short>_views.csv`` — id (A1.., B1.., ...) + one column per MDIM dim slug
- ``_scaffold.md``              — dimension legend, distinct values, auto-suggested
                                  value matches, and a ``mapping_rules.py`` template
- ``_sources.json``             — explorer slug + dim names and each MDIM's
                                  short/prefix/catalogPath/dim-slugs, so ``build_mapping.py``
                                  can emit a redirect ``mapping.json`` with full identifiers

Usage:
    .venv/bin/python .claude/skills/map-explorer-to-mdim/scripts/extract_views.py \
        --explorer natural-disasters \
        --mdim natural_disasters/latest/deaths#deaths \
        --mdim natural_disasters/latest/economic_damages#economic_damages \
        --mdim natural_disasters/latest/affected#affected \
        --out ai/natural-disasters-mdim-mapping
"""

import argparse
import csv
import json
import re
from pathlib import Path

from sqlalchemy import text

from etl.config import OWID_ENV

WIDGET_SUFFIXES = ("Dropdown", "Radio", "Checkbox")


def slugify(value: str) -> str:
    """Lowercase + underscores; only used to *suggest* value->slug matches."""
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.strip().lower())).strip("_")


def dim_name(col: str) -> str:
    """'Disaster Type Dropdown' -> 'Disaster Type'."""
    parts = col.split(" ")
    return " ".join(parts[:-1]) if parts and parts[-1] in WIDGET_SUFFIXES else col


# ----- Explorer -----------------------------------------------------------------


def parse_explorer_views(tsv: str):
    """Return (dim_names, rows) from the graphers block of an explorer TSV."""
    lines = tsv.split("\n")
    try:
        gi = next(i for i, ln in enumerate(lines) if ln.rstrip("\r") == "graphers")
    except StopIteration:
        raise SystemExit("No 'graphers' block found in explorer TSV.")

    header = lines[gi + 1].split("\t")
    dim_cols = [(j, dim_name(name)) for j, name in enumerate(header) if name.split(" ")[-1] in WIDGET_SUFFIXES]
    dim_names = [name for _, name in dim_cols]

    rows = []
    for ln in lines[gi + 2 :]:
        if not ln.startswith("\t"):  # blank line or next top-level key ends the block
            break
        fields = ln.split("\t")
        rows.append([fields[j] if j < len(fields) else "" for j, _ in dim_cols])
    return dim_names, rows


def write_explorer_csv(out: Path, dim_names, rows) -> Path:
    path = out / "explorer_views.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id"] + [f"dimension_{i + 1}" for i in range(len(dim_names))])
        for n, r in enumerate(rows, start=1):
            w.writerow([n] + r)
    return path


# ----- MDIM ---------------------------------------------------------------------


def short_name_of(catalog_path: str) -> str:
    """'ns/v/deaths#deaths' -> 'deaths'."""
    return catalog_path.split("#", 1)[1] if "#" in catalog_path else catalog_path.rstrip("/").split("/")[-1]


def get_mdim_views(catalog_path: str):
    """Return (dim_slugs, rows) for a published MDIM from multi_dim_data_pages.config."""
    df = OWID_ENV.read_sql(
        text("SELECT config FROM multi_dim_data_pages WHERE catalogPath = :cp"),
        params={"cp": catalog_path},
    )
    if df.empty:
        raise SystemExit(
            f"MDIM not found in multi_dim_data_pages: {catalog_path!r}. Is it published in the DB you're connected to?"
        )
    cfg = df["config"].iloc[0]
    cfg = json.loads(cfg) if isinstance(cfg, str) else cfg
    views = cfg.get("views", [])
    order = [d["slug"] for d in cfg.get("dimensions", [])]
    used = set().union(*(v["dimensions"].keys() for v in views)) if views else set()
    dim_slugs = [s for s in order if s in used]  # config order, only dims that vary in views
    rows = [[v["dimensions"].get(s, "") for s in dim_slugs] for v in views]
    return dim_slugs, rows


def write_mdim_csv(out: Path, short: str, prefix: str, dim_slugs, rows) -> Path:
    path = out / f"multidim_{short}_views.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id"] + dim_slugs)
        for n, r in enumerate(rows, start=1):
            w.writerow([f"{prefix}{n}"] + r)
    return path


# ----- Scaffold -----------------------------------------------------------------


def distinct_by_col(rows, ncols):
    return [sorted({r[j] for r in rows}) for j in range(ncols)]


def build_scaffold(out: Path, explorer_slug, dim_names, exp_rows, mdims) -> Path:
    """mdims: list of dicts {short, prefix, catalog_path, dim_slugs, rows}."""
    exp_distinct = distinct_by_col(exp_rows, len(dim_names))

    # Index every MDIM choice slug for auto-suggesting explorer-value -> slug matches.
    slug_index = {}  # choice_slug -> list of "short.dim_slug"
    for m in mdims:
        m_distinct = distinct_by_col(m["rows"], len(m["dim_slugs"]))
        for dslug, vals in zip(m["dim_slugs"], m_distinct):
            for v in vals:
                slug_index.setdefault(v, []).append(f"{m['short']}.{dslug}")

    lines = [f"# Mapping scaffold — explorer `{explorer_slug}`", ""]
    lines.append(f"Explorer views: **{len(exp_rows)}**\n")

    lines.append("## Explorer dimensions (column legend)\n")
    for i, (name, vals) in enumerate(zip(dim_names, exp_distinct), start=1):
        lines.append(f"- `dimension_{i}` = **{name}** ({len(vals)} values)")
        for v in vals:
            hits = slug_index.get(slugify(v), [])
            hint = f"  → auto-match: {', '.join(hits)}" if hits else ""
            lines.append(f"    - `{v}`{hint}")
    lines.append("")

    lines.append("## MDIMs\n")
    for m in mdims:
        m_distinct = distinct_by_col(m["rows"], len(m["dim_slugs"]))
        lines.append(f"### `{m['prefix']}` — {m['short']}  ({len(m['rows'])} views)")
        lines.append(f"`{m['catalog_path']}`\n")
        for dslug, vals in zip(m["dim_slugs"], m_distinct):
            lines.append(f"- **{dslug}**: {', '.join(f'`{v}`' for v in vals)}")
        lines.append("")

    lines.append("## Next step\n")
    lines.append(
        "Write `mapping_rules.py` in this folder (template below), then run `build_mapping.py --out <this folder>`.\n"
    )
    lines.append("```python")
    lines.append("# mapping_rules.py — fill in routing + value translation for this explorer.")
    lines.append("")
    lines.append(f"EXPLORER_DIMENSIONS = {dim_names!r}  # order of dimension_1..N (do not change)")
    lines.append(f"MDIMS = {[m['short'] for m in mdims]!r}  # order = prefixes {[m['prefix'] for m in mdims]!r}")
    lines.append("")
    lines.append("# Per-explorer-dimension value -> mdim choice slug. Seeded from auto-matches above;")
    lines.append("# verify and complete every value.")
    for i, (name, vals) in enumerate(zip(dim_names, exp_distinct), start=1):
        var = slugify(name).upper() + "_MAP"
        lines.append(f"{var} = {{")
        for v in vals:
            # slugify(v) is the best guess for the choice slug; when it matches a real
            # MDIM choice the auto-match comment above confirms which dim it lands in.
            lines.append(f"    {v!r}: {slugify(v)!r},")
        lines.append("}")
    lines.append("")
    lines.append("def route(dims):")
    lines.append('    """dims: {explorer dimension name -> value}. Return target MDIM short name."""')
    if len(mdims) == 1:
        lines.append(f"    return {mdims[0]['short']!r}")
    else:
        shorts = [m["short"] for m in mdims]
        lines.append(f"    # EDIT: pick the target MDIM ({', '.join(shorts)}) from the explorer's dimensions.")
        lines.append("    raise NotImplementedError")
    lines.append("")
    lines.append("def translate(dims, mdim):")
    lines.append('    """Return {mdim_dim_slug: choice_slug} for the target MDIM view."""')
    lines.append("    # EDIT: build the target dimension dict per MDIM, using the *_MAP dicts above.")
    lines.append("    raise NotImplementedError")
    lines.append("```")

    path = out / "_scaffold.md"
    path.write_text("\n".join(lines))
    return path


def write_sources_json(out: Path, explorer_slug, dim_names, mdims) -> Path:
    """Persist the identifiers build_mapping.py needs to emit the redirect mapping.json.

    mdims: list of dicts {short, prefix, catalog_path, dim_slugs, rows}.
    """
    data = {
        "explorer": {"slug": explorer_slug, "dimensions": dim_names},
        "mdims": [
            {
                "short": m["short"],
                "prefix": m["prefix"],
                "catalogPath": m["catalog_path"],
                "dimensions": m["dim_slugs"],
            }
            for m in mdims
        ],
    }
    path = out / "_sources.json"
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def check_db_connection():
    """Fail fast with actionable guidance if the grapher DB isn't reachable."""
    try:
        OWID_ENV.read_sql(text("SELECT 1"))
    except Exception as e:  # noqa: BLE001 - connectivity preflight, surface a friendly hint
        raise SystemExit(
            "Cannot reach the grapher DB via OWID_ENV:\n"
            f"  {type(e).__name__}: {str(e).splitlines()[0]}\n\n"
            "This skill reads the explorer + MDIMs from the grapher DB. Point OWID_ENV at a DB\n"
            "that has both, by one of:\n"
            "  - running on a `staging-site-<branch>` branch (no prefix needed), or\n"
            "  - `ENV_FILE=.env.prod DATA_API_ENV=production` (only if .env.prod exists), or\n"
            "  - `ENV_FILE=<your creds file> [DATA_API_ENV=production]`.\n"
            "If you don't have a credentials file, ask which one to use — don't hardcode secrets."
        )


def main():
    ap = argparse.ArgumentParser(description="Extract explorer + MDIM views into CSVs for redirect mapping.")
    ap.add_argument("--explorer", required=True, help="Explorer slug (explorers.slug)")
    ap.add_argument(
        "--mdim", required=True, action="append", help="MDIM catalogPath (repeatable), e.g. ns/v/short#short"
    )
    ap.add_argument("--out", required=True, help="Output folder")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    check_db_connection()

    # Explorer
    df = OWID_ENV.read_sql(text("SELECT tsv FROM explorers WHERE slug = :slug"), params={"slug": args.explorer})
    if df.empty:
        raise SystemExit(f"Explorer not found: {args.explorer!r}")
    dim_names, exp_rows = parse_explorer_views(df["tsv"].iloc[0])
    p = write_explorer_csv(out, dim_names, exp_rows)
    print(f"explorer: {len(exp_rows)} views, dims={dim_names} -> {p.name}")

    # MDIMs
    mdims = []
    for i, cp in enumerate(args.mdim):
        prefix = chr(ord("A") + i)
        short = short_name_of(cp)
        dim_slugs, rows = get_mdim_views(cp)
        p = write_mdim_csv(out, short, prefix, dim_slugs, rows)
        print(f"{prefix} {short}: {len(rows)} views, dims={dim_slugs} -> {p.name}")
        mdims.append({"short": short, "prefix": prefix, "catalog_path": cp, "dim_slugs": dim_slugs, "rows": rows})

    p = write_sources_json(out, args.explorer, dim_names, mdims)
    print(f"sources -> {p.name}")

    p = build_scaffold(out, args.explorer, dim_names, exp_rows, mdims)
    print(f"scaffold -> {p.name}  (write mapping_rules.py, then run build_mapping.py)")


if __name__ == "__main__":
    main()
