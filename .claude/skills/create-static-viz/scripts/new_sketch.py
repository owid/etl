"""Scaffold a static-viz sketch: a `viz://static` step in everything but its home.

A sketch is how a NEW static viz is prototyped before any ETL step exists — from a CSV, Excel or
parquet file, in a gitignored `ai/static-viz-sketches/<slug>/` directory, with no branch, PR, DAG
entry or newer-data check. The scaffold is written in the exact shape of a step (constants on top,
`run()`, helpers below it, a `LAYOUTS` registry, `export_frame` per frame) so `promote_sketch.py` can
turn it into one with a two-line edit. It renders as-is: a placeholder figure at the template's
proportions, so the round trip — render, verify, import into Figma — is proven before any chart code
is written.

Usage:
    new_sketch.py --slug <slug> --data <file> --template <name> [--template <name> ...]
                  [--source "<citation>"] [--title "<title>"] [--author "<name>"]
                  [--out-root <dir>] [--force]

Run it with the repo venv (`.venv/bin/python`): it imports `etl.viz.static` for the template list.
The first `--template` produces `<slug>.svg/.png`; each further one produces `<slug>_<template>` with
`-` written as `_`, which are the suffixes `verify_static_viz.py` picks a template from.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import string
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO_ROOT))

from verify_static_viz import FILENAME_TEMPLATE_HINTS  # noqa: E402

from etl.viz.static import TEMPLATES  # noqa: E402

DEFAULT_OUT_ROOT = REPO_ROOT / "ai" / "static-viz-sketches"
DATA_SUFFIXES = {".csv", ".xlsx", ".xls", ".parquet", ".feather"}
# A slug becomes a filename stem now and a step `short_name` later, so it follows the step rule.
SLUG = re.compile(r"^[a-z0-9][a-z0-9_]*$")
# The `LAYOUTS` registry of an existing sketch, and the frame names it declares. A §4 variant is a key
# a person adds by hand, so the file being replaced is the only thing that knows what it rendered.
LAYOUTS_REGISTRY = re.compile(r"^LAYOUTS = \{$(.*?)^\}$", re.MULTILINE | re.DOTALL)
LAYOUTS_KEY = re.compile(r'^\s+"([A-Za-z0-9_]+)":\s*\{', re.MULTILINE)

# Per-template slot positions in template px from the top edge, transcribed from TEMPLATES.md for the
# templates' placeholder two-line subtitle. `note_y` is the desktop `Note:` row; mobile has none.
# The header rows (title 16, subtitle 80, band top 118) and the 16 px margin are the same on all four.
SLOTS: dict[str, dict[str, float | bool | None]] = {
    "horizontal": {"chart_bottom_y": 559, "note_y": 559, "source_y": 591, "footer_y": 609, "full_footer": True},
    "vertical": {
        "chart_bottom_y": 1015.81,
        "note_y": 1015.81,
        "source_y": 1047.81,
        "footer_y": 1065.81,
        "full_footer": True,
    },
    "mobile": {"chart_bottom_y": 770, "note_y": None, "source_y": 770, "footer_y": 791, "full_footer": False},
    "mobile-square": {"chart_bottom_y": 486, "note_y": None, "source_y": 486, "footer_y": 507, "full_footer": False},
}

# `string.Template` rather than str.format: the emitted Python is full of braces. Raw so backslash
# escapes reach the sketch verbatim. The `*_LITERAL` slots take a ready-made Python string literal
# (`py_literal`), the docstring slots plain text (`doc_text`): the arguments are the person's own words
# and may hold quotes or backslashes.
SCAFFOLD = string.Template(
    r'''"""$TITLE — a static-viz sketch.

A sketch in the shape of a `viz://static` step, rendered from a local data file with no ETL behind it.
Promote it with `.claude/skills/create-static-viz/scripts/promote_sketch.py` once the visuals settle;
`.claude/skills/create-static-viz/reference/SKETCHING.md` has the whole loop.

Data: `$DATA_FILE`. Provenance: FILL IN where the file came from (a URL, a grapher slug, a
catalogPath), so promotion knows which dataset to point the step at.

Render:  .venv/bin/python $SKETCH_PATH
Verify:  .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py $SKETCH_STEM \
             --template $FIRST_TEMPLATE --expect-gid line__placeholder
         One stem per frame, not the directory: anything else you keep here — the old chart, a
         reference export — is not this sketch's to verify.
Then read the PNG.

Figma handoff
-------------
Filled in by /owid-staff:create-figma-chart's sketch mode when the SVG is imported. The promoted step's
docstring inherits it, so record names, not just ids:
- File: Charts (YYYY), key <fileKey>
- Page: <YYYYMMDD Title (Creator) [sketch]>
- Frames: <frame name> -> <node id>, cloned from <template name> <template node id>
- Deep link: <url>

Scaffolded $DATE by new_sketch.py.
"""

import logging
import math
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.patches import Rectangle
from owid.catalog import Table

from etl.viz.static import PIXELS_PER_INCH, TEMPLATES, SketchPaths, apply_svg_rcparams, export_frame, nice_year_ticks

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

paths = SketchPaths(__file__)  # PROMOTE: becomes PathFinder(__file__)

# The file this sketch draws from, copied next to it by new_sketch.py.
DATA_FILE = Path(__file__).with_name($DATA_FILE_LITERAL)  # PROMOTE: delete; the step loads from the catalog

TITLE = $TITLE_LITERAL
SUBTITLE = "What the chart shows, in one sentence."
NOTE = ""  # desktop templates only; mobile has no Note slot (TEMPLATES.md)
# The Data source string: a CSV carries no origins, a garden table does. run() reads it once.
# PROMOTE: delete, once run() derives the citation with source_citation.
SOURCE = $SOURCE_LITERAL
AUTHOR = $AUTHOR_LITERAL

# Exact template strings (TEMPLATES.md -> Exact strings the templates use).
TAGLINE = "OurWorldinData.org — Research and data to make progress against the world’s largest problems."
LICENSE = f"Licensed under CC-BY by the author {AUTHOR}"

# Two font stacks, because two readers want different answers (WRITING-THE-STEP.md). This one lands in
# the SVG's `font-family` verbatim, so naming Lato first asks Figma to render the import in the
# template's own face on arrival.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "sans-serif"]
# What this sketch measures and draws with. Deliberately does NOT name Lato, which is not installed on
# our machines: a face the sketch can ask for, not one it can measure in.
MEASURED_FONT_STACK = ["Arial", "Helvetica", "DejaVu Sans", "Liberation Sans", "sans-serif"]

matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK

# Drop the per-face misses for faces deliberately listed as alternatives, and nothing else. A blanket
# silence would also take "Falling back to DejaVu Sans", which says a whole stack failed.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)

# The invariant every measured width rests on: the face drawn IS the face measured.
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name}"

# Grapher's design language, read from its source (WRITING-THE-STEP.md -> Borrow grapher's design
# language). The sketch sets no palette: colors, fonts and background belong to the Figma template.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
TICK_COLOR = "#999999"
TEXT_COLOR = "#5b5b5b"
TITLE_COLOR = "#2d2e2d"
FOOTER_COLOR = "#858585"
GUIDE_COLOR = "#bbbbbb"

# A template pixel in points: 100 template px per inch, 72 pt per inch.
POINTS_PER_PIXEL = 72 / PIXELS_PER_INCH

# Template type sizes in px (TEMPLATES.md -> Slot positions): title 25, subtitle 16; footer rows 12/11
# on the 850-wide pair and 14 on mobile; the in-plot body rank is 14.
SLOT_PX = {
    "title": 25,
    "subtitle": 16,
    "note": 12,
    "source_desktop": 12,
    "footer_desktop": 11,
    "footer_mobile": 14,
    "body": 14,
}

# The band is the room between the header's bottom and the footer's top, inset 14 px at each end.
BAND_INSET = 14
# Room left of the plot for the placeholder's y tick labels, and below it for the x tick labels. A real
# chart measures its own (WRITING-THE-STEP.md); these only keep the placeholder's labels off the margin
# and off the footer, whose first row starts at the band's bottom on the mobile templates.
Y_TICK_COLUMN = 40
X_TICK_ROW = 22

# One entry per frame this sketch emits, keyed by the output filename. Slot positions are template px
# from the top edge, transcribed from TEMPLATES.md for the templates' placeholder two-line subtitle: a
# one-line subtitle gains 19 px of band, so re-verify against the live template at promotion
# (TEMPLATES.md's own rule). The verifier picks each frame's template from the filename suffix.
$LAYOUTS_BLOCK


def run() -> None:
    tb = load_data()
    paths.log.info(f"Loaded {len(tb)} rows; columns: {list(tb.columns)}")
    sanity_check_inputs(tb)
    # PROMOTE: source = source_citation(tb["<column>"]) — here, after load_data(), where `tb` exists.
    source = SOURCE
    for name, layout in LAYOUTS.items():
        fig = build(tb, layout, source)
        export_frame(paths, fig, name, template=layout["template"])
        plt.close(fig)


def load_data() -> Table:
    """Read the data file next to this sketch, with garden-style snake_case column names.

    PROMOTE: return paths.load_dataset("<short_name>").read("<table>") instead, and delete DATA_FILE.
    """
    suffix = DATA_FILE.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(DATA_FILE)
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(DATA_FILE)
    elif suffix == ".parquet":
        df = pd.read_parquet(DATA_FILE)
    elif suffix == ".feather":
        df = pd.read_feather(DATA_FILE)
    else:
        raise ValueError(f"unsupported data file {DATA_FILE.name}")
    return Table(df, short_name=paths.short_name, underscore=True)


def sanity_check_inputs(tb: Table) -> None:
    """Assert what the chart assumes about its input; extend these as the chart takes shape."""
    assert len(tb) > 0, "the data file is empty"
    empty = list(tb.columns[tb.isna().all()])
    assert not empty, f"fully-NaN columns: {empty}"


def px_to_pt(px: float) -> float:
    return px * POINTS_PER_PIXEL


def build(tb: Table, layout: dict, source: str) -> plt.Figure:
    """Draw one frame. Replace this placeholder wholesale with the chart; the guides go with it.

    As shipped it proves the round trip: the frame is at the template's proportions, every text slot
    sits where the template's does, the band is where the chart belongs and the layers are named — so
    the render, the verifier and the Figma import all work before a line of chart code exists.
    """
    template = TEMPLATES[layout["template"]]
    width_px, height_px = template.width_px, template.height_px
    fig = plt.figure(figsize=template.figsize)
    fig.patch.set_facecolor("white")  # legible when the PNG is reviewed; the SVG is saved transparent

    def fx(px: float) -> float:
        return px / width_px

    def fy(px: float) -> float:
        return 1 - px / height_px  # matplotlib's y runs upward

    margin = layout["margin"]
    content_w = width_px - 2 * margin
    footer_px = SLOT_PX["footer_desktop"] if layout["full_footer"] else SLOT_PX["footer_mobile"]
    source_px = SLOT_PX["source_desktop"] if layout["full_footer"] else SLOT_PX["footer_mobile"]

    # The template's own text slots, at the template's sizes, so the PNG is a preview and not a
    # proportion sketch. Figma drops these on import and fills its own slots.
    left = dict(ha="left", va="top")
    fig.text(fx(margin), fy(layout["title_y"]), TITLE, fontsize=px_to_pt(SLOT_PX["title"]), color=TITLE_COLOR, gid="title", **left)
    fig.text(fx(margin), fy(layout["subtitle_y"]), SUBTITLE, fontsize=px_to_pt(SLOT_PX["subtitle"]), color=TEXT_COLOR, gid="subtitle", **left)
    if layout["full_footer"] and NOTE:
        fig.text(fx(margin), fy(layout["note_y"]), f"Note: {NOTE}", fontsize=px_to_pt(SLOT_PX["note"]), color=FOOTER_COLOR, gid="note", **left)
    fig.text(fx(margin), fy(layout["source_y"]), f"Data source: {source}", fontsize=px_to_pt(source_px), color=FOOTER_COLOR, gid="data-source", **left)
    if layout["full_footer"]:
        fig.text(fx(margin), fy(layout["footer_y"]), TAGLINE, fontsize=px_to_pt(footer_px), color=FOOTER_COLOR, gid="tagline", **left)
        fig.text(fx(width_px - margin), fy(layout["footer_y"]), LICENSE, fontsize=px_to_pt(footer_px), color=FOOTER_COLOR, gid="license", ha="right", va="top")
    else:
        fig.text(fx(margin), fy(layout["footer_y"]), LICENSE, fontsize=px_to_pt(footer_px), color=FOOTER_COLOR, gid="license", **left)

    # The band, and a placeholder plot inside it.
    band_top, band_bottom = layout["chart_top_y"] + BAND_INSET, layout["chart_bottom_y"] - BAND_INSET
    ax = fig.add_axes(
        (
            fx(margin + Y_TICK_COLUMN),
            fy(band_bottom - X_TICK_ROW),
            (content_w - Y_TICK_COLUMN) / width_px,
            (band_bottom - X_TICK_ROW - band_top) / height_px,
        )
    )
    ax.patch.set_visible(False)  # the template supplies the background
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(TICK_COLOR)
    ax.yaxis.grid(True, linestyle=GRID_DASHES, color=GRID_COLOR, linewidth=1.0)
    ax.set_axisbelow(True)
    ax.tick_params(colors=TEXT_COLOR, labelsize=px_to_pt(SLOT_PX["body"]), length=5, width=1)
    ax.tick_params(axis="y", length=0)
    x, y = placeholder_series(tb)
    ax.plot(x, y, color="C0", linewidth=2, gid="line__placeholder")
    years = integer_years(x) if "year" in tb.columns else None
    if years:
        ticks = nice_year_ticks(min(years), max(years))
        ax.set_xticks(ticks)
        ax.set_xlim(min(ticks[0], min(years)), max(ticks[-1], max(years)))

    # Guides: the band's outline and a label saying what to replace. They leave with the placeholder.
    band_h = layout["chart_bottom_y"] - layout["chart_top_y"]
    fig.add_artist(
        Rectangle(
            (fx(margin), fy(layout["chart_bottom_y"])),
            content_w / width_px,
            band_h / height_px,
            transform=fig.transFigure,
            fill=False,
            linestyle="--",
            edgecolor=GUIDE_COLOR,
            linewidth=0.8,
            gid="guide__band",
        )
    )
    fig.text(
        0.5,
        fy((layout["chart_top_y"] + layout["chart_bottom_y"]) / 2),
        f"chart band {content_w}×{band_h:g} px — replace build()",
        ha="center",
        va="center",
        fontsize=px_to_pt(SLOT_PX["body"]),
        color=GUIDE_COLOR,
        gid="guide__label",
    )
    return fig


def integer_years(values: list) -> list[int] | None:
    """`values` as whole years, or None when they are not years at all.

    A `year` column may hold the source's own period labels — fiscal years ("2020/21"), dates, week
    codes — and those are a garden job, not a sketch one. Round year ticks are drawn only when the
    axis really is years; anything else keeps matplotlib's own ticks so the sketch still renders.
    """
    years = []
    for value in values:
        try:
            year = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(year) or year != int(year):
            return None
        years.append(int(year))
    return years or None


def placeholder_series(tb: Table) -> tuple[list, list]:
    """The first numeric column against `year` when there is one, else against the row index."""
    numeric = [c for c in tb.columns if c != "year" and pd.api.types.is_numeric_dtype(tb[c])]
    if not numeric:
        return [0, 1], [0, 1]
    y = tb[numeric[0]].astype(float).tolist()
    x = tb["year"].tolist() if "year" in tb.columns else list(range(len(tb)))
    return x, y


if __name__ == "__main__":
    run()
'''
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--slug", required=True, help="snake_case name; becomes the sketch dir and later the step short_name"
    )
    parser.add_argument("--data", required=True, type=Path, help="CSV, Excel, parquet or feather file to sketch from")
    parser.add_argument(
        "--template",
        action="append",
        required=True,
        dest="templates",
        choices=sorted(TEMPLATES),
        help="static-chart template to render; repeat for a desktop/mobile pair (the first is the unsuffixed frame)",
    )
    parser.add_argument("--source", default="<Producer> (<year>)", help='the Data source string, e.g. "WHO (2026)"')
    parser.add_argument("--title", help="chart title; defaults to the slug, humanised")
    parser.add_argument("--author", help="name on the licence line; defaults to git config user.name")
    parser.add_argument(
        "--out-root", type=Path, default=DEFAULT_OUT_ROOT, help=f"parent dir (default {DEFAULT_OUT_ROOT})"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing sketch.py; the frames it rendered are cleared too, every other file stays",
    )
    args = parser.parse_args()

    problems = validate(args.slug, args.data, args.templates)
    if problems:
        for problem in problems:
            print(f"error: {problem}", file=sys.stderr)
        return 2

    sketch_dir = args.out_root / args.slug
    sketch_path = sketch_dir / "sketch.py"
    # The directory may already exist without being a sketch: SKETCHING.md §0 pulls the data into it first.
    if sketch_path.exists() and not args.force:
        print(f"error: {sketch_path} exists; pass --force to overwrite", file=sys.stderr)
        return 2
    # A stale frame from an earlier render would pass the verifier, which scans every SVG in the dir.
    # Only a sketch being replaced has frames to clear: on a first scaffold every file here is someone
    # else's, including one already named `<slug>.svg` — the old chart, kept to compare against.
    cleared = clear_frames(sketch_dir, args.slug) if sketch_path.is_file() else []
    sketch_dir.mkdir(parents=True, exist_ok=True)
    data_copy = sketch_dir / args.data.name
    if not (data_copy.exists() and data_copy.samefile(args.data)):
        shutil.copy2(args.data, data_copy)

    title = args.title or args.slug.replace("_", " ").capitalize()
    sketch_path.write_text(
        SCAFFOLD.substitute(
            TITLE=doc_text(title),
            TITLE_LITERAL=py_literal(title),
            DATA_FILE=doc_text(args.data.name),
            DATA_FILE_LITERAL=py_literal(args.data.name),
            SOURCE_LITERAL=py_literal(args.source),
            AUTHOR_LITERAL=py_literal(args.author or git_user_name() or "[Name of author]"),
            SKETCH_PATH=doc_text(display_path(sketch_path)),
            SKETCH_STEM=doc_text(display_path(sketch_dir / args.slug)),
            FIRST_TEMPLATE=args.templates[0],
            LAYOUTS_BLOCK=layouts_block(args.slug, args.templates),
            DATE=date.today().isoformat(),
        )
    )
    ruff_format(sketch_path)
    if cleared:
        print(f"Cleared stale frames: {', '.join(p.name for p in cleared)}")

    print(
        f"Scaffolded {display_path(sketch_path)} (data: {args.data.name}; frames: {', '.join(output_names(args.slug, args.templates))})"
    )
    print(
        "Skipped, by design — say so when reporting: branch, worktree, PR, DAG entry, the Step 2 newer-data "
        "check, the tracker question. Promotion runs all of them."
    )
    print("Next:")
    print(f"  .venv/bin/python {display_path(sketch_path)}")
    # One stem per frame rather than the directory: the verifier scans every SVG it is given, and this
    # directory may hold images that are not this sketch's — the old chart, a reference export.
    for name, template in zip(output_names(args.slug, args.templates), args.templates):
        print(
            f"  .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py "
            f"{display_path(sketch_dir / name)} --template {template} --expect-gid line__placeholder"
        )
    print(f"  read {display_path(sketch_dir / (args.slug + '.png'))}")
    return 0


def validate(slug: str, data: Path, templates: list[str]) -> list[str]:
    problems = []
    if not SLUG.match(slug):
        problems.append(f"slug {slug!r} must match {SLUG.pattern} (snake_case, like a step short_name)")
    hint = next((s for s in FILENAME_TEMPLATE_HINTS if slug.endswith(s)), None)
    if hint:
        problems.append(
            f"slug {slug!r} ends in {hint!r}, which the verifier reads as a template hint; pick another name"
        )
    if not data.is_file():
        problems.append(f"data file {data} does not exist")
    elif data.suffix.lower() not in DATA_SUFFIXES:
        problems.append(f"data file {data.name} must be one of {sorted(DATA_SUFFIXES)}")
    if len(templates) != len(set(templates)):
        problems.append("each --template may be given once")
    return problems


def output_names(slug: str, templates: list[str]) -> list[str]:
    """The first template is the unsuffixed frame; the rest carry the verifier's suffixes."""
    return [slug if i == 0 else f"{slug}_{t.replace('-', '_')}" for i, t in enumerate(templates)]


def layouts_block(slug: str, templates: list[str]) -> str:
    entries = []
    for name, template in zip(output_names(slug, templates), templates):
        slots = SLOTS[template]
        entries.append(
            f'    "{name}": {{\n'
            f'        "template": "{template}",\n'
            f'        "margin": 16,\n'
            f'        "title_y": 16,\n'
            f'        "subtitle_y": 80,\n'
            f'        "chart_top_y": 118,\n'
            f'        "chart_bottom_y": {slots["chart_bottom_y"]},\n'
            f'        "note_y": {slots["note_y"]},\n'
            f'        "source_y": {slots["source_y"]},\n'
            f'        "footer_y": {slots["footer_y"]},\n'
            f'        "full_footer": {slots["full_footer"]},\n'
            f"    }},\n"
        )
    return "LAYOUTS = {\n" + "".join(entries) + "}"


def py_literal(value: str) -> str:
    """A double-quoted Python string literal for `value`, whatever quotes or backslashes it holds.

    JSON's string escapes are a subset of Python's, so `json.dumps` is exact; `ruff format` then picks
    the quote style.
    """
    return json.dumps(value, ensure_ascii=False)


def doc_text(value: str) -> str:
    """`value` as it can sit inside the scaffold's (non-raw, triple-double-quoted) docstring."""
    return value.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def frame_stems(sketch_dir: Path, slug: str) -> list[str]:
    """Every output stem whose frames the scaffold owns here: `slug` and its per-template suffixes,
    plus every `LAYOUTS` key of the sketch being replaced.

    A §4 variant is a key someone adds by hand, and it renders a pair of its own, so the file being
    replaced is the only thing that can say what it emitted.
    """
    stems = {slug, *(f"{slug}_{t.replace('-', '_')}" for t in TEMPLATES)}
    existing = sketch_dir / "sketch.py"
    if existing.is_file():
        registry = LAYOUTS_REGISTRY.search(existing.read_text())
        if registry:
            stems.update(LAYOUTS_KEY.findall(registry.group(1)))
    return sorted(stems)


def clear_frames(sketch_dir: Path, slug: str) -> list[Path]:
    """Remove the frames the scaffold itself rendered here, and return what was removed.

    Only stems the scaffold owns go. A reference image, a hand-edited SVG or notes the person left
    beside the sketch are not this script's to delete, whatever their extension — the directory is
    gitignored, so nothing removed here comes back.
    """
    removed = []
    for stem in frame_stems(sketch_dir, slug):
        for frame in (sketch_dir / f"{stem}.svg", sketch_dir / f"{stem}.png"):
            if frame.is_file():
                frame.unlink()
                removed.append(frame)
    return removed


def display_path(path: Path) -> str:
    """Repo-relative where possible, so the printed commands paste into a shell at the repo root."""
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def git_user_name() -> str | None:
    try:
        name = subprocess.run(
            ["git", "config", "user.name"], capture_output=True, text=True, check=False
        ).stdout.strip()
    except OSError:
        return None
    return name or None


def ruff_format(path: Path) -> None:
    """Canonical formatting for the emitted file — it will land under `etl/steps`, which `make check` lints."""
    ruff = Path(sys.executable).with_name("ruff")
    if ruff.exists():
        subprocess.run([str(ruff), "format", "--quiet", str(path)], check=False)


if __name__ == "__main__":
    sys.exit(main())
