"""Check that a static_viz step's emitted files honor the Figma handoff contract.

The contract exists because each of these failures is silent — the step runs, the PNG looks
plausible, and the problem only shows up once a designer opens the SVG:

- clipping left on, so shapes arrive cropped at the axes boundary
- text saved as outlines, so none of the copy can be edited or restyled
- unnamed nodes, so the layer panel is a list of "Path 41"
- a frame that no longer matches the template it targets, because something reintroduced
  ``bbox_inches="tight"`` and cropped the canvas to its content

Usage:
    verify_static_viz.py <step-dir-or-file-stem> [--template horizontal|vertical|mobile|mobile-square]
    verify_static_viz.py <step-dir> --template horizontal --expect-gid boys__median

A step directory is scanned for every ``<name>.svg`` with a sibling ``<name>.png``; a file stem
checks just that pair. Exits non-zero on the first failing pair, listing every failure for it.
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]

# Frame proportions of the static-chart templates, from TEMPLATES.md. A saved image should match
# its target to within TOLERANCE; anything looser stops catching a tight-bbox crop.
TEMPLATE_RATIOS = {
    "horizontal": (850, 638),
    "vertical": (850, 1095),
    "mobile": (540, 824),
    "mobile-square": (540, 540),
}
TOLERANCE = 0.002

# matplotlib names its own nodes figure_1 / axes_1 / line2d_3 / patch_7 / text_12. Those are not
# the named layers we are after, so they do not count toward the gid check.
GENERATED_ID = re.compile(r"^(figure|axes|line2d|patch|text|PathCollection|QuadMesh|xtick|ytick)_\d+$")

# A step commonly emits several frames from one directory (a desktop and a mobile version), so a
# single --template cannot cover the directory. Take the template from the filename suffix where
# there is one, and fall back to --template for the unsuffixed desktop frame.
FILENAME_TEMPLATE_HINTS = {
    "_mobile_square": "mobile-square",
    "_square": "mobile-square",
    "_mobile": "mobile",
    "_vertical": "vertical",
    "_horizontal": "horizontal",
}


def template_for(stem: str, default: str | None) -> str | None:
    """Template a given output should match, from its filename suffix, else the default."""
    for suffix, template in FILENAME_TEMPLATE_HINTS.items():
        if stem.endswith(suffix):
            return template
    return default


def check_svg(path: Path, expected_gids: list[str]) -> list[str]:
    """Return a list of contract failures for one SVG."""
    svg = path.read_text()
    failures = []

    clip_paths = svg.count("<clipPath")
    clip_refs = svg.count("clip-path=")
    if clip_paths or clip_refs:
        failures.append(
            f"clipping is on: {clip_paths} <clipPath> definitions, {clip_refs} clip-path references. "
            'Sweep it off before saving: for artist in fig.findobj(): artist.set_clip_on(False)'
        )

    n_text = len(re.findall(r"<text[ >]", svg))
    if n_text == 0:
        failures.append(
            'no <text> elements — the copy has been saved as outlines. '
            'Set matplotlib.rcParams["svg.fonttype"] = "none".'
        )

    # Glyphs rendered as <use> references to <symbol> paths mean fonttype is not "none" either,
    # even when some stray <text> exists.
    if n_text and svg.count("<use ") > n_text:
        failures.append(
            f"more <use> glyph references ({svg.count('<use ')}) than <text> elements ({n_text}) — "
            'text is being outlined. Check svg.fonttype = "none".'
        )

    ids = set(re.findall(r'\bid="([^"]+)"', svg))
    named = {i for i in ids if not GENERATED_ID.match(i)}
    if not named:
        failures.append(
            "no named nodes — Figma will show a layer list of anonymous paths. "
            "Pass gid= on each artist (ax.plot(..., gid='boys__median'))."
        )
    for gid in expected_gids:
        if gid not in ids:
            failures.append(f"expected gid {gid!r} is not an id in the SVG")

    return failures


def check_png(path: Path, template: str | None) -> list[str]:
    """Return a list of contract failures for one PNG."""
    if template is None:
        return []
    try:
        from PIL import Image
    except ImportError:
        return ["Pillow is not available, so the frame proportion could not be checked"]

    width, height = Image.open(path).size
    target_w, target_h = TEMPLATE_RATIOS[template]
    actual, target = width / height, target_w / target_h
    if abs(actual - target) > TOLERANCE:
        return [
            f"frame is {width}x{height} (ratio {actual:.4f}) but the {template} template is "
            f"{target_w}x{target_h} (ratio {target:.4f}). Something cropped the canvas — most "
            'likely bbox_inches="tight" was passed to export_fig.'
        ]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("target", help="a static_viz step directory, or a file stem without extension")
    parser.add_argument(
        "--template",
        choices=sorted(TEMPLATE_RATIOS),
        help=(
            "template whose proportions the PNG must match. A filename suffix (_mobile, _vertical, "
            "_square) wins over this, so a directory holding several frames checks each correctly; "
            "omit to skip the frame check entirely."
        ),
    )
    parser.add_argument(
        "--expect-gid",
        action="append",
        default=[],
        metavar="GID",
        help="a gid that must appear as an SVG id; repeatable",
    )
    args = parser.parse_args()

    target = Path(args.target)
    if not target.is_absolute():
        candidate = REPO_ROOT / target
        target = candidate if candidate.exists() or candidate.parent.exists() else target

    if target.is_dir():
        svgs = sorted(target.glob("*.svg"))
    else:
        svgs = [target.with_suffix(".svg")]
    svgs = [s for s in svgs if s.exists()]

    if not svgs:
        print(f"No SVG found for {args.target}", file=sys.stderr)
        return 2

    failed = False
    for svg in svgs:
        template = template_for(svg.stem, args.template)
        failures = check_svg(svg, args.expect_gid)
        png = svg.with_suffix(".png")
        if png.exists():
            failures += check_png(png, template)
        else:
            failures.append(f"no sibling PNG at {png.name} — export_fig should emit both formats")

        if failures:
            failed = True
            print(f"FAIL {svg.name}")
            for failure in failures:
                print(f"  - {failure}")
        else:
            extra = f", frame matches {template}" if template else ""
            named = len({i for i in re.findall(r'\bid="([^"]+)"', svg.read_text()) if not GENERATED_ID.match(i)})
            print(f"OK   {svg.name} (0 clip paths, {named} named layers{extra})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
