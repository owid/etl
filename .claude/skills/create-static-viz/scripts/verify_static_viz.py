"""Check that a static_viz step's emitted files honor the Figma handoff contract.

The contract exists because each of these failures is silent — the step runs, the PNG looks
plausible, and the problem only shows up once a designer opens the SVG:

- clipping left on, so shapes arrive cropped at the axes boundary
- text saved as outlines, so none of the copy can be edited or restyled
- unnamed nodes, so the layer panel is a list of "Path 41"; pass ``--expect-gid`` to check the
  layers that matter, since the unaided check can only catch a figure with no deliberate name at
  all (which artists *should* carry one is not knowable from the SVG — matplotlib emits a
  ``line2d_<n>`` group per tick mark just as it does for an unnamed data line)
- a frame that no longer matches the template it targets, because something reintroduced
  ``bbox_inches="tight"`` and cropped the canvas to its content. Checked on **both** the PNG and the
  SVG: the contract asks for two separate ``export_fig`` calls (the PNG opaque, the SVG transparent),
  so the two can be saved with different arguments and only one of them end up cropped — and the SVG
  is the file Figma consumes.

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

# matplotlib names its own nodes, and those names must not count toward the gid check — otherwise
# an SVG with no deliberate gid at all still looks "named" and passes. Three families, all
# auto-generated:
#   <class>_<n>   figure_1, axes_1, line2d_3, patch_7, text_12, legend_1, xtick_5,
#                 PathCollection_1, and the dotted matplotlib.axis_1 / matplotlib.axis_2
#   m<hash>       hashed <defs> paths for reused markers, e.g. m795dccd146
#   <Font>-<hex>  outlined-glyph defs when svg.fonttype is not "none", e.g. DejaVuSans-30
# The <class>_<n> rule is deliberately generic rather than an enumerated class list, which was
# always going to lag matplotlib. The trade-off: a deliberate gid ending in an underscore and
# digits (say "panel_1") is treated as generated. Name layers <subject>__<role> and it never bites.
GENERATED_ID = re.compile(
    r"^(?:[A-Za-z][A-Za-z0-9.]*_\d+|m[0-9a-f]{8,}|[A-Za-z][A-Za-z0-9]*-[0-9a-f]{2,6})$"
)

# A <use> pointing at one of those outlined-glyph defs means text was saved as curves. A <use>
# pointing at an m<hash> marker def is just normal marker reuse in a scatter, and must never be
# mistaken for it — so resolve each reference rather than comparing global <use>/<text> counts.
GLYPH_DEF_REF = re.compile(r"^[A-Za-z][A-Za-z0-9]*-[0-9a-f]{2,6}$")

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

# The saved SVG's own canvas, to check its proportions the way the PNG's are checked. matplotlib
# writes the root as width="612pt" height="459.36pt" viewBox="0 0 612 459.36"; take the width/height
# pair when it is there and fall back to the viewBox, since either alone pins the ratio.
SVG_ROOT = re.compile(r"<svg\b[^>]*>")
SVG_LENGTH = re.compile(r'\b(width|height)="([0-9.]+)(?:pt|px|mm|cm|in|pc)?"')
SVG_VIEWBOX = re.compile(r'\bviewBox="\s*[-+0-9.eE]+[,\s]+[-+0-9.eE]+[,\s]+([0-9.eE+]+)[,\s]+([0-9.eE+]+)')


def template_for(stem: str, default: str | None) -> str | None:
    """Template a given output should match, from its filename suffix, else the default."""
    for suffix, template in FILENAME_TEMPLATE_HINTS.items():
        if stem.endswith(suffix):
            return template
    return default


def frame_failures(kind: str, width: float, height: float, template: str) -> list[str]:
    """Return a failure if one saved frame's proportions do not match the template it targets."""
    target_w, target_h = TEMPLATE_RATIOS[template]
    actual, target = width / height, target_w / target_h
    if abs(actual - target) > TOLERANCE:
        return [
            f"{kind} frame is {width:g}x{height:g} (ratio {actual:.4f}) but the {template} template "
            f"is {target_w}x{target_h} (ratio {target:.4f}). Something cropped the canvas — most "
            'likely bbox_inches="tight" was passed to export_fig.'
        ]
    return []


def svg_frame_size(svg: str) -> tuple[float, float] | None:
    """Canvas size of an SVG from its root element, or None when it declares neither form."""
    root = SVG_ROOT.search(svg)
    if root is None:
        return None
    lengths = {name: float(value) for name, value in SVG_LENGTH.findall(root.group())}
    if "width" in lengths and "height" in lengths and lengths["width"] and lengths["height"]:
        return lengths["width"], lengths["height"]
    viewbox = SVG_VIEWBOX.search(root.group())
    if viewbox is not None:
        width, height = float(viewbox.group(1)), float(viewbox.group(2))
        if width and height:
            return width, height
    return None


def check_svg(path: Path, expected_gids: list[str], template: str | None) -> list[str]:
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

    # Text saved as curves shows up as <use> references to glyph defs. Catch that even when some
    # stray <text> survives, without tripping over a scatter's reused marker defs.
    glyph_refs = [ref for ref in re.findall(r"<use[^>]*?href=\"#([^\"]+)\"", svg) if GLYPH_DEF_REF.match(ref)]
    if glyph_refs:
        failures.append(
            f"{len(glyph_refs)} <use> references point at outlined-glyph defs "
            f"({', '.join(sorted(set(glyph_refs))[:3])}...) — text is being saved as curves. "
            'Set matplotlib.rcParams["svg.fonttype"] = "none".'
        )

    ids = set(re.findall(r'\bid="([^"]+)"', svg))
    named = {i for i in ids if not GENERATED_ID.match(i)}
    if not named:
        generated = sorted(i for i in ids if GENERATED_ID.match(i))
        failures.append(
            "no named nodes — every id is one matplotlib generated itself "
            f"({', '.join(generated[:4])}...), so Figma will show a layer list of anonymous paths. "
            "Pass gid= on each artist (ax.plot(..., gid='boys__median'))."
        )
    for gid in expected_gids:
        if gid not in ids:
            failures.append(f"expected gid {gid!r} is not an id in the SVG")

    if template is not None:
        size = svg_frame_size(svg)
        if size is None:
            failures.append(
                "the SVG root declares neither a width/height pair nor a viewBox, so its frame "
                "proportion could not be checked"
            )
        else:
            failures += frame_failures("SVG", size[0], size[1], template)

    return failures


def check_png(path: Path, template: str | None) -> list[str]:
    """Return a list of contract failures for one PNG."""
    if template is None:
        return []
    try:
        from PIL import Image
    except ImportError:
        return ["Pillow is not available, so the PNG frame proportion could not be checked"]

    width, height = Image.open(path).size
    return frame_failures("PNG", width, height, template)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("target", help="a static_viz step directory, or a file stem without extension")
    parser.add_argument(
        "--template",
        choices=sorted(TEMPLATE_RATIOS),
        help=(
            "template whose proportions both saved formats must match. A filename suffix (_mobile, "
            "_vertical, _square) wins over this, so a directory holding several frames checks each "
            "correctly; omit to skip the frame check entirely."
        ),
    )
    parser.add_argument(
        "--expect-gid",
        action="append",
        default=[],
        metavar="GID",
        help=(
            "a gid that must appear as an SVG id; repeatable. Pass the chart's data layers: without "
            "them the naming check only proves some node was named, which a figure with a named "
            "title and an unnamed data line already satisfies."
        ),
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
        failures = check_svg(svg, args.expect_gid, template)
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
