"""Check that a static_viz step's emitted files honor the Figma handoff contract.

The contract exists because each of these failures is silent — the step runs, the PNG looks
plausible, and the problem only shows up once a designer opens the SVG:

- clipping left on, so shapes arrive cropped at the axes boundary
- text saved as outlines, so none of the copy can be edited or restyled
- unnamed nodes, so the layer panel is a list of "Path 41"; pass ``--expect-gid`` to check the
  layers that matter, since the unaided check can only catch a figure with no deliberate name at
  all (which artists *should* carry one is not knowable from the SVG — matplotlib emits a
  ``line2d_<n>`` group per tick mark just as it does for an unnamed data line)
- paint with no deliberate name above it. Figma's re-import (`restyle_static_import.js` with
  ``inheritStyles``) learns which library style each colour carries by joining nodes on their
  ``<subject>__<role>`` gid, so a gridline, spine or tick label drawn without one cannot be paired
  with the style a designer bound on the previous frame. Reported on every run; pass
  ``--require-named-marks`` to make it a failure, which a new or refreshed step should. That flag
  also fails on a ``<subject>__<role>`` gid used by more than one node, since the re-import cannot
  tell such marks apart
- axes that differ from grapher's own download, which is what the design team's templates are
  balanced for: tick labels not at 15px on a 540-wide template (14px on an 850-wide one), y-axis tick
  labels not right-aligned against the plot, or a year axis with no tick marks (each axes judged on
  its own; an x axis is a year axis when its labels are bare integers, one of four digits, since
  grapher writes any other number with a thousands separator, unless an era suffix makes it a year,
  as in "12,000 BCE"). Judged on the ``<subject>__<axis>-tick-label-<value>`` and
  ``<subject>__x-tick-<value>`` gids ``etl.viz.static.name_furniture`` gives every axis, reading a
  label's text however deeply it is nested (MathText labels such as ``$10^5$`` are)
- a time axis (years, or dates however they are written) whose furniture differs from grapher's: tick
  marks not 5px long, 1px and ``#999``, or a horizontal stroke along the tick tops (the spine, a zero
  gridline, or a line the step drew itself) that is not 1px ``#999``. The marks are found by where
  matplotlib puts them (``xtick_<n>`` groups), not by gid, so a mark or line named by the step is judged
  too. Dates written in any form but grapher's (``Feb``, ``Feb 2023``, ``Feb 24``, ``Feb 24, 2023``)
  fail, as ``24 Feb 2023`` or ``2023-02-24`` would. A sub-yearly axis on grapher's calendar grid, marked
  ``<subject>__x-axis-calendar`` by ``style_time_axis(..., inset_ends=False)``, needs tick marks but is
  spared the year axis' end checks below, since grapher adds no first and last date to it
- a year axis that stops short of the data: a ``line__<Entity>`` series running on past the axis' last
  labelled tick. Grapher's time axis shows the data's first and last year as ticks, so the last tick
  is the last year plotted, never a round year before it
- a year axis whose ends differ from grapher's (a warning, not a failure, since a bar chart over
  years rightly pads its range and centers its labels under the bars, and an area or band read as no
  ``line__`` series may rightly carry the axis past the lines): labels past the data, where no series
  line reaches the first or last labelled year; the first tick label not left-aligned to its tick or the last not
  right-aligned (within half a template px), or a bottom spine that runs past the end ticks, through
  x-range padding or a projecting cap, or is not 1px wide
- a frame that no longer matches the template it targets, because something reintroduced
  ``bbox_inches="tight"`` and cropped the canvas to its content. Checked on **both** the PNG and the
  SVG: the contract asks for two separate ``export_fig`` calls (the PNG opaque, the SVG transparent),
  so the two can be saved with different arguments and only one of them end up cropped — and the SVG
  is the file Figma consumes. A layout that deliberately runs shorter or taller than its template
  declares its own size with ``--size <stem>=<W>x<H>``, which replaces the template's ratio for that
  frame.

Usage:
    verify_static_viz.py <step-dir-or-file-stem> [--template horizontal|vertical|mobile|mobile-square]
    verify_static_viz.py <step-dir> --template horizontal --expect-gid boys__median
    verify_static_viz.py <step-dir> --require-named-marks
    verify_static_viz.py <step-dir> --template horizontal --size height_for_age=850x615

A step directory is scanned for every ``<name>.svg`` with a sibling ``<name>.png``; a file stem
checks just that pair. Exits non-zero on the first failing pair, listing every failure for it.
"""

import argparse
import datetime as dt
import math
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import NamedTuple

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
# Template px per inch the steps lay out at (etl.viz.static.PIXELS_PER_INCH), to judge an SVG's scale.
PIXELS_PER_INCH = 100

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
GENERATED_ID = re.compile(r"^(?:[A-Za-z][A-Za-z0-9.]*_\d+|m[0-9a-f]{8,}|[A-Za-z][A-Za-z0-9]*-[0-9a-f]{2,6})$")

# A deliberate layer name in the `<subject>__<role>` convention, which is what the Figma re-import joins
# on. The template's own text slots are exempt: the import drops the step's copies of them by prefix.
DELIBERATE_GID = re.compile(r"__")
SLOT_ID = re.compile(r"^(?:title|subtitle|note|data-source|tagline|license)(?:-|$)")
PAINTABLE = {"path", "rect", "circle", "ellipse", "line", "polyline", "polygon", "text", "use", "image"}
STYLE_DECL = re.compile(r"\s*([a-z-]+)\s*:\s*([^;]+)")

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


def parse_size(value: str) -> tuple[str, tuple[int, int]]:
    """Parse a `--size <stem>=<W>x<H>` argument into the stem and its declared frame size."""
    stem, sep, size = value.partition("=")
    width, x, height = size.partition("x")
    if not (sep and x and stem and width.isdigit() and height.isdigit() and int(width) and int(height)):
        raise argparse.ArgumentTypeError(f"expected <stem>=<W>x<H>, e.g. height_for_age=850x615; got {value!r}")
    return stem, (int(width), int(height))


def frame_target(
    stem: str, template: str | None, sizes: dict[str, tuple[int, int]]
) -> tuple[str, tuple[int, int]] | None:
    """What one frame's proportions should match, and what to call it: a declared size wins over a template."""
    if stem in sizes:
        return "declared", sizes[stem]
    if template is not None:
        return f"{template} template", TEMPLATE_RATIOS[template]
    return None


def frame_failures(kind: str, width: float, height: float, target: tuple[str, tuple[int, int]]) -> list[str]:
    """Return a failure if one saved frame's proportions do not match the size it targets."""
    name, (target_w, target_h) = target
    actual, ratio = width / height, target_w / target_h
    if abs(actual - ratio) > TOLERANCE:
        return [
            f"{kind} frame is {width:g}x{height:g} (ratio {actual:.4f}) but the {name} size "
            f"is {target_w}x{target_h} (ratio {ratio:.4f}). Something cropped the canvas — most "
            'likely bbox_inches="tight" was passed to export_fig. A layout sized off the template on '
            "purpose declares its size with --size <stem>=<W>x<H>."
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


def _paint(node: ET.Element, inherited: dict[str, str]) -> dict[str, str]:
    """The fill, stroke and opacity a node paints with, after SVG inheritance."""
    paint = dict(inherited)
    for prop in ("fill", "stroke", "opacity", "fill-opacity", "stroke-opacity"):
        if node.get(prop) is not None:
            paint[prop] = node.get(prop).strip()
    for prop, value in STYLE_DECL.findall(node.get("style") or ""):
        paint[prop] = value.strip()
    return paint


def _paints(paint: dict[str, str]) -> bool:
    """Whether a mark puts any pixel down: a fill or a stroke that is not none, at non-zero opacity."""
    if paint.get("opacity", "1") in ("0", "0.0"):
        return False
    fill = paint.get("fill", "#000000") != "none" and paint.get("fill-opacity", "1") not in ("0", "0.0")
    stroke = paint.get("stroke", "none") != "none" and paint.get("stroke-opacity", "1") not in ("0", "0.0")
    return fill or stroke


def unnamed_marks(svg: str) -> dict[str, int]:
    """Painted marks with no `<subject>__<role>` gid on themselves or any ancestor, by nearest id.

    The nearest id is matplotlib's own (`line2d_7`, `patch_3`, `text_12`), which names the artist to
    give a gid to. `<defs>` are skipped: what paints is the `<use>` that places a def, and that is
    judged where it stands.
    """
    root = ET.fromstring(svg)
    found: dict[str, int] = {}

    def walk(node: ET.Element, named: bool, nearest: str, inherited: dict[str, str]) -> None:
        tag = node.tag.rsplit("}", 1)[-1]
        if tag == "defs":
            return
        node_id = node.get("id")
        if node_id:
            nearest = node_id
            if DELIBERATE_GID.search(node_id) or SLOT_ID.match(node_id):
                named = True
        paint = _paint(node, inherited)
        if tag in PAINTABLE and not named and _paints(paint):
            label = f"{nearest} <{tag}>"
            found[label] = found.get(label, 0) + 1
        for child in node:
            walk(child, named, nearest, paint)

    walk(root, False, "(root)", {})
    return found


def check_svg(
    path: Path,
    expected_gids: list[str],
    target: tuple[str, tuple[int, int]] | None,
    require_named_marks: bool = False,
) -> list[str]:
    """Return a list of contract failures for one SVG."""
    svg = path.read_text()
    failures = []

    clip_paths = svg.count("<clipPath")
    clip_refs = svg.count("clip-path=")
    if clip_paths or clip_refs:
        failures.append(
            f"clipping is on: {clip_paths} <clipPath> definitions, {clip_refs} clip-path references. "
            "Sweep it off before saving: for artist in fig.findobj(): artist.set_clip_on(False)"
        )

    n_text = len(re.findall(r"<text[ >]", svg))
    if n_text == 0:
        failures.append(
            "no <text> elements — the copy has been saved as outlines. "
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

    if require_named_marks:
        unnamed = unnamed_marks(svg)
        if unnamed:
            listed = ", ".join(f"{label} x{n}" for label, n in sorted(unnamed.items())[:6])
            failures.append(
                f"{sum(unnamed.values())} painted mark(s) sit under no <subject>__<role> gid ({listed}"
                f"{', ...' if len(unnamed) > 6 else ''}). Name each artist, furniture included "
                "(gridlines, spines, tick labels): Figma's re-import pairs a mark with its previous style "
                "by that name, so an unnamed one loses any style a designer bound on it."
            )
        all_ids = re.findall(r'\bid="([^"]+)"', svg)
        repeated = sorted({i for i in all_ids if DELIBERATE_GID.search(i) and all_ids.count(i) > 1})
        if repeated:
            failures.append(
                f"{len(repeated)} <subject>__<role> gid(s) are used by more than one node "
                f"({', '.join(repeated[:6])}{', ...' if len(repeated) > 6 else ''}). Give each artist "
                "its own name: the re-import pairs marks by name, so marks that share one cannot be told apart. "
                "A gid= passed to ax.bar lands on every bar; set one per bar from the returned container."
            )

    if target is not None:
        size = svg_frame_size(svg)
        if size is None:
            failures.append(
                "the SVG root declares neither a width/height pair nor a viewBox, so its frame "
                "proportion could not be checked"
            )
        else:
            failures += frame_failures("SVG", size[0], size[1], target)
        failures += scale_failures(svg, target)
    failures += axis_failures(svg, target)

    return failures


# Tick-label size, in template px, by template width (WRITING-THE-STEP.md -> grapher's axis): 15 on
# the 540-wide templates, 14 on the 850-wide ones. A label within LABEL_TOLERANCE of it passes.
TICK_LABEL_PX = {540: 15.0, 850: 14.0}
LABEL_TOLERANCE = 0.3
# Groups: subject, axis, side. Major and minor ticks alike (`name_furniture` names both), since an axis
# can be labelled on its minor ticks alone.
TICK_LABEL_GID = re.compile(r"^(.+)__([xy])-(?:minor-)?tick-label(2?)-.+$")  # label2: the right (or top) side
X_LABEL_VALUE = re.compile(r"^(.+)__x-(?:minor-)?tick-label-(.+)$")
X_MARK_VALUE = re.compile(r"^(.+)__x-(?:minor-)?tick-(?!label)(?!2-)(.+)$")
# What marks a y axis' plot edge: its gridlines, tick marks and left spine. Groups: subject.
Y_EDGE_GID = re.compile(r"^(.+)__(?:y-(?:minor-)?(?:grid|tick)-(?!label)|spine-left$)")
NUMBER = re.compile(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")
FONT_SIZE = re.compile(r"font-size:\s*([0-9.]+)px")
TEXT_ANCHOR = re.compile(r"text-anchor:\s*(\w+)")
TRANSFORM_STEP = re.compile(r"(\w+)\(([^)]*)\)")


class TickLabel(NamedTuple):
    subject: str
    axis: str
    anchor: str | None  # None when no text node declares one, as for a MathText label
    size: float | None  # the largest font size in the label: a superscript is smaller on purpose
    text: str
    # A MathText label's left edge and the x of its last glyph from there, in SVG units; None otherwise.
    left: float | None = None
    extent: float | None = None
    # That last glyph's font size, to estimate how far past its x the label ends.
    last_size: float | None = None
    # Drawn at an angle, plain or MathText; a MathText label placed by more than a translate counts too,
    # since where it sits then says nothing of its alignment.
    rotated: bool = False
    # Drawn on the axis' second side (`labelright=True`, `labeltop=True`): matplotlib's `tick-label2`.
    second: bool = False


def tick_labels(root: ET.Element) -> list[TickLabel]:
    """Every `<subject>__<axis>-tick-label-<value>` group, read from its text however deeply it is nested.

    A plain label is one `<text>` straight under the group. A MathText label (`$10^5$`) is a comment,
    a transform group, and a `<text>` holding one `<tspan>` per glyph, with no text-anchor (matplotlib
    places it by the transform). Glyphs on another baseline (a superscript or subscript) are joined
    with a space, so `10^5` reads `10 5` and never passes for the year `105`. A MathText label also
    carries where it sits (`left`, `extent`), the only trace its alignment leaves (`mathtext_anchor`).
    """
    labels = []
    for group in root.iter():
        match = TICK_LABEL_GID.match(group.get("id") or "")
        if match is None or group.tag.rsplit("}", 1)[-1] != "g":
            continue
        parts: list[str] = []
        sizes: list[float] = []
        glyph_xs: list[float] = []
        glyph_sizes: list[float | None] = []
        anchor = None
        baseline = None
        for node in group.iter():
            tag = node.tag.rsplit("}", 1)[-1]
            if tag not in ("text", "tspan"):
                continue
            style = node.get("style") or ""
            sizes += [float(s) for s in FONT_SIZE.findall(style)]
            if anchor is None and (m := TEXT_ANCHOR.search(style)):
                anchor = m.group(1)
            if node.text and node.text.strip():
                if tag == "tspan":
                    if parts and node.get("y") != baseline:
                        parts.append(" ")
                    baseline = node.get("y")
                    x = node.get("x")
                    if x and re.fullmatch(r"[-+0-9.eE]+", x):
                        glyph_xs.append(float(x))
                        glyph_sizes.append(max((float(f) for f in FONT_SIZE.findall(style)), default=None))
                parts.append(node.text.strip())
        left = extent = last_size = None
        rotated = _rotated(group)
        if anchor is None and glyph_xs:
            offset = _translate_x(group)
            if offset is None:
                rotated = True
            else:
                left, extent = offset + min(glyph_xs), max(glyph_xs) - min(glyph_xs)
                last_size = glyph_sizes[glyph_xs.index(max(glyph_xs))]
        size = max(sizes, default=None)
        labels.append(
            TickLabel(
                match.group(1),
                match.group(2),
                anchor,
                size,
                "".join(parts),
                left,
                extent,
                last_size,
                rotated,
                second=match.group(3) == "2",
            )
        )
    return labels


def _rotated(group: ET.Element) -> bool:
    """Whether any transform in a label group turns it: a `rotate()` by anything but zero.

    matplotlib writes a horizontal plain label with `rotate(-0 x y)` on its `<text>`, a no-op, and a
    rotated one with `translate(x y) rotate(-45)` there or on a MathText label's group.
    """
    for node in group.iter():
        for name, args in TRANSFORM_STEP.findall(node.get("transform") or ""):
            numbers = [float(n) for n in NUMBER.findall(args)]
            if name == "rotate" and numbers and numbers[0] != 0:
                return True
    return False


def _translate_x(group: ET.Element) -> float | None:
    """The x offset the transforms under a label group add up to, or None if any does more than translate.

    matplotlib writes a MathText label's transform as `translate(x y)`, followed by `rotate(-45)` when
    the label is rotated; a rotation of zero is a no-op and is ignored.
    """
    total = 0.0
    for node in group.iter():
        transform = node.get("transform")
        if node is group or transform is None or node.tag.rsplit("}", 1)[-1] != "g":
            continue
        for name, args in TRANSFORM_STEP.findall(transform):
            numbers = [float(n) for n in NUMBER.findall(args)]
            if name == "translate" and numbers:
                total += numbers[0]
            elif not (name == "rotate" and numbers and numbers[0] == 0):
                return None
    return total


def _draws(node: ET.Element, inherited: dict[str, str] | None = None) -> bool:
    """Whether anything under a node puts a pixel down, after paint inheritance; `<defs>` are skipped."""
    tag = node.tag.rsplit("}", 1)[-1]
    if tag == "defs":
        return False
    paint = _paint(node, inherited or {})
    if tag in PAINTABLE and _paints(paint):
        return True
    return any(_draws(child, paint) for child in node)


def _tick_draws(group: ET.Element, defs: dict[str, ET.Element], inherited: dict[str, str] | None = None) -> bool:
    """Whether a tick-mark group draws a visible mark.

    matplotlib draws a tick as a `<use>` of a line path in `<defs>`. A line has no area, so only its
    stroke can show: the mark is drawn when the stroke, after inheritance from the group and the
    `<use>`, is not none, is not transparent, and is wider than 0. `tick_params(width=0)` leaves the
    `<use>` in place with a strokeless path, which `_draws` would count as a default black fill.
    """
    paint = _paint(group, inherited or {})
    for child in group:
        tag = child.tag.rsplit("}", 1)[-1]
        if tag == "use":
            ref = (child.get("{http://www.w3.org/1999/xlink}href") or child.get("href") or "").lstrip("#")
            target = defs.get(ref)
            mark = _paint(target, _paint(child, paint)) if target is not None else _paint(child, paint)
            if target is not None:
                for prop, value in STYLE_DECL.findall(target.get("style") or ""):
                    mark[prop] = value.strip()
            width = mark.get("stroke-width", "1").replace("px", "")
            try:
                wide = float(width) > 0
            except ValueError:
                wide = True
            stroke = mark.get("stroke", "none") != "none" and mark.get("stroke-opacity", "1") not in ("0", "0.0")
            if stroke and wide and mark.get("opacity", "1") not in ("0", "0.0"):
                return True
        elif tag == "g":
            if _tick_draws(child, defs, paint):
                return True
        elif tag in PAINTABLE and _draws(child, paint):
            return True
    return False


def mathtext_anchor(labels: list[TickLabel]) -> str | None:
    """The alignment one axis' MathText labels were drawn with, read off where they sit, or None.

    matplotlib places each label so that its left edge (start), middle or right edge (end) lands on
    one x shared by the whole axis, so the alignment is the edge that lines up best. The right edge is
    approximated by the last glyph's x, which only differs by that glyph's own advance. It can only be
    told when the labels differ in width, by at least 0.3 of their font size; labels of one width
    line up on every edge, and return None (`mathtext_past_plot_edge` then judges them).
    """
    placed = [label for label in labels if label.left is not None and label.extent is not None]
    if len(placed) < 2:
        return None
    lefts = [label.left for label in placed]
    extents = [label.extent for label in placed]
    size = max((label.size for label in placed if label.size is not None), default=0.0)
    if max(extents) - min(extents) < 0.3 * size or size == 0:
        return None

    def spread(edges: list[float]) -> float:
        return max(edges) - min(edges)

    edges = {
        "start": lefts,
        "middle": [lo + e / 2 for lo, e in zip(lefts, extents)],
        "end": [lo + e for lo, e in zip(lefts, extents)],
    }
    return min(edges, key=lambda name: spread(edges[name]))


def plot_left_edges(root: ET.Element) -> dict[str, float]:
    """The x of each axes' left plot edge, where its y gridlines, y tick marks or left spine start.

    matplotlib writes a gridline or spine as a path in SVG units (`M 48.6 332.4 L 349.9 332.4`) and a
    tick mark as a `<use x=...>` of a mark in `<defs>`, all starting at the plot's left edge. The
    first tick group holds that `<defs>` itself, whose mark sits at its own origin (`M 0 0`), so
    `<defs>` are skipped. A hidden one is not written and a zero-length tick is an empty group, so
    each subject takes the leftmost x of what is there.
    """

    def drawn(node: ET.Element) -> list[ET.Element]:
        if node.tag.rsplit("}", 1)[-1] == "defs":
            return []
        return [node, *(found for child in node for found in drawn(child))]

    edges: dict[str, float] = {}
    for group in root.iter():
        match = Y_EDGE_GID.match(group.get("id") or "")
        if match is None:
            continue
        xs = []
        for node in drawn(group):
            tag = node.tag.rsplit("}", 1)[-1]
            start = re.match(rf"\s*M\s*({NUMBER.pattern})", node.get("d") or "") if tag == "path" else None
            if start:
                xs.append(float(start.group(1)))
            elif tag == "use" and NUMBER.fullmatch(node.get("x") or ""):
                xs.append(float(node.get("x") or 0))
        if xs:
            subject = match.group(1)
            edges[subject] = min([*xs, edges.get(subject, min(xs))])
    return edges


def mathtext_past_plot_edge(labels: list[TickLabel], edge: float | None) -> bool | None:
    """Whether one axis' MathText labels reach past the plot's left edge; None if there is no edge to judge by.

    Labels of one width line up on every edge, so `mathtext_anchor` cannot tell their alignment; what
    still shows is where they end. Right-aligned, matplotlib ends them a pad short of the plot's left
    edge; left-aligned or centred, the same pad is where they start or centre, and they run into the
    plot. A label's right edge is taken as its last glyph's x plus half that glyph's size, short of
    any digit's advance, so a right-aligned label never reads as past the edge.
    """
    placed = [label for label in labels if label.left is not None and label.extent is not None]
    if not placed:
        return False
    if edge is None:
        return None
    return any(
        (label.left or 0) + (label.extent or 0) + 0.5 * (label.last_size or label.size or 0) > edge + 0.01
        for label in placed
    )


# Grapher's x-axis furniture (`grapher/src/axis/AxisViews.tsx`): the axis line and tick marks are 1px,
# `SOLID_TICK_COLOR`, and the marks 5px long.
AXIS_COLOR = "#999999"
TICK_MARK_PX = 5.0
FURNITURE_TOLERANCE_PX = 0.5
XTICK_GROUP = re.compile(r"^xtick_\d+$")
CALENDAR_AXIS = re.compile(r"^(.+)__x-axis-calendar$")
PATH_POINTS = re.compile(r"[ML]\s*([-+0-9.eE]+)[\s,]+([-+0-9.eE]+)")
MONTH_NAMES = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
# Grapher's sub-yearly tick labels (`timeAxisTicks.ts`): `MMM`, `MMM YYYY`, `MMM D`, `MMM D, YYYY`.
GRAPHER_DATE_LABEL = re.compile(rf"(?:{MONTH_NAMES})(?: \d{{1,2}})?(?: \d{{4}}|, \d{{4}})?")
# A label that reads as a date in any other form: made only of month names, quarters, numbers and date
# punctuation (`24 Feb 2023`, `2023-02-24`, `24/02/2023`, `Q1 2023`), with at least one of them not a bare
# number. A category that merely contains a month name (`May improve`, `Marching`) is not a date.
MONTH_WORD = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:t(?:ember)?)?"
    r"|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?(?![a-z])"
)
DATE_PARTS = re.compile(rf"(?i)(?:{MONTH_WORD}|Q[1-4]|\d+(?:st|nd|rd|th)?|[\s,./'’-])+")
DATE_SIGNAL = re.compile(
    rf"(?i)\b{MONTH_WORD}|\b\d{{4}}-\d{{1,2}}(?:-\d{{1,2}})?\b|\b\d{{1,2}}/\d{{1,2}}/\d{{2,4}}\b|\bQ[1-4]\b"
)


def is_date_label(text: str) -> bool:
    """Whether a tick label is a whole date, in grapher's form or any other (US order, ISO, numeric)."""
    text = text.strip()
    return bool(text and DATE_PARTS.fullmatch(text) and DATE_SIGNAL.search(text))


def is_date_axis(texts: list[str]) -> bool:
    """Whether an axis reads as dates throughout: some labels dates, every other one a year."""
    drawn = [t.strip() for t in texts if t.strip()]
    return any(is_date_label(t) for t in drawn) and all(is_date_label(t) or YEAR_LABEL.fullmatch(t) for t in drawn)


def _hex(color: str | None) -> str | None:
    """A paint value as lowercase #rrggbb, or None for none, a url or a named color it cannot read."""
    if not color:
        return None
    color = color.strip().lower()
    if re.fullmatch(r"#[0-9a-f]{3}", color):
        return "#" + "".join(c * 2 for c in color[1:])
    return color if re.fullmatch(r"#[0-9a-f]{6}", color) else None


def _svg_px(root: ET.Element) -> float:
    """Template px per SVG unit: matplotlib writes points (0.72 to a template px) unless told otherwise."""
    return 1 / 0.72 if re.search(r"pt$", root.get("width") or "") else 1.0


def calendar_axes(root: ET.Element) -> set[str]:
    """Subjects whose x axis is on grapher's calendar grid (`style_time_axis(..., inset_ends=False)`).

    A sub-yearly series labelled on January ticks reads `2024`, `2025`, `2026` like a year axis, but
    grapher adds no first and last date to a calendar grid, so the year axis' end checks skip it.
    """
    return {m.group(1) for node in root.iter() if (m := CALENDAR_AXIS.match(node.get("id") or ""))}


def is_sub_yearly_axis(texts: list[str]) -> bool:
    """Whether an axis' labels read as grapher's sub-yearly time axis: its date forms, with bare years allowed."""
    drawn = [t.strip() for t in texts if t.strip()]
    if not drawn or not any(GRAPHER_DATE_LABEL.fullmatch(t) for t in drawn):
        return False
    return all(GRAPHER_DATE_LABEL.fullmatch(t) or YEAR_LABEL.fullmatch(t) for t in drawn)


def date_label_failures(labels: list[TickLabel]) -> list[str]:
    """Failures for x tick labels that read as dates but not in grapher's formats (US order, `Feb 24, 2023`).

    Judged only on an x axis that reads as dates throughout (`is_date_axis`), so a category axis whose
    labels happen to hold a month name is the step's own.
    """
    texts_of: dict[str, list[str]] = {}
    for label in labels:
        if label.axis == "x" and label.text.strip():
            texts_of.setdefault(label.subject, []).append(label.text.strip())
    bad: dict[str, list[str]] = {}
    for subject, texts in texts_of.items():
        if not is_date_axis(texts):
            continue
        for text in texts:
            if is_date_label(text) and not (GRAPHER_DATE_LABEL.fullmatch(text) or YEAR_LABEL.fullmatch(text)):
                bad.setdefault(subject, []).append(text)
    return [
        f"the x-axis tick labels of {subject} write dates as {', '.join(repr(t) for t in texts[:4])}. Grapher labels a "
        "sub-yearly axis 'Feb', 'Feb 2023', 'Feb 24' or 'Feb 24, 2023' (month first, as the style guide asks), and "
        "a quarter by its months; etl.viz.static.time_axis_ticks writes them (WRITING-THE-STEP.md)."
        for subject, texts in sorted(bad.items())
    ]


def _stroke_of(node: ET.Element, inherited: dict[str, str], defs: dict[str, ET.Element]) -> dict[str, str]:
    """The paint of a `<path>` or of the def a `<use>` points to, after inheritance."""
    paint = _paint(node, inherited)
    if node.tag.rsplit("}", 1)[-1] == "use":
        ref = (node.get("{http://www.w3.org/1999/xlink}href") or node.get("href") or "").lstrip("#")
        target = defs.get(ref)
        if target is not None:
            for prop, value in STYLE_DECL.findall(target.get("style") or ""):
                paint.setdefault(prop, value.strip())
            paint.setdefault("stroke-width", "1")
    return paint


class XTickMark(NamedTuple):
    gid: str  # the mark's own group id, the step's or name_furniture's
    x: float  # where the mark meets the axis, in SVG units
    y: float
    length: float
    paint: dict[str, str]
    label: str  # the text of its tick label, "" when it has none


def _x_axes(root: ET.Element, defs: dict[str, ET.Element]) -> list[list[XTickMark]]:
    """The drawn bottom tick marks of each x axis (each `matplotlib.axis_<n>` holding `xtick_<n>` groups).

    A mark is found by where matplotlib puts it (the first child group of an `xtick_<n>` group, whose
    `<use>` is the mark), not by gid, so a mark a step named itself (`timeline__tick-2024`) is judged too.
    """
    axes: list[list[XTickMark]] = []

    def mark_of(tick: ET.Element, inherited: dict[str, str]) -> XTickMark | None:
        paint = _paint(tick, inherited)
        groups = [child for child in tick if child.tag.rsplit("}", 1)[-1] == "g"]
        label = "".join("".join(g.itertext()) for g in groups[1:]).strip()
        if not groups:
            return None
        mark = groups[0]
        for use in mark.iter():
            if use.tag.rsplit("}", 1)[-1] != "use":
                continue
            ref = (use.get("{http://www.w3.org/1999/xlink}href") or use.get("href") or "").lstrip("#")
            target = defs.get(ref)
            ys = [float(y) for _, y in PATH_POINTS.findall(target.get("d") or "")] if target is not None else []
            try:
                x, y = float(use.get("x") or 0), float(use.get("y") or 0)
            except ValueError:
                return None
            # A mark pointing up is the top side's (tick2line), not the axis' own.
            if not ys or max(ys) <= 0:
                return None
            return XTickMark(
                mark.get("id") or "", x, y, max(ys) - min(ys), _stroke_of(use, _paint(mark, paint), defs), label
            )
        return None

    def walk(node: ET.Element, inherited: dict[str, str]) -> None:
        paint = _paint(node, inherited)
        ticks = [c for c in node if c.tag.rsplit("}", 1)[-1] == "g" and XTICK_GROUP.match(c.get("id") or "")]
        if ticks:
            marks = [m for m in (mark_of(t, paint) for t in ticks) if m is not None]
            if marks:
                axes.append(marks)
        for child in node:
            if child not in ticks:
                walk(child, paint)

    walk(root, {})
    return axes


def _horizontal_strokes(root: ET.Element) -> list[tuple[str, float, float, float, dict[str, str]]]:
    """Every two-point horizontal stroked `<path>`: nearest named id, x0, x1, y (SVG units) and paint."""
    found = []

    def walk(node: ET.Element, inherited: dict[str, str], name: str) -> None:
        tag = node.tag.rsplit("}", 1)[-1]
        if tag == "defs":
            return
        node_id = node.get("id") or ""
        if node_id and not GENERATED_ID.match(node_id):
            name = node_id
        paint = _paint(node, inherited)
        if tag == "path" and paint.get("stroke", "none") != "none":
            points = [(float(a), float(b)) for a, b in PATH_POINTS.findall(node.get("d") or "")]
            if len(points) == 2 and abs(points[0][1] - points[1][1]) < 1e-6 and points[0][0] != points[1][0]:
                (x0, y), (x1, _) = points
                found.append((name or node_id, min(x0, x1), max(x0, x1), y, paint))
        for child in node:
            walk(child, paint, name)

    walk(root, {}, "")
    return found


def _strokes(paint: dict[str, str]) -> bool:
    """Whether a line puts a stroke down: not none, not transparent, wider than 0 (`tick_params(width=0)`)."""
    if paint.get("opacity", "1") in ("0", "0.0") or paint.get("stroke-opacity", "1") in ("0", "0.0"):
        return False
    try:
        wide = float(paint.get("stroke-width", "1").replace("px", "")) > 0
    except ValueError:
        wide = True
    return paint.get("stroke", "none") != "none" and wide


def _stroke_px(paint: dict[str, str], px: float) -> float:
    """A stroke's width in template px; infinite when it cannot be read, so nothing is excused by it."""
    try:
        return float(paint.get("stroke-width", "1").replace("px", "")) * px
    except ValueError:
        return math.inf


def _stroke_problems(paint: dict[str, str], px: float) -> list[str]:
    """How a stroke differs from grapher's axis furniture: #999 and 1px."""
    problems = []
    if _hex(paint.get("stroke")) != AXIS_COLOR:
        problems.append(f"stroke {paint.get('stroke')}")
    try:
        width = float(paint.get("stroke-width", "1").replace("px", "")) * px
    except ValueError:
        return [*problems, "an unreadable width"]
    if abs(width - 1) > FURNITURE_TOLERANCE_PX / 2:
        problems.append(f"{width:.3g}px wide")
    return problems


def x_axis_furniture_failures(svg: str) -> list[str]:
    """Failures where a time axis' line or tick marks differ from grapher's: 1px, #999, marks 5px long.

    Judged on each x axis whose labels read as years or dates (`is_year_axis`, `is_sub_yearly_axis`,
    or `is_date_axis` for dates written another way). The axis line is any horizontal stroke lying
    along the tops of its tick marks across most of their range — the bottom spine, a zero gridline,
    or a line the step drew itself — so a hand-drawn axis in the text color is caught whatever it is
    called. A stroke drawn under a conforming axis line, and no wider, is hidden by it (a y=0 gridline
    under the bottom spine), so it is let through.
    """
    try:
        root = ET.fromstring(svg)
    except ET.ParseError:
        return []
    defs = {node.get("id"): node for node in root.iter() if node.get("id")}
    px = _svg_px(root)
    strokes = _horizontal_strokes(root)
    off_marks: list[str] = []
    off_lines: list[str] = []
    for axis in _x_axes(root, defs):
        texts = [m.label for m in axis]
        if not (is_year_axis(texts) or is_sub_yearly_axis(texts) or is_date_axis(texts)):
            continue
        marks = [m for m in axis if m.length > 0 and _strokes(m.paint)]
        for mark in marks:
            problems = _stroke_problems(mark.paint, px)
            if abs(mark.length * px - TICK_MARK_PX) > FURNITURE_TOLERANCE_PX:
                problems.append(f"{mark.length * px:.3g}px long")
            if problems:
                off_marks.append(f"{mark.gid or mark.label or 'a mark'} ({', '.join(problems)})")
        if len(marks) < 2:
            continue
        xs = sorted(m.x for m in marks)
        top = sorted(m.y for m in marks)[len(marks) // 2]
        # In document order, so a later stroke is drawn over an earlier one.
        along = [
            (name, paint, _stroke_problems(paint, px))
            for name, x0, x1, y, paint in strokes
            if _strokes(paint)
            and abs(y - top) * px <= 1.5
            and min(x1, xs[-1]) - max(x0, xs[0]) >= 0.9 * (xs[-1] - xs[0])
        ]
        for i, (name, paint, problems) in enumerate(along):
            if not problems:
                continue
            covered = any(not later for _, _, later in along[i + 1 :])
            if covered and _stroke_px(paint, px) <= 1 + FURNITURE_TOLERANCE_PX / 2:
                continue
            off_lines.append(f"{name or 'a line'} ({', '.join(problems)})")
    failures = []
    if off_marks:
        failures.append(
            f"time-axis tick marks differ from grapher's 5px long, 1px, #999: {'; '.join(off_marks[:4])}"
            f"{' and more' if len(off_marks) > 4 else ''}. etl.viz.static.style_time_axis sets them; keep "
            "matplotlib's own ticks rather than drawing marks yourself (WRITING-THE-STEP.md)."
        )
    if off_lines:
        failures.append(
            f"the time-axis line differs from grapher's 1px #999: {'; '.join(off_lines[:3])}. Draw it with the "
            "bottom spine and etl.viz.static.style_time_axis, never a line of your own, and don't thicken it to "
            "stress time (WRITING-THE-STEP.md)."
        )
    return failures


def axis_failures(svg: str, target: tuple[str, tuple[int, int]] | None) -> list[str]:
    """Return failures where the axes differ from grapher's own download (module docstring)."""
    failures = []
    try:
        root = ET.fromstring(svg)
    except ET.ParseError as error:
        return [f"the SVG does not parse as XML ({error}), so its axes could not be checked"]
    labels = tick_labels(root)
    rotated = sorted({label.subject for label in labels if label.axis == "y" and label.rotated})
    if rotated:
        failures.append(
            f"the y-axis tick labels of {', '.join(rotated)} are rotated, so they no longer form a column against "
            "the plot. Grapher sets y-axis tick labels horizontal and right-aligned; drop the rotation."
        )
    right = sorted({label.subject for label in labels if label.axis == "y" and label.second})
    if right:
        failures.append(
            f"the y-axis tick labels of {', '.join(right)} are drawn on the right (labelright). Grapher puts them on the "
            "left, right-aligned against the plot; draw them there (labelleft=True, labelright=False)."
        )
    top = sorted({label.subject for label in labels if label.axis == "x" and label.second})
    if top:
        failures.append(
            f"the x-axis tick labels of {', '.join(top)} are drawn at the top (labeltop). Grapher puts them below the "
            "plot, over its tick marks; draw them there (labelbottom=True, labeltop=False)."
        )
    failures += date_label_failures(labels)
    labels_left = [label for label in labels if not (label.axis == "y" and label.second)]
    anchors = {label.anchor for label in labels_left if label.axis == "y" and label.anchor is not None}
    # A MathText label declares no anchor, so its axis' alignment is read off where the labels sit,
    # and for labels of one width, off where they end against the plot's left edge.
    edges = plot_left_edges(root)
    past, unread = [], []
    for subject in sorted({label.subject for label in labels_left if label.axis == "y"}):
        axis = [label for label in labels_left if label.axis == "y" and label.subject == subject]
        anchor = mathtext_anchor(axis)
        anchors.add(anchor)
        reach = mathtext_past_plot_edge(axis, edges.get(subject)) if anchor is None else False
        if reach:
            past.append(subject)
        elif reach is None:
            unread.append(subject)
    off = sorted(a for a in anchors if a not in (None, "end"))
    if off:
        failures.append(
            f"y-axis tick labels are anchored {', '.join(off)}, not right-aligned against the plot. Grapher "
            "right-aligns them; leave matplotlib's default (ha='right') or set it back."
        )
    if past:
        failures.append(
            f"the y-axis tick labels of {', '.join(past)} run past the plot's left edge, so they are not "
            "right-aligned against it. Grapher right-aligns them; leave matplotlib's default (ha='right') or set it back."
        )
    if unread:
        failures.append(
            f"the y-axis tick labels of {', '.join(unread)} are MathText of one width with no y gridline, y tick or "
            "left spine to measure them against, so their alignment cannot be read from the SVG. Grapher writes a "
            "log axis in plain numbers (10, 100, 1,000): format the ticks so (ax.yaxis.set_major_formatter), or draw "
            "grapher's horizontal gridlines."
        )
    # Each axes is judged on its own, so a year axis next to a non-year one is still checked.
    x_texts: dict[str, list[str]] = {}
    for label in labels:
        if label.axis == "x":
            x_texts.setdefault(label.subject, []).append(label.text)
    defs = {node.get("id"): node for node in root.iter() if node.get("id")}
    # Each labelled year needs its own drawn mark, matched by tick value: `name_furniture` gives the
    # label `<subject>__x[-minor]-tick-label-<v>` and its mark `<subject>__x[-minor]-tick-<v>`.
    drawn: dict[tuple[str, str], bool] = {}
    labelled: dict[str, list[str]] = {}
    for group in root.iter():
        gid = group.get("id") or ""
        if group.tag.rsplit("}", 1)[-1] != "g":
            continue
        lab = X_LABEL_VALUE.match(gid)
        if lab:
            if "".join(group.itertext()).strip():
                labelled.setdefault(lab.group(1), []).append(lab.group(2))
            continue
        mark = X_MARK_VALUE.match(gid)
        if mark:
            key = (mark.group(1), mark.group(2))
            drawn[key] = drawn.get(key, False) or _tick_draws(group, defs)
    for subject in sorted(x_texts):
        if not (is_year_axis(x_texts[subject]) or is_sub_yearly_axis(x_texts[subject])):
            continue
        # matplotlib leaves an empty group for a zero-length mark, none at all for a hidden one
        # (`bottom=False`, an invisible tick1line) and a fully transparent one for an alpha of 0;
        # in each case nothing is drawn.
        values = labelled.get(subject, [])
        missing = [v for v in values if not drawn.get((subject, v), False)]
        if missing:
            which = "no tick marks" if len(missing) == len(values) else f"no tick mark at {', '.join(missing[:6])}"
            failures.append(
                f"the year axis of {subject} has {which} (each labelled year needs a visible x-tick mark). Grapher draws "
                "them 5px long, 1px, #999, below the axis, which matplotlib takes in points: "
                'ax.tick_params(axis="x", length=5 * 0.72, width=0.72). '
                "If this axis is not years, label it as grapher labels numbers, with a thousands separator "
                "(2,000), and this check leaves it alone."
            )
    # A year axis opens on the data's first year and closes on its last: no series line starts before
    # its first labelled tick or runs on past its last.
    year_axes = {subject for subject in x_texts if is_year_axis(x_texts[subject])} - calendar_axes(root)
    for side, subject, label, lines, year in year_axis_overruns(root, year_axes):
        # With a single labelled tick there is no scale to read a year from.
        if side == "start":
            reach = f"start in about {year}" if year is not None else "start before it"
            gap = f"starts its labels at {label}, but {', '.join(lines[:4])} {reach}"
            fix = "add the first year plotted as the first tick"
        else:
            reach = f"run on to about {year}" if year is not None else "run on past it"
            gap = f"ends its labels at {label}, but {', '.join(lines[:4])} {reach}"
            fix = "add the last year plotted as the final tick"
        failures.append(
            f"the year axis of {subject} {gap}. Grapher keeps its round ticks and adds the data's first and last "
            f"year to them; {fix} (etl.viz.static.year_axis_ticks) and set_xlim to the first and last tick "
            "(WRITING-THE-STEP.md)."
        )
    if target is not None and labels:
        width = target[1][0]
        want = TICK_LABEL_PX.get(width)
        root = SVG_ROOT.search(svg)
        in_pt = bool(root and re.search(r'\bwidth="[0-9.]+pt"', root.group()))
        sizes = {label.size for label in labels if label.size is not None}
        if want and sizes:
            px = {round(s / 0.72 if in_pt else s, 2) for s in sizes}
            wrong = sorted(p for p in px if abs(p - want) > LABEL_TOLERANCE)
            if wrong:
                failures.append(
                    f"tick labels are {', '.join(f'{p:g}' for p in wrong)}px; on a {width}-wide template they are "
                    f"{want:g}px, grapher's own download size (WRITING-THE-STEP.md). Changing it means rebalancing "
                    "the template's type with the design team."
                )
    failures += x_axis_furniture_failures(svg)
    return failures


def _xs(node: ET.Element) -> list[float]:
    """The x of every vertex a mark draws: each coordinate pair of its paths and each `<use>`, `<defs>` skipped.

    matplotlib writes a line in absolute SVG units (`M 62.3 332.4 L 176.4 60.3`) and each of its markers
    as a `<use x=... y=...>` of a mark in `<defs>`, so the even-placed numbers of a path are its x.
    """
    tag = node.tag.rsplit("}", 1)[-1]
    if tag == "defs":
        return []
    xs: list[float] = []
    if tag == "path":
        xs = [float(n) for n in NUMBER.findall(node.get("d") or "")[0::2]]
    elif tag == "use" and NUMBER.fullmatch(node.get("x") or ""):
        xs = [float(node.get("x") or 0)]
    return xs + [x for child in node for x in _xs(child)]


def year_axis_overruns(root: ET.Element, year_axes: set[str]) -> list[tuple[str, str, str, list[str], int | None]]:
    """Each end of a year axis that a series line runs past: the side, subject, that end's label, the lines, their year."""
    return _year_axis_ends(root, year_axes)[0]


def year_axis_shortfalls(root: ET.Element, year_axes: set[str]) -> list[tuple[str, str, str, int]]:
    """Each end of a year axis that no series line reaches: the side, subject, that end's label, the year the lines reach.

    The mirror of `year_axis_overruns`, read on the same scale: the axis is labelled past the data when
    every line starts more than half a year after the first labelled tick, or ends more than half a
    year before the last (on a date axis, when no line reaches the first or last labelled year). An
    axis with a single labelled tick has no scale and is left alone.
    """
    return _year_axis_ends(root, year_axes)[1]


def _year_axis_ends(
    root: ET.Element, year_axes: set[str]
) -> tuple[list[tuple[str, str, str, list[str], int | None]], list[tuple[str, str, str, int]]]:
    """The overruns and shortfalls of every year axis (`year_axis_overruns`, `year_axis_shortfalls`).

    The side is `start` when lines begin before the first labelled tick and `end` when they run on past
    the last.

    A series line is a `line__<Entity>` group, the name grapher gives one and the Figma restyle reads. It
    belongs to the nearest axes group above it (the `<g>` holding a `matplotlib.axis_*` group), so an
    inset's line is judged on the inset's axis. The axis' scale comes from where its tick marks sit
    (`<use x=...>`) and the years their labels read, not the tick values in their gids: on a date
    axis those are matplotlib's day counts, and on a categorical one positions 0, 1, 2. An axis whose
    values advance about 365 per labelled year is a date axis. On any other, a line runs past an end
    when a vertex lies
    more than half a year beyond its labelled tick. On a date axis a year's label marks its first day,
    so the axis is measured in those days and the calendar: a line runs past the last tick once it
    reaches January 1 of the next year, however long that year is, and past the first tick as soon as
    it starts before that day. An axis with a single placed, labelled mark has no scale: there a line
    runs past an end when it reaches more than a pixel either side of that mark, and no year is given.
    An axis with no placed, labelled mark is left alone, since the tick-mark check already fails it.
    """
    parent = {child: node for node in root.iter() for child in node}

    def is_axes(node: ET.Element) -> bool:
        return any((child.get("id") or "").startswith("matplotlib.axis") for child in node)

    def subject_of(axes: ET.Element) -> str | None:
        for child in axes:
            if (child.get("id") or "").startswith("matplotlib.axis"):
                for node in child.iter():
                    match = X_LABEL_VALUE.match(node.get("id") or "")
                    if match:
                        return match.group(1)
        return None

    def axes_of(node: ET.Element) -> ET.Element | None:
        while node in parent:
            node = parent[node]
            if is_axes(node):
                return node
        return None

    # Where each year tick's mark sits ({value: x}), the year each label reads ({value: year}), and the
    # text each label draws ({value: text}).
    placed: dict[str, dict[float, float]] = {}
    label_years: dict[str, dict[float, float]] = {}
    label_texts: dict[str, dict[float, str]] = {}
    for group in root.iter():
        gid = group.get("id") or ""
        lab = X_LABEL_VALUE.match(gid)
        if lab:
            text = "".join(group.itertext()).strip()
            subject, value = lab.group(1), lab.group(2)
            year = _label_year(text)
            if subject in year_axes and NUMBER.fullmatch(value) and year is not None:
                label_years.setdefault(subject, {})[float(value)] = year
                label_texts.setdefault(subject, {})[float(value)] = text
            continue
        mark = X_MARK_VALUE.match(gid)
        if mark and mark.group(1) in year_axes and NUMBER.fullmatch(mark.group(2)):
            xs = [x for child in group for x in _xs(child)]
            if xs:
                placed.setdefault(mark.group(1), {})[float(mark.group(2))] = xs[0]

    # The first and last x each series line reaches.
    spans: dict[str, dict[str, tuple[float, float]]] = {}
    for group in root.iter():
        gid = group.get("id") or ""
        if not gid.startswith("line__") or group.tag.rsplit("}", 1)[-1] != "g":
            continue
        axes = axes_of(group)
        subject = subject_of(axes) if axes is not None else None
        xs = _xs(group)
        if subject in year_axes and xs:
            spans.setdefault(subject, {})[gid] = (min(xs), max(xs))

    overruns = []
    shortfalls: list[tuple[str, str, str, int]] = []
    for subject in sorted(spans):
        marks = placed.get(subject, {})
        years = label_years.get(subject, {})
        pairs = sorted((year, marks[value]) for value, year in years.items() if value in marks)
        if not pairs:
            continue
        if len(pairs) == 1:
            value, x = next((value, marks[value]) for value in years if value in marks)
            early = sorted(gid for gid, (start, _) in spans[subject].items() if start < x - 1)
            late = sorted(gid for gid, (_, end) in spans[subject].items() if end > x + 1)
            if early:
                overruns.append(("start", subject, label_texts[subject][value], early, None))
            if late:
                overruns.append(("end", subject, label_texts[subject][value], late, None))
            continue
        (y0, x0), (y1, x1) = pairs[0], pairs[-1]
        if x1 == x0 or y1 == y0:
            continue
        texts = label_texts[subject]
        first_value, last_value = min(years), max(years)
        # A date axis is valued in days: its tick values advance about 365 per labelled year. Any other
        # values, such as a categorical axis' positions 0, 1, 2, are read through the label years.
        days_per_year = (last_value - first_value) / (years[last_value] - years[first_value] or 1)
        if 360 <= days_per_year <= 370:
            found = _date_axis_overruns(spans[subject], marks, years)
        else:
            per_year = (x1 - x0) / (y1 - y0)

            def year_at(x: float) -> int:
                return round(y0 + (x - x0) / per_year)

            first_x = x0 + (years[first_value] - y0) * per_year
            last_x = x0 + (years[last_value] - y0) * per_year
            found = {
                "start": [
                    (gid, year_at(start))
                    for gid, (start, _) in spans[subject].items()
                    if (first_x - start) / per_year > 0.5
                ],
                "end": [
                    (gid, year_at(end)) for gid, (_, end) in spans[subject].items() if (end - last_x) / per_year > 0.5
                ],
            }
            data_start = min(start for start, _ in spans[subject].values())
            data_end = max(end for _, end in spans[subject].values())
            if (data_start - first_x) / per_year > 0.5:
                found["short_start"] = [("", year_at(data_start))]
            if (last_x - data_end) / per_year > 0.5:
                found["short_end"] = [("", year_at(data_end))]
        if found["start"]:
            lines, reached = zip(*found["start"])
            overruns.append(("start", subject, texts[first_value], sorted(lines), min(reached)))
        if found["end"]:
            lines, reached = zip(*found["end"])
            overruns.append(("end", subject, texts[last_value], sorted(lines), max(reached)))
        if found.get("short_start"):
            shortfalls.append(("start", subject, texts[first_value], found["short_start"][0][1]))
        if found.get("short_end"):
            shortfalls.append(("end", subject, texts[last_value], found["short_end"][0][1]))
    return overruns, shortfalls


def _date_axis_overruns(
    spans: dict[str, tuple[float, float]], marks: dict[float, float], years: dict[float, float]
) -> dict[str, list[tuple[str, int]]]:
    """The lines that start before a date axis' first labelled year or reach the year after its last, with their year.

    Also, under `short_start` and `short_end`, the year the data reaches when no line reaches the first
    labelled year (every line starting in a later year) or the last (every line ending in an earlier one).
    Each labelled tick is taken to sit on January 1 of the year it reads, so a vertex's date is that day
    plus its distance in days, and the year after the last label begins that year's own length (365 or
    366 days) later. Half a day is allowed for rounding in the SVG's coordinates.
    """
    found: dict[str, list[tuple[str, int]]] = {"start": [], "end": []}
    placed = sorted((value, marks[value]) for value in years if value in marks)
    (v0, x0), (v1, x1) = placed[0], placed[-1]
    first_value, last_value = min(years), max(years)
    first_year, last_year = int(years[first_value]), int(years[last_value])
    if x1 == x0 or not 1 <= first_year <= last_year < 9999:
        return found
    per_day = (x1 - x0) / (v1 - v0)

    def year_of(x: float, anchor_value: float, anchor_year: int) -> int:
        day = v0 + (x - x0) / per_day
        return (dt.date(anchor_year, 1, 1) + dt.timedelta(days=round(day - anchor_value))).year

    next_year_value = last_value + (dt.date(last_year + 1, 1, 1) - dt.date(last_year, 1, 1)).days
    for gid, (start, end) in spans.items():
        if v0 + (start - x0) / per_day < first_value - 0.5:
            found["start"].append((gid, year_of(start, first_value, first_year)))
        if v0 + (end - x0) / per_day >= next_year_value - 0.5:
            found["end"].append((gid, year_of(end, last_value, last_year)))
    data_start = min(start for start, _ in spans.values())
    data_end = max(end for _, end in spans.values())
    second_year_value = first_value + (dt.date(first_year + 1, 1, 1) - dt.date(first_year, 1, 1)).days
    if v0 + (data_start - x0) / per_day >= second_year_value - 0.5:
        found["short_start"] = [("", year_of(data_start, first_value, first_year))]
    if v0 + (data_end - x0) / per_day < last_value - 0.5:
        found["short_end"] = [("", year_of(data_end, last_value, last_year))]
    return found


def _label_year(text: str) -> float | None:
    """The year a tick label reads (`2024`, `-500`, `12,000 BCE` as -12000), or None when it reads as none."""
    text = text.strip()
    match = YEAR_LABEL.fullmatch(text)
    if not match:
        return None
    year = float((match.group(1) or match.group(2)).replace(",", ""))
    return -year if text.startswith(("-", "−")) or text.endswith("BCE") else year


# A tick label that reads as a year: a bare integer, maybe signed or marked BCE/CE. Grapher writes
# years with no thousands separator and every other number with one (2,000), so a separator is a
# count unless an era suffix makes it a year: long-run axes write "12,000 BCE".
YEAR_LABEL = re.compile(r"[-\u2212]?(?:(\d+)(?: BCE| CE)?|(\d{1,3}(?:,\d{3})+) (?:BCE|CE))")


def is_year_axis(texts: list[str]) -> bool:
    """Whether an axis' tick labels, as drawn, read as years.

    Judged on the label text rather than the tick values, which cannot tell years from a count axis
    that also ends at 2000: grapher labels that count axis `2,000` and the year `2000`. Every drawn
    label must be a bare integer (or a grouped one with an era suffix) and at least one must have
    four digits, which admits long-run axes that start at 0 or before year 0 and projections that run
    past 2100, and leaves out axes of shares, rates or small counts. Blank labels (a step thinning
    the ticks) are skipped.
    """
    drawn = [t.strip() for t in texts if t.strip()]
    years = [YEAR_LABEL.fullmatch(t) for t in drawn]
    digits = [(m.group(1) or m.group(2)).replace(",", "") for m in years if m]
    return bool(years) and all(years) and any(len(d) >= 4 for d in digits)


# How far, in template px, an end label's outer edge may sit from its tick, or the bottom spine run past
# the end ticks, before it shows.
END_TOLERANCE_PX = 0.5
# Grapher's axis line, in template px.
AXIS_LINE_PX = 1.0


def year_axis_end_warnings(svg: str) -> list[str]:
    """Return warnings where a year axis' ends differ from grapher's x axis.

    Grapher's axis runs from the data's first year to its last, so it is labelled past the data when
    no series line reaches its first or last labelled year (`year_axis_shortfalls`). It left-aligns the
    first tick label to its tick, right-aligns the last, and ends a 1px, butt-capped axis line on the
    two end ticks. In the SVG an end label is a `<text>` whose x is its
    anchor, so a `start` (first) or `end` (last) anchor within END_TOLERANCE_PX of its tick's `<use x>`
    puts its outer edge on the tick. The bottom spine (`<subject>__spine-bottom`) is a path whose stroke
    reaches past its end points by half its width under a `square` or `round` cap, matplotlib's default
    being `square`. Judged on each year axis with at least two placed, labelled ticks; a rotated or
    MathText label, a hidden spine and an axis with a single tick are left alone.

    Warnings rather than failures: a bar chart over years needs half a bar of range past each end tick
    and its labels centered under the bars, and its labels read as years like any other; and only
    `line__` series are read, so an area or band that carries the axis past the lines would be failed.
    """
    try:
        root = ET.fromstring(svg)
    except ET.ParseError:
        return []
    head = SVG_ROOT.search(svg)
    unit = 0.72 if head and re.search(r'\bwidth="[0-9.]+pt"', head.group()) else 1.0
    tolerance = END_TOLERANCE_PX * unit

    # Each axes' x labels ({value: (text, anchor, x, rotated)}), tick-mark x ({value: x}) and spine.
    labels: dict[str, dict[float, tuple[str, str | None, float | None, bool]]] = {}
    marks: dict[str, dict[float, float]] = {}
    spines: dict[str, ET.Element] = {}
    for group in root.iter():
        gid = group.get("id") or ""
        if group.tag.rsplit("}", 1)[-1] != "g":
            continue
        if gid.endswith("__spine-bottom"):
            spines[gid.removesuffix("__spine-bottom")] = group
            continue
        lab = X_LABEL_VALUE.match(gid)
        if lab:
            text = "".join(group.itertext()).strip()
            if not text or not NUMBER.fullmatch(lab.group(2)):
                continue
            node = next((n for n in group.iter() if n.tag.rsplit("}", 1)[-1] == "text"), None)
            anchor = TEXT_ANCHOR.search(node.get("style") or "") if node is not None else None
            x = node.get("x") if node is not None else None
            labels.setdefault(lab.group(1), {})[float(lab.group(2))] = (
                text,
                anchor.group(1) if anchor else None,
                float(x) if x and NUMBER.fullmatch(x) else None,
                _rotated(group),
            )
            continue
        mark = X_MARK_VALUE.match(gid)
        if mark and NUMBER.fullmatch(mark.group(2)):
            xs = [x for child in group for x in _xs(child)]
            if xs:
                marks.setdefault(mark.group(1), {})[float(mark.group(2))] = xs[0]

    warnings = []
    year_axes = {subject for subject in labels if is_year_axis([text for text, *_ in labels[subject].values()])}
    year_axes -= calendar_axes(root)
    # Labelled past the data: every line opens after the first labelled year, or closes before the last.
    short = {(subject, side): (label, year) for side, subject, label, year in year_axis_shortfalls(root, year_axes)}
    for subject in sorted(year_axes):
        problems = []
        for side, verb in (("start", "starts its labels at"), ("end", "ends its labels at")):
            if (subject, side) in short:
                label, year = short[(subject, side)]
                problems.append(f"it {verb} {label}, but its lines {side} in about {year}")
        ends = sorted((marks[subject][v], v) for v in labels[subject] if v in marks.get(subject, {}))
        if len(ends) < 2:
            if problems:
                warnings.append(f"the year axis of {subject} does not end as grapher's does: {'; '.join(problems)}.")
            continue
        (first_x, first_v), (last_x, last_v) = ends[0], ends[-1]
        for side, tick_x, value, want, align in (
            ("first", first_x, first_v, "start", "left"),
            ("last", last_x, last_v, "end", "right"),
        ):
            text, anchor, x, rotated = labels[subject][value]
            if rotated or anchor is None or x is None:
                continue
            if anchor != want:
                how = {"middle": "centered on", "start": "left-aligned to", "end": "right-aligned to"}.get(
                    anchor, f"anchored {anchor} at"
                )
                problems.append(f"its {side} label, {text}, is {how} its tick, not {align}-aligned")
            elif abs(x - tick_x) > tolerance:
                problems.append(f"its {side} label, {text}, sits {abs(x - tick_x) / unit:.1f}px off its tick")
        spine = spines.get(subject)
        paths = [n for n in spine.iter() if n.tag.rsplit("}", 1)[-1] == "path"] if spine is not None else []
        if paths:
            style = dict(STYLE_DECL.findall(paths[0].get("style") or ""))
            try:
                width = float(style.get("stroke-width", "1").strip().replace("px", ""))
            except ValueError:
                width = 1.0
            cap = style.get("stroke-linecap", "butt").strip()
            reach = width / 2 if cap in ("square", "round") else 0.0
            xs = [x for path in paths for x in _xs(path)]
            over = max(first_x - (min(xs) - reach), (max(xs) + reach) - last_x)
            if over > tolerance:
                if cap in ("square", "round") and over - reach <= tolerance:
                    fix = f'its {cap} cap, which spines["bottom"].set_capstyle("butt") removes'
                else:
                    fix = "padding in the x range, which set_xlim(first, last) removes"
                problems.append(f"its axis line runs {over / unit:.1f}px past the end ticks, from {fix}")
            if abs(width / unit - AXIS_LINE_PX) > 0.05:
                problems.append(f"its axis line is {width / unit:.2g}px wide, not {AXIS_LINE_PX:g}px")
        if problems:
            warnings.append(
                f"the year axis of {subject} does not end as grapher's does: {'; '.join(problems)}. Grapher's axis "
                "runs from the data's first year to its last, left-aligns the first label to its tick and right-aligns the "
                "last, on a 1px butt-capped line; etl.viz.static.style_year_axis(ax, first, last) does all of it, "
                "after set_xticks (WRITING-THE-STEP.md). A bar chart, whose bars need the padding, can leave it."
            )
    return warnings


def scale_failures(svg: str, target: tuple[str, tuple[int, int]]) -> list[str]:
    """Return a failure if a matplotlib SVG's canvas is at the right ratio but the wrong scale.

    matplotlib writes the canvas in points, at 72 per inch, and the step lays out at
    PIXELS_PER_INCH template px per inch, so a frame W px wide is W * 0.72pt. A canvas at the right
    ratio but another scale needs a uniform rescale into the frame, which shrinks or grows every
    font size with it. Only a width in pt is judged: without the unit, the scale is unknown.
    """
    root = SVG_ROOT.search(svg)
    width = re.search(r'\bwidth="([0-9.]+)pt"', root.group()) if root else None
    if width is None:
        return []
    name, (target_w, _) = target
    want = target_w * 72 / PIXELS_PER_INCH
    got = float(width.group(1))
    if abs(got - want) / want > TOLERANCE:
        return [
            f"SVG canvas is {got:g}pt wide but the {name} size of {target_w}px needs {want:g}pt "
            f"(at {PIXELS_PER_INCH}px per inch), so the scale is {got / want:.3g}x and every font size "
            "moves with it on the way into Figma. Build the figure at "
            f"figsize = px / {PIXELS_PER_INCH}."
        ]
    return []


def check_png(path: Path, target: tuple[str, tuple[int, int]] | None) -> list[str]:
    """Return a list of contract failures for one PNG."""
    if target is None:
        return []
    try:
        from PIL import Image
    except ImportError:
        return ["Pillow is not available, so the PNG frame proportion could not be checked"]

    width, height = Image.open(path).size
    return frame_failures("PNG", width, height, target)


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
        "--size",
        action="append",
        default=[],
        type=parse_size,
        metavar="STEM=WxH",
        help=(
            "the declared frame size of one output, by file stem, for a layout that runs shorter or "
            "taller than its template on purpose (height_for_age=850x615); repeatable. Wins over both "
            "--template and the filename suffix for that frame."
        ),
    )
    parser.add_argument(
        "--require-named-marks",
        action="store_true",
        help=(
            "fail when any painted mark sits under no <subject>__<role> gid. Without it the count is "
            "still printed. Pass it for a new or refreshed step: Figma's re-import pairs a mark with "
            "the style a designer bound on it by that name."
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

    sizes = dict(args.size)
    unknown = sorted(set(sizes) - {svg.stem for svg in svgs})
    if unknown:
        print(f"--size names no output here: {', '.join(unknown)}", file=sys.stderr)
        return 2

    failed = False
    for svg in svgs:
        target = frame_target(svg.stem, template_for(svg.stem, args.template), sizes)
        failures = check_svg(svg, args.expect_gid, target, args.require_named_marks)
        png = svg.with_suffix(".png")
        if png.exists():
            failures += check_png(png, target)
        else:
            failures.append(f"no sibling PNG at {png.name} — export_fig should emit both formats")

        if failures:
            failed = True
            print(f"FAIL {svg.name}")
            for failure in failures:
                print(f"  - {failure}")
        else:
            extra = f", frame matches {target[0]} {target[1][0]}x{target[1][1]}" if target else ""
            text = svg.read_text()
            named = len({i for i in re.findall(r'\bid="([^"]+)"', text) if not GENERATED_ID.match(i)})
            unnamed = sum(unnamed_marks(text).values())
            print(f"OK   {svg.name} (0 clip paths, {named} named layers, {unnamed} unnamed marks{extra})")
        for warning in year_axis_end_warnings(svg.read_text()):
            print(f"  ! warning: {warning}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
