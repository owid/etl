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
# Groups: subject, axis. Matched on a whole id, so `-tick-label2-` (the opposite side) is left out.
TICK_LABEL_GID = re.compile(r"^(.+)__([xy])-tick-label(2?)-.+$")  # label2: the right (or top) side
TICK_MARK_GID = re.compile(r"^(.+)__x-tick-(?!label)")
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
    # A MathText label placed by more than a translate (rotated), so where it sits says nothing of its alignment.
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
        rotated = False
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
    tick mark as a `<use x=...>` of a mark in `<defs>`, all starting at the plot's left edge. A hidden
    one is not written and a zero-length tick is an empty group, so each subject takes the leftmost x
    of what is there.
    """
    edges: dict[str, float] = {}
    for group in root.iter():
        match = Y_EDGE_GID.match(group.get("id") or "")
        if match is None:
            continue
        xs = []
        for node in group.iter():
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
            f"the y-axis tick labels of {', '.join(rotated)} are rotated MathText, whose alignment cannot be read "
            "from the SVG. Grapher sets y-axis tick labels horizontal and right-aligned; drop the rotation."
        )
    right = sorted({label.subject for label in labels if label.axis == "y" and label.second})
    if right:
        failures.append(
            f"the y-axis tick labels of {', '.join(right)} are drawn on the right (labelright). Grapher puts them on the "
            "left, right-aligned against the plot; draw them there (labelleft=True, labelright=False)."
        )
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
    drawn_marks: dict[str, list[bool]] = {}
    for group in root.iter():
        m = TICK_MARK_GID.match(group.get("id") or "")
        if m and group.tag.rsplit("}", 1)[-1] == "g":
            drawn_marks.setdefault(m.group(1), []).append(_tick_draws(group, defs))
    for subject in sorted(x_texts):
        # matplotlib leaves an empty group for a zero-length mark, none at all for a hidden one
        # (`bottom=False`, an invisible tick1line) and a fully transparent one for an alpha of 0;
        # in each case nothing is drawn.
        if is_year_axis(x_texts[subject]) and not any(drawn_marks.get(subject, [])):
            failures.append(
                f"the year axis of {subject} has no tick marks (no x-tick group draws a visible one). Grapher draws "
                "them 5px long, 1px, #999, below the axis, which matplotlib takes in points: "
                'ax.tick_params(axis="x", length=5 * 0.72, width=0.72). '
                "If this axis is not years, label it as grapher labels numbers, with a thousands separator "
                "(2,000), and this check leaves it alone."
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
    return failures


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

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
