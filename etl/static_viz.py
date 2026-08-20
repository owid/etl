"""Shared helpers for `export://static_viz` steps.

Every static viz is a matplotlib figure that has to satisfy the same handoff contract before it can
be opened in Figma: real `<text>` rather than outlined glyphs, no clipping, deterministic output, and
the proportions of one of the static-chart templates. Each step used to restate that contract in its
own preamble, and four of the five got some of it wrong — two shipped SVGs with every glyph outlined
(568 and 1123 outlined-glyph references), and two more shipped clip paths. `verify_static_viz.py`
catches all of it, but only if you remember to run it.

So the contract lives here as code instead. `export_frame` applies it on the way out, which makes
those four failures structurally impossible rather than merely documented.

What belongs in this module: anything every static viz needs and none of them should re-derive —
the rcParams, the template proportions, the source citation, the save discipline. What does not:
colors, fonts, the logo, or any visual treatment the Figma template provides. Those are applied in
Figma, and setting them here is work that gets thrown away.

Typical use:

    from etl.helpers import PathFinder
    from etl.static_viz import TEMPLATES, apply_svg_rcparams, export_frame, source_citation

    paths = PathFinder(__file__)
    apply_svg_rcparams()

    def run() -> None:
        tb = paths.load_dataset("population").read("historical")
        fig = build(tb, source_citation(tb["population"]))
        export_frame(paths, fig, paths.short_name, template="horizontal")
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NamedTuple

import matplotlib

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters for type checkers
    from etl.helpers import PathFinder

# A template pixel is 0.72pt, and these figures are built at 100 template px per inch. Figma imports
# at 96px/in, so a figure saved at this scale arrives 0.96x and needs one uniform rescale in
# `/create-figma-chart` Step 7 — never a bigger `figsize`, which would put the slot conversion and
# every point-denominated font size out by 1.39x.
PIXELS_PER_INCH = 100

# Deterministic SVG output: without a fixed salt, matplotlib's internal ids change between runs and
# every re-render produces a diff that says nothing.
SVG_HASHSALT = "owid-static-viz"


class Template(NamedTuple):
    """A static-chart template's proportions.

    `node_id` is its node in the design team's yearly Charts file, recorded so a step and the Figma
    page it feeds cannot disagree about which frame it was laid out against.
    """

    name: str
    width_px: int
    height_px: int
    node_id: str

    @property
    def figsize(self) -> tuple[float, float]:
        return (self.width_px / PIXELS_PER_INCH, self.height_px / PIXELS_PER_INCH)

    @property
    def ratio(self) -> float:
        return self.width_px / self.height_px


# Keys match `verify_static_viz.py`'s filename suffixes, so a file named `<short>_mobile.svg` is
# checked against the template it was built for.
TEMPLATES: dict[str, Template] = {
    "horizontal": Template("Static Chart Template_Horizontal", 850, 638, "5332:75"),
    "vertical": Template("Static Chart Template_Vertical", 850, 1095, "5332:93"),
    "mobile": Template("Static Chart Template_Mobile (example 2)", 540, 824, "24590:32"),
    "mobile-square": Template("Static Chart Template_Mobile (example 1)", 540, 540, "24590:20"),
}


def apply_svg_rcparams() -> None:
    """Make matplotlib emit Figma-editable, deterministic SVG.

    Call once at module import, before any figure is created — `svg.hashsalt` is read when a figure
    is built, not when it is saved.

    `svg.fonttype="none"` is the one that matters most: the default outlines every glyph, so the copy
    arrives in Figma as vector paths that cannot be edited, restyled or spell-checked. Two shipped
    steps omitted this line and neither failure is visible in the rendered PNG.
    """
    matplotlib.rcParams["svg.fonttype"] = "none"
    matplotlib.rcParams["svg.hashsalt"] = SVG_HASHSALT


def unclip(fig: Any) -> None:
    """Turn off clipping on every artist in the figure.

    matplotlib clips artists to their axes and writes that out as `<clipPath>`, which Figma imports
    as a mask over the whole chart — so every layer arrives clipped and the first edit that moves
    anything reveals the seam. Sweeping the whole figure is deliberate: the artists that get clipped
    are rarely the ones you would think to name.
    """
    for artist in fig.findobj():
        artist.set_clip_on(False)


def export_frame(
    paths: PathFinder,
    fig: Any,
    short_name: str,
    *,
    template: str | None = None,
    dpi: int = 300,
    unclip_first: bool = True,
) -> None:
    """Save one frame as the PNG/SVG pair the Figma handoff expects.

    Two save passes, not one, and they are not interchangeable:

    - **PNG** keeps the figure's opaque canvas, because it is the flat reference copy a reviewer
      looks at.
    - **SVG** is saved `transparent=True`, because the Figma template provides the background. An
      opaque `patch_1` in the SVG paints over it — a defect `verify_static_viz.py` does not check
      for and which looks identical in the PNG.

    Both come out reproducible — `PathFinder.export_fig` strips matplotlib's version stamp — so a
    byte diff on a committed output means the picture changed, not that matplotlib was upgraded.

    `template` is optional and only validates: it asserts the figure's `figsize` matches that
    template, catching a `figsize` typo or a stray `bbox_inches="tight"` before the file is written.
    """
    if unclip_first:
        unclip(fig)

    if template is not None:
        if template not in TEMPLATES:
            raise ValueError(f"unknown template {template!r}; expected one of {sorted(TEMPLATES)}")
        want = TEMPLATES[template].figsize
        got = (fig.get_figwidth(), fig.get_figheight())
        # Both dimensions, not just their ratio: a figure at the template's aspect but twice its
        # scale needs a uniform rescale on the way into the frame, and that divides every
        # point-denominated font size by the same factor — the whole type hierarchy lands off the
        # template's, invisibly. `verify_static_viz.py` can only compare the ratio, because a saved
        # file no longer carries the figsize it was built at; here the figure is still in hand, so
        # check the thing that actually has to hold. Tolerance matches the verifier's 0.002.
        if any(abs(g - w) / w > 0.002 for g, w in zip(got, want)):
            raise AssertionError(
                f"figure size {got[0]:.4f}x{got[1]:.4f} does not match template {template!r}. "
                f"Expected figsize {want} — the aspect and the scale both have to match, because "
                f"the figure is built at {PIXELS_PER_INCH} template px per inch. "
                'A `bbox_inches="tight"` on save also breaks this — it crops to the ink.'
            )

    # PNG first: opaque canvas, the flat reference copy.
    paths.export_fig(fig, short_name, ["png"], dpi=dpi)
    # SVG second: transparent, so the template's own background shows through.
    paths.export_fig(fig, short_name, ["svg"], transparent=True)


def source_citation(
    *columns: Any,
    key: str = "producer",
    prefix: str = "",
    join: str = "; ",
) -> str:
    """Build the `Data source:` string from the origins on the indicators actually plotted.

    Follows grapher's own footer convention of `producer (year)`, and groups by producer so two
    products from one producer cite as one entry carrying both release years rather than as two
    separate sources.

    Derived from the data rather than typed, so it cannot silently go stale at the next update —
    which is the same reason the step reads its boundary years off the table instead of hardcoding
    them.

    :param columns: the indicator columns whose origins to cite, e.g. ``tb["population"]``.
    :param key: origin field to group by — ``"producer"`` (grapher's convention) or
        ``"attribution_short"`` when the short form is the one readers know.
    :param prefix: prepended verbatim, e.g. ``"Data sources: "``. Default empty, so the caller can
        put the label in the template's own slot instead.
    """
    years: dict[str, list[str]] = {}
    for col in columns:
        origins: Iterable[Any] = getattr(col.metadata, "origins", []) or []
        for origin in origins:
            name = getattr(origin, key, None)
            if not name:
                continue
            year = (origin.date_published or "").split("-")[0] if origin.date_published else ""
            seen = years.setdefault(name, [])
            if year and year not in seen:
                seen.append(year)
    parts = [f"{name} ({join.join(sorted(ys))})" if ys else name for name, ys in sorted(years.items())]
    return prefix + join.join(parts)


def nice_year_ticks(year_min: int, year_max: int, *, max_ticks: int = 8) -> list[int]:
    """Round year ticks spanning [year_min, year_max], on a 1-2-5 x power-of-ten step.

    Picks the coarsest step that still yields at least three ticks, so a 400-year span gets
    centuries and a 40-year span gets decades without the caller choosing.
    """
    if year_max <= year_min:
        return [year_min]
    span = year_max - year_min
    for mag in range(0, 6):
        for mult in (1, 2, 5):
            step = mult * 10**mag
            if span / step <= max_ticks:
                first = -(-year_min // step) * step  # ceil to a multiple of step
                ticks = list(range(int(first), year_max + 1, step))
                if len(ticks) >= 3:
                    return ticks
    return [year_min, year_max]
