"""How much bilateral development aid is spent on climate-related projects?

Static chart for the article "How much development aid is spent on climate change?" (filename
`climate-related-bilateral-oda.png`). It shows DAC members' bilateral ODA commitments in the latest year as two
bars:

1. all bilateral aid, split into non-allocable aid (outside the scope of the Rio markers) and allocable aid;
2. allocable aid, split into non-climate aid and climate-related aid (mitigation only, adaptation only, both),
   with a bracket and a label for climate-related aid.

A data update of the 2024 frame drawn by hand in Figma (Charts (2026) file `s6Sv60bakebRRW2TxsMQbF`, page
"20261015 How much development aid is spent on climate-related projects? 2024 data (Hannah)", frame `29154:6`,
540 x 540). The design is unchanged, so this step reproduces that frame: its size, its slot positions, its type
sizes and its colors are copied from it (read with `use_figma`). Positions below are in frame px, y measured from
the top.

Data: table `climate_related_oda_by_donor` of `garden/oecd/2026-10-07/climate_related_official_development_assistance`,
entity "DAC members", latest year: total bilateral ODA (CRS), bilateral allocable ODA (RioMarkers) and climate-related
ODA split into mitigation only, adaptation only and both. The European Investment Bank is left out of all of them
(see the garden step).

Layout rules, all derived from the data:
- The left bar always spans y = 178 to 476 (298 px) for all bilateral aid, so the scale is 298 px over its total. The
  right bar uses the same scale and ends at 476.
- Labels inside a bar sit 8 px (name) and 28 px (values) below its top, at x = 69 (left bar) or 324 (right of the
  right bar). The heading above the right bar sits 51.07 px (name) and 22.07 px (value) above it.
- The climate segment labels are centered on their segments; the "Climate-related" label sits 33.68 px (name) and
  16.68 px (share) above the top of the climate segments, beside a bracket spanning them.

This step's PNG is a preview in Arial, which sets wider than the frame's Lato: there the "Data source" row touches
"CC BY", while in the frame it clears it by 18 px. The line-fit checks use Lato's widths.

Figma handoff
-------------
The frame is NOT an import of this step's SVG. It is a copy of the previous frame, with its numbers, texts and
positions set from this step's layout, which keeps every style binding of the hand-drawn original.

Where: Charts (2026) file `s6Sv60bakebRRW2TxsMQbF`, page "20261015 How much development aid is spent on
climate-related projects? 2024 data (Hannah)" (`29154:5`). Frames, left to right: the 2023 version (`29155:5`,
crossed out), the first 2024 version (`29154:6`, EIB included) and the current one (`29664:5`, EIB excluded), all
named `climate-related-bilateral-oda` (the exported filename). Leave older frames untouched.

To update it:
1. Duplicate the rightmost frame and place it 191 px to the right of it (`clone()`, then x = old x + 540 + 191).
2. Run this step and take the values it logs. With scale = 298 / total bilateral aid:
   allocable top = 178 + non-allocable x scale; climate top = allocable top + non-climate x scale; each climate
   segment's height = its value x scale (mitigation, then adaptation, then overlap; the last one ends at 476).
3. Set, by node name: "bar: Non-allocable" (y 178 to allocable top), "bar: Allocable" and "bar: Other
   (non-climate)" (from allocable top), "Rectangle 1" (the band: allocable top + 1.93 to 475), the three climate
   bars, "bracket: line" (climate top + 1 to 475) and "bracket: top tick" (y = climate top + 1); "bracket: bottom
   tick" stays at y 474.
4. Texts (load each node's fonts first; set `characters` only on single-style nodes):
   - "head: All bilateral aid value", "head: Allocable value" (y = allocable top - 22.07), "head: Bilateral
     allocable ODA" (y = allocable top - 51.07), "lbl: Non-allocable value", "lbl: Allocable" and "lbl: Other
     (non-climate)" (y = allocable top + 8), "lbl: Allocable value" and "lbl: Other value" (y = allocable top + 28),
     "lbl: Climate-related" (y = climate top - 33.68) and "lbl: Climate-related share" (y = climate top - 16.68).
   - The three climate segment labels mix a bold colored name with a regular value: edit the value with
     `deleteCharacters`/`insertCharacters` so both runs keep their styles, and center each on its segment
     (y = segment middle - 8).
   - Subtitle (in "Frame 36"): SUBTITLE_LINES joined with "\\n", with the share the step logs. Note: edit only the year, with
     `insertCharacters`, to keep "Note:" bold. The title and the "Data source" row do not change.
5. Check the frame against this step's PNG: the subtitle must stay at 3 lines (57 px tall), and the Note at one.

Styles in the frame, by name (keys in brackets): background "Website/Background/White"; title "Instagram/Title"
(Playfair Display SemiBold 25/29, "climate-related projects" in #578145); subtitle "Instagram/Subtitle" (Lato 16);
footer "Instagram/Source" (Lato 14/16, labels in bold); bar fills: mitigation "Default Palette/Dark Olive Green",
adaptation "Default Palette/Lime", overlap "Default Palette/Light Teal"; segment names "Default Palette/Dark Olive
Green", "Line and Slope Charts/Lime" and "Line and Slope Charts/Light Teal"; "Climate-related" label "Default
Palette/Olive Green"; values and bracket "Text/Gray 80". The other fills (#93989f, #d4ac7f, #f5e2cd, #c29767,
#7f96ba) are not bound to styles.
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.patches import Rectangle
from matplotlib.textpath import TextPath

from etl.helpers import PathFinder
from etl.viz.static import apply_svg_rcparams, export_frame

paths = PathFinder(__file__)
apply_svg_rcparams()

# Fonts: emit Lato (what the frame uses) and measure/draw with what is installed. See
# `.claude/skills/create-static-viz/reference/WRITING-THE-STEP.md`.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "Liberation Sans", "sans-serif"]
MEASURED_FONT_STACK = ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)
_DRAWN_FACE = findfont(FontProperties(family=EMITTED_FONT_STACK))
_MEASURED_FACE = findfont(FontProperties(family=MEASURED_FONT_STACK))
assert _DRAWN_FACE == _MEASURED_FACE, f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name}"

ENTITY = "DAC members"

# Frame.
FRAME_W, FRAME_H = 540, 540
MARGIN = 16
PX_PER_INCH = 100
PT_PER_PX = 0.72
# Lato's line height when Figma's is AUTO, as a multiple of the font size.
AUTO_LINE_HEIGHT = 1.2
# Lato is narrower than Arial (TEMPLATES.md): about 0.8% at 16 px (the subtitle), 2.4% in regular and 6.6% in bold at
# footer sizes. Line-fit checks use these, so they judge the width the frame will set, not the step's own.
LATO_WIDTH_RATIO = 0.992
LATO_FOOTER_RATIO = {False: 0.976, True: 0.934}
# Smallest gap between the source and the license on their shared row.
LICENSE_GAP = 8

# Bars: left bar (all bilateral aid) and right bar (allocable aid), and the band joining them.
LEFT_X, LEFT_W = 61, 120
RIGHT_X, RIGHT_W = 216, 90
BARS_TOP, BARS_BOTTOM = 178, 476
BAND_X0, BAND_X1, BAND_TOP_OFFSET, BAND_BOTTOM = 61, 306, 1.93, 475

# Labels.
LEFT_LABEL_X, RIGHT_LABEL_X, RIGHT_HEAD_X = 69, 324, 218
NAME_OFFSET, VALUE_OFFSET = 8, 28  # below the top of a segment
HEAD_NAME_ABOVE, HEAD_VALUE_ABOVE = 51.07, 22.07  # above the top of the right bar
CLIMATE_NAME_ABOVE, CLIMATE_SHARE_ABOVE = 33.68, 16.68  # above the top of the climate segments
SEGMENT_LABEL_H = 16  # box height of a climate segment label, centered on its segment
ALL_HEAD_Y, ALL_VALUE_Y = 140, 156

# Bracket beside the climate segments.
BRACKET_X, BRACKET_TICK_X, BRACKET_W, BRACKET_TICK_W = 314, 310, 1.5, 5.5

# Header and footer.
TITLE_Y, TITLE_LINE_PX, TITLE_SIZE = 9, 29, 25
SUBTITLE_Y, SUBTITLE_SIZE = 9 + 58 + 6, 16  # below a two-line title, 6 px apart
SUBTITLE_MAX_LINES = 3  # a fourth line would run into the heading above the left bar
CONTENT_W = FRAME_W - 2 * MARGIN
NOTE_Y, SOURCE_Y, FOOTER_LINE_PX, FOOTER_SIZE = 488, 508, 16, 14

# Colors, read off the 2024 frame (the library styles they are bound to are listed in the Figma handoff).
TITLE_COLOR = "#2d2e2d"
TITLE_HIGHLIGHT = "#578145"
TEXT_COLOR = "#5b5b5b"
FOOTER_COLOR = "#858585"
COLORS = {
    "non_allocable": "#93989f",
    "allocable": "#d4ac7f",
    "band": "#f5e2cd",
    "allocable_head": "#c29767",
    "non_climate": "#7f96ba",
    "climate": "#578145",
}
# Climate segments, bottom to top of the stack drawn here as top to bottom:
# (column, label, segment fill, label color).
CLIMATE_SEGMENTS = [
    ("mitigation_only", "Mitigation only", "#18470f", "#18470f"),
    ("adaptation_only", "Adaptation only", "#3b8e1d", "#338711"),
    ("mitigation_and_adaptation", "Overlap (both)", "#58ac8c", "#2c8465"),
]

# Texts.
TITLE_LINES = [
    [("How much bilateral development aid", TITLE_COLOR)],
    [("is spent on ", TITLE_COLOR), ("climate-related projects", TITLE_HIGHLIGHT), ("?", TITLE_COLOR)],
]
# The subtitle's share of development aid that is bilateral is computed from table `total_oda` of the same dataset
# (DAC members' bilateral ODA over all their ODA, both commitments, counting EU institutions as a donor without
# counting the DAC countries' contributions to them twice). It is written as a round number ("almost 90%", for 88.4% in
# 2024). Counting only DAC countries, with EU institutions as multilateral (the OECD's usual presentation), it would
# instead be about three-quarters.
# Lines broken by hand, as in the frame: the second sentence starts its own line (Lato widths 473, 491, 323 of 508 px).
SUBTITLE_LINES = [
    "Bilateral aid is given from one country (or EU institution) to another.",
    "It makes up {bilateral_share} of development aid; the rest is multilateral aid",
    "(money pooled by international organizations).",
]
SOURCE = "Based on OECD RioMarkers and Creditor Reporting System."
# The data products the source line names, which the plotted indicators must come from.
SOURCE_PRODUCTS = ["RioMarkers", "Creditor Reporting System"]
LICENSE = "CC BY"


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("climate_related_official_development_assistance")
    tb = ds.read("climate_related_oda_by_donor")
    tb_total_oda = ds.read("total_oda")

    #
    # Process data.
    #
    tb = tb[tb["country"] == ENTITY]
    year = int(tb["year"].max())
    row = tb[tb["year"] == year].iloc[0]
    values = {
        column: float(row[column]) / 1e9
        for column in ["oda_bilateral_total", "oda_bilateral_allocable", "climate_related", "non_climate"]
        + [column for column, *_ in CLIMATE_SEGMENTS]
    }
    values["non_allocable"] = values["oda_bilateral_total"] - values["oda_bilateral_allocable"]
    sanity_check_inputs(values)
    sanity_check_source(tb)
    # Share of all development aid that is bilateral, for the subtitle.
    total_oda = tb_total_oda[(tb_total_oda["country"] == ENTITY) & (tb_total_oda["year"] == year)]
    assert len(total_oda) == 1, f"No total ODA of {ENTITY} in {year}."
    bilateral_share = float(total_oda["oda_bilateral_share_of_total"].iloc[0])
    paths.log.info(
        f"{ENTITY} in {year}: "
        + ", ".join(f"{k} {v:.2f}bn" for k, v in values.items())
        + f"; bilateral share of all ODA {bilateral_share:.1f}%"
    )

    #
    # Save outputs.
    #
    fig = create_visualization(values, year, bilateral_share)
    export_frame(paths, fig, paths.short_name, template="mobile-square")
    plt.close(fig)


def sanity_check_inputs(values: dict[str, float]) -> None:
    """Each bar must add up to its total, so the stacks close at the bottom of the bars."""
    # The data is stored as float32, so sums agree to about a millionth, not exactly.
    tolerance = 1e-5 * values["oda_bilateral_total"]
    segments = sum(values[column] for column, *_ in CLIMATE_SEGMENTS)
    assert abs(segments - values["climate_related"]) < tolerance, (
        "Climate segments do not add up to climate-related ODA."
    )
    assert abs(values["non_climate"] + values["climate_related"] - values["oda_bilateral_allocable"]) < tolerance, (
        "Non-climate and climate-related ODA do not add up to allocable ODA."
    )
    assert 0 < values["non_allocable"] < values["oda_bilateral_total"], "Unexpected non-allocable ODA."
    # Plausible magnitudes (US$ billions), so a unit change upstream fails here.
    assert 150 < values["oda_bilateral_total"] < 300, "Unexpected total bilateral ODA."


def sanity_check_source(tb) -> None:
    """The source line names the two data products; the plotted indicators must come from them."""
    titles = {
        origin.title
        for column in ["oda_bilateral_total", "oda_bilateral_allocable", "climate_related"]
        for origin in tb[column].metadata.origins
    }
    for product in SOURCE_PRODUCTS:
        assert any(product in title for title in titles), f"No origin from {product}: update SOURCE."


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------


def create_visualization(values: dict[str, float], year: int, bilateral_share: float):
    fig = plt.figure(figsize=(FRAME_W / PX_PER_INCH, FRAME_H / PX_PER_INCH))
    fig.patch.set_facecolor("white")  # the PNG stays legible; the SVG is saved transparent

    draw_header(fig, round_share(bilateral_share))

    # Scale: the left bar spans the same height every year.
    scale = (BARS_BOTTOM - BARS_TOP) / values["oda_bilateral_total"]
    allocable_top = BARS_TOP + values["non_allocable"] * scale
    climate_top = allocable_top + values["non_climate"] * scale

    # Band joining the two bars, behind them.
    rect(fig, BAND_X0, allocable_top + BAND_TOP_OFFSET, BAND_X1, BAND_BOTTOM, COLORS["band"], "band__allocable")

    # Left bar: all bilateral aid.
    text(fig, LEFT_LABEL_X - 5, ALL_HEAD_Y, 15, "All bilateral aid", TEXT_COLOR, "head__all")
    text(
        fig,
        LEFT_LABEL_X - 5,
        ALL_VALUE_Y,
        15,
        billions(values["oda_bilateral_total"]),
        TEXT_COLOR,
        "head__all-value",
        bold=True,
    )
    for key, top, bottom, name, label_color in [
        ("non-allocable", BARS_TOP, allocable_top, "Non-allocable", "#ffffff"),
        ("allocable", allocable_top, BARS_BOTTOM, "Allocable", TITLE_COLOR),
    ]:
        column = "non_allocable" if key == "non-allocable" else "oda_bilateral_allocable"
        rect(fig, LEFT_X, top, LEFT_X + LEFT_W, bottom, COLORS[column.replace("oda_bilateral_", "")], f"bar__{key}")
        text(fig, LEFT_LABEL_X, top + NAME_OFFSET, 14, name, label_color, f"label__{key}", bold=True)
        lines = [billions(values[column]), percent(values[column], values["oda_bilateral_total"])]
        multiline(fig, LEFT_LABEL_X, top + VALUE_OFFSET, 13, lines, label_color, f"label__{key}-value")

    # Right bar: allocable aid. Heading above it.
    multiline(
        fig,
        RIGHT_HEAD_X,
        allocable_top - HEAD_NAME_ABOVE,
        15,
        ["Allocable", "bilateral aid"],
        COLORS["allocable_head"],
        "head__allocable",
        line_px=15,
    )
    text(
        fig,
        RIGHT_HEAD_X,
        allocable_top - HEAD_VALUE_ABOVE,
        15,
        billions(values["oda_bilateral_allocable"]),
        COLORS["allocable_head"],
        "head__allocable-value",
        bold=True,
    )

    # Non-climate segment and its label.
    rect(fig, RIGHT_X, allocable_top, RIGHT_X + RIGHT_W, climate_top, COLORS["non_climate"], "bar__non-climate")
    text(
        fig,
        RIGHT_LABEL_X,
        allocable_top + NAME_OFFSET,
        14,
        f"Other (non-climate): {billions(values['non_climate'])}",
        COLORS["non_climate"],
        "label__non-climate",
        bold=True,
    )
    text(
        fig,
        RIGHT_LABEL_X,
        allocable_top + VALUE_OFFSET,
        13,
        f"{percent(values['non_climate'], values['oda_bilateral_allocable'])} of allocable aid",
        TEXT_COLOR,
        "label__non-climate-value",
    )

    # Climate segments, each labeled beside its middle.
    top = climate_top
    for column, name, fill, label_color in CLIMATE_SEGMENTS:
        bottom = top + values[column] * scale
        key = name.lower().split(" (")[0].replace(" ", "-")
        rect(fig, RIGHT_X, top, RIGHT_X + RIGHT_W, bottom, fill, f"bar__{key}")
        label_top = (top + bottom) / 2 - SEGMENT_LABEL_H / 2
        value = f"{short_billions(values[column])} · {percent(values[column], values['oda_bilateral_allocable'])}"
        runs(
            fig,
            RIGHT_LABEL_X,
            label_top,
            13,
            [(f"{name}  ", label_color, True), (value, TEXT_COLOR, False)],
            f"label__{key}",
        )
        top = bottom
    assert abs(top - BARS_BOTTOM) < 0.01, "The right bar does not end where the left one does."

    # Bracket and label for all climate-related aid.
    rect(fig, BRACKET_X, climate_top + 1, BRACKET_X + BRACKET_W, BARS_BOTTOM - 1, TEXT_COLOR, "bracket__line")
    rect(
        fig,
        BRACKET_TICK_X,
        climate_top + 1,
        BRACKET_TICK_X + BRACKET_TICK_W,
        climate_top + 2.5,
        TEXT_COLOR,
        "bracket__top",
    )
    rect(
        fig,
        BRACKET_TICK_X,
        BARS_BOTTOM - 2,
        BRACKET_TICK_X + BRACKET_TICK_W,
        BARS_BOTTOM - 0.5,
        TEXT_COLOR,
        "bracket__bottom",
    )
    climate_value = values["climate_related"]
    text(
        fig,
        RIGHT_LABEL_X,
        climate_top - CLIMATE_NAME_ABOVE,
        14,
        f"Climate-related: {short_billions(climate_value, space=False)}",
        COLORS["climate"],
        "label__climate-related",
        bold=True,
    )
    text(
        fig,
        RIGHT_LABEL_X,
        climate_top - CLIMATE_SHARE_ABOVE,
        12,
        f"{percent(climate_value, values['oda_bilateral_allocable'])} of allocable aid",
        TEXT_COLOR,
        "label__climate-related-share",
    )

    draw_footer(fig, year)
    return fig


def draw_header(fig, bilateral_share: str) -> None:
    """Title (two lines, with a highlighted run) and subtitle (three lines)."""
    for i, line in enumerate(TITLE_LINES):
        runs(
            fig,
            MARGIN,
            TITLE_Y + i * TITLE_LINE_PX,
            TITLE_SIZE,
            [(s, color, True) for s, color in line],
            f"header__title-{i}",
            line_px=TITLE_LINE_PX,
        )
    lines = [line.format(bilateral_share=bilateral_share) for line in SUBTITLE_LINES]
    assert len(lines) <= SUBTITLE_MAX_LINES, "The subtitle has too many lines; shorten it."
    for line in lines:
        width = text_width(line, SUBTITLE_SIZE) * LATO_WIDTH_RATIO
        assert width <= CONTENT_W, f"Subtitle line wraps in the frame ({width:.0f} px): {line!r}"
    multiline(fig, MARGIN, SUBTITLE_Y, SUBTITLE_SIZE, lines, TEXT_COLOR, "header__subtitle")


def draw_footer(fig, year: int) -> None:
    """Note and source on their own rows, the license right-aligned on the source row."""
    note = f"Aid is official development assistance (ODA) commitments. Data is for {year}."
    runs(
        fig,
        MARGIN,
        NOTE_Y,
        FOOTER_SIZE,
        [("Note: ", FOOTER_COLOR, True), (note, FOOTER_COLOR, False)],
        "footer__note",
        line_px=FOOTER_LINE_PX,
    )
    runs(
        fig,
        MARGIN,
        SOURCE_Y,
        FOOTER_SIZE,
        [("Data source: ", FOOTER_COLOR, True), (SOURCE, FOOTER_COLOR, False)],
        "footer__source",
        line_px=FOOTER_LINE_PX,
    )
    fig.text(
        fx(FRAME_W - MARGIN),
        fy(baseline(SOURCE_Y, FOOTER_LINE_PX, FOOTER_SIZE)),
        LICENSE,
        fontsize=FOOTER_SIZE * PT_PER_PX,
        color=FOOTER_COLOR,
        ha="right",
        va="baseline",
        gid="footer__license",
    )
    for row, lead, body in [("note", "Note: ", note), ("source", "Data source: ", SOURCE)]:
        width = (
            advance(lead, FOOTER_SIZE, True) * LATO_FOOTER_RATIO[True]
            + text_width(body, FOOTER_SIZE) * LATO_FOOTER_RATIO[False]
        )
        limit = CONTENT_W - (text_width(LICENSE, FOOTER_SIZE) + LICENSE_GAP if row == "source" else 0)
        assert width <= limit, f"The {row} row no longer fits one line ({width:.0f} px of {limit:.0f})."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def round_share(share: float) -> str:
    """A share as a round number for prose: the nearest multiple of 5, as "almost", "about" or "over" it."""
    nearest = 5 * round(share / 5)
    if abs(share - nearest) < 0.5:
        return f"about {nearest}%"
    return f"{'almost' if share < nearest else 'over'} {nearest}%"


def billions(value: float) -> str:
    return f"${value:.0f} billion"


def short_billions(value: float, space: bool = True) -> str:
    return f"${value:.0f}{' ' if space else ''}bn"


def percent(part: float, whole: float) -> str:
    return f"{part / whole * 100:.0f}%"


def fx(x_px: float) -> float:
    return x_px / FRAME_W


def fy(y_px: float) -> float:
    return 1 - y_px / FRAME_H


def prop(size_px: float, bold: bool = False) -> FontProperties:
    return FontProperties(family=MEASURED_FONT_STACK, size=size_px * PT_PER_PX, weight="bold" if bold else "normal")


def text_width(s: str, size_px: float, bold: bool = False) -> float:
    """Ink width of `s` in frame px."""
    return TextPath((0, 0), s, prop=prop(size_px, bold)).get_extents().width / PT_PER_PX


def advance(s: str, size_px: float, bold: bool = False) -> float:
    """Advance of a run, trailing spaces included (a sentinel glyph keeps them from being trimmed)."""
    return text_width(s + "|", size_px, bold) - text_width("|", size_px, bold)


def cap_height(size_px: float) -> float:
    return TextPath((0, 0), "0", prop=prop(size_px)).get_extents().ymax / PT_PER_PX


def baseline(top: float, line_px: float, size_px: float) -> float:
    """Baseline of a line whose box starts at `top`, with the cap height centered in the line box."""
    return top + line_px / 2 + cap_height(size_px) / 2


def text(fig, x: float, top: float, size: float, s: str, color: str, gid: str, bold: bool = False) -> None:
    """One line of text whose box (at Lato's auto line height) starts at `top`."""
    fig.text(
        fx(x),
        fy(baseline(top, size * AUTO_LINE_HEIGHT, size)),
        s,
        fontsize=size * PT_PER_PX,
        color=color,
        fontweight="bold" if bold else "normal",
        ha="left",
        va="baseline",
        gid=gid,
    )


def multiline(
    fig, x: float, top: float, size: float, lines: list[str], color: str, gid: str, line_px: float | None = None
) -> None:
    """Several lines, one call per line (named `<gid>`, `<gid>-line1`, ...), at Figma's line pitch."""
    line_px = line_px or size * AUTO_LINE_HEIGHT
    for i, line in enumerate(lines):
        fig.text(
            fx(x),
            fy(baseline(top + i * line_px, line_px, size)),
            line,
            fontsize=size * PT_PER_PX,
            color=color,
            ha="left",
            va="baseline",
            gid=gid if i == 0 else f"{gid}-line{i}",
        )


def runs(
    fig, x: float, top: float, size: float, parts: list[tuple[str, str, bool]], gid: str, line_px: float | None = None
) -> None:
    """A row of runs in different colors or weights, laid out by advance. No run may start with a space."""
    line_px = line_px or size * AUTO_LINE_HEIGHT
    for i, (s, color, bold) in enumerate(parts):
        assert not s.startswith(" "), f"run {s!r} starts with a space"
        fig.text(
            fx(x),
            fy(baseline(top, line_px, size)),
            s.rstrip(),
            fontsize=size * PT_PER_PX,
            color=color,
            fontweight="bold" if bold else "normal",
            ha="left",
            va="baseline",
            gid=f"{gid}-{i}",
        )
        x += advance(s, size, bold)


def rect(fig, x0: float, top: float, x1: float, bottom: float, color: str, gid: str) -> None:
    fig.add_artist(
        Rectangle(
            (fx(x0), fy(bottom)),
            (x1 - x0) / FRAME_W,
            (bottom - top) / FRAME_H,
            facecolor=color,
            edgecolor="none",
            gid=gid,
        )
    )
