"""Global number of children aged 5-17 in hazardous and non-hazardous child labor, 2000-2024.

Chart for a data insight: "Child labor has nearly halved in a generation, with progress coming from hazardous work".
Seven stacked columns (2000, 2004, ..., 2024): non-hazardous child labor at the bottom, hazardous child labor on top,
the total above each column and both segment values inside every column. There is no legend: the subtitle names the two
categories in their colors. Values are whole millions, rounded half up (170.5 -> 171), so a column's two segment values
always add up to its total. Only the first column carries the unit ("246 million" above it, "171M" and "75M" inside);
the others show bare numbers.

Data: table `child_labor` of `garden/un/2026-04-08/child_labor_report` (ILO-UNICEF Global Estimates of Child Labour
2024), World, sex "total", age "5-17", columns `number_child_labor` and `number_hazardous_work`. Hazardous work is a
subset of child labor (ILO-UNICEF 2024 report, methodology, p. 59), so non-hazardous child labor = child labor -
hazardous work. 2000-2012 come from the report's trend chart and 2016-2024 from its statistical annex (see the snapshot
script).

This step draws the chart area only, at the size of the band it fills in the Figma frame (below). The title,
subtitle, source and article line live in the template's own slots, so they are not drawn here. Its PNG is a preview in
the fallback font (DejaVu Sans on Linux), which sets wider than the frame's Lato.

Figma handoff
-------------
Where: Charts (2026) file `s6Sv60bakebRRW2TxsMQbF`. Page "20261107 The global decline in child labor has been
driven by a sharp reduction in hazardous work (Esteban)" (`31010:5`), placed right below the
`-----------------------------------------` divider page. Frame `child-labor-hazardous-decline` (`31010:6`, the
exported PNG's filename), 540 x 540, cloned from `DI_Template` (`6799:1859`). On its left, a raster of the
requester's first mockup (`31011:82`), kept as a reference.

Texts (template slots; set `characters`, then restore the mixed weights and colors):
| Slot | Text | Weight and color |
|---|---|---|
| Title (`Instagram/Title`, 3 lines) | "Child labor has nearly halved in a generation, with progress coming from hazardous work" | "hazardous work" filled with `Default Palette/Dark Copper` |
| Subtitle (`Instagram/Subtitle`, 3 lines) | "Number of children aged 5–17 in hazardous and non-hazardous\\nchild labor, in millions. Hazardous means work in dangerous\\nindustries or occupations, or for 43 or more hours a week." | the first "hazardous" filled with `Default Palette/Dark Copper`, "non-hazardous" with `Line and Slope Charts/Turquoise` (they replace the legend) |
| Data source (`Instagram/Source`, footer `Frame 37` at y 472) | "Data source: " + source_citation(...) = "International Labour Organization and UNICEF (2025)" | "Data source:" bold |
| CC BY | unchanged | - |
| Article line (node "Note": a clone of the source row inside the footer, `layoutPositioning` ABSOLUTE, x 0, y 20, width 508, 2 lines, Lato 12, fill `Data Insights/Source`) | "Read more about the definitions in our article:\\n138 million children are in child labor. What does this actually mean?" | first line bold (as on the causes-of-death IG frames that point to an article) |

Band: header bottom 166 (3-line title 87 + 6 + 3-line subtitle 57, from y 16). Footer: the source row at y 472, the
article line 4 px below it, ending at 524 (footer y = 524 - 16 - 4 - 32). Band 306 px; the chart canvas is 508 x 280 =
the band minus a 13 px gap on each side (280 rather than 278 because 2.78 in x 300 dpi does not land on a whole pixel,
which the verifier reads as a cropped PNG). If any text above changes its line count, re-measure and change
CANVAS_H_PX.

Import: `upload_assets` the SVG (never `createNodeFromSvg`). It arrives as a FRAME at 487.68 x 268.8 (matplotlib
writes points, Figma reads them at 96 dpi: 508 x 0.96). `rescale(508 / frame.width)` (= 1.0417 = 100 / 96), then
move its children into the clone at (16, 179) offset by their own x/y, delete the import frame and group them as
`chart`. Delete `patch_1` (transparent, but it sets the group's box). Then run `rebuild_native_marks.js` on the
frame so the bars are native rectangles.

Restyle, in the same call as the fit (import each style with `figma.importStyleByKeyAsync(key)`; bind the style, then
set node opacity, since binding replaces the paint and its opacity). Each node's role is the nearest ancestor whose
name carries "__":
- `bar__other-child-labor` rectangles (non-hazardous): `Default Palette/Turquoise` (key
  6de8dd756690b393fad97f50de5224d427c3d04f), node opacity 0.9.
- `bar__hazardous-child-labor` rectangles: `Default Palette/Dark Copper` (key
  8a4dfad63dc15253a09b9c50acf21f807f573b25), node opacity 0.9. The design follows Hannah's stacked-column DIs (page
  "20260520 Global sales combustion engine cars peaked (Hannah)", which runs its fills at 0.85); 0.9 here because the
  white values on the copper segments need 4.5:1 (4.78 at 0.9, 4.30 at 0.85).
- Subtitle words: `Line and Slope Charts/Turquoise` (key 4054b66b4fb2e791e7ba868f85281dfce2df620e, #008291) is the
  darker variant for text on white (4.56:1, against 2.75:1 for the fill color).
- `value__total-*`: Lato Bold 13 (the 2000 one left-aligned on its column, the rest centred). `value__other-*`: Lato
  Regular 13. Both filled with the file's local paint style `Instagram/Title` (#2d2e2d). `value__hazardous-*`: Lato
  Regular 13, `Data Insights/Background White` (#ffffff). Set the font and size, then re-anchor each label (centre or
  left edge, by its own alignment), since the face change moves it.
- `horizontal-axis__<year>`: Lato Regular 12, `Instagram/Subtitle` (#5b5b5b). Baseline and tick marks
  (`horizontal-axis__zero-line`, `horizontal-axis__ticks`): 1 px, #999999, unbound (as on Hannah's page).

Expected after the fit: the `chart` group spans x 16 to 524 and y 179 to 459, i.e. the content box and a 13 px gap
to the header (166) and to the footer (472). Expected audit numbers (`color_audit.py '#4cb2c1,#a4623e'`, the fills
composited at 0.9 on white): min ΔE 46.5 (protanopia), grayscale seam 1.92:1; #2d2e2d on the turquoise fill 5.48:1.
"""

import logging
from decimal import ROUND_HALF_UP, Decimal

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection, PatchCollection
from matplotlib.patches import Rectangle

from etl.helpers import PathFinder
from etl.viz.static import apply_svg_rcparams, export_frame, source_citation

paths = PathFinder(__file__)
apply_svg_rcparams()

log = logging.getLogger(__name__)

# Fonts: name Lato first so Figma sets the import in Lato; matplotlib falls back to what is installed.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: not any(f"Font family '{face}' not found" in record.getMessage() for face in EMITTED_FONT_STACK)
)

# Rows plotted.
COUNTRY, SEX, AGE = "World", "total", "5-17"
EXPECTED_YEARS = [2000, 2004, 2008, 2012, 2016, 2020, 2024]
# The values the ILO-UNICEF 2024 report prints in its trend chart (Fig. 2, p. 16), in millions.
REPORT_TOTAL = [245.5, 222.3, 215.2, 168.0, 151.6, 160.0, 137.6]
REPORT_HAZARDOUS = [170.5, 128.4, 115.3, 85.3, 72.5, 79.0, 54.0]

# Canvas, in template px (100 px per inch; 1 px = 0.72 pt). See "Band" in the docstring.
CANVAS_W_PX, CANVAS_H_PX = 508, 280
PX_PER_INCH = 100
PT = 0.72

# Layout, in template px.
LABEL_COLUMN_PX = 0  # none: the subtitle names the two categories in their colors, so the columns span the width
BAR_FRACTION = 0.68  # column width as a share of the slot
TOP_PX = 19.7  # above the tallest column, for its total (its Lato box in the frame ends exactly at the canvas top)
BOTTOM_PX = 18.7  # below the baseline: 5 px tick, 3 px gap, then the year (its Lato box ends at the canvas bottom)
TICK_PX = 5
VALUE_GAP_PX = 4  # between a column's top and its total

# Type, in template px.
SIZE_VALUE = 13
SIZE_YEAR = 12
# Unit on the first column's segment values ("171M"): grapher's own abbreviation for millions in chart labels.
SEGMENT_UNIT = "M"

# Colors are placeholders the Figma pass binds to library styles (see the docstring); the hexes are those styles.
OTHER_FILL = "#38aaba"  # Default Palette/Turquoise
HAZARDOUS_FILL = "#9a5129"  # Default Palette/Dark Copper
FILL_OPACITY = 0.9
DARK_TEXT = "#2d2e2d"  # Text/Gray 100
AXIS_TEXT = "#5b5b5b"  # Text/Gray 80
AXIS_LINE = "#999999"  # grapher's zero line and tick marks


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("child_labor_report")
    tb = ds.read("child_labor")

    #
    # Process data.
    #
    tb = tb[(tb["country"] == COUNTRY) & (tb["sex"] == SEX) & (tb["age"] == AGE)]
    tb = tb[["year", "number_child_labor", "number_hazardous_work"]].dropna().sort_values("year").reset_index(drop=True)
    sanity_check_inputs(tb)

    # Other child labor is everything that is not hazardous work, since hazardous work is a subset of child labor.
    tb["total"] = tb["number_child_labor"] / 1e6
    tb["hazardous"] = tb["number_hazardous_work"] / 1e6
    tb["other"] = tb["total"] - tb["hazardous"]

    # Printed values: whole millions, rounded half up from the exact counts (Python's round() sends 170.5 to 170).
    tb["total_label"] = [millions_half_up(v) for v in tb["number_child_labor"]]
    tb["hazardous_label"] = [millions_half_up(v) for v in tb["number_hazardous_work"]]
    tb["other_label"] = [
        millions_half_up(cl - hw) for cl, hw in zip(tb["number_child_labor"], tb["number_hazardous_work"])
    ]
    sanity_check_labels(tb)

    # The template's source slot is filled in Figma; log the string it takes.
    source = source_citation(tb["number_child_labor"], tb["number_hazardous_work"])
    log.info(f"Data source slot: {source}")

    #
    # Save outputs.
    #
    fig = build(tb)
    export_frame(paths, fig, paths.short_name)
    plt.close(fig)


def sanity_check_inputs(tb) -> None:
    assert list(tb["year"]) == EXPECTED_YEARS, f"Unexpected years: {list(tb['year'])}."
    assert (tb["number_hazardous_work"] <= tb["number_child_labor"]).all(), "Hazardous work exceeds child labor."
    # The values should be the ones the report itself prints.
    total = [round(v / 1e6, 1) for v in tb["number_child_labor"]]
    hazardous = [round(v / 1e6, 1) for v in tb["number_hazardous_work"]]
    assert total == REPORT_TOTAL, f"Child labor differs from the report: {total}."
    assert hazardous == REPORT_HAZARDOUS, f"Hazardous work differs from the report: {hazardous}."


def millions_half_up(count) -> int:
    """Round a count to whole millions, half up."""
    return int((Decimal(int(count)) / Decimal(10**6)).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def sanity_check_labels(tb) -> None:
    # A column's printed segments must add up to its printed total, or the chart reads as an arithmetic error.
    mismatched = tb[tb["hazardous_label"] + tb["other_label"] != tb["total_label"]]
    assert mismatched.empty, f"Rounded segments do not add up to the rounded total in {list(mismatched['year'])}."


def build(tb):
    fig = plt.figure(figsize=(CANVAS_W_PX / PX_PER_INCH, CANVAS_H_PX / PX_PER_INCH))
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_axis_off()
    ax.patch.set_visible(False)

    # Map template px to data units: x in column slots, y in millions.
    n = len(tb)
    slot_px = (CANVAS_W_PX - LABEL_COLUMN_PX) / (n - 1 + BAR_FRACTION)
    px_x = 1 / slot_px
    left = -BAR_FRACTION / 2
    ax.set_xlim(left, left + CANVAS_W_PX * px_x)
    px_y = tb["total"].max() / (CANVAS_H_PX - TOP_PX - BOTTOM_PX)
    ax.set_ylim(-BOTTOM_PX * px_y, (CANVAS_H_PX - BOTTOM_PX) * px_y)

    xs = list(range(n))
    half = BAR_FRACTION / 2

    # Stacked columns, one collection per category so each category is one named layer.
    for gid, bottoms, heights, color in [
        ("bar__other-child-labor", [0.0] * n, tb["other"], OTHER_FILL),
        ("bar__hazardous-child-labor", tb["other"], tb["hazardous"], HAZARDOUS_FILL),
    ]:
        rects = [Rectangle((x - half, b), BAR_FRACTION, h) for x, b, h in zip(xs, bottoms, heights)]
        ax.add_collection(
            PatchCollection(rects, facecolor=color, edgecolor="none", linewidth=0, alpha=FILL_OPACITY, gid=gid)
        )

    # Baseline and tick marks.
    ax.add_collection(
        LineCollection(
            [[(left, 0), (xs[-1] + half, 0)]], colors=AXIS_LINE, linewidths=1 * PT, gid="horizontal-axis__zero-line"
        )
    )
    ax.add_collection(
        LineCollection(
            [[(x, 0), (x, -TICK_PX * px_y)] for x in xs],
            colors=AXIS_LINE,
            linewidths=1 * PT,
            gid="horizontal-axis__ticks",
        )
    )

    for x, row in zip(xs, tb.itertuples()):
        year = int(row.year)
        # The first column carries the unit: "246 million" above it (left-aligned on the column, since it is wider
        # than the column) and "171M" / "75M" inside it. The other columns show bare numbers.
        first = year == EXPECTED_YEARS[0]
        # Total above the column.
        ax.text(
            x - half if first else x,
            row.total + VALUE_GAP_PX * px_y,
            f"{row.total_label} million" if first else f"{row.total_label}",
            ha="left" if first else "center",
            va="bottom",
            fontsize=SIZE_VALUE * PT,
            fontweight="bold",
            color=DARK_TEXT,
            gid=f"value__total-{year}",
        )
        # Year below the tick mark.
        ax.text(
            x,
            -(TICK_PX + 3) * px_y,
            str(year),
            ha="center",
            va="top",
            fontsize=SIZE_YEAR * PT,
            color=AXIS_TEXT,
            gid=f"horizontal-axis__{year}",
        )
        # Segment values, centered on their segments.
        ax.text(
            x,
            row.other / 2,
            f"{row.other_label}{SEGMENT_UNIT if first else ''}",
            ha="center",
            va="center",
            fontsize=SIZE_VALUE * PT,
            color=DARK_TEXT,
            gid=f"value__other-{year}",
        )
        ax.text(
            x,
            row.other + row.hazardous / 2,
            f"{row.hazardous_label}{SEGMENT_UNIT if first else ''}",
            ha="center",
            va="center",
            fontsize=SIZE_VALUE * PT,
            color="#ffffff",
            gid=f"value__hazardous-{year}",
        )

    fig.patch.set_facecolor("white")  # the PNG keeps an opaque canvas; export_frame saves the SVG transparent
    return fig
