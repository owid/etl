"""Recreate the 'Expected height of boys and girls' growth-curve chart.

Each panel shows two nested bands from the WHO growth reference standards and the median. The outer
band runs from -2 SD to +2 SD, so its lower edge is the stunting threshold: the boundary a reader has
to find carries two encodings at once -- where the tint stops and a dashed line -- and everything below
the shaded area is the stunted region.

Both bands are symmetric about the median, and both edges of the outer one are the same kind of cut.
An earlier version ran the threshold as a faint dotted line *inside* a band spanning the 0.1st to the
99.9th percentile, which put two near-parallel boundaries a few pixels apart at the bottom of each
panel -- one a labelled band edge, one an unlabelled line -- and gave the reader no cue as to which
side of the line was 'too short'. Cutting the band at the threshold removed the competing edge; making
the far edge +2 SD rather than the 99.9th percentile then removed the asymmetry that replaced it, where
one edge of a band was a standard deviation and the other a percentile. The cost is the tall upper
tail: the band now tops out around 191 cm rather than 199, which the 200 cm axis still contains.

Neither panel repeats the other sex's median. The two medians run within a few millimetres of each
other from birth to about age 9, so a second line traces the panel's own median for two thirds of the
range -- the same doubling that splitting the sexes into panels was meant to remove. Where the two
sexes differ can be read off the panels at a shared gridline.

An encoding diagram names each part of the chart -- see `draw_encoding_diagram`. There is no legend
in either version.

Two versions are emitted, following the static-chart templates:

- desktop, 850x638: panels side by side, diagram inside the Boys panel, footer carrying Note, Data
  source, the OurWorldinData.org tagline and the license line.
- mobile, 540x824: panels side by side in the portrait frame, diagram in the header, footer reduced
  to Data source plus the license, which is all that template has room for. It has no Note slot, so
  the standards-versus-reference distinction appears only on desktop; the shared subtitle calls the
  whole range a reference, which is what keeps mobile from over-claiming without it. Its panels are
  217px wide, which is why the diagram cannot sit inside a panel.

Both layouts put their panels side by side rather than stacked. Stacked in the portrait frame each
panel is a 2:1 landscape box, about 222px of height for a 165 cm range, and the adolescent growth
spurt is not visible in it; side by side gives each panel 2.4x the vertical resolution.

Replaces the hand-drawn 'Expected Healthy Growth Curves for Boys and Girls' image used on the
human-height topic page and the stunting-definition article.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this step
fixes is the structure: which text slots exist, in what order, and which share a row.

Figma
-----
The whole handoff, written out so it can be redone in a later session with nothing but this file.

**Target.** File `Charts (2026)`, key `s6Sv60bakebRRW2TxsMQbF`. Page
`20260812 Expected height of boys and girls, from birth to age 19 (Pablo A)`, sitting at the top of
the dated block -- insert after the `-----------` divider page, not at a counted index. Two frames,
each named for the slug the website exports by, with a reference copy of this step's own render to
their left:

| Frame | Node | Open it | Cloned from | Size |
|---|---|---|---|---|
| `expected-height-boys-girls` | `26869:1501` | [link](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=26869-1501) | `5332:75` Static Chart Template_Horizontal | 850x638 |
| `expected-height-boys-girls-mobile` | `26869:1515` | [link](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=26869-1515) | `24590:32` Static Chart Template_Mobile (example 2) | 540x824 |

The node ids are a convenience, not the join: they die if anyone rebuilds a frame from the template
rather than swapping its chart -- which happened here on 2026-08-27, when the design team's rebuild of
`Static Chart Template_Horizontal` made re-cloning worthwhile and both frames got new ids. **The frame name is what actually identifies a chart** -- it is the
kebab-case slug the website exports the PNG by, so it is the same string in the Figma layer panel, in
the exported filename and in this table. Lost the ids? Search the file's page list for `height`; the
pages are named `YYYYMMDD <Title> (<Creator>)` and this one is dated 20260812, the day it was first
placed, which does not change when the chart is refreshed.

**Import.** Upload with `upload_assets` and POST the file to the returned `submitUrl`
(`curl -F "file=@<path>"`); never `createNodeFromSvg`, which caps at 50k characters. The upload lands
on whatever page Figma has open, so move it. Then:

1. Move the import's children out of its wrapper FRAME and delete the frame: it carries a white fill
   that would cover the template's background, and `resize()` on it rewraps every text node.
2. `rescale(100 / 96)`. matplotlib declares the root in points, Figma imports at 96px per inch, and
   this figure is built at 100 template px per inch. `rescale(clone.width / imported.width)` is the
   same number and self-correcting.
3. Keep that group as the reference copy; `clone()` it for the working copy and append the clone to
   the template frame. Rebuild from the reference, never by patching the working copy.
4. From the working copy delete `patch_1`, `title`, `subtitle`, `note`, `data-source`, `tagline` and
   `license`. The template's own slots carry those strings; left in place they are duplicated.

**Template text slots.** Fill them from this step's own constants, restoring the mixed weights the
templates ship -- setting `characters` propagates the first character's style over the whole string:

| Slot | Content | Weights |
|---|---|---|
| Title | `TITLE` | template's Playfair |
| Subtitle | `SUBTITLE` | Lato Regular |
| `Note:` (desktop only) | `build_note(...)` | `Note:` Bold, rest Regular |
| `Data source:` | `Data source: ` + `source_citation(...)` from `etl.static_viz` | `Data source:` Bold, rest Regular |
| Tagline (desktop) | leave the template's | -- |
| License | `Licensed under ` / `CC-BY` / ` by the author ` / `AUTHOR` | Medium / Bold / Medium / Bold |

Both templates carry the same footer slots, so both get the same license string; mobile just stacks
its two rows (`Frame 15`, source at y=770 and license at y=791) where desktop shares one row with the
tagline.

Two positions are derived rather than the template's fixed y, because the template pins them for a
two-line title and a two-line subtitle:

- `subtitle.y = 16.216 + title.height + 6`. Reset `title.y = 16.216` first -- the desktop header is
  not an auto-layout frame, so Figma re-centres a title that shrinks to one line.
- `note.y = 591 - 4 - note.height`, so a fourth line eats into the chart area rather than the source
  row. The 4 is `Frame 22`'s own `itemSpacing`; read it off the clone rather than typing it, since it
  was 5.4 in the template's previous build. Mobile's header is auto-layout and needs neither.

**Colors.** Bind each panel's median *and its threshold* to the library style, and derive that panel's
bands from it; the library carries no tints. The gid names a group, so descend to its `VECTOR` children
before calling the setter.

The threshold has to be bound too, and to the same style as the median beside it. The step draws both
in the panel's own colour on purpose -- colour says which panel a mark belongs to, style says which mark
it is -- so binding only the median splits the pair in Figma: the median moves to the library colour
while the threshold keeps matplotlib's `#4c72b0` / `#dd8452`. Binding a paint style leaves
`dashPattern` alone, so the dash survives the binding. The encoding diagram's marks are not in this
table: they stay the step's grey, which is what marks the diagram as a key rather than as data.

| Layer | Treatment |
|---|---|
| `boys___50` | `setStrokeStyleIdAsync` -> `Default Palette/Denim`, key `e1538d9330d7b22168f0c19fa562897aa8975f90` |
| `girls___50` | `setStrokeStyleIdAsync` -> `Default Palette/Rusty Orange`, key `65bab597d085689b1ea82a69f4d785cb9212c234` |
| `<sex>__stunting-threshold` | the same style as `<sex>___50` |
| `<sex>__19-in-20-children` | that style's color blended 0.90 towards white |
| `<sex>__8-in-10-children` | blended 0.74 towards white |

Denim and Rusty Orange separate by dE 70 at worst; their grayscale seam is 1.14:1, which does not gate
here because the two series sit in separate, text-titled panels. Which panel takes which is set by
`PANEL_COLOR_INDEX`, not here -- keep the two in step.

**In-plot text.** Figma substitutes Inter for matplotlib's family, so restyle to Lato at three ranks
and re-anchor every label on its mark. Do both in one call and in that order: the widths only settle
on the next call, and a later coordinate patch would use anchors that the fit has already moved.

| Rank | Size | Weight |
|---|---|---|
| Facet titles (`Boys`, `Girls`) | 16 | Bold |
| Tick labels, `Age in years` | 14 | `Age in years` Bold, ticks Regular |
| Diagram labels | 12 | Regular |

Anchors: y ticks by their right edge; the first x tick by its left and the last by its right, the rest
centred; `Median` by its right edge; the band labels by their left; the stunting label by its centre.

**Fit.** Centre the group in the band between the header's bottom and the footer's first visible row
(`footer.y + min(0, source.y)`). No rescale is needed -- this step sizes the plot to the template --
and one would move every font off its rank.

**Audit before showing it.** Expect sizes {16, 14, 12} only, Lato Regular and Bold only, both medians
*and both thresholds* reporting a bound style -- and each threshold reporting the same colour as its
own median, which is the check that catches a stale band or threshold selector -- no ink outside
16..W-16, and gaps of about 14 on desktop and 20 on mobile.
"""

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties
from matplotlib.lines import Line2D
from matplotlib.textpath import TextPath
from matplotlib.ticker import FuncFormatter
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.static_viz import PIXELS_PER_INCH, apply_svg_rcparams, export_frame, source_citation

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

paths = PathFinder(__file__)

# One panel per sex. Colors are seaborn "deep" positions rather than raw hexes, so the
# chart shifts with the shared palette instead of pinning its own. Position 0 is the palette's blue
# and 1 its orange, which Figma rebinds to Denim and Rusty Orange respectively -- so this mapping is
# what decides which library colour each panel ends up in, and the Colors table in the Figma section
# has to move with it.
PANEL_COLOR_INDEX = {"Boys": 0, "Girls": 1}

# The stunting threshold's stroke. It carries no colour of its own: in a panel it takes that panel's
# colour, and in the encoding diagram it takes the diagram's grey, so colour says which panel a mark
# belongs to and style says which mark it is. A neutral slate here instead read as chart furniture --
# gridlines and annotation are grey -- which is the wrong rank for the chart's most important idea.
#
# Dashed rather than dotted, at a weight that puts it third behind the median (2.6pt) and ahead of the
# gridlines (1.0pt), and that survives both print and the 217px mobile panel. At 0.8pt dotted it was
# the faintest stroke in the chart.
#
# The pattern is in multiples of the line's own width, NOT points: matplotlib multiplies a dash
# sequence by the linewidth (`rcParams["lines.scale_dashes"]`, on by default), so a pattern written in
# points comes out `linewidth` times longer than intended. At 1.4pt this draws a 4.5pt dash with a
# 2.8pt gap -- the SVG carries `stroke-dasharray: 4.48,2.8`, which is what to check against. Reading
# the same numbers as points shipped a 7pt dash on a 1.4pt stroke, five times the stroke width, which
# reads as stretched at any size and looked like a Figma import defect rather than a step one.
STUNTING_LINEWIDTH = 1.4
STUNTING_DASHES = (0, (3.2, 2.0))

# What the threshold is called in the encoding diagram. It leads with the direction because the mark
# it names is the region below the line rather than the line itself, and it keeps the plain-language
# gloss rather than deferring it to the Note, which the mobile template has no room for.
STUNTING_LABEL = "Stunted: below this line, too short for their age"

# Neutral grey for the encoding diagram's bands, median and threshold. Grey is what marks the diagram
# as a key rather than as data, and it is why the diagram has to separate the median from the threshold
# by style alone -- solid against dashed -- which is the distinction the panels rely on too.
DIAGRAM_COLOR = "#666666"

# Nested percentile bands, drawn widest first, as (lower column, upper column, how far the fill is
# blended towards white, layer name). Each band is a flat tint, not a translucent fill: an alpha fill
# composites onto whatever is behind it, and the SVG is saved transparent for the Figma template to
# supply the background. A tint renders the same on any backdrop and gives Figma one flat fill each.
BANDS = [
    ("height_sd_minus_2", "height_sd_plus_2", 0.90, "19-in-20-children"),
    ("height_percentile_10", "height_percentile_90", 0.74, "8-in-10-children"),
]

# What 2 SD is worth as a percentile: the share of children beyond the threshold at either end. The
# note states it and the outer band's label is derived from it, so the two can't drift apart. Carried
# to seven figures rather than rounded, because both derived strings are printed to one decimal and
# 2.275 would round the band's label up to 95.5%.
#
# The conversion is exact rather than approximate, which is what lets a band bounded in standard
# deviations be labelled as a share at all: WHO's height-for-age standard sets the LMS skewness
# parameter L to 1 at every age, so the distribution is normal and -2 SD is the 2.275th percentile
# rather than an age-varying centile. `assert_threshold_is_a_fixed_percentile` checks L is still 1 in
# the data before the label ships.
STUNTED_SHARE = 2.2750132

# The encoding diagram names each band by the share of children inside it, and a cut point is not a
# share: 2.3% of children fall below -2 SD and the same share above +2 SD, so the band between them
# holds 100 - 2 x 2.3 = 95.4%. The inner band runs from the 10th percentile to the 90th, holding 80%.
BAND_LABELS = [f"{100 - 2 * STUNTED_SHARE:.1f}% of children", "80% of children"]

# The inner band's half-width as a share of the outer's, for the encoding diagram's schematic. Both
# bands are fixed multiples of the standard deviation -- the 10th and 90th percentiles sit at -+1.2816
# SD -- so the ratio is 1.2816 / 2 and holds at every age rather than being eyeballed. Under the old
# asymmetric band it was an approximation (0.42) fitted to one end of the range.
DIAGRAM_INNER_RATIO = 1.2816 / 2

# Percentiles drawn as lines on top of the bands, as (column, line width), so a specific centile
# can be read off rather than only a range. Named in the encoding diagram, not on the line.
QUANTILE_LINES = [
    ("height_percentile_50", 2.6),
]

MEDIAN_COLUMN = "height_percentile_50"
STUNTING_COLUMN = "height_sd_minus_2"

# Axis treatment copied from grapher so the static chart reads like our interactive ones.
# Values from owid-grapher: TICK_COLOR and GRID_LINE_DASH_PATTERN in
# packages/@ourworldindata/grapher/src/axis/AxisViews.tsx, GRAPHER_DARK_TEXT (= GRAY_80) in
# .../color/ColorConstants.ts. Grapher dashes its gridlines rather than drawing them solid and
# labels axes in bold. The y axis carries no line: its gridlines carry the reading.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
GRID_LINEWIDTH = 1.0

# Height gridline spacing, in cm. The y limits are snapped out to whole steps of this, so the
# outermost gridlines land exactly on the plot's edges -- see where the limits are set.
HEIGHT_STEP = 20
TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"

# x-axis tick marks and baseline, both from AxisViews.tsx. The marks come from
# HorizontalAxisComponent (5px long, 1px wide, SOLID_TICK_COLOR, hanging below the axis, and
# LineChart passes showTickMarks={true}). The line they hang from is not an axis line -- grapher has
# no such component -- it is VerticalAxisZeroLine, the same colour and width, spanning the plot at
# y=0. This chart's y axis does not reach zero, so there is no zero line to draw; the same treatment
# is applied to the baseline instead, which is what makes the end ticks close it like an elbow.
TICK_COLOR = "#999999"
TICK_LENGTH = 5
TICK_WIDTH = 1

# Facet titles, from FacetChart.tsx: bold (FACET_LABEL_FONT_WEIGHT = 700), in GRAPHER_DARK_TEXT like
# the tick labels rather than in the series colour, sitting above the panel and left-aligned with its
# content, with half a line of padding under them (labelPadding = 0.5 * facetLabelFontSize). Grapher
# derives its facet base font size as facetLabelFontSize / GRAPHER_FONT_SCALE_12 * 0.9, so the label
# ends up about 1/0.9 of the tick size.
FACET_TITLE_SCALE = 1 / 0.9
FACET_TITLE_PAD = 0.5

TITLE = "Expected height of boys and girls, from birth to age 19"

# Credited as the author of the visualization on the license line, mirroring the slot the
# static-chart templates leave for it.
AUTHOR = "Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world\u2019s largest problems."

# The two layouts, taken from the static-chart template frames. All geometry is in the
# templates' own pixel units, measured from the top-left as Figma reports them, and converted
# to figure fractions below; the figure is sized at 100 template px per inch so the saved
# image has the template's exact proportions. `full_footer` is what separates the desktop
# templates (Note and tagline present) from the mobile ones (neither).
#
# Row positions come from "Static Chart Template_Horizontal" (850x638) and "Static Chart
# Template_Mobile (example 2)" (540x824); the tall mobile frame is the one that gives two
# side-by-side panels enough height to read. Font sizes are derived from each slot's height in the
# template: a template px is 0.72pt, and a line of text occupies about 1.8x its point size.
LAYOUTS = {
    "height_for_age": {
        "size": (850, 638),
        "template": "horizontal",
        "margin": 16,
        "title_y": 16,
        # The three footer rows of `Static Chart Template_Horizontal`, measured 2026-08-27 after the
        # design team rebuilt it: Note at 559, Data source at 591, and the tagline/licence row at 609,
        # inside a `Frame 22` that starts at 559 and is 63 tall. `chart_bottom_y` is the Note's top
        # for a two-line note, which is the shape the template ships. The previous values (556 / 589)
        # came from the template's earlier build and left every row about 2px high.
        "chart_bottom_y": 559,
        "source_y": 591,
        "footer_y": 609,
        "nrows": 1,
        "ncols": 2,
        "full_footer": True,
        "age_ticks": [0, 5, 10, 15, 19],
        "diagram": "panel",
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 7.75,
        # Space reserved inside the chart area for the y tick labels, and below the plot for the
        # tick marks, the x tick labels and the bold "Age in years" label.
        "y_label_space": 58,
        "x_label_space": 60,
    },
    "height_for_age_mobile": {
        "size": (540, 824),
        "template": "mobile",
        "margin": 16,
        "title_y": 16,
        # The mobile templates' footer is a two-row block at y=770: Data source, then the license 21px
        # under it. Both run the full content width, so neither shares a row with the other.
        "chart_bottom_y": 770,
        "source_y": 770,
        "footer_y": 791,
        "nrows": 1,
        "ncols": 2,
        "full_footer": False,
        "age_ticks": [0, 5, 10, 15, 19],
        "diagram": "header",
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 8.75,
        "y_label_space": 58,
        "x_label_space": 60,
    },
}

# Both layouts share this, which is what lets the mobile template's two-line subtitle slot hold it:
# it fits in about 114 characters at the template's type size. Calling the whole range a *reference*
# is also what keeps mobile honest without a Note slot to put a caveat in -- it claims only that this
# is how the reference population's heights are distributed, not that they are heights to aim for.
SUBTITLE = (
    "Global growth reference for infants, children, and adolescents, as defined by the World Health Organization."
)

# The mobile template has no Note slot, so a condensed form of the note rides in the subtitle instead.
# It says what causes the two visible steps, which is what a reader needs to know they are real rather
# than drawing errors -- and the age-5 clause carries the standards-versus-reference distinction too,
# since that switch is what the step at 5 is. Every line it adds comes out of the plot's height.
MOBILE_NOTE = (
    "The steps mark a switch from lying to standing measurement at age {first:.0f}, "
    "and an older-age reference at {second:.0f}."
)

# A template pixel in points: the figure is 100 template px per inch and there are 72 points
# to the inch, so one pixel is 0.72pt. Used to convert the templates' geometry for text
# measurement, which matplotlib does in points.
POINTS_PER_PIXEL = 0.72

# One dash plus one gap, in POINTS: the dash units are multiples of the line width, so this is what
# one repetition of the pattern measures. `even_dashes` converts it to display pixels with the
# figure's own dpi rather than a constant, because the two are not the same number -- this figure
# renders at 200 dpi while its geometry is laid out in 100-per-inch template pixels, so assuming
# template pixels made every segment half a period and left the dash as uneven as before.
STUNTING_DASH_PERIOD_PT = sum(STUNTING_DASHES[1]) * STUNTING_LINEWIDTH

# Font size for the encoding diagram's labels, in points, relative to the body size. The design
# team's floor is 12px and a point here renders as 100/72 px, so this must stay above 8.64pt.
DIAGRAM_FONTSIZE_DROP = 1.8

# Gap between the title block and the subtitle, in template pixels. Calibrated so that a
# two-line title puts the subtitle at the templates' own y=80.
TITLE_SUBTITLE_GAP = 6

# Vertical rhythm below the subtitle, in multiples of a text line.
SUBTITLE_GAP = 0.15
DIAGRAM_CHART_GAP = 0.8

# The y the templates give their chart area, and the breathing room to leave inside it, in template
# pixels. Filling the area edge to edge leaves the drawn block about 5px from the header and the
# footer, which reads as cramped; the design team's own pages sit at 12-16px. Only the header-diagram
# layout needs this, because there the block starts at the chart area's top: the desktop layout
# centres what is left of a band its one-line title and subtitle have already widened.
CHART_AREA_TOP = 118
CHART_AREA_INSET = 14

# Height reserved for the encoding diagram when it sits in the header rather than inside a panel, in
# template pixels: the curve, plus the row of stunting text below it and the leader reaching it. This
# is a wide row, so the height is what decides whether the miniature reads as a growth curve or as a
# flat smear -- it buys its extra height from the plot below, not from the frame, since the block's
# top and bottom are both fixed.
HEADER_DIAGRAM_HEIGHT = 112


def run() -> None:
    """Load data, render and save both versions of the chart."""
    tb = load_growth_reference()
    paths.log.info(f"Loaded {len(tb)} rows covering ages {tb['age_years'].min():.1f}-{tb['age_years'].max():.1f}")

    assert_threshold_is_a_fixed_percentile(tb)

    citation = source_citation(tb[MEDIAN_COLUMN], key="producer")
    paths.log.info(f"Source citation: {citation}")

    breaks = find_discontinuities(tb)
    paths.log.info(f"Steps down in the median at ages: {[round(age, 2) for age in breaks]}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(tb, citation, breaks, layout)
        # No bbox_inches="tight" on either: cropping to the drawn content would change the frame,
        # and the point is to hand Figma an image at the template's exact proportions.
        #
        # export_frame owns the save discipline: the clip sweep, the opaque-PNG /
        # transparent-SVG split, and the template-aspect assertion. `template` is a check, not a
        # setting -- it fails the run if this layout's figsize has drifted off the frame it is
        # laid out against.
        export_frame(paths, fig, short_name, template=layout["template"])
        plt.close(fig)


def assert_threshold_is_a_fixed_percentile(tb: Table) -> None:
    """Check the premise behind the outer band's label.

    The band is labelled as a *share of children* while its lower edge is defined in *standard
    deviations*, and that conversion only holds because WHO's height-for-age standard sets the LMS
    skewness parameter L to 1 at every age, making the distribution normal. If a future revision
    introduced skewness, -2 SD would become an age-varying centile and the label would silently start
    overstating or understating how many children the band holds -- a wrong number on a published
    chart, with nothing else in the step to catch it.
    """
    skewness = tb["lms_l_skewness"].unique()
    assert set(skewness) == {1}, (
        f"Height-for-age is no longer a normal distribution (L = {skewness}), so -2 SD is no longer "
        f"the {STUNTED_SHARE}th percentile and BAND_LABELS overstates the outer band."
    )

    # The same claim checked against the percentile columns rather than the parameter: 2.275 sits
    # between the 1st and the 3rd, so the threshold must too, at every age and for both sexes.
    outside = (tb["height_sd_minus_2"] <= tb["height_percentile_1"]) | (
        tb["height_sd_minus_2"] >= tb["height_percentile_3"]
    )
    assert not outside.any(), (
        f"-2 SD escapes the 1st-3rd percentile range in {int(outside.sum())} rows, so it is not the "
        f"{STUNTED_SHARE}th percentile the band label assumes."
    )

    # The outer band is drawn as symmetric about the median and labelled with one share doubled, so the
    # two thresholds have to be equidistant from it. They are by construction under L = 1, which makes
    # this a check on the columns rather than on the maths.
    lower_gap = tb["height_percentile_50"] - tb["height_sd_minus_2"]
    upper_gap = tb["height_sd_plus_2"] - tb["height_percentile_50"]
    skew = (lower_gap - upper_gap).abs().max()
    assert skew < 0.01, (
        f"-+2 SD are not equidistant from the median (worst gap {skew:.3f} cm), so the outer band is "
        "not symmetric and BAND_LABELS cannot double one tail's share."
    )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_growth_reference() -> Table:
    """Load the spliced WHO height-for-age reference from garden."""
    ds = paths.load_dataset("height_for_age")
    return ds.read("height_for_age")


def resample_for_even_dashes(x, y, transform, period_px: float):
    """Resample a polyline so every segment spans exactly one dash period on the page.

    Matplotlib dashes continuously along a path, so its own render does not care where the vertices
    fall. **Figma does**: it fits a whole number of dash repetitions into each segment individually,
    stretching or squeezing the pattern to make them fit. So the rendered dash length becomes a
    function of vertex spacing, and a line whose vertices are unevenly spaced comes out with visibly
    different dash frequencies along its length.

    That is exactly what a growth curve produces. Matplotlib's path simplification keeps vertices
    where the curvature is high and drops them where the line is straight, so the threshold arrived
    in Figma with 51 vertices at a 6.7pt mean spacing against a 7.28pt dash period -- 35 of its 50
    segments shorter than a single repetition. The steep part near birth collapsed into dots, the
    flat part ran as long dashes, and the encoding diagram's miniature, densest of all, read as a
    solid line rather than a dashed one.

    Measured in Figma on four otherwise identical lines: at 6.7px spacing the pattern renders as
    dots, at 13px as over-long dashes, and only at >=50px as specified. Rather than push the spacing
    up -- which would cost the curve its shape, since the whole path is only ~470px long -- put
    *exactly one* repetition in each segment. Then there is nothing to round: every segment renders
    one dash and one gap, at any spacing, in both renderers.

    Sampling is along the original polyline, so the curve's shape and its two step discontinuities
    survive; only the vertex positions change.
    """
    points = transform.transform(np.column_stack([x, y]))
    spans = np.hypot(*np.diff(points, axis=0).T)
    distance = np.concatenate([[0.0], np.cumsum(spans)])
    if distance[-1] <= period_px:
        return x, y
    steps = max(2, int(round(distance[-1] / period_px)) + 1)
    targets = np.linspace(0.0, distance[-1], steps)
    return np.interp(targets, distance, x), np.interp(targets, distance, y)


def even_dashes(fig: plt.Figure, period_pt: float) -> int:
    """Respace every dashed threshold in the figure so Figma renders its dash evenly.

    Finds the lines by gid rather than being handed them, so a threshold added to a future panel or
    diagram is picked up without plumbing. Returns how many it respaced, which is what the caller
    logs -- a zero there means the gids drifted and the fix silently stopped applying.
    """
    # A transform lands in display pixels, which are the figure's dpi per inch and NOT the 100
    # template pixels per inch its geometry is written in. Take the conversion from the figure.
    period_px = period_pt * fig.dpi / 72
    respaced = 0
    for line in fig.findobj(Line2D):
        gid = line.get_gid()
        if not gid or not gid.endswith("__stunting-threshold"):
            continue
        x, y = line.get_data()
        x, y = resample_for_even_dashes(np.asarray(x), np.asarray(y), line.get_transform(), period_px)
        line.set_data(x, y)
        # `set_data` invalidates the cached path; force it to rebuild now so that simplification --
        # which is what made the spacing uneven to begin with -- can be switched off on the result
        # before anything draws it. Left on, it would drop vertices from exactly the flat stretches
        # this resampling exists to keep evenly spaced.
        line.get_path().should_simplify = False
        respaced += 1
    return respaced


def find_discontinuities(tb: Table) -> list[float]:
    """Return the ages, in years, where the median steps down.

    There are two, both in the source data: the switch from lying to standing measurement
    at age 2, and the join between WHO's two products at age 5. Reading them off the data
    rather than hardcoding them keeps the footnote honest if the source ever changes.
    """
    ages = set()
    for _, tb_sex in tb.groupby("sex", observed=True):
        tb_sex = tb_sex.sort_values("age_days")
        median = tb_sex[MEDIAN_COLUMN].to_numpy()
        years = tb_sex["age_years"].to_numpy()
        for i in np.flatnonzero(np.diff(median) < 0):
            ages.add(round(float(years[i + 1]), 1))
    return sorted(ages)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def tint(color, weight: float) -> tuple[float, float, float]:
    """Blend a color towards white. weight=0 keeps it, weight=1 turns it white."""
    r, g, b = to_rgb(color)
    return (r + (1 - r) * weight, g + (1 - g) * weight, b + (1 - b) * weight)


def draw_encoding_diagram(
    ax,
    fontsize: float,
    left: float = 0.34,
    right: float = 0.60,
    middle: float = 0.24,
    outer_half: float = 0.105,
    rise: float = 0.12,
    label_gap: float = 0.05,
) -> None:
    """Draw a miniature growth curve carrying the encoding, with each part named beside it.

    Shaped like the chart it explains rather than as a flat block: a rising median with both bands
    widening symmetrically around it, the outer one's lower edge being the -2 SD line, so the reader
    recognises the marks by their shape and not only by their colour. It is a schematic, not a data slice -- the bands are drawn
    wider than the real ones so the four labelled marks separate at the curve's right-hand end, where
    the labels attach.

    Grey, so it reads as a key rather than as a third sex; the tints and line styles are the chart's.

    Where each label goes, and why:

    - Each band gets a square bracket at the curve's right end, spanning the band's full height there.
      A band is a range, and a tick at its boundary would read as naming the boundary. The brackets
      nest outwards and their labels attach to the top cap, which is what keeps the inner label from
      having to cross the outer bracket -- the panel is too narrow for it to clear.
    - The median's label sits at the line's left end, and the stunting label below the curve with a
      short leader. A leader is drawn only where a label cannot sit against the thing it names. The
      stunting label is the wider of the two and runs beneath the median's, so `label_gap` has to hold
      it a clear line below rather than merely below.

    Geometry is in axes fractions of whatever `ax` it is given, so the same drawing serves both
    layouts: the empty triangle below the growth curve on desktop, and its own axes across the header
    on mobile, whose 217px panels are narrower than the 89px and 105px labels.
    """
    # A growth curve climbs fast and then flattens, so the exponent is well below 1.
    t = np.linspace(0.0, 1.0, 80)
    x = left + (right - left) * t
    median = middle - rise / 2 + rise * t**0.55
    # The bands widen with age in the data, so they widen along the schematic too.
    outer = outer_half * (0.35 + 0.65 * t)
    # Both bands are symmetric about the median, as the chart's are, and the outer band's own lower
    # edge is the -2 SD threshold -- so the schematic shows the same single boundary at its foot that
    # the panels do, with nothing else running near it.
    inner = outer * DIAGRAM_INNER_RATIO
    minus_2sd = median - outer

    for half, weight, name in ((outer, 0.90, "outer-band"), (inner, 0.74, "inner-band")):
        ax.fill_between(
            x,
            median - half,
            median + half,
            facecolor=tint(DIAGRAM_COLOR, weight),
            linewidth=0,
            transform=ax.transAxes,
            zorder=7,
            gid=f"diagram__{name}",
        )
    ax.plot(x, median, color=DIAGRAM_COLOR, linewidth=2.0, transform=ax.transAxes, zorder=8, gid="diagram__median")
    ax.plot(
        x,
        minus_2sd,
        color=DIAGRAM_COLOR,
        linestyle=STUNTING_DASHES,
        linewidth=STUNTING_LINEWIDTH,
        dash_capstyle="butt",
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__stunting-threshold",
    )

    # Nested brackets at the curve's right end: the big one around the whole band, the small one
    # around the middle 80%. Each label sits immediately right of its own bracket, with no leader --
    # the outer band's label level with the big bracket's top arm, the inner band's level with the
    # small bracket's middle. The big bracket is the *nearer* of the two, which is what lets both
    # labels sit against their own bracket: the top label then clears the small bracket entirely, and
    # the middle label starts to the right of both.
    for half_end, bracket_x, label_x, at_top, name, text in (
        (outer[-1], right + 0.020, right + 0.035, True, "19-in-20", BAND_LABELS[0]),
        (inner[-1], right + 0.050, right + 0.065, False, "8-in-10", BAND_LABELS[1]),
    ):
        ax.plot(
            [bracket_x - 0.012, bracket_x, bracket_x, bracket_x - 0.012],
            [median[-1] - half_end, median[-1] - half_end, median[-1] + half_end, median[-1] + half_end],
            color=MUTED_COLOR,
            linewidth=0.8,
            solid_capstyle="butt",
            transform=ax.transAxes,
            zorder=8,
            gid=f"diagram__bracket-{name}",
        )
        ax.text(
            label_x,
            median[-1] + half_end if at_top else median[-1],
            text,
            transform=ax.transAxes,
            fontsize=fontsize,
            color=TEXT_COLOR,
            ha="left",
            va="center",
            zorder=8,
            gid=f"diagram__label-{name}",
        )

    # The median is named where its line starts, so it needs no leader.
    ax.text(
        left - 0.02,
        median[0],
        "Median",
        transform=ax.transAxes,
        fontsize=fontsize,
        color=TEXT_COLOR,
        ha="right",
        va="center",
        zorder=8,
        gid="diagram__label-median",
    )

    # The stunting label sits below the curve, where there is room for one line, with a leader dropping
    # from the threshold at the curve's midpoint. The threshold is now the band's own lower edge, so
    # the leader starts on that edge and crosses nothing on its way down -- it used to start inside the
    # band and cross its lower boundary, which left it pointing at two marks at once. The label says
    # *below this line* because the mark it names is a region, not a line: everything under the band.
    mid = len(t) // 2
    label_y = float(minus_2sd.min()) - label_gap
    ax.plot(
        [x[mid]] * 2,
        [minus_2sd[mid], label_y],
        color=MUTED_COLOR,
        linewidth=0.8,
        solid_capstyle="butt",
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__leader-stunted",
    )
    ax.text(
        x[mid],
        label_y,
        STUNTING_LABEL,
        transform=ax.transAxes,
        fontsize=fontsize,
        color=TEXT_COLOR,
        ha="center",
        va="top",
        zorder=8,
        gid="diagram__label-stunted",
    )


def wrap_to_content_width(text: str, layout: dict, fontsize: float) -> str:
    """Wrap text to fill the content width between the template's side margins.

    Lines are built greedily against the *measured* width of the rendered glyphs rather than a
    character count. Estimating from the font size systematically under-fills -- characters
    average closer to 0.45 than 0.5 of their point size in this font, which left the note
    wrapping some 10% narrow than the space available.
    """
    max_points = (layout["size"][0] - 2 * layout["margin"]) * POINTS_PER_PIXEL
    font = FontProperties(size=fontsize)

    def measure(candidate: str) -> float:
        return TextPath((0, 0), candidate, prop=font).get_extents().width if candidate.strip() else 0.0

    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and measure(candidate) > max_points:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\n".join(lines)


def build_note(breaks: list[float], layout: dict) -> str:
    """Compose the Note row: the two source discontinuities and what each product is."""
    text = (
        f"Note: The curves step down slightly at age {breaks[0]:.0f}, where height starts being measured standing "
        f"up rather than lying down, and at age {breaks[1]:.0f}, where WHO\u2019s standards for under-fives give way to "
        "its reference for older children. The under-fives standards show how children grow in good conditions; the "
        "reference for older children describes how an earlier sample did grow. A child is stunted if they are more "
        "than two standard deviations shorter than the median for their age, which is the shaded area\u2019s lower edge: "
        f"{STUNTED_SHARE:.1f}% of the reference population falls below it."
    )
    return wrap_to_content_width(text, layout, layout["footer_fontsize"])


def create_visualization(tb: Table, citation: str, breaks: list[float], layout: dict) -> plt.Figure:
    """Build one version of the two-panel growth-curve chart.

    Layout notes:
    - One panel per sex, sharing a y-axis, each with two nested bands as flat tints, both symmetric
      about the median
    - The outer band runs -+2 SD, so its lower edge is the stunting threshold, dashed over the tint edge
    - Median drawn solid on top of the bands
    - The median, the -2 SD stunting threshold and both bands are named in the encoding diagram
    - No spines; light horizontal gridlines carry the height reading
    - Axis limits, ticks and footnote ages all derived from the data
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    body_fontsize = layout["body_fontsize"]
    facet_fontsize = body_fontsize * FACET_TITLE_SCALE
    # Room the facet titles need above each panel: one line plus grapher's half-line of padding.
    facet_title_space_px = (1 + FACET_TITLE_PAD) * facet_fontsize / POINTS_PER_PIXEL
    age_max = float(tb["age_years"].max())
    # The outermost band decides both ends of the height axis, so a change to what is drawn cannot
    # leave the axis sized for a series the chart no longer shows.
    band_lower, band_upper = BANDS[0][0], BANDS[0][1]
    height_max = float(tb[band_upper].max())
    # Snap the height axis out to whole gridline steps, so the outermost gridlines sit exactly on the
    # plot's top and bottom edges. That is how grapher avoids a gridline running a few pixels clear of
    # an edge: its y domain is [lowest tick, highest tick], so there is only ever one line there. The
    # bottom one coincides with the baseline, which draws it solid, so its gridline is suppressed
    # below rather than dashed over the top of it.
    height_ticks = np.arange(
        np.floor(float(tb[band_lower].min()) / HEIGHT_STEP) * HEIGHT_STEP,
        np.ceil(height_max / HEIGHT_STEP) * HEIGHT_STEP + 1,
        HEIGHT_STEP,
    )

    width_px, height_px = layout["size"]
    margin_px = layout["margin"]

    def fx(x_px: float) -> float:
        """Template x, in pixels from the left edge, as a figure fraction."""
        return x_px / width_px

    def fy(y_px: float) -> float:
        """Template y, in pixels from the *top* edge as Figma reports it, as a figure fraction."""
        return 1 - y_px / height_px

    def px(points: float) -> float:
        """A line of text at this point size, in template pixels (1 px = 0.72 pt)."""
        return 1.3 * points / 0.72

    fig, axes = plt.subplots(
        layout["nrows"],
        layout["ncols"],
        figsize=(width_px / PIXELS_PER_INCH, height_px / PIXELS_PER_INCH),
        sharey=True,
        sharex=True,
    )

    # The PNG keeps an opaque canvas so it is legible when reviewed against a dark editor
    # background; the SVG drops it at save time (see run()), because in Figma the template supplies
    # the background and a white patch would cover it.
    fig.patch.set_facecolor("white")

    for ax, (sex, color_index) in zip(axes, PANEL_COLOR_INDEX.items()):
        color = palette[color_index]
        tb_sex = tb[tb["sex"] == sex].sort_values("age_days")
        age = tb_sex["age_years"].to_numpy()

        ax.set_axisbelow(True)
        # Horizontal gridlines only, dashed, as grapher draws them on a line chart (it sets
        # hideGridlines on the x axis of LineChart).
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=GRID_LINEWIDTH, linestyle=GRID_DASHES)
        ax.xaxis.grid(False)
        # No spines except the baseline the tick marks hang from, in the zero line's own colour and
        # weight. The y values are read off the gridlines, which is why that axis carries no line.
        for name, spine in ax.spines.items():
            spine.set_visible(name == "bottom")
        ax.spines["bottom"].set_color(TICK_COLOR)
        ax.spines["bottom"].set_linewidth(TICK_WIDTH * POINTS_PER_PIXEL)

        # --- nested percentile bands, widest first ---
        # gid becomes the SVG element id, so Figma shows named layers instead of "Path 41".
        # Mirrors grapher, which stamps its own SVG nodes with makeFigmaId().
        slug = sex.lower()
        for lower, upper, weight, band_name in BANDS:
            ax.fill_between(
                age,
                tb_sex[lower].to_numpy(),
                tb_sex[upper].to_numpy(),
                facecolor=tint(color, weight),
                linewidth=0,
                zorder=2,
                gid=f"{slug}__{band_name}",
            )

        # --- stunting threshold, drawn over the outer band's lower edge, which is the same series.
        # Doubling the boundary as a tint edge and a dashed stroke is what makes it findable without a
        # label in the panel: the tint stops there, and the region below it is the stunted one. It is
        # named in the encoding diagram. ---
        stunting = tb_sex[STUNTING_COLUMN].to_numpy()
        ax.plot(
            age,
            stunting,
            color=color,
            linestyle=STUNTING_DASHES,
            linewidth=STUNTING_LINEWIDTH,
            dash_capstyle="butt",
            zorder=4,
            gid=f"{slug}__stunting-threshold",
        )
        # --- percentile lines on top of the bands ---
        for column, line_width in QUANTILE_LINES:
            values = tb_sex[column].to_numpy()
            ax.plot(age, values, color=color, linewidth=line_width, zorder=5, gid=f"{slug}__{column[-3:]}")

        if layout["diagram"] == "panel" and ax is axes[0]:
            draw_encoding_diagram(ax, body_fontsize - DIAGRAM_FONTSIZE_DROP)

        # --- panel title, above the plot and left-aligned with it, as grapher labels a facet ---
        ax.set_title(
            sex,
            loc="left",
            fontsize=facet_fontsize,
            fontweight="bold",
            color=TEXT_COLOR,
            pad=FACET_TITLE_PAD * facet_fontsize,
        )
        ax.title.set_gid(f"{slug}__label")

        # The x range starts and ends on the outermost ticks, as grapher's does, so that those two
        # marks sit at the ends of the baseline and close it.
        ax.set_xlim(0, age_max)
        ax.set_ylim(height_ticks[0], height_ticks[-1])
        ax.set_yticks(height_ticks)
        # The baseline already draws a solid line at the lowest tick, so its gridline would be a
        # dashed lighter stroke laid over the top of it.
        ax.yaxis.get_gridlines()[0].set_visible(False)
        ticks = layout["age_ticks"]
        ax.set_xticks(ticks)
        labels = ax.set_xticklabels(["Birth" if tick == 0 else str(tick) for tick in ticks])
        # Grapher anchors its outermost tick labels inwards -- text-anchor start on the first, end on
        # the last -- so both sit inside the plot instead of half-overhanging it.
        labels[0].set_horizontalalignment("left")
        labels[-1].set_horizontalalignment("right")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f} cm"))
        ax.tick_params(axis="y", length=0, labelsize=body_fontsize, labelcolor=TEXT_COLOR)
        ax.tick_params(
            axis="x",
            length=TICK_LENGTH * POINTS_PER_PIXEL,
            width=TICK_WIDTH * POINTS_PER_PIXEL,
            color=TICK_COLOR,
            direction="out",
            labelsize=body_fontsize,
            labelcolor=TEXT_COLOR,
        )
        # Grapher renders axis labels bold (fontWeight 700 in Axis.ts). Both layouts put the
        # panels in one row, so each carries its own label; the axes[-1] arm keeps a stacked
        # layout correct (shared x axis, label on the bottom panel only) if one is ever added.
        if layout["ncols"] > 1 or ax is axes[-1]:
            ax.set_xlabel("Age in years", fontsize=body_fontsize, color=TEXT_COLOR, fontweight="bold", labelpad=10)

    # --- header: title, then subtitle directly beneath it ---
    # The templates put the subtitle at a fixed y=80, but that assumes the two-line title their
    # placeholder uses. Deriving it from the title's actual height keeps the pair tight when the
    # title only needs one line, and reproduces the template's y=80 exactly when it needs two.
    title = wrap_to_content_width(TITLE, layout, layout["title_fontsize"])
    title_lines = title.count("\n") + 1
    subtitle_y = layout["title_y"] + title_lines * px(layout["title_fontsize"]) + TITLE_SUBTITLE_GAP

    subtitle = SUBTITLE
    if not layout["full_footer"]:
        subtitle = f"{subtitle} {MOBILE_NOTE.format(first=breaks[0], second=breaks[1])}"
    subtitle = wrap_to_content_width(subtitle, layout, body_fontsize)
    subtitle_lines = subtitle.count("\n") + 1

    fig.text(
        fx(margin_px),
        fy(layout["title_y"]),
        title,
        ha="left",
        va="top",
        fontsize=layout["title_fontsize"],
        color="#111111",
        gid="title",
    )
    fig.text(
        fx(margin_px),
        fy(subtitle_y),
        subtitle,
        ha="left",
        va="top",
        fontsize=body_fontsize,
        color="#555555",
        gid="subtitle",
    )

    # Our subtitle runs longer than the template's two-line placeholder, so whatever comes next
    # starts below wherever the subtitle actually ends rather than at the template's fixed
    # chart-area top.
    subtitle_bottom_px = subtitle_y + subtitle_lines * px(body_fontsize) + px(body_fontsize) * SUBTITLE_GAP
    chart_top_px = subtitle_bottom_px + DIAGRAM_CHART_GAP * px(body_fontsize) + facet_title_space_px

    if layout["diagram"] == "header":
        # A 217px-wide mobile panel cannot hold the diagram -- the two band labels alone are wider
        # than the panel -- but this row is the full 508px content width, which fits it with room to
        # spare. Keeping the same explanatory device in both versions matters more than the vertical
        # cost, since the pair gets published together.
        # The diagram is the top of the drawn block, so it is what the floor has to hold down --
        # applied to the plot instead, the block still starts above the template's chart area.
        diagram_top_px = max(subtitle_bottom_px, CHART_AREA_TOP) + CHART_AREA_INSET
        diagram_axes = fig.add_axes(
            (
                fx(margin_px),
                fy(diagram_top_px + HEADER_DIAGRAM_HEIGHT),
                1 - 2 * fx(margin_px),
                HEADER_DIAGRAM_HEIGHT / height_px,
            )
        )
        diagram_axes.set_axis_off()
        # No background patch: the SVG is saved transparent so the Figma template shows through.
        diagram_axes.patch.set_visible(False)
        draw_encoding_diagram(
            diagram_axes,
            body_fontsize - DIAGRAM_FONTSIZE_DROP,
            left=0.33,
            right=0.57,
            middle=0.49,
            outer_half=0.21,
            rise=0.47,
            label_gap=0.04,
        )
        # The same gap the desktop layout leaves under its subtitle goes here, between the diagram's
        # box and the facet titles below it. Without it the two blocks are separated only by whatever
        # empty box the diagram happens to leave under its lowest label, and the titles read as part
        # of the key rather than of the panels.
        chart_top_px = (
            diagram_top_px + HEADER_DIAGRAM_HEIGHT + DIAGRAM_CHART_GAP * px(body_fontsize) + facet_title_space_px
        )

    # --- footer, in the slots the static-chart templates define ---
    # Desktop: Note -> Data source -> tagline and license sharing one row, left and right.
    # Mobile: Data source -> license, stacked, which is all that template has room for.
    footer_fontsize = layout["footer_fontsize"]

    if layout["full_footer"]:
        note = build_note(breaks, layout)
        # The note grows upwards from its template row so that a longer note eats into the
        # chart area rather than running off the bottom of the frame.
        note_lines = note.count("\n") + 1
        note_top_px = layout["chart_bottom_y"] - (note_lines - 2) * px(footer_fontsize)
        fig.text(
            fx(margin_px),
            fy(note_top_px),
            note,
            ha="left",
            va="top",
            fontsize=footer_fontsize,
            color=MUTED_COLOR,
            gid="note",
        )
        chart_bottom_px = note_top_px
    else:
        chart_bottom_px = layout["chart_bottom_y"] - (CHART_AREA_INSET if layout["diagram"] == "header" else 0)

    fig.text(
        fx(margin_px),
        fy(layout["source_y"]),
        f"Data source: {citation}",
        ha="left",
        va="top",
        fontsize=footer_fontsize,
        color="#888888",
        gid="data-source",
    )

    # Desktop puts the tagline on its own row and right-aligns the license beside it. Mobile has no
    # tagline row and gives the license one of its own, left-aligned under the source -- so both
    # templates carry the same author credit, and only the alignment differs.
    shares_tagline_row = layout["full_footer"]
    if shares_tagline_row:
        fig.text(
            fx(margin_px),
            fy(layout["footer_y"]),
            TAGLINE,
            ha="left",
            va="top",
            fontsize=footer_fontsize,
            color="#888888",
            gid="tagline",
        )
    fig.text(
        fx(width_px - margin_px if shares_tagline_row else margin_px),
        fy(layout["footer_y"]),
        f"Licensed under CC-BY by the author {AUTHOR}",
        ha="right" if shares_tagline_row else "left",
        va="top",
        fontsize=footer_fontsize,
        color="#888888",
        gid="license",
    )

    fig.subplots_adjust(
        left=fx(margin_px + layout["y_label_space"]),
        right=fx(width_px - margin_px),
        top=fy(chart_top_px),
        bottom=fy(chart_bottom_px - layout["x_label_space"]),
        wspace=0.1,
        hspace=0.35,
    )

    # Every dashed threshold -- both panels and whichever diagram this layout drew -- is respaced now
    # rather than where it was plotted, because `resample_for_even_dashes` measures on the page and
    # the axes only reach their final size on the line above. Each line's own transform is the right
    # one to measure through: the panels' is `transData`, the diagram's `transAxes`, and both land in
    # display pixels.
    even_dashes(fig, STUNTING_DASH_PERIOD_PT)

    return fig
