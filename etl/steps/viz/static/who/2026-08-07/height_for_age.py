"""Recreate the 'Expected Healthy Growth Curves for Boys and Girls' chart from WHO's height-for-age curves.

Each panel shows the median and a single band running from -2 SD to +2 SD around it. The band's lower
edge is the stunting threshold, so the boundary a reader has to find carries two encodings at once --
where the tint stops and a dashed line -- and everything below the shaded area is the stunted region.

One band, because stunting is the concept the chart exists to explain. A 10th-90th percentile band
nested inside this one, and the miniature encoding diagram it took to tell the two apart, were dropped
after design review: two shades read as two thresholds, and the inner one has no role in the stunting
definition. That leaves three things worth naming -- the median, the threshold and the share of children
the band holds -- so each is labelled directly in the Boys panel, see `draw_direct_labels`. The Girls panel repeats the encoding and carries
no labels of its own. Standard deviations are not labelled in the plot; the Note defines the threshold
in those terms, and the mobile template, which has no Note, still names it through the in-plot label.

The title says "growth curves", not "healthy growth curves" as the replaced image did. Only the
under-fives standards rest on children selected for good health and nutrition; the 5-19 reference is
rebuilt from US surveys of 1963-1975 that were not, so "healthy" would over-claim for two thirds of the
age axis. The subtitle names both WHO products and the ages each covers, which is now the only place
that split is stated.

Neither panel repeats the other sex's median. The two medians stay within about 2 cm of each other
from birth until girls overtake boys at age 9.2 -- a couple of pixels on a 40-200 cm axis -- so a
second line traces the panel's own median for two thirds of the range, the same doubling that
splitting the sexes into panels was meant to remove. Where the two sexes differ can be read off the
panels at a shared gridline.

There is no legend in either version.

Two versions are emitted, following the static-chart templates:

- desktop, 850x638: panels side by side, direct labels in the Boys panel, footer carrying Note, Data
  source, the OurWorldinData.org tagline and the license line.
- mobile, 540x824: panels side by side in the portrait frame, the same direct labels placed for its
  217px panels, footer reduced to Data source plus the license, which is all that template has room
  for. It has no Note slot, which is why the in-plot stunting label carries the share of children
  below the cutoff on both layouts.

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
`20260812 Growth curves for boys and girls, from birth to age 19 (Pablo A)` (renamed from `Expected
height of ...` when the title changed on 2026-10-05; same page, id `25284:5`), sitting at the top of
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

**Import.** Upload each SVG TWICE with one `upload_assets` call (`count: 4` for both layouts) and POST
the files to the returned `submitUrl`s (`curl -F "file=@<path>"`); never `createNodeFromSvg`, which caps
at 50k characters. Then run `/create-figma-chart`'s `scripts/restyle_static_import.js` as one call, with
`families: []` (colours are bound separately, below) and both body/dark text-parent patterns set to
match nothing. Per frame it:

1. rescales the import by `frameWidth / canvasWidth` -- 850 / 816 desktop, 540 / 518.4 mobile, i.e.
   100 / 96: matplotlib declares the root in points, Figma imports at 96px per inch, and this figure is
   built at 100 template px per inch;
2. strips the unpainted `patch_N` background groups (they would set the chart's box to the artboard)
   and the step's own `title`, `subtitle`, `note`, `data-source`, `tagline` and `license` copies, which
   the template's slots replace;
3. sets Lato, swaps the import into the frame as `chart` at the bottom of the z-order, removes the old
   `chart`, crops it to its ink and snaps it onto the content column;
4. parks the second upload, unstyled, to the frame's left as `<frame> — original SVG (unstyled)`.
   Delete the previous reference copies first, or the new ones land on top of them.

**Template text slots.** Fill them from this step's own constants, restoring the mixed weights the
templates ship -- setting `characters` propagates the first character's style over the whole string:

| Slot | Content | Weights |
|---|---|---|
| Title | `TITLE` | template's Playfair |
| Subtitle | `SUBTITLE` | Lato Regular |
| `Note:` (desktop only) | `build_note(...)` | `Note:` Bold, rest Regular |
| `Data source:` | `Data source: ` + `source_citation(...)` from `etl.viz.static` | `Data source:` Bold, rest Regular |
| Tagline (desktop) | leave the template's | -- |
| License | `Licensed under ` / `CC-BY` / ` by the author ` / `AUTHOR` | Medium / Bold / Medium / Bold |

Both templates carry the same footer slots, so both get the same license string; mobile just stacks
its two rows (`Frame 15`, source at y=770 and license at y=791) where desktop shares one row with the
tagline.

Two positions are derived rather than the template's fixed y, because the template pins them for a
two-line title and a two-line subtitle:

- Both headers (`Frame 20` desktop, `Frame 26` mobile) are now vertical auto-layout frames, so the
  subtitle follows the title by itself, 6px under it.
- Desktop's footer, `Frame 22`, hugs its rows and is pinned at its top, so a shorter note pulls the
  source row up. Re-pin it after setting the note: `footer.y = 638 - 16 - footer.height`, which puts
  the two-line note at the template's own y=559. Mobile's footer has no note and needs nothing.

**Colors.** Bind each panel's median *and its threshold* to the library style, and derive that panel's
bands from it; the library carries no tints. The gid names a group, so descend to its `VECTOR` children
before calling the setter.

The threshold has to be bound too, and to the same style as the median beside it. The step draws both
in the panel's own colour on purpose -- colour says which panel a mark belongs to, style says which mark
it is -- so binding only the median splits the pair in Figma: the median moves to the library colour
while the threshold keeps matplotlib's `#4c72b0` / `#dd8452`. Binding a paint style leaves
`dashPattern` alone, so the dash survives the binding. The direct labels and the stunting leader are
not in this table: they stay the step's greys, which ranks them as annotation rather than data.

| Layer | Treatment |
|---|---|
| `boys___50` | `setStrokeStyleIdAsync` -> `Default Palette/Denim`, key `e1538d9330d7b22168f0c19fa562897aa8975f90` |
| `girls___50` | `setStrokeStyleIdAsync` -> `Default Palette/Rusty Orange`, key `65bab597d085689b1ea82a69f4d785cb9212c234` |
| `<sex>__stunting-threshold` | the same style as `<sex>___50` |
| `<sex>__within-2-sd` | that style's color blended `BAND_TINT` (0.78) towards white |

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
| Direct labels (`label__median`, `label__within-2-sd`, `label__stunting-cutoff`) | 12 | Regular |

Anchors: y ticks by their right edge; the first x tick by its left and the last by its right, the rest
centred; `label__median` by its bottom-right corner; `label__within-2-sd` by its bottom-left corner,
lines left-aligned, with `leader__within-2-sd` running from its bottom-right corner into the band;
`label__stunting-cutoff` by its top-left corner, lines left-aligned, with `leader__stunting-cutoff`
running from that corner up to the dashed line.

**Fit.** After the restyle script's crop, set the `chart` frame's box to the content column
(`resizeWithoutConstraints(header.width, h)`, `x = header.x`) and centre it in the band between the
header's bottom and the footer's first visible row (`footer.y + min(0, row.y)`). Never `rescale` here,
which would move every font off its rank.

The column snap is under a pixel and it is not optional. The crop measures ink, and the last x tick's
1px stroke is centred on the plot's right edge, so the ink ends 0.5px past the column. Anything larger
is a step defect, not something to absorb in Figma: the median's round cap once ran 1.8px past the edge,
which is why `QUANTILE_LINES` are drawn with `solid_capstyle="butt"`. The left edge is the widest y
tick label, whose width is Lato's in Figma and Arial's here; `LATO_OVER_MEASURED_ADVANCE` keeps that to
a few tenths of a pixel.

**Audit before showing it.** Expect sizes {16, 14, 12} only, Lato Regular and Bold only, both medians
*and both thresholds* reporting a bound style -- and each threshold reporting the same colour as its
own median, which is the check that catches a stale band or threshold selector -- no ink outside
16..W-16, and gaps of 13.4 / 13.4 on desktop and 19.67 / 19.67 on mobile (measured 2026-10-05).

Two `verify_page.js` rows fail by design. Desktop `text-floor` flags the template's own tagline and
license row, which ships at 11px. Mobile `gap` flags 19.67 against the 12-16 target: the plot is laid
out from the template's chart-area rows, and its ink is shorter than the box reserved for it. The
series, furniture and colour rows SKIP, because they key on grapher's layer names (`line__*`,
`horizontal-grid-lines`), which this step does not emit.
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.lines import Line2D
from matplotlib.textpath import TextPath, TextToPath
from matplotlib.ticker import FuncFormatter
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.viz.static import PIXELS_PER_INCH, apply_svg_rcparams, export_frame, source_citation

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

paths = PathFinder(__file__)

# Two stacks, because two different readers want different answers.
#
# This one lands in the SVG's `font-family` verbatim -- matplotlib copies the rcParam out rather than
# writing the face it resolved -- so naming Lato first is a request to whoever opens the file. Figma
# then renders the import in the template's own typeface on arrival, which makes the parked reference
# copy look like the deliverable and turns `/create-figma-chart`'s font pass, and the anchor pass that
# exists only to undo it, into no-ops. Without it Figma resolves none of Arial/Helvetica/DejaVu and
# substitutes Inter, which is wider.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "sans-serif"]
# And this one is what the step measures and draws with. It deliberately does NOT name Lato, which is
# not installed on our machines: Lato is a font this step can ask for, not one it could measure in.
MEASURED_FONT_STACK = ["Arial", "Helvetica", "DejaVu Sans", "Liberation Sans", "sans-serif"]

matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK

# Drop the per-face misses for faces we deliberately list as alternatives, and nothing else. A blanket
# silence would also take "Falling back to DejaVu Sans", which is the one that says a whole stack
# failed and every measurement has just moved ~15% against what gets drawn.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)

# No filter can protect the invariant the allowances actually rest on, so assert it. Silencing a
# declared face also silences the case where every face of a stack is missing; and Lato-first has its
# own trap -- a machine that HAS Lato draws Lato while the measured stack still resolves Arial, and
# nothing warns at all. This passes on a Mac without Lato (Arial/Arial) and on a box with neither
# (DejaVu/DejaVu -- a different face, still self-consistent), and fails on the two drifting machines.
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, (
    f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name} -- every width in this step "
    "was measured in a face it will not be drawn in"
)

# What one template pixel of Arial advance becomes once Figma re-renders the import in Lato.
#
# The two stacks above cost a width, not just a name: the deliverable's glyphs are Lato and every
# width this step can measure is Arial's, and Lato runs wider. That matters wherever a string's own
# width decides where its ink starts -- the right-aligned y tick column is the case, since matplotlib
# writes the anchor and the renderer supplies the advance leftwards from it.
#
# Measured off the two shipped frames, Figma's box left against the SVG's anchor: "200 cm" takes
# 47.219px there against 45.91 here (1.0285) and "40 cm" 38.889 against 38.12 (1.0202). The spread is
# real -- Lato's digit, space and letter advances each differ from Arial's by their own amount, so a
# single ratio cannot be exact for every string -- and the larger of the two is used deliberately:
# overshooting leaves a sub-pixel gap that the Figma fit closes, while undershooting puts label ink
# outside the content box, which the margins check flags and no fit can recover without a squeeze.
LATO_OVER_MEASURED_ADVANCE = 1.0285

# One panel per sex. Colors are seaborn "deep" positions rather than raw hexes, so the
# chart shifts with the shared palette instead of pinning its own. Position 0 is the palette's blue
# and 1 its orange, which Figma rebinds to Denim and Rusty Orange respectively -- so this mapping is
# what decides which library colour each panel ends up in, and the Colors table in the Figma section
# has to move with it.
PANEL_COLOR_INDEX = {"Boys": 0, "Girls": 1}

# The stunting threshold's stroke. It carries no colour of its own: it takes its panel's colour, so
# colour says which panel a mark belongs to and style says which mark it is. A neutral slate here instead read as chart furniture --
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

# The single band, as (lower column, upper column, layer name). It is a flat tint, not a translucent
# fill: an alpha fill composites onto whatever is behind it, and the SVG is saved transparent for the
# Figma template to supply the background. A tint renders the same on any backdrop and gives Figma one
# flat fill.
BAND = ("height_sd_minus_2", "height_sd_plus_2", "within-2-sd")

# How far the band's fill is blended towards white. Darker than the 0.90 the outer of two nested bands
# used, because a lone band has no inner shade to carry the panel's colour and at 0.90 it read as a
# smudge; light enough that the median and the direct label sitting on it keep their contrast.
BAND_TINT = 0.78

# What 2 SD is worth as a percentile: the share of children below the threshold. Both the Note and the
# in-plot stunting label are formatted from it, so the two can't drift apart.
#
# The conversion is exact rather than approximate, which is what lets a threshold defined in standard
# deviations be stated as a share at all: WHO's height-for-age standard sets the LMS skewness
# parameter L to 1 at every age, so the distribution is normal and -2 SD is the 2.275th percentile
# rather than an age-varying centile. `assert_threshold_is_a_fixed_percentile` checks L is still 1 in
# the data before the label ships.
STUNTED_SHARE = 2.2750132

# The two direct labels. The stunting one keeps its share rather than leaving it to the Note, because
# the mobile template has no Note to put it in.
MEDIAN_LABEL = "Median"
STUNTING_LABEL = f"Stunting cutoff: roughly {STUNTED_SHARE:.1f}% of children are below this line"

# The share of children the band holds. A cut point is not a share: 2.3% of children fall below -2 SD
# and the same share above +2 SD, so the band between them holds 100 - 2 x 2.3 = 95.4%. Carried from
# STUNTED_SHARE's seven figures, since rounding 2.275 first would print 95.5%.
BAND_LABEL = f"{100 - 2 * STUNTED_SHARE:.1f}% of children fall within the shaded area"

# Length of the stunting label's leader, from the dashed line down to the label's top, in points.
LEADER_PT = 14

# Percentiles drawn as lines on top of the band, as (column, line width), so a specific centile
# can be read off rather than only a range. Labelled directly in the Boys panel.
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
# A template pixel in points: the figure is 100 template px per inch and there are 72 points to the
# inch, so one pixel is 0.72pt. Used to convert the templates' geometry for text measurement, which
# matplotlib does in points.
POINTS_PER_PIXEL = 0.72

# The design team's type ladder for the text INSIDE the plot, in template pixels -- which is what
# Figma shows, because the figure is drawn at the template's own width.
#
# Emit exactly these rather than sizes near them. A point is 100/72 template px, so 14px is 10.08pt
# and the import then arrives already on the ladder, needing no size pass. That is not merely tidier:
# snapping a size in Figma changes every glyph width in the label, which moves the label off its
# anchor and is most of what the anchor pass is left correcting. Emitting 10.5pt instead put every
# rank about 4% high -- 14.58 / 16.20 / 12.08 -- and bought a round of snapping and re-anchoring for
# nothing.
#
# It also makes the step's own PNG the size the frame will be, so a wrap decided here is decided at
# the size that ships.
LADDER_PX = {"facet": 16, "body": 14, "label": 12}
LADDER_PT = {rank: px * POINTS_PER_PIXEL for rank, px in LADDER_PX.items()}
FACET_TITLE_PAD = 0.5

TITLE = "Growth curves for boys and girls, from birth to age 19"

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
        # Where the stunting label attaches, in years, and how wide it may wrap, in template px. See
        # `draw_direct_labels` for why it sits where it does.
        "stunting_label_age": 13,
        "stunting_label_width": 110,
        # The band label's bottom-left corner, as (age, cm), in the empty top-left of the Boys panel,
        # and the age its leader reaches into the band at.
        "band_label_xy": (0.8, 142),
        "band_label_target_age": 8.3,
        "band_label_width": 125,
        "title_fontsize": 16,
        "body_fontsize": LADDER_PT["body"],
        "footer_fontsize": 7.75,
        # Space reserved below the plot for the tick marks, the x tick labels and the bold
        # "Age in years" label. The y tick column is not reserved here -- it is measured from the
        # labels themselves, see `y_tick_column_px`.
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
        "stunting_label_age": 9.5,
        "stunting_label_width": 100,
        "band_label_xy": (0.6, 151),
        "band_label_target_age": 10.2,
        "band_label_width": 100,
        "title_fontsize": 16,
        "body_fontsize": LADDER_PT["body"],
        "footer_fontsize": 8.75,
        "x_label_space": 60,
    },
}

# Both layouts share this: one line on desktop, the template's two-line slot on mobile. It names WHO's
# two products by what each is -- *standards* under 5, a *reference* from 5 to 19 -- rather than
# merging them into one phrase, because the Note no longer explains the split and mobile has no Note.
#
# No geography word, deliberately. The under-fives standards earn one -- six countries, and WHO's own
# claim that they apply to children everywhere -- but the 5-19 half is a reconstruction of a single
# national sample, 22,917 US children measured between 1963 and 1975, so calling the whole range
# 'global' over-claims on exactly that half.
SUBTITLE = "Growth standards (under age 5) and growth reference (ages 5 to 19) of the World Health Organization."


# One dash plus one gap, in POINTS: the dash units are multiples of the line width, so this is what
# one repetition of the pattern measures. `even_dashes` converts it to display pixels with the
# figure's own dpi rather than a constant, because the two are not the same number -- this figure
# renders at 200 dpi while its geometry is laid out in 100-per-inch template pixels, so assuming
# template pixels made every segment half a period and left the dash as uneven as before.
STUNTING_DASH_PERIOD_PT = sum(STUNTING_DASHES[1]) * STUNTING_LINEWIDTH


# Gap between the title block and the subtitle, in template pixels. Calibrated so that a
# two-line title puts the subtitle at the templates' own y=80.
TITLE_SUBTITLE_GAP = 6

# Vertical rhythm below the subtitle, in multiples of a text line.
SUBTITLE_GAP = 0.15
HEADER_CHART_GAP = 0.8

# The y the templates give their chart area, and the breathing room to leave inside it, in template
# pixels. Filling the area edge to edge leaves the drawn block about 5px from the header and the
# footer, which reads as cramped; the design team's own pages sit at 12-16px. Only the mobile layout
# needs this, because there the block starts at the chart area's top: the desktop layout centres what
# is left of a band its one-line title and subtitle have already widened.
CHART_AREA_TOP = 118
CHART_AREA_INSET = 14


def run() -> None:
    """Load data, render and save both versions of the chart."""
    tb = load_growth_reference()
    paths.log.info(f"Loaded {len(tb)} rows covering ages {tb['age_years'].min():.1f}-{tb['age_years'].max():.1f}")

    assert_threshold_is_a_fixed_percentile(tb)

    citation = source_citation(tb[MEDIAN_COLUMN], key="producer")
    paths.log.info(f"Source citation: {citation}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(tb, citation, layout)
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
    """Check the premise behind the share of children printed under the stunting threshold.

    The threshold is stated as a *share of children* while it is defined in *standard deviations*, and
    that conversion only holds because WHO's height-for-age standard sets the LMS skewness parameter L
    to 1 at every age, making the distribution normal. If a future revision introduced skewness, -2 SD
    would become an age-varying centile and the label and the Note would silently misstate how many
    children fall below it -- a wrong number on a published chart, with nothing else to catch it.
    """
    skewness = tb["lms_l_skewness"].unique()
    assert set(skewness) == {1}, (
        f"Height-for-age is no longer a normal distribution (L = {skewness}), so -2 SD is no longer "
        f"the {STUNTED_SHARE}th percentile that STUNTING_LABEL and the Note state."
    )

    # The same claim checked against the percentile columns rather than the parameter: 2.275 sits
    # between the 1st and the 3rd, so the threshold must too, at every age and for both sexes.
    outside = (tb["height_sd_minus_2"] <= tb["height_percentile_1"]) | (
        tb["height_sd_minus_2"] >= tb["height_percentile_3"]
    )
    assert not outside.any(), (
        f"-2 SD escapes the 1st-3rd percentile range in {int(outside.sum())} rows, so it is not the "
        f"{STUNTED_SHARE}th percentile the stunting label states."
    )

    # The band is drawn as symmetric about the median, and the Note's "shaded area" reads as two
    # standard deviations either side of it, so the two edges have to be equidistant from it. They are
    # by construction under L = 1, which makes this a check on the columns rather than on the maths.
    lower_gap = tb["height_percentile_50"] - tb["height_sd_minus_2"]
    upper_gap = tb["height_sd_plus_2"] - tb["height_percentile_50"]
    skew = (lower_gap - upper_gap).abs().max()
    assert skew < 0.01, (
        f"-+2 SD are not equidistant from the median (worst gap {skew:.3f} cm), so the band is not "
        "symmetric about the median it is drawn around."
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


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def tint(color, weight: float) -> tuple[float, float, float]:
    """Blend a color towards white. weight=0 keeps it, weight=1 turns it white."""
    r, g, b = to_rgb(color)
    return (r + (1 - r) * weight, g + (1 - g) * weight, b + (1 - b) * weight)


def draw_direct_labels(ax, tb_sex: Table, layout: dict, fontsize: float) -> None:
    """Name the median, the band and the stunting threshold on the curves themselves.

    The median and stunting labels, and the band label's leader, are anchored to the data at an age
    rather than at a fixed height, so a revision that moves the curves moves them too. The band label
    itself sits at a fixed (age, cm) per layout, in space the curves leave empty.

    - The median's label sits just above the line's right end, right-aligned on it, where the line has
      flattened out after the growth spurt. On the rising stretch any label clear of the line floats a
      line-height or more above it, level with the band's upper edge, and reads as naming that edge
      instead. Right-aligned, a wider face in Figma only grows the label leftwards, never past the edge.
    - The band's label sits in the empty space above the band at the young ages, with a leader into
      the upper half of the band -- between the median and the band's top edge, so the leader touches
      neither line. Inside the band there is no room for it: the band rises too steeply for a
      horizontal label to stay between its edges.
    - The stunting label hangs below the dashed line from a short leader, left-aligned on it. The line
      rises to the right, so text extending rightwards from the anchor moves away from it; centred,
      the label's first line ran into the line on its left. Below the threshold is the region the
      label is about, and from the anchor rightwards it is empty down to the baseline.
    """
    age = tb_sex["age_years"].to_numpy()

    def height_at(column: str, years: float) -> float:
        return float(np.interp(years, age, tb_sex[column].to_numpy()))

    median_age = float(age.max())
    ax.annotate(
        MEDIAN_LABEL,
        xy=(median_age, height_at(MEDIAN_COLUMN, median_age)),
        xytext=(0, 4),
        textcoords="offset points",
        ha="right",
        va="bottom",
        fontsize=fontsize,
        color=TEXT_COLOR,
        zorder=6,
        gid="label__median",
    )

    target_age = layout["band_label_target_age"]
    band_label = ax.annotate(
        wrap_to_width(BAND_LABEL, layout["band_label_width"], fontsize),
        xy=(target_age, (height_at(MEDIAN_COLUMN, target_age) + height_at(BAND[1], target_age)) / 2),
        xytext=layout["band_label_xy"],
        textcoords="data",
        ha="left",
        va="bottom",
        multialignment="left",
        fontsize=fontsize,
        color=TEXT_COLOR,
        zorder=6,
        gid="label__within-2-sd",
        arrowprops={
            "arrowstyle": "-",
            "color": MUTED_COLOR,
            "linewidth": 0.8,
            "shrinkA": 2,
            "shrinkB": 0,
            # From the label's bottom-right corner, the point nearest the band, so the leader is a
            # short stub across the band's edge rather than a line across the empty corner.
            "relpos": (1.0, 0.0),
        },
    )
    assert band_label.arrow_patch is not None
    band_label.arrow_patch.set_gid("leader__within-2-sd")

    stunting_age = layout["stunting_label_age"]
    label = ax.annotate(
        wrap_to_width(STUNTING_LABEL, layout["stunting_label_width"], fontsize),
        xy=(stunting_age, height_at(STUNTING_COLUMN, stunting_age)),
        xytext=(0, -LEADER_PT),
        textcoords="offset points",
        ha="left",
        va="top",
        multialignment="left",
        fontsize=fontsize,
        color=TEXT_COLOR,
        zorder=6,
        gid="label__stunting-cutoff",
        # `relpos` starts the leader at the text's top-left corner, directly under the anchor; without
        # it the line runs from the middle of the text block, through the label.
        arrowprops={
            "arrowstyle": "-",
            "color": MUTED_COLOR,
            "linewidth": 0.8,
            "shrinkA": 2,
            "shrinkB": 3,
            "relpos": (0.0, 1.0),
        },
    )
    assert label.arrow_patch is not None
    label.arrow_patch.set_gid("leader__stunting-cutoff")


def height_tick_label(value: float, _position: int | None = None) -> str:
    """One y tick label: a whole number of centimetres.

    Named rather than a lambda inside the formatter, because `y_tick_column_px` sizes the column from
    these strings and has to measure the one the axis will draw.
    """
    return f"{value:.0f} cm"


def y_tick_column_px(labels: list[str], fontsize: float) -> float:
    """Width to reserve for the right-aligned y tick label column, in template pixels.

    The widest label's advance plus the tick pad. matplotlib writes the label's *anchor* into the SVG
    and leaves the renderer to lay the string out leftwards from it, so what decides where the ink
    starts is the advance -- which is also the width Figma's text box carries. Ink extents (what
    `wrap_to_content_width` measures, and the right tool for wrapping) would be a couple of pixels
    short of it.

    The tick pad comes from the rcParam because that is what matplotlib will use: this axis is drawn
    with `length=0`, so the pad is the whole distance from the spine to the anchor.
    """
    prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)
    to_path = TextToPath()
    widest = max(to_path.get_text_width_height_descent(label, prop, ismath=False)[0] for label in labels)
    return (widest * LATO_OVER_MEASURED_ADVANCE + matplotlib.rcParams["ytick.major.pad"]) / POINTS_PER_PIXEL


def wrap_to_content_width(text: str, layout: dict, fontsize: float) -> str:
    """Wrap text to fill the content width between the template's side margins."""
    return wrap_to_width(text, layout["size"][0] - 2 * layout["margin"], fontsize)


def wrap_to_width(text: str, width_px: float, fontsize: float) -> str:
    """Wrap text to a width in template pixels.

    Lines are built greedily against the *measured* width of the rendered glyphs rather than a
    character count. Estimating from the font size systematically under-fills -- characters
    average closer to 0.45 than 0.5 of their point size in this font, which left the note
    wrapping some 10% narrow than the space available.
    """
    max_points = width_px * POINTS_PER_PIXEL
    font = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)

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
    # No widows: a last line of one word ("19" under the mobile title) borrows the word before it.
    if len(lines) > 1 and " " not in lines[-1] and " " in lines[-2]:
        head, moved = lines[-2].rsplit(" ", 1)
        lines[-2:] = [head, f"{moved} {lines[-1]}"]
    return "\n".join(lines)


def build_note(layout: dict) -> str:
    """Compose the Note row: the stunting definition, tied to the band's lower edge.

    It no longer explains the small steps in the curves at ages 2 and 5 (the switch from lying to
    standing measurement, and the join between WHO's two products): at this scale they are a pixel
    or two, so the note was explaining something the reader cannot see.
    """
    text = (
        "Note: A child is considered stunted if their height is more than two standard deviations below the median "
        "for children of the same age, shown here by the lower edge of the shaded area. In the reference population, "
        f"roughly {STUNTED_SHARE:.1f}% of children fall below this cutoff."
    )
    return wrap_to_content_width(text, layout, layout["footer_fontsize"])


def create_visualization(tb: Table, citation: str, layout: dict) -> plt.Figure:
    """Build one version of the two-panel growth-curve chart.

    Layout notes:
    - One panel per sex, sharing a y-axis, each with one band, -+2 SD around the median, as a flat tint
    - The band's lower edge is the stunting threshold, dashed over the tint edge
    - Median drawn solid on top of the band
    - The median and the threshold are labelled directly in the Boys panel
    - No spines; light horizontal gridlines carry the height reading
    - Axis limits and ticks derived from the data
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    # `set_style` REPLACES `font.sans-serif` with its own Arial-first list, so the module-level
    # assignment above is gone by here and the step would emit a stack it never chose. This is how it
    # came to ship `'Arial', 'DejaVu Sans', 'Liberation Sans', 'Bitstream Vera Sans'`.
    matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
    palette = sns.color_palette("deep")

    body_fontsize = layout["body_fontsize"]
    facet_fontsize = LADDER_PT["facet"]
    # Room the facet titles need above each panel: one line plus grapher's half-line of padding.
    facet_title_space_px = (1 + FACET_TITLE_PAD) * facet_fontsize / POINTS_PER_PIXEL
    age_max = float(tb["age_years"].max())
    # The band decides both ends of the height axis, so a change to what is drawn cannot leave the axis
    # sized for a series the chart no longer shows.
    band_lower, band_upper, band_name = BAND
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
    # Sized to the labels it holds, not to a constant: the widest one starts on the margin, so the
    # plot takes every pixel the column does not need. A hand-tuned 58 shipped here and over-reserved
    # by 5.92px on both layouts, which is what left the plot's ink 6px right of the title.
    y_label_space = y_tick_column_px([height_tick_label(tick) for tick in height_ticks], body_fontsize)
    assert 0 < y_label_space < 0.2 * (width_px - 2 * margin_px), (
        f"the y tick column measured {y_label_space:.2f}px inside a {width_px - 2 * margin_px}px content "
        "box -- either the labels or the face they were measured in are not what this layout expects"
    )

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

        # --- the -+2 SD band ---
        # gid becomes the SVG element id, so Figma shows named layers instead of "Path 41".
        # Mirrors grapher, which stamps its own SVG nodes with makeFigmaId().
        slug = sex.lower()
        ax.fill_between(
            age,
            tb_sex[band_lower].to_numpy(),
            tb_sex[band_upper].to_numpy(),
            facecolor=tint(color, BAND_TINT),
            linewidth=0,
            zorder=2,
            gid=f"{slug}__{band_name}",
        )

        # --- stunting threshold, drawn over the band's lower edge, which is the same series.
        # Doubling the boundary as a tint edge and a dashed stroke is what makes it findable in the
        # unlabelled Girls panel too: the tint stops there, and the region below it is the stunted
        # one. ---
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
        # --- percentile lines on top of the band ---
        # Butt caps, so the line ends on the plot's right edge. Seaborn's round cap runs half the stroke
        # (1.8px) past it, which puts the chart's ink outside the template's content column in Figma.
        for column, line_width in QUANTILE_LINES:
            values = tb_sex[column].to_numpy()
            ax.plot(
                age,
                values,
                color=color,
                linewidth=line_width,
                solid_capstyle="butt",
                zorder=5,
                gid=f"{slug}__{column[-3:]}",
            )

        if ax is axes[0]:
            draw_direct_labels(ax, tb_sex, layout, LADDER_PT["label"])

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
        ax.yaxis.set_major_formatter(FuncFormatter(height_tick_label))
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

    subtitle = wrap_to_content_width(SUBTITLE, layout, body_fontsize)
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
    if layout["full_footer"]:
        chart_top_px = subtitle_bottom_px + HEADER_CHART_GAP * px(body_fontsize) + facet_title_space_px
    else:
        # Mobile: the drawn block starts at the template's chart-area top, inset from it, unless the
        # subtitle runs past that row.
        chart_top_px = max(subtitle_bottom_px, CHART_AREA_TOP) + CHART_AREA_INSET + facet_title_space_px

    # --- footer, in the slots the static-chart templates define ---
    # Desktop: Note -> Data source -> tagline and license sharing one row, left and right.
    # Mobile: Data source -> license, stacked, which is all that template has room for.
    footer_fontsize = layout["footer_fontsize"]

    if layout["full_footer"]:
        note = build_note(layout)
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
        chart_bottom_px = layout["chart_bottom_y"] - CHART_AREA_INSET

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
        left=fx(margin_px + y_label_space),
        right=fx(width_px - margin_px),
        top=fy(chart_top_px),
        bottom=fy(chart_bottom_px - layout["x_label_space"]),
        wspace=0.1,
        hspace=0.35,
    )

    # Both panels' dashed thresholds are respaced now rather than where they were plotted, because
    # `resample_for_even_dashes` measures on the page and the axes only reach their final size on the
    # line above.
    respaced = even_dashes(fig, STUNTING_DASH_PERIOD_PT)
    assert respaced == len(axes), f"respaced {respaced} dashed thresholds, expected one per panel"

    return fig
