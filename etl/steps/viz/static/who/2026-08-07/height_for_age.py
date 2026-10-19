"""Recreate the 'Expected Healthy Growth Curves for Boys and Girls' chart from WHO's height-for-age curves.

Each panel shows the median and a single band running from -2 SD to +2 SD around it. The band's lower
edge is the stunting threshold, so the boundary a reader has to find carries two encodings at once --
where the tint stops and a dashed line -- and everything below the shaded area is the stunted region.

One band, because stunting is the concept the chart exists to explain. A 10th-90th percentile band
nested inside this one, and the miniature encoding diagram it took to tell the two apart, were dropped
after design review: two shades read as two thresholds, and the inner one has no role in the stunting
definition. That leaves three things worth naming -- the median, the threshold and the share of children
the band holds -- so each is labelled directly in the Boys panel, the two explanatory labels with a
curved arrow each, see `draw_direct_labels`. The Girls panel repeats the encoding and carries no labels
of its own. Standard deviations are not labelled in the plot, where "-2 SD" would be jargon; the
stunting definition in those terms goes in the Note on desktop and in a second subtitle sentence on
mobile, which has no Note.

The styling follows the design team's pass on the desktop frame (its copy, node `29144:824`, on the
same Figma page): whole-pixel line weights (3px median, 2px threshold), 14px explanatory labels with
a halo, curved house arrows instead of straight leaders, the facet titles inside the panels, the
stunting label right-aligned with "Stunting cutoff:" on a line of its own, a 13px "Median" with a halo,
a translucent band and a single "Age in years" label. Mobile takes the same treatment, placed for its
narrower panels. The space around the chart is the house one, equal above and below: the copy's first
version showed more only because its chart group carried internal padding, which the design team
confirmed was unintended and removed.

The title says "expected growth curves", not "expected healthy growth curves" as the replaced image
did. Only the under-fives standards rest on children selected for good health and nutrition; the 5-19
reference is rebuilt from US surveys of 1963-1975 that were not, so "healthy" would over-claim for two
thirds of the age axis. "Expected" holds across the whole range: the curves are the heights children of
each age are expected to have. The subtitle stays simple and makes no health claim either.

Neither panel repeats the other sex's median. The two medians stay within about 2 cm of each other
from birth until girls overtake boys at age 9.2 -- a couple of pixels on a 40-200 cm axis -- so a
second line traces the panel's own median for two thirds of the range, the same doubling that
splitting the sexes into panels was meant to remove. Where the two sexes differ can be read off the
panels at a shared gridline.

There is no legend in either version.

Two versions are emitted, following the static-chart templates:

- desktop, 850x615 (the horizontal template, 23px shorter so the panels keep their proportions): panels
  side by side, direct labels in the Boys panel, footer carrying Note, Data source, the
  OurWorldinData.org tagline and the license line.
- mobile, 540x824: panels side by side in the portrait frame, the same direct labels placed for its
  217px panels, footer reduced to Data source plus the license, which is all that template has room
  for. It has no Note slot, so its subtitle adds a sentence defining stunting in standard deviations
  (`MOBILE_SUBTITLE`). The in-plot stunting label says in plain words what stunted means on both
  layouts. The 2.3% share below the cutoff is stated only in the desktop Note, where it can be qualified
  as the reference population's.

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
`20260812 Expected growth curves for boys and girls (Pablo A)` (it is renamed whenever the title
changes; same page, id `25284:5`), sitting at the top of
the dated block -- insert after the `-----------` divider page, not at a counted index. Two frames,
each named for the slug the website exports by, with a reference copy of this step's own render to
their left. The published image being replaced, `Healthy20height20growth20curves.png`, sits leftmost
as the "before" (`create-static-viz`'s `scripts/fetch_original_image.py` fetches it):

| Frame | Node | Open it | Cloned from | Size |
|---|---|---|---|---|
| `expected-height-boys-girls` | `26869:1501` | [link](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=26869-1501) | `5332:75` Static Chart Template_Horizontal | 850x615 (template 850x638) |
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
| Title | `TITLE`; on mobile, break the line after "curves" (a line separator), or "girls" is left alone on the second line | template's Playfair |
| Subtitle | `SUBTITLE` on desktop, `MOBILE_SUBTITLE` on mobile (three lines; it carries the stunting definition). On mobile, the second sentence starts its own line: break after "Organization." and "the median" with explicit line breaks (the design team's frame narrows the box to 421px to the same effect) | Lato Regular |
| `Note:` (desktop only) | `build_note(...)` | `Note:` Bold, rest Regular |
| `Data source:` | `Data source: ` + `source_citation(...)` from `etl.viz.static` | `Data source:` Bold, rest Regular |
| Tagline (desktop) | leave the template's | -- |
| License | `Licensed under ` / `CC-BY` / ` by the author ` / `AUTHOR` | Regular / Bold / Regular / Bold |

Both templates carry the same footer slots, so both get the same license string; mobile just stacks
its two rows (`Frame 15`, source at y=770 and license at y=791) where desktop shares one row with the
tagline.

Two positions are derived rather than the template's fixed y, because the template pins them for a
two-line title and a two-line subtitle:

- Both headers (`Frame 20` desktop, `Frame 26` mobile) are now vertical auto-layout frames, so the
  subtitle follows the title by itself, 6px under it.
- Desktop's footer, `Frame 22`, hugs its rows and is pinned at its top, so a shorter note pulls the
  source row up. Shorten the frame to 615 (`resizeWithoutConstraints(850, 615)`), then re-pin the
  footer after setting the note: `footer.y = 615 - 16 - footer.height`, which puts the two-line note at
  y=536, the step's `chart_bottom_y`. Mobile's footer has no note and needs nothing.

**Colors.** Bind each panel's median, its threshold *and its band* to the library style. The gid names a
group, so descend to its `VECTOR` children before calling the setter.

The threshold has to be bound too, and to the same style as the median beside it. The step draws both
in the panel's own colour on purpose -- colour says which panel a mark belongs to, style says which mark
it is -- so binding only the median splits the pair in Figma: the median moves to the library colour
while the threshold keeps matplotlib's `#4c72b0` / `#dd8452`. Binding a paint style leaves
`dashPattern` alone, so the dash survives the binding. `label__median` is bound too: it names a line,
so it takes that line's colour and weight, the house convention for a line's own label. The two
explanatory labels are not in this table: they stay the step's grey, which ranks them as annotation
rather than data, and their arrows are bound under **Arrows** below.

The band takes the same style as its median, at `BAND_ALPHA` (0.2) opacity. Binding a fill style
replaces the import's paint, and with it the SVG's `fill-opacity`, so set the opacity on the node
(`vector.opacity = 0.2`) after binding, not on the paint.

`label__median` sits on the band, where Denim text measures about 4.0:1 against it, under the 4.5:1
usually asked of 13px text (5.3:1 on the canvas). Accepted on purpose: moving the label off the band
puts it 15 cm clear of the line it names.

| Layer | Treatment |
|---|---|
| `boys___50` | `setStrokeStyleIdAsync` -> `Default Palette/Denim`, key `e1538d9330d7b22168f0c19fa562897aa8975f90` |
| `girls___50` | `setStrokeStyleIdAsync` -> `Default Palette/Rusty Orange`, key `65bab597d085689b1ea82a69f4d785cb9212c234` |
| `<sex>__stunting-threshold` | the same style as `<sex>___50` |
| `<sex>__within-2-sd` | `setFillStyleIdAsync` -> the same style as `<sex>___50`, then node opacity 0.2 |
| `label__median` (Boys panel) | `setFillStyleIdAsync` on its TEXT -> `Default Palette/Denim` |

Denim and Rusty Orange separate by dE 70 at worst; their grayscale seam is 1.14:1, which does not gate
here because the two series sit in separate, text-titled panels. Which panel takes which is set by
`PANEL_COLOR_INDEX`, not here -- keep the two in step.

**In-plot text.** Figma substitutes Inter for matplotlib's family, so restyle to Lato at the ranks below
and re-anchor every label on its mark. Do both in one call and in that order: the widths only settle
on the next call, and a later coordinate patch would use anchors that the fit has already moved.

| Rank | Size | Weight |
|---|---|---|
| Facet titles (`<sex>__label`) | 16 | Bold |
| Tick labels, `label__age` ("Age in years") | 14 | `Age in years` Bold, ticks Regular |
| Explanatory labels (`label__within-2-sd`, `label__stunting-cutoff`) | 14 | Regular |
| `label__median` | 13 | Bold, in the line's colour |

Anchors: y ticks by their right edge; the first x tick by its left and the last by its right, the rest
centred; the facet titles and `label__age` by their top-left corner; `label__median` by its
bottom-right corner; `label__within-2-sd` by its bottom-left corner, lines left-aligned;
`label__stunting-cutoff` by its top-right corner, lines right-aligned.

A wrapped label imports as one TEXT node per line, each placed by its left edge, so the two
right-aligned labels come in ragged once Lato's wider advances are applied. Re-anchor each line on its
right edge: `label__median` on the panel's right edge minus 7px, and each line of
`label__stunting-cutoff` on `stunting_label_xy`'s age (19.37 desktop, 19.6 mobile, as a share of the
plot's 0-19 width).

Halos, as outside strokes on every line's TEXT node (`strokeAlign = "OUTSIDE"`), so a label stays
readable where it crosses a gridline or the band:

| Text | Halo |
|---|---|
| `label__within-2-sd`, `label__stunting-cutoff` | 3px, `setStrokeStyleIdAsync` -> the frame's own canvas style, `Website/Background/Beige` (`#fffbf5`). The design team's copy binds `Instagram/Beige Background` (`#fbf9f3`), another template family's canvas; the house rule is the frame's own, and the two cannot be told apart on screen |
| `label__median` | 2px, the band's colour composited on the canvas -- Denim at 20% on `#fffbf5` is `#dbdee3` -- unbound, since the library has no such style |

**Arrows.** The step's `leader__within-2-sd` and `leader__stunting-cutoff` are matplotlib arcs that
fix where each arrow starts and where it points; in Figma each is replaced by a clone of the house
curvy arrow (`/create-figma-chart` -> `reference/ANNOTATIONS-AND-ARROWS.md`): a 1px stroked curve plus a
filled dart, both bound to `Data Insights/Annotations` (`setStrokeStyleIdAsync` on the curve,
`setFillStyleIdAsync` on the dart). Lay its tail and tip on the step's arrow, then delete the step's:

- band: tail ~6px right of the end of the label's last line (5.9px desktop, 6.6px mobile; the copy
  keeps 6), at mid-height; curving clockwise, out to the right then down, to a tip midway between the
  median and the band's top edge at `band_label_target_age`;
- stunting: tail ~8px left of "Stunting cutoff:" (7.7px desktop, 7.3px mobile; the copy keeps 7.9), at
  mid-height; curving clockwise, out to the left then up, to a tip 3-7px under the dashed line at
  `stunting_label_age`.

The house arrows to clone are the design team's own, in their copy of the desktop frame
(`29144:824`): curves `Arrow 4` (band) / `Arrow 3` (stunting), `29146:1057` / `29146:1063`, and their
darts `29146:1058` / `29146:1064`, with tails at (232, 189) / (336, 315) and tips at (281, 225) /
(286, 276) in frame coordinates. Clone curve and dart separately and fit each to the step's arrow, read
off the curve VECTOR of `leader__<role>` (its first vertex is the tail, its last the tip):

1. rotate the dart rigidly by the angle between the house chord (tail to tip) and the step's, and put
   its tip on the step's tip;
2. take the curve from the step's tail to where the rotated dart's rear now sits, scaling it uniformly
   with `resize(w * s, h * s)` -- never `rescale`, which thins the 1px stroke -- and rotating it onto
   that chord;
3. copy each clone's transform in frame coordinates (`absoluteTransform` minus the frame's origin), since
   `clone()` keeps only its own transform and drops the mirror its parent group carries;
4. group the two as `annotation__arrow-<role>` in the frame, not in `chart`, so the arrows do not count
   as data marks. Add them after the fit's crop, or measure the crop on `chart` alone, so they never
   widen it.

On desktop the house arrows need only a ~0.9-1.05 scale and under a degree of rotation; on mobile the
chords are shorter and steeper (scale 0.58 / 0.69, rotation 21 / 34 degrees).

**Fit.** Crop the `chart` frame to its ink (text by its render bounds vertically), then set its box to
the content column (`resizeWithoutConstraints(header.width, h)`, `x = header.x`). It needs no vertical
move: the step already places the plot against the Figma header and footer with equal gaps
(`CHART_TOP_GAP`, `CHART_BOTTOM_GAP`). Never `rescale` here, which would move every font off its rank.

The column snap is not optional, and the restyle script's own crop does not land it. That crop pads
every stroke by half its weight on all four sides, caps or not, so the 3px median makes it report
the chart 1.5px wider than the column on both layouts. Check the real edge with
`absoluteRenderBounds` instead: it ends at 834.5 / 524.5, the last x tick's 1px stroke centred on the
plot's edge, and that is what the snap absorbs. Anything past half a pixel is a step defect, not
something to absorb in Figma: the median's round cap once really did run 1.8px past the edge, which is
why `QUANTILE_LINES` are drawn with `solid_capstyle="butt"`. The left edge is the widest y
tick label, whose width is Lato's in Figma and Arial's here; `LATO_OVER_MEASURED_ADVANCE` keeps that to
a few tenths of a pixel.

**Audit before showing it.** Expect sizes {16, 14, 13} only, Lato Regular and Bold only, both medians,
*both thresholds* and both bands reporting a bound style -- and each threshold and band reporting the
same colour as its own median, which is the check that catches a stale band or threshold selector --
both bands at opacity 0.2, the three labels in the Boys panel carrying their halos, no ink outside
16..W-16, gridlines at 1px with a 4/4 dash, and ink gaps of 13.9 above and below the chart on both
layouts, with each header ending at its layout's `chart_area_top`. A top gap off by more than a pixel
means `chart_area_top` no longer matches the Figma header: re-measure it rather than moving the chart
by hand.

Then diff the desktop frame against the design team's copy with `/create-figma-chart`'s
`scripts/diff_frames.js` (`referenceId` `29144:824`, `frameId` `26869:1501`). Expect every node paired
and only these deltas: the two wrapped labels as one TEXT node there and one per line here (with the
alignment that comes with it), the explanatory labels' halos on the frame's canvas style rather than
the copy's Instagram one, the Note one 818px box wide here where the copy's is 729, the gridlines, and
the two arrows within a few px of the copy's. Anything else is drift from the design.

Run `verify_page.js` with `shadedAreas: "within-2-sd"`, which exempts the two bands the arrows point
into, and the default gap target. One row then fails by design: desktop `text-floor` flags the
template's own tagline and license row, which ships at 11px. The series, furniture and colour rows
SKIP, because they key on grapher's layer names
(`line__*`, `horizontal-grid-lines`), which this step does not emit, and `off-palette` asks for review
of the unbound #5b5b5b text, as the design team's copy also leaves it. `color_audit.py` fails the two
bands against each other (dE ~15 once composited); that is accepted, because each sits in its own
titled panel and the lines carry the colour.

The stunting arrow is the one arrow ending on a line, so it gets the four-render pixel probe
(`measure_pixels.py arrow-gap`): 4.3px on desktop and 5.5px on mobile, zero touching. On mobile the
dashed line climbs steeply, so the arrow is aimed at age 8.5, where it meets the line head-on, and
keeps 6px of clearance; aimed further right it ran alongside the line and crossed it.

`diff_against_template.js` should report only the footer's first row, which differs from the
template's by the one space after its bold prefix: the desktop template sets the space after `Note:`
in Lato Medium, and the mobile one bolds the space after `Data source:`. Pass those in `expected`. Any other footer drift is real: the license row's plain runs are Lato Regular, not
Medium.
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
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

# A template pixel in points: the figure is 100 template px per inch and there are 72 points to the
# inch, so one pixel is 0.72pt. Stroke weights below are written in template pixels, which is what
# Figma shows, and converted with this.
POINTS_PER_PIXEL = 0.72

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
# Dashed rather than dotted, at a weight that puts it second behind the median and ahead of the
# gridlines, and that survives both print and the 217px mobile panel. At 0.8pt dotted it was the
# faintest stroke in the chart. Both weights are whole template pixels, 3 for the median and 2 here, as
# the design team set them in Figma; fractional weights render unevenly.
#
# The dash is fixed in points, 4.48pt on and 2.8pt off (6.2px and 3.9px), and converted to the multiples
# of the line's own width that matplotlib takes: it multiplies a dash sequence by the linewidth
# (`rcParams["lines.scale_dashes"]`, on by default), so a pattern written in points comes out
# `linewidth` times longer than intended. The SVG carries `stroke-dasharray: 4.48,2.8`, which is what to
# check against. Reading the multiples as points once shipped a 7pt dash on a 1.4pt stroke, five times
# the stroke width, which read as stretched at any size and looked like a Figma import defect.
STUNTING_LINEWIDTH = 2 * POINTS_PER_PIXEL
STUNTING_DASH_PT = (4.48, 2.8)
STUNTING_DASHES = (0, tuple(length / STUNTING_LINEWIDTH for length in STUNTING_DASH_PT))

# The single band, as (lower column, upper column, layer name).
BAND = ("height_sd_minus_2", "height_sd_plus_2", "within-2-sd")

# The band is the panel's own colour at this opacity, as the design team set it in Figma, rather than a
# flat tint blended towards white. The library carries no tints, so a translucent fill is what lets
# Figma bind the band to the same library style as the median and the threshold beside it, and the
# dashed gridlines show faintly through it. It composites onto the template's cream canvas, which is the
# backdrop it was chosen against.
BAND_ALPHA = 0.2

# What 2 SD is worth as a percentile: the share of children below the threshold. Both the Note and the
# in-plot stunting label are formatted from it, so the two can't drift apart.
#
# The conversion is exact rather than approximate, which is what lets a threshold defined in standard
# deviations be stated as a share at all: WHO's height-for-age standard sets the LMS skewness
# parameter L to 1 at every age, so the distribution is normal and -2 SD is the 2.275th percentile
# rather than an age-varying centile. `assert_threshold_is_a_fixed_percentile` checks L is still 1 in
# the data before the label ships.
STUNTED_SHARE = 2.2750132

# The two direct labels. The stunting one says what stunted means in plain words, which mobile has no
# Note to carry. It deliberately leaves out the 2.3% share: out of context that reads as the share of
# children who are stunted, when it is a property of the reference population only -- the Note states it
# with that qualifier.
# The line break after "cutoff:" is the design team's: the name of the line gets a line of its own.
MEDIAN_LABEL = "Median"
STUNTING_LABEL = "Stunting cutoff:\na child below this line is too short for their age."

# The share of children the band holds. A cut point is not a share: 2.3% of children fall below -2 SD
# and the same share above +2 SD, so the band between them holds 100 - 2 x 2.3 = 95.4%. Carried from
# STUNTED_SHARE's seven figures, since rounding 2.275 first would print 95.5%.
# "In the reference population", the Note's own wording, so the share cannot be read as the share of all
# children in the world -- the same misreading that keeps the 2.3% out of the stunting label.
BAND_LABEL = (
    f"{100 - 2 * STUNTED_SHARE:.1f}% of children in the reference population are expected to fall within the "
    "shaded area."
)

# Line spacing of the two wrapped labels, as a multiple of the font size. matplotlib's default 1.2 puts
# their lines 15px apart in Figma, where a typed 14px Lato label (line height AUTO) runs at 17px, as the
# design team's labels do; 1.36 is 1.2 x 17 / 15.
LABEL_LINESPACING = 1.36

# The arrows joining a label to its mark: curved, with a filled head, in the annotation grey and at the
# house arrows' 1px. In Figma each is replaced by a clone of the house curvy arrow laid over it, so what
# this step fixes is where an arrow starts and where it points; see `draw_direct_labels`. The lighter
# #777777 used before all but vanished on the mobile frame, where the band arrow ends on the band.
LEADER_COLOR = "#5b5b5b"  # the annotation grey, as TEXT_COLOR
LEADER_WIDTH = 1 * POINTS_PER_PIXEL
# A filled dart about 8px long and 7px across, the house arrowhead's size, in points (the arrow is drawn
# with `mutation_scale=1`, so these are not multiples of the font size).
LEADER_HEAD = "-|>,head_length=5.8,head_width=2.5"
# Air between an arrow's tail and its label, along the arc, in template px, set so each tail lands where
# the design team's copy has it: ~6px past the band label's last line, ~8px before "Stunting cutoff:".
# The band's is small because `line_end`'s Lato allowance overshoots on that line; the stunting arrow
# leaves at a slant, so it needs more along the arc to keep its distance across.
LEADER_TAIL_CLEARANCE_PX = {"within-2-sd": 0.5, "stunting-cutoff": 9}

# Percentiles drawn as lines on top of the band, as (column, line width), so a specific centile
# can be read off rather than only a range. Labelled directly in the Boys panel.
QUANTILE_LINES = [
    ("height_percentile_50", 3 * POINTS_PER_PIXEL),
]

MEDIAN_COLUMN = "height_percentile_50"
STUNTING_COLUMN = "height_sd_minus_2"

# Axis treatment copied from grapher so the static chart reads like our interactive ones.
# Values from owid-grapher: TICK_COLOR and GRID_LINE_DASH_PATTERN in
# packages/@ourworldindata/grapher/src/axis/AxisViews.tsx, GRAPHER_DARK_TEXT (= GRAY_80) in
# .../color/ColorConstants.ts. Grapher dashes its gridlines rather than drawing them solid and
# labels axes in bold. The y axis carries no line: its gridlines carry the reading.
# Grapher's gridline is 1px with a 4px dash and 4px gap. The width is written in template pixels and the
# dash in multiples of it (matplotlib scales a dash by the linewidth), so both land on whole pixels; 1.0pt
# here once came out 1.39px with a 5.6px dash.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
GRID_LINEWIDTH = 1 * POINTS_PER_PIXEL

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

# Facet titles: bold (grapher's FACET_LABEL_FONT_WEIGHT = 700), in GRAPHER_DARK_TEXT like the tick
# labels rather than in the series colour. Unlike grapher's, they sit INSIDE the panel, in the empty
# top-left corner under the top gridline, where the design team moved them in Figma; that hands the
# row grapher reserves above each panel back to the plot. The offsets are template px from the panel's
# top-left corner to the title's top-left. Boys is indented to line up with the band label under it,
# Girls has no label to line up with and sits close to the panel's edge, both as placed in Figma.
FACET_TITLE_OFFSET_PX = {"Boys": (16, 15.5), "Girls": (4, 15.5)}

# The design team's type ladder for the text INSIDE the plot, in template pixels -- which is what
# Figma shows, because the figure is drawn at the template's own width. The two explanatory labels are
# annotation M 14, the size of the tick labels; "Median" is a rung under them at S 13, as set in Figma.
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
LADDER_PX = {"facet": 16, "body": 14, "annotation": 14, "line_label": 13}
LADDER_PT = {rank: px * POINTS_PER_PIXEL for rank, px in LADDER_PX.items()}

# The age range is on the x axis, so the title leaves it out.
TITLE = "Expected growth curves for boys and girls"

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
        # 23px shorter than the template's 850x638, as the design team set it: at the full height the
        # house gaps stretch the two panels taller than the curves need, so the frame loses the slack
        # instead and the plot keeps its proportions.
        "size": (850, 615),
        "template": "horizontal",
        "margin": 16,
        "title_y": 16,
        # The three footer rows of `Static Chart Template_Horizontal` (Note, Data source, then the
        # tagline/licence row, 32 and 18px apart, inside a 63-tall `Frame 22` 16px above the frame's
        # bottom), moved up with the shorter frame. `chart_bottom_y` is the Note's top for a two-line
        # note, which is the shape the template ships.
        "chart_bottom_y": 536,
        "source_y": 568,
        "footer_y": 586,
        "nrows": 1,
        "ncols": 2,
        "full_footer": True,
        # Where the header ends in the Figma frame: a one-line title and a one-line subtitle (16 + 29 +
        # 6 + 19).
        "chart_area_top": 70,
        "age_ticks": [0, 5, 10, 15, 19],
        # The stunting label's top-right corner, as (age, cm), and how wide it may wrap, in template px;
        # and the age its arrow points at on the dashed line. Its right edge runs 7px past the panel,
        # into the gutter, as placed in Figma. See `draw_direct_labels` for why it sits where it does.
        "stunting_label_xy": (19.37, 115.4),
        "stunting_label_width": 155,
        "stunting_label_age": 11.4,
        # Air between the stunting arrow's tip and the dashed line it points at, set from the pixel probe's
        # 3-7px band. The line climbs steeply in the narrow mobile panel and the dart's corner comes closer
        # to it than the tip, so mobile takes 6.
        "stunting_arrow_clearance_px": 4,
        # The band label's bottom-left corner, as (age, cm), in the empty top-left of the Boys panel,
        # and the age its arrow reaches into the band at. The age puts its left edge 15px into the
        # panel, under the Boys title (`FACET_TITLE_OFFSET_PX`).
        "band_label_xy": (0.78, 162.5),
        "band_label_target_age": 11.1,
        "band_label_width": 215,
        "title_fontsize": 16,
        "body_fontsize": LADDER_PT["body"],
        "footer_fontsize": 7.75,
        # Space reserved below the plot for the tick marks, the x tick labels and the bold
        # "Age in years" label: their ink runs 44px below the baseline in Figma, so `CHART_BOTTOM_GAP`
        # lands as set. The y tick column is not reserved here -- it is measured from the labels
        # themselves, see `y_tick_column_px`.
        "x_label_space": 44,
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
        "stunting_label_xy": (19.6, 110.8),
        "stunting_label_width": 120,
        # Where the arrow meets the steep mobile line head-on; aimed further right it runs alongside the
        # line and crosses it.
        "stunting_label_age": 8.5,
        "stunting_arrow_clearance_px": 6,
        # Where the header ends in the Figma frame: the template's serif sets the title on two lines and
        # the two-sentence subtitle takes three (16 + 58 + 6 + 57). The step measures neither face, so it
        # takes the number rather than its own wrap.
        "chart_area_top": 137,
        # The words each header line ends on in that frame. The step's own faces are narrower and would
        # set the header on one title line and two subtitle lines, which leaves a gap of ~70px above the
        # chart in this step's PNG that the Figma frame does not have. Breaking where Figma does makes
        # the PNG's header end at `chart_area_top` too. The subtitle's second sentence starts a line of
        # its own, as the design team set it.
        "title_breaks_after": ["curves"],
        "subtitle_breaks_after": ["Organization.", "the median"],
        # Narrow, at six lines, because the band rises steeply through the panel's top-left: at four
        # lines the wider rows run into the shading.
        "band_label_xy": (1.4, 158),
        "band_label_target_age": 10.8,
        "band_label_width": 125,
        "title_fontsize": 16,
        "body_fontsize": LADDER_PT["body"],
        "footer_fontsize": 8.75,
        "x_label_space": 44,
    },
}

# Both layouts share this. It names no product and makes no health claim: the standards-versus-reference
# split means little to most readers, and "healthy" holds for the under-fives standards only.
#
# No geography word, deliberately. The under-fives standards earn one -- six countries, and WHO's own
# claim that they apply to children everywhere -- but the 5-19 half is a reconstruction of a single
# national sample, 22,917 US children measured between 1963 and 1975, so calling the whole range
# 'global' over-claims on exactly that half.
SUBTITLE = "Height by age, according to the World Health Organization."

# Mobile has no Note slot, so its subtitle carries the stunting definition the desktop Note gives: the
# link between the band's lower edge and "2 standard deviations", which the plot does not label.
MOBILE_SUBTITLE = (
    f"{SUBTITLE} Children more than 2 standard deviations below the median height for their age are considered stunted."
)


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

# The space between the chart's ink and the Figma header above it and the footer below it, in template
# pixels: the same above and below, inside the house 12-16px band, measured on what the reader sees
# (the top of the "200 cm" label, the bottom of "Age in years"), not on the chart group's box.
CHART_TOP_GAP = 14
CHART_BOTTOM_GAP = 14

# How far the top y tick label's ink rises above the top gridline, in template px, measured in Figma. The
# plot's top edge is that gridline, so it starts this much below where the ink may start.
TOP_TICK_OVERHANG = 5


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
        # A layout may run a little shorter or taller than its template, which its comment explains:
        # `height_px` holds it to the template's width at its own height.
        export_frame(paths, fig, short_name, template=layout["template"], height_px=layout["size"][1])
        plt.close(fig)


def assert_threshold_is_a_fixed_percentile(tb: Table) -> None:
    """Check the premise behind the shares the chart prints: 2.3% below the threshold, 95.4% in the band.

    The threshold is stated as a *share of children* while it is defined in *standard deviations*, and
    that conversion only holds because WHO's height-for-age standard sets the LMS skewness parameter L
    to 1 at every age, making the distribution normal. If a future revision introduced skewness, -2 SD
    would become an age-varying centile and the band label and the Note would silently misstate how many
    children fall below it -- a wrong number on a published chart, with nothing else to catch it.
    """
    skewness = tb["lms_l_skewness"].unique()
    assert set(skewness) == {1}, (
        f"Height-for-age is no longer a normal distribution (L = {skewness}), so -2 SD is no longer "
        f"the {STUNTED_SHARE}th percentile that BAND_LABEL and the Note are built on."
    )

    # The same claim checked against the percentile columns rather than the parameter: 2.275 sits
    # between the 1st and the 3rd, so the threshold must too, at every age and for both sexes.
    outside = (tb["height_sd_minus_2"] <= tb["height_percentile_1"]) | (
        tb["height_sd_minus_2"] >= tb["height_percentile_3"]
    )
    assert not outside.any(), (
        f"-2 SD escapes the 1st-3rd percentile range in {int(outside.sum())} rows, so it is not the "
        f"{STUNTED_SHARE}th percentile that BAND_LABEL and the Note are built on."
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


def draw_direct_labels(ax, tb_sex: Table, layout: dict, color) -> None:
    """Name the median, the band and the stunting threshold on the curves themselves.

    The median label and the points both arrows aim at are anchored to the data at an age rather than
    at a fixed height, so a revision that moves the curves moves them too. The two explanatory labels
    sit at a fixed (age, cm) per layout, in space the curves leave empty, and an arrow carries each one
    to its mark.

    - The median's label sits just above the line's right end, right-aligned a few pixels in from it,
      where the line has flattened out after the growth spurt. On the rising stretch any label clear of
      the line floats a line-height or more above it, level with the band's upper edge, and reads as
      naming that edge instead. Right-aligned, a wider face in Figma only grows the label leftwards,
      never past the edge.
    - The band's label sits in the empty space above the band at the young ages, left-aligned. Its
      arrow leaves from the end of the label's last line and curves down into the shading, ending
      midway between the band's top edge and the median, so it points at the area rather than at either
      line. Inside the band there is no room for the label itself: the band rises too steeply for a
      horizontal label to stay between its edges.
    - The stunting label sits below the dashed line, right-aligned near the panel's right edge, in the
      region the label is about, which is empty down to the baseline from the growth spurt onwards. Its
      arrow leaves from just left of the first line, "Stunting cutoff:", and curves up to the dashed line.
    """
    age = tb_sex["age_years"].to_numpy()

    def height_at(column: str, years: float) -> float:
        return float(np.interp(years, age, tb_sex[column].to_numpy()))

    label_fontsize = LADDER_PT["annotation"]

    median_age = float(age.max())
    ax.annotate(
        MEDIAN_LABEL,
        xy=(median_age, height_at(MEDIAN_COLUMN, median_age)),
        xytext=(-7 * POINTS_PER_PIXEL, 3.2),
        textcoords="offset points",
        ha="right",
        va="bottom",
        fontsize=LADDER_PT["line_label"],
        # The line's own label: its colour and bold, so it reads as the line's name. The two labels
        # below explain the encoding and stay in the annotation grey.
        color=color,
        fontweight="bold",
        zorder=6,
        gid="label__median",
    )

    target_age = layout["band_label_target_age"]
    band_text = wrap_to_width(BAND_LABEL, layout["band_label_width"], label_fontsize)
    band_label = ax.annotate(
        band_text,
        xy=(target_age, (height_at(MEDIAN_COLUMN, target_age) + height_at(BAND[1], target_age)) / 2),
        xytext=layout["band_label_xy"],
        textcoords="data",
        ha="left",
        va="bottom",
        multialignment="left",
        linespacing=LABEL_LINESPACING,
        fontsize=label_fontsize,
        color=TEXT_COLOR,
        zorder=6,
        gid="label__within-2-sd",
        arrowprops=arrow_props(
            *line_end(
                band_text, label_fontsize, row=-1, side="right", clearance_px=LEADER_TAIL_CLEARANCE_PX["within-2-sd"]
            ),
            rad=-0.35,
            clearance_px=0,
        ),
    )
    assert band_label.arrow_patch is not None
    band_label.arrow_patch.set_gid("leader__within-2-sd")

    stunting_age = layout["stunting_label_age"]
    stunting_text = wrap_to_width(STUNTING_LABEL, layout["stunting_label_width"], label_fontsize)
    label = ax.annotate(
        stunting_text,
        xy=(stunting_age, height_at(STUNTING_COLUMN, stunting_age)),
        xytext=layout["stunting_label_xy"],
        textcoords="data",
        ha="right",
        va="top",
        multialignment="right",
        linespacing=LABEL_LINESPACING,
        fontsize=label_fontsize,
        color=TEXT_COLOR,
        zorder=6,
        gid="label__stunting-cutoff",
        arrowprops=arrow_props(
            *line_end(
                stunting_text,
                label_fontsize,
                row=0,
                side="left",
                clearance_px=LEADER_TAIL_CLEARANCE_PX["stunting-cutoff"],
            ),
            rad=-0.35,
            clearance_px=layout["stunting_arrow_clearance_px"],
        ),
    )
    assert label.arrow_patch is not None
    label.arrow_patch.set_gid("leader__stunting-cutoff")


def arrow_props(origin: tuple[float, float], gap: float, rad: float, clearance_px: float) -> dict:
    """Arrow properties for an annotation whose arrow leaves from `origin` on its text, `gap` points clear.

    `rad` bends the arrow into an arc, as the house curvy arrows do; negative bends it clockwise from
    the label to its mark. `clearance_px` is the air left between the tip and the point it aims at:
    a few pixels when that point is on a line, as the house arrows keep, and none when it is inside an
    area.
    """
    return {
        "arrowstyle": LEADER_HEAD,
        "mutation_scale": 1,
        "connectionstyle": f"arc3,rad={rad}",
        "color": LEADER_COLOR,
        "linewidth": LEADER_WIDTH,
        "shrinkA": gap,
        "shrinkB": clearance_px * POINTS_PER_PIXEL,
        "relpos": origin,
        # matplotlib otherwise clips the arrow to the text's box plus 4pt of padding, so it starts on the
        # box's edge rather than at the point chosen above.
        "patchA": None,
    }


def line_end(text: str, fontsize: float, row: int, side: str, clearance_px: float) -> tuple[tuple[float, float], float]:
    """Where an arrow leaves a wrapped label: beside one of its lines, as a `relpos` fraction of the box.

    `row` picks the line and `side` which end of it, "right" for a left-aligned label (the end of the
    line) and "left" for a right-aligned one (its start). Returns the point at mid-height of that line,
    and the gap to leave from it, in points: `clearance_px` of air plus what the line grows by once
    Figma sets it in Lato, which runs wider than the face measured here. A label grows away from its
    anchored edge, so all of that growth lands on the free end, where the arrow is.
    """
    font = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)
    lines = text.split("\n")
    widths = [TextPath((0, 0), line, prop=font).get_extents().width if line.strip() else 0.0 for line in lines]
    widest = max(widths)
    row = row % len(lines)
    x = widths[row] / widest if side == "right" else 1 - widths[row] / widest
    y = (len(lines) - row - 0.5) / len(lines)
    gap = clearance_px * POINTS_PER_PIXEL + (LATO_OVER_MEASURED_ADVANCE - 1) * widths[row]
    return (x, y), gap


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

    A line break already in the text is kept, and each paragraph is wrapped on its own.
    """
    if "\n" in text:
        return "\n".join(wrap_to_width(paragraph, width_px, fontsize) for paragraph in text.split("\n"))
    max_points = width_px * POINTS_PER_PIXEL
    font = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)

    def measure(candidate: str) -> float:
        return TextPath((0, 0), candidate, prop=font).get_extents().width if candidate.strip() else 0.0

    lines: list[str] = []
    current = ""
    # Split on plain spaces only, so a no-break space holds its words together.
    for word in text.split(" "):
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


def break_after(text: str, anchors: list[str]) -> str:
    """Put a line break after each anchor, in place of the space that follows it."""
    for anchor in anchors:
        assert f"{anchor} " in text, f"line-break anchor {anchor!r} is not in {text!r}; re-read the Figma header"
        text = text.replace(f"{anchor} ", f"{anchor}\n", 1)
    return text


def build_note(layout: dict) -> str:
    """Compose the Note row: the stunting definition, tied to the band's lower edge.

    It no longer explains the small steps in the curves at ages 2 and 5 (the switch from lying to
    standing measurement, and the join between WHO's two products): at this scale they are a pixel
    or two, so the note was explaining something the reader cannot see.
    """
    text = (
        "Note: A child is considered stunted if their height is more than 2 standard deviations below the median "
        "for children of the same age, shown here by the dashed line, the lower edge of the shaded area. In the "
        "reference population, "
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
            facecolor=color,
            alpha=BAND_ALPHA,
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
            draw_direct_labels(ax, tb_sex, layout, color)

        # --- panel title, inside the panel's top-left corner, under the top gridline ---
        title_dx, title_dy = FACET_TITLE_OFFSET_PX[sex]
        ax.annotate(
            sex,
            xy=(0, 1),
            xycoords="axes fraction",
            xytext=(title_dx * POINTS_PER_PIXEL, -title_dy * POINTS_PER_PIXEL),
            textcoords="offset points",
            ha="left",
            va="top",
            fontsize=facet_fontsize,
            fontweight="bold",
            color=TEXT_COLOR,
            zorder=6,
            gid=f"{slug}__label",
        )

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
        # Grapher renders axis labels bold (fontWeight 700 in Axis.ts). One label serves both panels,
        # since they share the axis: under the first panel, left-aligned on its left edge, its box
        # starting 27.6px below the baseline, as the design team set it in Figma.
        if ax is axes[0]:
            ax.set_xlabel(
                "Age in years", fontsize=body_fontsize, color=TEXT_COLOR, fontweight="bold", labelpad=6.4, loc="left"
            )
            ax.xaxis.label.set_gid("label__age")

    # --- header: title, then subtitle directly beneath it ---
    # The templates put the subtitle at a fixed y=80, but that assumes the two-line title their
    # placeholder uses. Deriving it from the title's actual height keeps the pair tight when the
    # title only needs one line, and reproduces the template's y=80 exactly when it needs two.
    title_text = break_after(TITLE, layout.get("title_breaks_after", []))
    title = wrap_to_content_width(title_text, layout, layout["title_fontsize"])
    title_lines = title.count("\n") + 1
    subtitle_y = layout["title_y"] + title_lines * px(layout["title_fontsize"]) + TITLE_SUBTITLE_GAP

    subtitle_text = break_after(
        SUBTITLE if layout["full_footer"] else MOBILE_SUBTITLE, layout.get("subtitle_breaks_after", [])
    )
    subtitle = wrap_to_content_width(subtitle_text, layout, body_fontsize)
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

    # The chart starts a fixed gap below where the Figma header ends. That header, not the step's own
    # (set in narrower faces), is the one that ships, so it decides; the step's only has to fit above it.
    # The facet titles sit inside the panels, so all that rises above the plot is the top y tick label.
    subtitle_bottom_px = subtitle_y + subtitle_lines * px(body_fontsize) + px(body_fontsize) * SUBTITLE_GAP
    assert subtitle_bottom_px < layout["chart_area_top"] + CHART_TOP_GAP, (
        f"the step's subtitle ends at {subtitle_bottom_px:.0f}px, inside the gap above the chart"
    )
    chart_top_px = layout["chart_area_top"] + CHART_TOP_GAP + TOP_TICK_OVERHANG

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
        chart_bottom_px = note_top_px - CHART_BOTTOM_GAP
    else:
        chart_bottom_px = layout["chart_bottom_y"] - CHART_BOTTOM_GAP

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
