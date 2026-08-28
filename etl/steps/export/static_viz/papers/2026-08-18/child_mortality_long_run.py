"""Recreate the 'long-run history of child mortality' chart.

Three layers on one time axis: an average across hunter-gatherer societies, which carries no date and
so sits off to the left of the axis; 21 dated pre-modern societies, each labelled with its own rate,
with a rule through their average; and the global rate since 1950, dropping to the bottom right.

Replaces the hand-drawn `Youth-mortality-rates-over-last-two-millennia-updated-to-2022.png` used on
the child-mortality topic page and the 'Mortality in the past' article.

Every figure comes from `garden/papers/2026-08-18/child_mortality_in_the_past`, which is also where
the selection of societies and the splice in the global series live. Nothing here is typed.

Design choices carried over from the published chart, deliberately rather than by inheritance:

- **No y-axis tick labels.** Every mark carries its own value, which is the direct labelling our
  chart guidance prefers to a legend or an axis lookup. The gridlines stay, so heights can still be
  compared against one another. Adding labels would be cheap and is worth asking design about.
- **Each society sits at the middle of the period it covers**, and the average rule spans the first
  to the last of those midpoints.
- **The hunter-gatherer average is drawn as a short rule, left of the first tick.** It has no date,
  so putting it on the time axis would date it; keeping it inside the same frame is what lets a
  reader compare it with the dated points.

One departure, which needs design sign-off rather than passing as polish: the published chart drew a
faded line from Sweden's 18th-century point down to the 1950 global rate. No data supports it - UN
IGME and UN WPP begin at 1990 and 1950 - and it reads as one country's history continuing into the
world's. It is dropped, and the global series simply begins where its sources do.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this step
fixes is the data, the structure, the proportions and the axis conventions.

Template geometry
-----------------
Laid out against `Static Chart Template_Vertical` (`5332:93`), verified against the live frame on
2026-08-27 with `create-figma-chart/scripts/verify_templates.js` (no drift on any of the ten
templates). Two things about it are not obvious and both are load-bearing:

- **Almost none of the template's y values are constants.** Header and footer are auto-layout blocks
  that hug their text, so the title's line count moves everything below it. `derive_header` implements
  the rhythm and `assert_header_rhythm` checks it against both positions measured off the frame - the
  two-line placeholder the template ships (band at 118) and the one-line case (70).
- **The logo is the header's SIBLING, not its child**, so it contributes nothing to the header's height
  and the band moves with the text alone. That changed under this step: until the design team flattened
  the header, the logo sat inside the title row and set it whenever the title was shorter than 41.26px,
  which cost this chart's one-line title 12.5px of dead space above the subtitle. What keeps the title
  clear of the logo now is the width of the template's own title node - 737.84px against an 818px
  content box - which is what `assert_title_clears_logo` measures against.

The step's own title, subtitle, note and footer are drawn at the template's measured sizes (25, 16, 12
and 11 template px) rather than at sizes that merely look right, so the PNG previews the frame's
spacing instead of showing a hole the frame does not have.

The license line drops the words `the author`: with two names the template's own phrasing overruns the
263 px slot and prints on top of the tagline it shares a row with. `assert_license_clears_tagline`
measures the string that is actually drawn.

Label placement
---------------
21 labelled points, 11 of them between 1600 and 1900, do not fit beside their marks by luck. The
placement is a deterministic greedy search (`place_labels`): points are taken from the highest rate
down, and each is offered candidate slots - either side of its mark, at growing vertical offsets -
until one is found whose *measured* text box clears every box already placed, the average rule and
its label, the global series, and the plot's own edges. Widths come from `TextPath`, never from a
character count.

That is deliberately not a table of hand-tuned offsets. The historical half of this chart never
changes, but the global series gains a year annually, and a search re-solves around it where fixed
offsets would silently start colliding.

Figma
-----
Written here rather than handed over in chat, because a link given once is gone by the next session
and "where in Figma is this?" then means searching a ~200-page file by eye.

**Where.** File `Charts (2026)`, key `s6Sv60bakebRRW2TxsMQbF`. Page
`20260827 The long-run history of child mortality (Pablo A)` (`26888:5`), first placed 2026-08-27 and
sitting at the top of the dated block, immediately after the `-----------` divider page.

| Frame | Node | Deep link |
|---|---|---|
| `long-run-history-child-mortality` | `26888:6` | https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=26888-6 |
| `long-run-history-child-mortality - step render (reference)` | `26888:799` | this step's own PNG, parked 80px to the left |

**The node ids are a convenience and go stale; the frame name is the durable join.** It is the same
kebab-case slug the website exports the PNG by, so it is the one string shared by the layer panel, the
exported filename and this file. Search the file for it, or for the page name above.

**How it was built**, so it can be redone rather than reverse-engineered:

- Clone `5332:93` (`Static Chart Template_Vertical`, 850x1095) onto a new page. Run
  `create-figma-chart/scripts/verify_templates.js` first - a `DRIFT` verdict stops the run.
- Import both files with `upload_assets` + a POST to the returned `submitUrl`, never
  `createNodeFromSvg`. The raster arrives in a 400x300 frame with a FILL-mode fill, so the PNG has to
  be resized to the frame's own 850x1095 or it ships cropped.
- Run `create-figma-chart/scripts/restyle_static_import.js` over the SVG. It rescales by
  `850 / 816` - matplotlib writes the root in points, Figma imports at 96px/in, and this figure is
  built at 100 template px per inch - drops this step's own copies of the six template slots, restyles
  the text to Lato and re-anchors every label.
- Its `families.members` list must name **every** gid suffix the accent covers, or a mark keeps its
  matplotlib colour while the rest move: `points`, `mean_rule`, `mean_marker`, `series`,
  `marker-start`, `marker-end`, `highest-marker`, `lowest-marker`, and the four red annotation blocks
  `mean_label-1/-2`, `label-start-1/-2`, `label-end-1/-2`. All at weight 0, bound to
  `Default Palette/Coral`, key `e9f3a0fed2eeeae607b6aaf75f79806830fcda78`. Gray labels take
  `Text/Gray 80`, key `5a7394f8beea1df86bd3ad44cd77182c6625f525`.
- Fill the template's own slots from this file's constants. The footer on this template is entirely
  **unbound**, so nothing needs re-binding - only the weight runs, which assigning `characters`
  collapses to the first character's: `Note:` is `Bold + Medium + Regular`, `Data source:` is
  `Bold + Regular`, and the license is `Medium + Bold + Medium + Bold`. The tagline is left alone.
- The tagline/license row is `SPACE_BETWEEN` auto-layout, so a license wider than its 263px
  placeholder is right-aligned by the layout rather than needing a nudge.

**Expected afterwards:** `diff_against_template.js` reports the frame matches `5332:93`, and the
header's own `headerBottom` reads **89** - the same number `derive_header` predicts for this chart's
one-line title over a two-line subtitle.

**One thing that does not apply here but is worth not re-deriving.** Figma fits a whole number of dash
repetitions into each *segment* of a path, so a dashed line's rendered dash follows its vertex spacing
rather than the pattern asked for. This chart's gridlines are two-vertex spans of 589pt - one segment
each, far above the ~50px where it starts to matter - so they render as specified and need no
resampling. A dashed line that followed a *curve* here would.
"""

import math

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.static_viz import PIXELS_PER_INCH, TEMPLATES, apply_svg_rcparams, export_frame, source_citation

paths = PathFinder(__file__)

# The Figma handoff contract - editable text, deterministic ids - lives in etl.static_viz rather than
# being restated here. Called at import, because svg.hashsalt is read when a figure is built.
apply_svg_rcparams()

# Which static-chart template this is laid out against. `export_frame` validates the figure against
# it on the way out, so a figsize typo cannot ship.
TEMPLATE = "vertical"

TITLE = "The long-run history of child mortality"

SUBTITLE = (
    "Shown is the share of children who died before reaching the end of puberty. The age cut-off varies "
    "slightly between studies, but is generally around 15 years."
)

# Credited as the author of the visualization on the license line. The data is updated and the design
# broadly preserved, so the original author is credited alongside whoever refreshed it.
AUTHOR = "Max Roser and Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

# Source abbreviations written out for the footer, as the citation renders them (name, space, bracket).
# Only the ones a reader of the chart would not recognise: "UN WPP" and "UN IGME" stay as they are.
SOURCE_NAME_EXPANSIONS = {"HMD (": "Human Mortality Database ("}

# Axis treatment copied from grapher so the static chart reads like our interactive ones. Values from
# owid-grapher: GRID_LINE_DASH_PATTERN and TICK_COLOR in
# packages/@ourworldindata/grapher/src/axis/AxisViews.tsx, GRAPHER_DARK_TEXT (= GRAY_80) in
# .../color/ColorConstants.ts.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
GRID_LINEWIDTH = 1.0
TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"
TICK_COLOR = "#999999"
TICK_LENGTH = 5
TICK_WIDTH = 1

# Gridline spacing on the value axis, in percentage points. The limits are snapped out to whole steps
# of this so the outermost gridlines land on the plot's edges, as grapher's domain does.
RATE_STEP = 10

# Where the undated hunter-gatherer average sits, in years before the earliest dated society. A layout
# choice: far enough left of the dated points to read as separate from them, close enough that the two
# levels can still be compared. How much axis is left beyond it is derived from its label's width.
HUNTER_GATHERER_LEAD_YEARS = 700

# Half-width of the two average rules, in years, so each reads as a level rather than a point.
HUNTER_GATHERER_RULE_HALF_WIDTH = 110

# The dated ticks. Neither outermost tick sits at an edge of the axis, which is a deliberate
# departure from grapher's convention of pinning the range to them: the axis carries an undated
# lead-in on the left for the hunter-gatherer marker, and a margin on the right wide enough for the
# labels of the 1600-1900 cluster, whose marks are only ~50px from the last year. The published chart
# does the same, and without that margin eleven labels have nowhere to go.
#
# **There is no tick at 0, because there is no year 0** - the calendar runs 1 BCE straight into 1 CE,
# so 0 on this scale is the boundary between them rather than a year to label. The published chart
# labelled it "0".
#
# The BCE tick is the other half of that fix: three of the societies are dated in BCE and the axis
# used to carry no tick left of the era boundary at all, so a third of it had no scale. -500 is the
# round century that falls between the hunter-gatherer marker and the earliest dated society, so it
# gives the BCE stretch a reading without landing under the undated marker and appearing to date it.
YEAR_TICKS = [-500, 500, 1000, 1500]

LAYOUT = {
    # "Static Chart Template_Vertical", node 5332:93, re-read from the file on 2026-08-18 and matching
    # TEMPLATES.md exactly. Everything is in template pixels, y measured from the top edge as Figma
    # reports it.
    #
    # The header and footer are auto-layout blocks, so almost none of the template's own y values are
    # constants: the header grows down from the title and the footer up from the frame's bottom margin.
    # What is fixed is the rhythm - see `derive_header` - and these are its inputs.
    # From etl.static_viz rather than repeated here, so the frame this is laid out against and the one
    # `export_frame` validates against cannot drift apart.
    "size": (TEMPLATES[TEMPLATE].width_px, TEMPLATES[TEMPLATE].height_px),
    "margin": 16,
    # The header block's own top edge. It carries no inner padding, so the title's ink starts here.
    "origin_y": 16.0,
    # The header is a flat auto-layout of [title, subtitle] and the logo is its SIBLING, so the logo
    # contributes nothing to the header's height and the band moves with the text alone. It was not
    # always so: until the design team flattened it (between 2026-08-18 and 2026-08-20) the logo sat
    # inside the title row and, below its own 41.26px height, set that row - which cost a one-line
    # title 12.5px of dead space above the subtitle. Verified against the live frame on 2026-08-27
    # via `create-figma-chart/scripts/verify_templates.js`, which returned no drift on any template.
    "title_line_px": 29.0,
    "subtitle_line_px": 19.0,
    "note_line_px": 14.0,
    "title_subtitle_gap_px": 6.0,
    # The Note's ink bottom. The footer's rows are pinned to the frame's bottom margin, so the source
    # and tagline rows do not move when the Note reflows - only the Note's own top edge does, upwards,
    # eating the chart's height.
    "note_ink_bottom_y": 1043.81,
    "source_y": 1047.81,
    "footer_y": 1065.81,
    # Font sizes as template pixels, measured off the live templates on 2026-08-17. Drawn at the
    # template's own sizes rather than at sizes that merely look right: the band is derived for the
    # frame, so a step whose subtitle is a size smaller ends higher than the frame's and shows a hole
    # the frame does not have.
    "title_font_px": 25,
    "subtitle_font_px": 16,
    "note_font_px": 12,
    "source_font_px": 12,
    "tagline_font_px": 11,
    # In-plot type, which the template says nothing about because the import replaces the plot.
    "body_fontsize": 10.5,
    "label_fontsize": 8,
    # Reserved below the plot for the tick marks, the tick labels and the bold axis label.
    "x_label_space": 46,
}

# Inset at each end of the band between the subtitle's ink and the footer's. The design team asks for
# 12-16 px; 14 is the middle. Both edges are ink, not frame - insetting from the footer frame's own y
# would inset twice and leave a visibly loose bottom.
BAND_INSET = 14

# The two calibration points for the header rhythm on this frame, as
# (title_lines, subtitle_lines) -> band_top. The first is the template's own placeholder, measured at
# headerBottom 118; the second is what a one-line title and subtitle reflow it to.
HEADER_CALIBRATION = {(2, 2): 118.0, (1, 1): 70.0}

# A template pixel in points: the figure is 100 template px per inch and there are 72 points to the
# inch. Used to convert the template's geometry for text measurement, which matplotlib does in points.
POINTS_PER_PIXEL = 0.72

# The width the template gives its own title node, which is narrower than the 818px content box. That
# is what keeps the title clear of the logo beside it, so a candidate title is measured against this
# rather than against the content width.
TITLE_NODE_WIDTH_PX = 737.84

# How far a label sits from the mark it names, in template pixels, and the clearance kept between two
# labels. Small, because adjacency is what removes the need for a leader line.
LABEL_GAP_PX = 4
LABEL_PADDING_PX = 1.5

# Clearance kept around each data mark, in template pixels, so a label placed beside its own mark does
# not run over a neighbour's.
MARKER_CLEARANCE_PX = 3

# Line spacing for in-plot text, as a multiple of the font size. Used both to space the lines of a
# multi-line label and to give a single-line label's reservation box its height.
LINE_SPACING = 1.3

# How far an average's label sits below its rule, in percentage points.
AVERAGE_LABEL_DROP = 1.4

# How far a label may sit from its mark, in percentage points, before it gets a hairline joining the
# two. Set just above the smallest candidate offset, so a label nudged one slot is still read by
# adjacency and only a genuinely displaced one is drawn a line.
LEADER_THRESHOLD = 1.4

# Candidate vertical offsets for a label, in percentage points, tried in this order: level with its
# mark first, then alternating above and below.
LABEL_OFFSETS = [0.0, 1.3, -1.3, 2.6, -2.6, 3.9, -3.9, 5.2, -5.2, 6.5, -6.5, 7.8, -7.8]


def run() -> None:
    """Load data, render and save the chart."""
    ds = paths.load_dataset("child_mortality_in_the_past")
    tb_historical = ds.read("historical_societies")
    tb_hunter_gatherer = ds.read("hunter_gatherer_societies")
    tb_global = ds.read("global_child_mortality")
    tb_extremes = ds.read("country_extremes")
    tb_summary = ds.read("summary")

    paths.log.info(
        f"{len(tb_historical)} historical societies averaging "
        f"{float(tb_summary['historical_mean'].iloc[0]):.2f}%, "
        f"{int(tb_summary['hunter_gatherer_societies'].iloc[0])} hunter-gatherer societies averaging "
        f"{float(tb_summary['hunter_gatherer_mean_published'].iloc[0]):.1f}% as published, "
        f"global series {int(tb_global['year'].min())}-{int(tb_global['year'].max())}"
    )

    # Keyed on attribution_short rather than producer: UN IGME's under-fifteen series combines two of
    # its own releases whose producer strings differ, and grouping by producer cites them as two
    # separate sources. The short forms also keep a five-source footer to one line.
    citation = expand_opaque_source_names(
        source_citation(
            tb_historical["share_dying_before_15"], tb_global["share_dying_before_15"], key="attribution_short"
        )
    )
    paths.log.info(f"Source citation: {citation}")

    fig = create_visualization(tb_historical, tb_hunter_gatherer, tb_global, tb_extremes, tb_summary, citation)

    # export_frame owns the save discipline: the clip sweep, the opaque PNG, the transparent SVG, and
    # the check that the figure still matches the template it was laid out against.
    export_frame(paths, fig, paths.short_name, template=TEMPLATE)
    plt.close(fig)


def expand_opaque_source_names(citation: str) -> str:
    """Write out the source abbreviations a reader would not recognise.

    Keying the citation on `attribution_short` is what collapses UN IGME's two releases into one entry,
    and it gives the right short form for the UN products - but for the Human Mortality Database it
    gives an acronym that means nothing to a reader of the finished chart, where the producer's own
    name is both readable and short. The trailing bracket is part of the match so it can only hit a
    citation entry, never an acronym inside some other name.
    """
    for short, full in SOURCE_NAME_EXPANSIONS.items():
        # A replace that matches nothing returns the string unchanged and reports success, which would
        # ship the acronym while this function looked like it had run.
        assert short in citation, (
            f"Expected {short!r} in the citation so it could be written out, but got: {citation!r}. "
            "Either the origin's attribution_short changed or this expansion is no longer needed."
        )
        citation = citation.replace(short, full)
    return citation


def build_note(tb_global: Table, tb_historical: Table, tb_summary: Table) -> str:
    """Compose the Note row: what the pre-modern estimates are, and where the global series joins.

    The disagreement quoted is the one measured *within* the splice year, which garden computes. The
    step visible on the chart between the last UN WPP year and the first UN IGME year is bigger, because
    it also contains a real one-year fall.
    """
    spans = {
        str(name): (int(group["year"].min()), int(group["year"].max()))
        for name, group in tb_global.groupby("data_source", observed=True)
    }
    n_studies = tb_historical["study"].nunique()
    splice_year = int(tb_summary["global_splice_year"].iloc[0])
    difference = float(tb_summary["global_splice_difference"].iloc[0])

    return (
        f"Note: Each pre-modern point is a single estimate for one population, drawn from {n_studies} separate "
        "studies, and the periods they cover differ in length. The global series joins two sources: "
        + ", ".join(f"{name} for {start}-{end}" for name, (start, end) in sorted(spans.items(), key=lambda x: x[1]))
        + f". For {splice_year}, the year both estimate, they differ by {difference:.1f} percentage points."
    )


def create_visualization(
    tb_historical: Table,
    tb_hunter_gatherer: Table,
    tb_global: Table,
    tb_extremes: Table,
    tb_summary: Table,
    source_citation: str,
) -> plt.Figure:
    """Build the chart."""
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")
    # Position 3 is the palette's red. Referenced by position rather than as a hex so the chart moves
    # with the shared palette.
    accent = palette[3]

    width_px, height_px = LAYOUT["size"]
    margin_px = LAYOUT["margin"]
    label_fontsize = LAYOUT["label_fontsize"]

    def fx(x_px: float) -> float:
        """Template x, in pixels from the left edge, as a figure fraction."""
        return x_px / width_px

    def fy(y_px: float) -> float:
        """Template y, in pixels from the *top* edge as Figma reports it, as a figure fraction."""
        return 1 - y_px / height_px

    fig, ax = plt.subplots(figsize=(width_px / PIXELS_PER_INCH, height_px / PIXELS_PER_INCH))
    # Opaque for the PNG a human reviews; dropped at save time for the SVG (see run()).
    fig.patch.set_facecolor("white")

    # --- header and footer: the template's own slots, at the template's own sizes ---
    assert_header_rhythm()

    title = wrap_to_content_width(TITLE, LAYOUT, _points(LAYOUT["title_font_px"]))
    subtitle = wrap_to_content_width(SUBTITLE, LAYOUT, _points(LAYOUT["subtitle_font_px"]))
    note = wrap_to_content_width(
        build_note(tb_global, tb_historical, tb_summary), LAYOUT, _points(LAYOUT["note_font_px"])
    )
    assert_title_clears_logo(title)

    subtitle_y, band_top_px = derive_header(_lines(title), _lines(subtitle))
    note_y = LAYOUT["note_ink_bottom_y"] - _lines(note) * LAYOUT["note_line_px"]

    fig.text(
        fx(margin_px),
        fy(LAYOUT["origin_y"]),
        title,
        ha="left",
        va="top",
        fontsize=_points(LAYOUT["title_font_px"]),
        color="#111111",
        gid="title",
    )
    fig.text(
        fx(margin_px),
        fy(subtitle_y),
        subtitle,
        ha="left",
        va="top",
        fontsize=_points(LAYOUT["subtitle_font_px"]),
        color="#555555",
        gid="subtitle",
    )
    fig.text(
        fx(margin_px),
        fy(note_y),
        note,
        ha="left",
        va="top",
        fontsize=_points(LAYOUT["note_font_px"]),
        color=MUTED_COLOR,
        gid="note",
    )
    fig.text(
        fx(margin_px),
        fy(LAYOUT["source_y"]),
        f"Data source: {source_citation}",
        ha="left",
        va="top",
        fontsize=_points(LAYOUT["source_font_px"]),
        color="#888888",
        gid="data-source",
    )
    fig.text(
        fx(margin_px),
        fy(LAYOUT["footer_y"]),
        TAGLINE,
        ha="left",
        va="top",
        fontsize=_points(LAYOUT["tagline_font_px"]),
        color="#888888",
        gid="tagline",
    )
    license_text = f"Licensed under CC-BY by {AUTHOR}"
    assert_license_clears_tagline(license_text)
    fig.text(
        fx(width_px - margin_px),
        fy(LAYOUT["footer_y"]),
        license_text,
        ha="right",
        va="top",
        fontsize=_points(LAYOUT["tagline_font_px"]),
        color="#888888",
        gid="license",
    )

    # --- plot band: inset at both ends from ink, not from the footer frame's edge ---
    chart_top_px = band_top_px + BAND_INSET
    chart_bottom_px = note_y - BAND_INSET

    fig.subplots_adjust(
        left=fx(margin_px),
        right=fx(width_px - margin_px),
        top=fy(chart_top_px),
        bottom=fy(chart_bottom_px - LAYOUT["x_label_space"]),
    )

    # --- axis limits and ticks ---
    latest_year = int(tb_global["year"].max())
    earliest_mid = float(tb_historical["period_mid"].min())
    hunter_gatherer_x = earliest_mid - HUNTER_GATHERER_LEAD_YEARS

    # The hunter-gatherer label is centred under its marker, so the axis has to start far enough left
    # to hold half of it. Measured, because the label carries a count from the data and a wording
    # change would silently push it off the frame.
    hunter_gatherer_label = build_hunter_gatherer_label(tb_summary)
    hunter_gatherer_half_px = max(_measure_px(line, label_fontsize) for line in hunter_gatherer_label.split("\n")) / 2

    highest_rate = float(
        max(tb_historical["share_dying_before_15"].max(), tb_hunter_gatherer["share_dying_before_15"].max())
    )
    rate_ticks = np.arange(0, np.ceil(highest_rate / RATE_STEP) * RATE_STEP + 1, RATE_STEP)

    # Margins in pixels at both ends of the axis: on the left, half the hunter-gatherer label; on the
    # right, the widest label that hangs off a mark - the 1600-1900 cluster's, whose marks sit within
    # ~50px of the last year, and the four that annotate the global series.
    plot_width_px = width_px - 2 * margin_px
    lead_px = hunter_gatherer_half_px + LABEL_GAP_PX
    trail_px = (
        max(
            *(_measure_px(text, label_fontsize) for text in build_society_labels(tb_historical)),
            *(
                _measure_px(line, LAYOUT["body_fontsize"])
                for _, _, text, _ in build_modern_labels(tb_global, tb_extremes, accent)
                for line in text.split("\n")
            ),
        )
        + 2 * LABEL_GAP_PX
    )
    # Both margins are in pixels but cost years, and how many years a pixel is worth depends on the
    # range the margins are part of. Solving the two together: the span from the hunter-gatherer marker
    # to the last year has to fit in whatever width the margins leave.
    content_px = plot_width_px - lead_px - trail_px
    assert content_px > plot_width_px / 2, (
        f"Labels want {lead_px:.0f}px on the left and {trail_px:.0f}px on the right of a {plot_width_px:.0f}px plot, "
        f"leaving only {content_px:.0f}px for the data itself. Shorten the longest labels or drop the label size."
    )
    years_per_px = (latest_year - hunter_gatherer_x) / content_px

    ax.set_xlim(hunter_gatherer_x - lead_px * years_per_px, latest_year + trail_px * years_per_px)
    ax.set_ylim(rate_ticks[0], rate_ticks[-1])
    ax.set_yticks(rate_ticks)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=GRID_COLOR, linewidth=GRID_LINEWIDTH, linestyle=GRID_DASHES)
    ax.xaxis.grid(False)

    # No spines except the baseline the tick marks hang from, in the zero line's own colour and
    # weight. This axis does reach zero, so that baseline *is* grapher's VerticalAxisZeroLine.
    for name, spine in ax.spines.items():
        spine.set_visible(name == "bottom")
    ax.spines["bottom"].set_color(TICK_COLOR)
    ax.spines["bottom"].set_linewidth(TICK_WIDTH * POINTS_PER_PIXEL)
    # The baseline already draws a solid line at the lowest tick, so its gridline would be a dashed
    # lighter stroke laid over the top of it.
    ax.yaxis.get_gridlines()[0].set_visible(False)

    ticks = [*YEAR_TICKS, latest_year]
    ax.set_xticks(ticks)
    ax.set_xticklabels([year_tick_label(tick) for tick in ticks])
    # Every tick label stays centred on its mark. Grapher's inward anchoring exists to stop the
    # outermost label half-overhanging the plot, and neither of these sits at an edge - see YEAR_TICKS.
    # The value axis carries no labels; every mark is labelled with its own rate instead.
    ax.tick_params(axis="y", length=0, labelleft=False)
    ax.tick_params(
        axis="x",
        length=TICK_LENGTH * POINTS_PER_PIXEL,
        width=TICK_WIDTH * POINTS_PER_PIXEL,
        color=TICK_COLOR,
        direction="out",
        labelsize=LAYOUT["body_fontsize"],
        labelcolor=TEXT_COLOR,
    )
    ax.set_xlabel("Year", fontsize=LAYOUT["body_fontsize"], color=TEXT_COLOR, fontweight="bold", labelpad=8)

    # --- the global series ---
    draw_global_series(ax, tb_global, tb_extremes, accent, LAYOUT["body_fontsize"])
    assert_modern_labels_fit(ax, tb_global, tb_extremes, accent)

    # --- the two averages ---
    historical_mean = float(tb_summary["historical_mean"].iloc[0])
    hunter_gatherer_mean = float(tb_summary["hunter_gatherer_mean_published"].iloc[0])
    rule_span = (float(tb_historical["period_mid"].min()), float(tb_historical["period_mid"].max()))

    ax.plot(
        rule_span,
        [historical_mean, historical_mean],
        color=accent,
        linewidth=2.0,
        solid_capstyle="butt",
        zorder=4,
        gid="historical__mean_rule",
    )
    ax.plot(
        [hunter_gatherer_x - HUNTER_GATHERER_RULE_HALF_WIDTH, hunter_gatherer_x + HUNTER_GATHERER_RULE_HALF_WIDTH],
        [hunter_gatherer_mean, hunter_gatherer_mean],
        color=accent,
        linewidth=2.0,
        solid_capstyle="butt",
        zorder=4,
        gid="hunter_gatherer__mean_rule",
    )
    ax.plot(
        [hunter_gatherer_x],
        [hunter_gatherer_mean],
        marker="o",
        markersize=4,
        color=accent,
        zorder=5,
        gid="hunter_gatherer__mean_marker",
    )

    # --- the 21 dated societies, and their labels ---
    ax.plot(
        tb_historical["period_mid"].to_numpy(),
        tb_historical["share_dying_before_15"].to_numpy(),
        linestyle="none",
        marker="o",
        markersize=2.6,
        color=accent,
        zorder=6,
        gid="historical__points",
    )

    reserved = reserve_drawn_areas(
        ax, tb_historical, tb_global, rule_span, historical_mean, hunter_gatherer_x, hunter_gatherer_mean
    )
    placements, unplaced = place_labels(ax, tb_historical, label_fontsize, reserved)
    assert not unplaced, (
        "No clear slot was found for these labels, so they would sit on top of something: "
        f"{unplaced}. Widen the plot, shrink the label size, or extend LABEL_OFFSETS."
    )
    for text, (x, y, horizontal_alignment, mark_x, mark_y) in placements.items():
        slug = _slug(text)
        # A label pushed clear of its neighbours can end up a few percentage points from the mark it
        # names, and with 21 marks in one band the reader then cannot tell which is which. Where that
        # happens, a hairline joins the two; where the label sits level with its mark, adjacency does
        # the work and no line is drawn.
        if abs(y - mark_y) > LEADER_THRESHOLD:
            ax.plot(
                [mark_x, x],
                [mark_y, y],
                color=GRID_COLOR,
                linewidth=0.6,
                solid_capstyle="butt",
                zorder=3,
                gid=f"historical__leader-{slug}",
            )
        ax.text(
            x,
            y,
            text,
            fontsize=label_fontsize,
            color=TEXT_COLOR,
            ha=horizontal_alignment,
            va="center",
            zorder=7,
            gid=f"historical__label-{slug}",
        )

    # --- the two average labels ---
    draw_text_block(
        ax,
        hunter_gatherer_x,
        hunter_gatherer_mean - AVERAGE_LABEL_DROP,
        hunter_gatherer_label,
        ha="center",
        va="top",
        fontsize=label_fontsize,
        color=TEXT_COLOR,
        gid="hunter_gatherer__mean_label",
    )
    draw_text_block(
        ax,
        (rule_span[0] + rule_span[1]) / 2,
        historical_mean - AVERAGE_LABEL_DROP,
        f"{_round_half_up(historical_mean)}% is the average across these\n{len(tb_historical)} historical societies",
        ha="center",
        va="top",
        fontsize=label_fontsize,
        color=accent,
        gid="historical__mean_label",
    )

    # Drop clipping everywhere so labels that sit outside the axes survive into the SVG and Figma
    # receives whole shapes rather than cropped ones.
    for artist in fig.findobj():
        artist.set_clip_on(False)

    return fig


def draw_global_series(ax, tb_global: Table, tb_extremes: Table, accent, fontsize: float) -> None:
    """Draw the global series and label its ends, plus the highest and lowest countries today.

    All four labels sit in the axis margin to the right of the latest year, as the published chart
    puts them. To the left is the series itself, which any label placed there would either cross or
    have to stand clear of - and the drop from 26% to 4% leaves no clear column beside it.
    """
    years = tb_global["year"].to_numpy()
    rates = tb_global["share_dying_before_15"].to_numpy()
    ax.plot(years, rates, color=accent, linewidth=2.2, zorder=5, gid="global__series")

    first_year, first_rate = int(years[0]), float(rates[0])
    last_year, last_rate = int(years[-1]), float(rates[-1])
    for year, rate, name in ((first_year, first_rate, "start"), (last_year, last_rate, "end")):
        ax.plot([year], [rate], marker="o", markersize=5, color=accent, zorder=6, gid=f"global__marker-{name}")

    gap = _px_to_data_x(ax, LABEL_GAP_PX * 2)
    # A two-line label centred on a mark puts its lower line half a block below it. The lowest-rate
    # mark sits at a few tenths of a percent, so centring that one would hang its second line under
    # the axis - it sits on the mark instead.
    half_block = _px_to_data_y(ax, LINE_SPACING * fontsize / POINTS_PER_PIXEL)
    for gid, rate, text, color in build_modern_labels(tb_global, tb_extremes, accent):
        # The 1950 label hangs off the start of the series, the other three off its latest year.
        anchor = first_year if gid == "global__label-start" else last_year
        if gid.startswith("country__"):
            marker_gid = gid.removesuffix("-label") + "-marker"
            ax.plot([last_year], [rate], marker="o", markersize=2.6, color=accent, zorder=6, gid=marker_gid)
        draw_text_block(
            ax,
            anchor + gap,
            rate,
            text,
            ha="left",
            va="bottom" if rate - half_block < ax.get_ylim()[0] else "center",
            fontsize=fontsize,
            color=color,
            gid=gid,
        )


def build_modern_labels(tb_global: Table, tb_extremes: Table, accent) -> list[tuple[str, float, str, object]]:
    """The four labels that sit in the axis margin, as (gid, rate, text, color).

    Built in one place so the same strings that get drawn are the ones whose measured widths decide
    how much margin the axis reserves for them. Each is two lines, because one line of the same text
    would be wide enough to squeeze the dated part of the axis.
    """
    years = tb_global["year"].tolist()
    rates = tb_global["share_dying_before_15"].tolist()
    highest = tb_extremes[tb_extremes["role"] == "highest"].iloc[0]
    lowest = tb_extremes[tb_extremes["role"] == "lowest"]

    return [
        (
            "global__label-start",
            float(rates[0]),
            f"Global rate in {int(years[0])}:\n{_round_half_up(float(rates[0]))}%",
            accent,
        ),
        ("global__label-end", float(rates[-1]), f"Global rate in {int(years[-1])}:\n{float(rates[-1]):.1f}%", accent),
        (
            "country__highest-label",
            float(highest["share_dying_before_15"]),
            f"{highest['country']} has the highest\nrate: {_round_half_up(float(highest['share_dying_before_15']))}%",
            TEXT_COLOR,
        ),
        ("country__lowest-label", float(lowest["share_dying_before_15"].max()), _lowest_label(lowest), TEXT_COLOR),
    ]


def derive_header(title_lines: int, subtitle_lines: int) -> tuple[float, float]:
    """Where the subtitle starts and where the plot's band begins, in template pixels.

    The header is an auto-layout block that hugs its text, so neither position is a constant: a title
    that wraps to a different number of lines than the template's placeholder moves everything below
    it. Hard-coding the template's own 80 and 118 is right for its two-line placeholder and wrong for
    any other length - which is what leaves a line of dead space above a plot, invisibly to every
    contract check.
    """
    subtitle_y = LAYOUT["origin_y"] + title_lines * LAYOUT["title_line_px"] + LAYOUT["title_subtitle_gap_px"]
    return subtitle_y, subtitle_y + subtitle_lines * LAYOUT["subtitle_line_px"]


def assert_header_rhythm() -> None:
    """Check the rhythm still reproduces the positions measured off the live template.

    Both ends, because the two-line case is the only one the template itself exercises and the
    one-line case is the one this chart is in.
    """
    for (title_lines, subtitle_lines), expected in HEADER_CALIBRATION.items():
        _, band_top = derive_header(title_lines, subtitle_lines)
        assert abs(band_top - expected) < 0.02, (
            f"The header rhythm puts a {title_lines}-line title and {subtitle_lines}-line subtitle's band at "
            f"{band_top:.2f}, but the template measures {expected}. One of the inputs no longer matches the frame."
        )


def assert_title_clears_logo(title: str) -> None:
    """Check the title's ink fits the width the template gives it, which is not the content width.

    The logo sits beside the title rather than above the subtitle, and what keeps the two apart is
    that the template sizes its title node narrower than the content box - 737.84 against 818. A title
    measured against the content width instead would clear every check here and still run under the
    logo in the frame.
    """
    widest = max(_measure_px(line, _points(LAYOUT["title_font_px"])) for line in title.split("\n"))
    assert widest <= TITLE_NODE_WIDTH_PX, (
        f"The title's widest line measures {widest:.0f}px against the {TITLE_NODE_WIDTH_PX}px the template gives its "
        f"title node, so it would run under the logo. Shorten it or let it wrap: {title!r}"
    )


def assert_license_clears_tagline(license_text: str) -> None:
    """Check the license does not print on top of the tagline it shares a row with.

    TEMPLATES.md measured this for two names and found it overruns: the template's own placeholder fits
    the 263px slot, but `Licensed under CC-BY by the authors <two names>` needs 387px against the 351px
    the tagline leaves. Dropping the words `the author(s)` is what buys the room back - so this checks
    the string that is actually drawn, in the font it is drawn in.
    """
    fontsize = _points(LAYOUT["tagline_font_px"])
    content_px = LAYOUT["size"][0] - 2 * LAYOUT["margin"]
    room = content_px - _measure_px(TAGLINE, fontsize)
    needed = _measure_px(license_text, fontsize)
    assert needed <= room, (
        f"The license line needs {needed:.0f}px but the tagline leaves only {room:.0f}px on their shared row, so it "
        f"would print on top of it. Shorten the phrasing, never a name: {license_text!r}"
    )


def assert_modern_labels_fit(ax, tb_global: Table, tb_extremes: Table, accent) -> None:
    """Check the four margin labels stay inside the plot, by measurement rather than by eye.

    The axis reserves room for the widest of them, so this is the check that the reservation and the
    drawing agree. A near-miss here is invisible on a render at review size and obvious once the SVG
    is placed in the template.
    """
    gap = _px_to_data_x(ax, LABEL_GAP_PX * 2)
    x_max = ax.get_xlim()[1]
    last_year = int(tb_global["year"].max())
    first_year = int(tb_global["year"].min())

    overflowing = []
    for gid, _, text, _ in build_modern_labels(tb_global, tb_extremes, accent):
        anchor = first_year if gid == "global__label-start" else last_year
        widest = max(_measure_px(line, LAYOUT["body_fontsize"]) for line in text.split("\n"))
        right_edge = anchor + gap + _px_to_data_x(ax, widest)
        if right_edge > x_max:
            overflowing.append(f"{gid} overruns the plot by {_data_x_to_px(ax, right_edge - x_max):.1f}px")
    assert not overflowing, "Labels in the right-hand margin do not fit: " + "; ".join(overflowing)


def draw_text_block(ax, x: float, y: float, text: str, *, ha: str, va: str, fontsize: float, color, gid: str) -> None:
    """Draw a possibly multi-line label as one text call per line, each carrying its own anchor.

    A `"\\n"` handed to a single `ax.text` gets no `text-anchor` at all: matplotlib centres each line
    by baking a per-line `translate` in, using its own metrics. The lines then arrive in Figma as
    independent left-anchored boxes and lose their alignment on each other - and on whatever they
    label - the moment the font changes. That is the one case the anchor rule cannot rescue, because
    there is no anchor to preserve.

    Each line sits on an explicit baseline rather than on `va="center"`, which would reserve room for
    descenders that most of these lines do not use and so ride about a tenth of a line high.
    """
    lines = text.split("\n")
    step = _px_to_data_y(ax, LINE_SPACING * fontsize / POINTS_PER_PIXEL)
    cap_half = _px_to_data_y(ax, _cap_half_px(fontsize))

    # `y` is where the block's `va` edge belongs; convert it to the first line's visual centre.
    if va == "top":
        first_centre = y - cap_half
    elif va == "bottom":
        first_centre = y + (len(lines) - 1) * step + cap_half
    else:
        first_centre = y + (len(lines) - 1) * step / 2

    for index, line in enumerate(lines):
        ax.text(
            x,
            first_centre - index * step - cap_half,
            line,
            fontsize=fontsize,
            color=color,
            ha=ha,
            va="baseline",
            zorder=7,
            gid=f"{gid}-{index + 1}" if len(lines) > 1 else gid,
        )


def year_tick_label(year: int) -> str:
    """Label a year tick, naming the era wherever a bare number could be read either way.

    A BCE tick says so; a CE year under 1000 says so too, because "500" beside "500 BCE" is exactly
    the pair a reader can misread. Four-digit years need neither.
    """
    if year < 0:
        return f"{-year} BCE"
    return f"{year} CE" if year < 1000 else f"{year}"


def build_hunter_gatherer_label(tb_summary: Table) -> str:
    """Label the undated hunter-gatherer average, quoting the figure the paper publishes.

    Deliberately the published mean rather than a recomputation from the paper's own table, which does
    not reproduce it - see the garden step's docstring.
    """
    societies = int(tb_summary["hunter_gatherer_societies"].iloc[0])
    mean = float(tb_summary["hunter_gatherer_mean_published"].iloc[0])
    return f"Average across {societies} hunter-gatherer\nsocieties: {_round_half_up(mean)}%"


def society_label(society: str, period: str, rate: float) -> str:
    """Label one society with its period and its rate, rounded to a whole percent.

    Rounded because these are estimates from skeletal remains and parish registers, and printing
    "61.9%" for one claims a precision the study cannot support. It is also what the published chart
    did. Full precision stays in the garden dataset.
    """
    return f"{society} {period}: {_round_half_up(rate)}%"


def build_society_labels(tb_historical: Table) -> list[str]:
    """Every society's label, so their measured widths can size the axis before anything is drawn."""
    return [
        society_label(society, period, rate)
        for society, period, rate in zip(
            tb_historical["society"], tb_historical["period_label"], tb_historical["share_dying_before_15"]
        )
    ]


def place_labels(ax, tb_historical: Table, fontsize: float, reserved: list[tuple[float, float, float, float]]):
    """Find a slot for every society's label, or report the ones with nowhere to go.

    Greedy and deterministic: highest rate first, and for each, the first candidate slot whose
    measured box clears everything placed so far. Returns the accepted anchors and any failures.
    """
    boxes = list(reserved)
    placements: dict[str, tuple[float, float, str, float, float]] = {}
    unplaced: list[str] = []

    x_min, x_max = ax.get_xlim()
    y_min, y_max = ax.get_ylim()
    gap_x = _px_to_data_x(ax, LABEL_GAP_PX)
    pad_x = _px_to_data_x(ax, LABEL_PADDING_PX)
    pad_y = _px_to_data_y(ax, LABEL_PADDING_PX)

    tb = tb_historical.sort_values("share_dying_before_15", ascending=False)
    for society, period, mid, rate in zip(
        tb["society"], tb["period_label"], tb["period_mid"], tb["share_dying_before_15"]
    ):
        text = society_label(society, period, rate)
        width = _px_to_data_x(ax, _measure_px(text, fontsize))
        height = _px_to_data_y(ax, LINE_SPACING * fontsize / POINTS_PER_PIXEL)

        for offset in LABEL_OFFSETS:
            for alignment, left in (("left", mid + gap_x), ("right", mid - gap_x - width)):
                y = rate + offset
                box = (left - pad_x, y - height / 2 - pad_y, left + width + pad_x, y + height / 2 + pad_y)
                if box[0] < x_min or box[2] > x_max or box[1] < y_min or box[3] > y_max:
                    continue
                if any(_overlaps(box, other) for other in boxes):
                    continue
                boxes.append(box)
                anchor_x = mid + gap_x if alignment == "left" else mid - gap_x
                placements[text] = (anchor_x, y, alignment, float(mid), float(rate))
                break
            else:
                continue
            break
        else:
            unplaced.append(text)

    return placements, unplaced


def reserve_drawn_areas(
    ax,
    tb_historical: Table,
    tb_global: Table,
    rule_span: tuple[float, float],
    historical_mean: float,
    hunter_gatherer_x: float,
    hunter_gatherer_mean: float,
) -> list[tuple[float, float, float, float]]:
    """Boxes the society labels must not land on: every mark, the two average rules and their labels,
    and the global series with the labels hanging off it.

    The marks matter as much as the rest. Eleven of the twenty-one sit between 1600 and 1900, so a
    label placed beside its own mark runs straight over its neighbours' unless they are reserved -
    which is what the first render did.
    """
    height = _px_to_data_y(ax, LINE_SPACING * LAYOUT["label_fontsize"] / POINTS_PER_PIXEL)
    body_height = _px_to_data_y(ax, LINE_SPACING * LAYOUT["body_fontsize"] / POINTS_PER_PIXEL)
    marker_half_x = _px_to_data_x(ax, MARKER_CLEARANCE_PX)
    marker_half_y = _px_to_data_y(ax, MARKER_CLEARANCE_PX)

    reserved = [
        (mid - marker_half_x, rate - marker_half_y, mid + marker_half_x, rate + marker_half_y)
        for mid, rate in zip(tb_historical["period_mid"], tb_historical["share_dying_before_15"])
    ]
    reserved += [
        # The historical average rule, and the two lines of label under its middle.
        (rule_span[0], historical_mean - height, rule_span[1], historical_mean + height / 2),
        (
            (rule_span[0] + rule_span[1]) / 2 - _px_to_data_x(ax, 100),
            historical_mean - 3 * height,
            (rule_span[0] + rule_span[1]) / 2 + _px_to_data_x(ax, 100),
            historical_mean,
        ),
        # The hunter-gatherer rule and its label.
        (
            hunter_gatherer_x - _px_to_data_x(ax, 90),
            hunter_gatherer_mean - 3 * height,
            hunter_gatherer_x + _px_to_data_x(ax, 90),
            hunter_gatherer_mean + height / 2,
        ),
    ]

    # The global series, as a corridor around the line, plus the column its labels occupy to the left
    # of the latest year.
    years = tb_global["year"].to_numpy()
    rates = tb_global["share_dying_before_15"].to_numpy()
    corridor = _px_to_data_x(ax, 6)
    for year, rate in zip(years, rates):
        reserved.append(
            (float(year) - corridor, float(rate) - height / 2, float(year) + corridor, float(rate) + height / 2)
        )
    reserved.append(
        (
            float(years[0]) - _px_to_data_x(ax, 130),
            0.0,
            float(years[-1]),
            float(rates[0]) + 2 * body_height,
        )
    )
    return reserved


def wrap_to_content_width(text: str, layout: dict, fontsize: float) -> str:
    """Wrap text to fill the content width between the template's side margins.

    Lines are built greedily against the *measured* width of the rendered glyphs rather than a
    character count, which systematically under-fills.
    """
    max_points = (layout["size"][0] - 2 * layout["margin"]) * POINTS_PER_PIXEL
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and _measure_points(candidate, fontsize) > max_points:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\n".join(lines)


def _lowest_label(lowest: Table) -> str:
    """Name the countries at the lowest rate, naming the leader and counting the rest when many share it.

    Several countries round to the same tenth of a percentage point, and how many depends on the year,
    so the label has to hold either case. Naming the lowest and counting the others keeps it to two
    lines and still tells the reader who is there.
    """
    countries = lowest.sort_values("share_dying_before_15")["country"].tolist()
    rate = float(lowest["share_dying_before_15"].max())
    if len(countries) == 1:
        return f"{countries[0]} has the\nlowest rate: {rate:.1f}%"
    if len(countries) == 2:
        return f"{countries[0]} and {countries[1]} have\nthe lowest rate: {rate:.1f}%"
    return f"Lowest rate: {rate:.1f}%, in {countries[0]}\nand {len(countries) - 1} other countries"


def _lines(text: str) -> int:
    """How many lines a wrapped block occupies."""
    return text.count("\n") + 1


def _cap_half_px(fontsize: float) -> float:
    """Half a cap height at this size, in template pixels.

    What turns a row's centre into a baseline. Measured off a digit, which has no descender, so lines
    with and without one land on the same baseline.
    """
    extents = TextPath((0, 0), "0", prop=FontProperties(size=fontsize)).get_extents()
    return extents.ymax / 2 / POINTS_PER_PIXEL


def _points(template_px: float) -> float:
    """A size in template pixels, as points. A template pixel is 0.72pt."""
    return template_px * POINTS_PER_PIXEL


def _round_half_up(value: float) -> int:
    """Round to the nearest whole number, with .5 going up.

    Not `round()` and not `f"{value:.0f}"`, both of which round a tie to the even number: Wari's 52.5%
    would print as 52%, where the source's own convention - and the published chart - give 53%. Three
    of the twenty-one rates land exactly on a half.
    """
    return math.floor(value + 0.5)


def _measure_points(text: str, fontsize: float) -> float:
    """Width of one line of rendered text, in points."""
    if not text.strip():
        return 0.0
    return TextPath((0, 0), text, prop=FontProperties(size=fontsize)).get_extents().width


def _measure_px(text: str, fontsize: float) -> float:
    """Width of one line of rendered text, in template pixels."""
    return _measure_points(text, fontsize) / POINTS_PER_PIXEL


def _px_to_data_x(ax, value_px: float) -> float:
    """Template pixels as a span on the x axis.

    The plot's size is read back off the axes rather than recomputed from the template rows: the band
    depends on how many lines the title, subtitle and note actually wrapped to, so any figure derived
    from the rows alone drifts as soon as a wording change reflows one of them - and a label box built
    from a drifted conversion clears its neighbour in the arithmetic but not on the page.
    """
    x_min, x_max = ax.get_xlim()
    width_px = ax.get_position().width * LAYOUT["size"][0]
    return value_px / width_px * (x_max - x_min)


def _data_x_to_px(ax, value_years: float) -> float:
    """A span on the x axis, in template pixels."""
    x_min, x_max = ax.get_xlim()
    return value_years / (x_max - x_min) * ax.get_position().width * LAYOUT["size"][0]


def _px_to_data_y(ax, value_px: float) -> float:
    """Template pixels as a span on the y axis."""
    y_min, y_max = ax.get_ylim()
    height_px = ax.get_position().height * LAYOUT["size"][1]
    return value_px / height_px * (y_max - y_min)


def _overlaps(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> bool:
    """Whether two (x0, y0, x1, y1) boxes intersect."""
    return a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]


def _slug(text: str) -> str:
    """A layer name Figma can show, from a label."""
    keep = [character.lower() if character.isalnum() else "-" for character in text]
    return "".join(keep).strip("-").replace("---", "-").replace("--", "-")
