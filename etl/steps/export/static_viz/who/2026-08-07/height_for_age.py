"""Recreate the 'Expected height of boys and girls' growth-curve chart.

Each panel shows nested percentile bands from the WHO growth reference standards, the
median, and the -2 SD stunting threshold, plus a dashed copy of the other sex's median -- in
that sex's own color, which is what saves it needing a label -- so the crossover in early
adolescence stays visible once boys and girls are split apart.

Two versions are emitted, following the static-chart templates:

- desktop: panels side by side, footer carrying Note, Data source, the OurWorldinData.org
  tagline and the license line. The encoding is explained by a diagram in the empty triangle
  under the Boys curve -- a miniature cross-section of the bands with each part named -- as the
  chart this replaces did, rather than by a legend.
- mobile: panels side by side too, in the portrait frame, and a footer reduced to Data source
  plus the license, which is all the mobile template has room for. The template has no Note
  slot, so the caveat that the two age ranges rest on different foundations moves into the
  subtitle rather than being dropped -- see MOBILE_SUBTITLE_TAIL. There is no room for the
  diagram in a 214px-wide panel, so mobile keeps the legend.

Stacking the panels in the portrait frame was tried and rejected: it gives each panel a 2:1
landscape box, about 222px of height for a 165 cm range, which flattens the curves so far that
the adolescent growth spurt stops being visible. Side by side gives each 2.4x the vertical
resolution. That is why the mobile layout carries fewer age ticks -- a 214px-wide panel cannot
hold six.

Replaces the hand-drawn 'Expected Healthy Growth Curves for Boys and Girls' image used on
the human-height topic page and the stunting-definition article.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What
this step fixes is the structure: which text slots exist, in what order, and which share a row.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath
from matplotlib.ticker import FuncFormatter
from owid.catalog import Table

from etl.helpers import PathFinder

# Use non-path text so SVGs stay editable in Figma
matplotlib.rcParams["svg.fonttype"] = "none"
# Set deterministic hash for reproducible SVG output
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)

# One panel per sex. Colors are seaborn "deep" positions rather than raw hexes, so the
# chart shifts with the shared palette instead of pinning its own.
PANEL_COLOR_INDEX = {"Boys": 1, "Girls": 0}

# Color for reference lines and their labels.
REFERENCE_LINE_COLOR = "#6c7a89"

# Neutral grey for anything that explains the chart rather than carrying data -- the encoding
# diagram's bands and median, and the legend's swatches. Grey is what marks them as a key.
DIAGRAM_COLOR = "#666666"

# Nested percentile bands, drawn widest first, as (lower column, upper column, how far the fill
# is blended towards white, label). Each band is a flat tint rather than a translucent fill: an
# alpha fill has to composite onto something, and the canvas is deliberately transparent so the
# Figma template supplies the background, which left the fan depending on whatever sat behind the
# SVG. A precomputed tint renders the same on any backdrop and gives Figma one flat fill per band.
BANDS = [
    ("height_percentile_0_1", "height_percentile_99_9", 0.90, "Almost all children (999 in 1,000)"),
    ("height_percentile_10", "height_percentile_90", 0.74, "8 in 10 children"),
]

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
# .../color/ColorConstants.ts. Grapher dashes its gridlines rather than drawing them solid,
# labels axes in bold, and draws no axis line -- the gridlines carry the reading.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
GRID_LINEWIDTH = 1.0
TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"

TITLE = "Expected height of boys and girls, from birth to age 19"

# Credited as the author of the visualization on the license line, mirroring the slot the
# static-chart templates leave for it.
AUTHOR = "Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

# The two layouts, taken from the static-chart template frames. All geometry is in the
# templates' own pixel units, measured from the top-left as Figma reports them, and converted
# to figure fractions below; the figure is sized at 100 template px per inch so the saved
# image has the template's exact proportions. `full_footer` is what separates the desktop
# templates (Note and tagline present) from the mobile ones (neither).
#
# Row positions come from "Static Chart Template_Horizontal" (850x638) and "Static Chart
# Template_Mobile (example 2)" (540x824); the tall mobile frame is the one that gives two
# side-by-side panels enough height to read. Font sizes are derived from each slot's height in the template (a template px is
# 0.72pt, and a line of text occupies about 1.8x its point size).
LAYOUTS = {
    "height_for_age": {
        "size": (850, 638),
        "margin": 16,
        "title_y": 16,
        "chart_bottom_y": 556,
        "source_y": 589,
        "footer_y": 609,
        "nrows": 1,
        "ncols": 2,
        "full_footer": True,
        "age_ticks": [0, 2, 5, 10, 15, 19],
        "diagram": "panel",
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 7.75,
        # Space reserved inside the chart area for the y tick labels, and below the plot for
        # the x tick labels plus the bold "Age in years" label.
        "y_label_space": 58,
        "x_label_space": 52,
    },
    "height_for_age_mobile": {
        "size": (540, 824),
        "margin": 16,
        "title_y": 16,
        "chart_bottom_y": 792,
        "source_y": 792,
        "footer_y": 792,
        "nrows": 1,
        "ncols": 2,
        "full_footer": False,
        "age_ticks": [0, 5, 10, 15, 19],
        "diagram": "header",
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 8.75,
        "y_label_space": 52,
        "x_label_space": 52,
    },
}

# Appended to the mobile subtitle. The mobile template has no Note slot, and this caveat is
# about what the chart claims rather than about a visual artifact, so it cannot simply be
# dropped -- without it the older half of the age range reads as an optimal-growth standard.
MOBILE_SUBTITLE_TAIL = (
    "Up to age 5 these are standards, showing how children grow in good conditions; from age 5 they are a "
    "reference, showing how an earlier sample did grow."
)

# A template pixel in points: the figure is 100 template px per inch and there are 72 points
# to the inch, so one pixel is 0.72pt. Used to convert the templates' geometry for text
# measurement, which matplotlib does in points.
POINTS_PER_PIXEL = 0.72

# Template pixels per inch. The figure is sized so that one template pixel is one hundredth
# of an inch, which keeps the saved image at the template's proportions.
PIXELS_PER_INCH = 100

# Gap between the title block and the subtitle, in template pixels. Calibrated so that a
# two-line title puts the subtitle at the templates' own y=80.
TITLE_SUBTITLE_GAP = 6

# Vertical rhythm below the subtitle, in multiples of a text line.
SUBTITLE_GAP = 0.15
DIAGRAM_CHART_GAP = 0.8

# Height reserved for the encoding diagram when it sits in the header rather than inside a panel,
# in template pixels: the slab, plus the row of stunting text below it and the leader reaching it.
HEADER_DIAGRAM_HEIGHT = 92


def run() -> None:
    """Load data, render and save both versions of the chart."""
    tb = load_growth_reference()
    paths.log.info(f"Loaded {len(tb)} rows covering ages {tb['age_years'].min():.1f}-{tb['age_years'].max():.1f}")

    source_citation = build_source_citation(tb)
    paths.log.info(f"Source citation: {source_citation}")

    breaks = find_discontinuities(tb)
    paths.log.info(f"Steps down in the median at ages: {[round(age, 2) for age in breaks]}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(tb, source_citation, breaks, layout)
        # No bbox_inches="tight" on either: cropping to the drawn content would change the frame,
        # and the point is to hand Figma an image at the template's exact proportions.
        #
        # The two formats want opposite things from the canvas, so they are saved separately. The
        # PNG stays opaque, because it is the copy a human reviews and a transparent one is
        # unreadable against a dark editor background. The SVG is saved transparent, because it
        # goes into a Figma template that supplies its own background -- and matplotlib's white
        # figure patch is its own SVG group, so it would sit over that background and would not be
        # uncovered by deleting the text.
        paths.export_fig(fig, short_name, ["png"], dpi=300)
        paths.export_fig(fig, short_name, ["svg"], transparent=True)
        plt.close(fig)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_growth_reference() -> Table:
    """Load the spliced WHO height-for-age reference from garden."""
    ds = paths.load_dataset("height_for_age")
    return ds.read("height_for_age")


def build_source_citation(tb: Table) -> str:
    """List the data products behind the chart, from the origins on the median indicator.

    Returned without a label, so the caller supplies the template's "Data source:" slot name.
    """
    seen = set()
    parts = []
    for origin in tb[MEDIAN_COLUMN].metadata.origins:
        # Origin titles carry the indicator after a colon; the product name is enough here.
        product = origin.title.split(":")[0].strip()
        year = origin.date_published.split("-")[0] if origin.date_published else ""
        key = (product, year)
        if key not in seen:
            seen.add(key)
            parts.append(f"{product} ({year})")
    return "; ".join(parts)


def find_crossover(tb: Table) -> tuple[float, float]:
    """Return the age range, in years, over which girls are taller than boys on average.

    The subtitle states this as one span, so check that it really is a single contiguous
    stretch. Two separate windows would otherwise be reported as one wide range that
    includes ages where boys are in fact taller.
    """
    medians = tb.pivot(index="age_years", columns="sex", values=MEDIAN_COLUMN).sort_index()
    taller = np.flatnonzero((medians["Girls"] > medians["Boys"]).to_numpy())
    assert len(taller) > 0, "Girls are never taller than boys, so the crossover sentence does not apply."
    assert (np.diff(taller) == 1).all(), (
        "Girls are taller than boys over more than one separate age range, which the subtitle "
        "would collapse into a single span."
    )
    ages = medians.index.to_numpy()
    return float(ages[taller[0]]), float(ages[taller[-1]])


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
    left: float = 0.50,
    right: float = 0.68,
    middle: float = 0.24,
    outer_half: float = 0.105,
    label_gap: float = 0.03,
) -> None:
    """Draw a miniature cross-section of the encoding, with each part named beside it.

    The chart it replaces explained itself this way rather than with a legend, and it reads better: a
    swatch in a legend asks the reader to carry a color across the frame to a shape, while a small
    exemplar of the real thing shows the band-within-a-band directly, in the order it appears. It
    also gives the stunting line somewhere to be named other than on top of the plot.

    Two conventions matter here:

    - The bands get square brackets spanning their full height, not a tick at one edge. A band is a
      range, and a tick pointing at its boundary invites the reader to take that boundary as the
      thing being named.
    - The median gets no leader at all: its label sits on the line, inside the slab. The stunting
      label does, because it sits below the slab -- which is what lets it be one line rather than the
      two or three that the space beside the slab forces.

    Everything is grey rather than the panel's color, so the block reads as a key rather than as a
    third sex; the tints and line styles still match the chart exactly.

    Geometry is in axes fractions of whatever `ax` it is given, so the same drawing serves both
    layouts: inside the first panel on desktop, and in its own axes across the header on mobile,
    where a 217px-wide panel cannot hold labels that are 89px and 105px wide.

    On desktop it lives in the empty triangle below the growth curve -- the widest clear space either
    panel has. Two consequences of that shape:

    - The brackets sit on opposite sides, because both bands share the same midpoint. Nested on one
      side, either their labels collide or one label has to cross the other bracket, and the panel is
      too narrow for the second label to clear it.
    - The stunting label sits *below* the slab, centred, with a short leader dropping from the
      dotted line. Beside the slab it would only have the 145px the triangle leaves clear at that
      height, forcing it onto two or three rows; below the slab the triangle is wide enough for one.
    """
    # The inner band is the middle 80%, the outer the middle 99.8%, so it is about 0.42 as tall.
    inner_half = outer_half * 0.42
    # Where -2 SD falls inside the schematic: the outer band's edge is the 99.9th percentile, at
    # about z = 3.09, so 2 SD sits at 2/3.09 of the half-width. Keeping that ratio right is what
    # makes the diagram show the true nesting -- the stunting line is inside the outer band, below
    # the inner one, rather than at the bottom edge.
    minus_2sd = middle - outer_half * 2 / 3.09
    for lower, upper, weight, name in (
        (middle - outer_half, middle + outer_half, 0.90, "outer-band"),
        (middle - inner_half, middle + inner_half, 0.74, "inner-band"),
    ):
        ax.fill_between(
            [left, right],
            [lower] * 2,
            [upper] * 2,
            facecolor=tint(DIAGRAM_COLOR, weight),
            linewidth=0,
            transform=ax.transAxes,
            zorder=7,
            gid=f"diagram__{name}",
        )
    ax.plot(
        [left, right],
        [middle] * 2,
        color=DIAGRAM_COLOR,
        linewidth=2.0,
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__median",
    )
    ax.plot(
        [left, right],
        [minus_2sd] * 2,
        color=REFERENCE_LINE_COLOR,
        linestyle=":",
        linewidth=0.8,
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__stunting-threshold",
    )

    # Square brackets embracing each band, with the label at the bracket's own midpoint. `cap` is
    # how far the end caps reach towards the slab; `side` is which way the label runs. The longer
    # label goes on the left, where there is room for it; the right side runs out of panel.
    for half, bracket_x, cap, side, name, text in (
        (outer_half, left - 0.015, 0.015, "right", "almost-all", "Almost all children"),
        (inner_half, right + 0.015, -0.015, "left", "8-in-10", "8 in 10 children"),
    ):
        ax.plot(
            [bracket_x + cap, bracket_x, bracket_x, bracket_x + cap],
            [middle - half, middle - half, middle + half, middle + half],
            color=MUTED_COLOR,
            linewidth=0.7,
            solid_capstyle="butt",
            transform=ax.transAxes,
            zorder=8,
            gid=f"diagram__bracket-{name}",
        )
        ax.text(
            bracket_x - cap,
            middle,
            text,
            transform=ax.transAxes,
            fontsize=fontsize,
            color=TEXT_COLOR,
            ha=side,
            va="center",
            zorder=8,
            gid=f"diagram__label-{name}",
        )

    # The median needs no leader: its label sits on the line, inside the slab.
    ax.text(
        left + 0.012,
        middle + 0.006,
        "Median",
        transform=ax.transAxes,
        fontsize=fontsize,
        color=TEXT_COLOR,
        ha="left",
        va="bottom",
        zorder=8,
        gid="diagram__label-median",
    )

    # The stunting label sits below the slab, where there is room for one line, with a leader
    # dropping from the middle of the dotted line to it.
    leader_x = (left + right) / 2
    label_y = middle - outer_half - label_gap
    ax.plot(
        [leader_x] * 2,
        [minus_2sd, label_y],
        color=MUTED_COLOR,
        linewidth=0.7,
        solid_capstyle="butt",
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__leader-stunted",
    )
    ax.text(
        leader_x,
        label_y,
        "Stunted: too short for their age",
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


def build_subtitle(tb: Table, layout: dict) -> str:
    """Compose the subtitle, folding in the standards-vs-reference caveat on mobile."""
    crossover_start, crossover_end = find_crossover(tb)
    text = (
        "Read it like this: the line is the height at which half of children of that age are taller and half are "
        "shorter. The darker band covers the middle 8 in 10 children, and the lighter band almost all of them — "
        "999 in every 1,000. Girls are taller than boys, on average, between the ages of about "
        f"{crossover_start:.0f} and {crossover_end:.0f}."
    )
    if not layout["full_footer"]:
        text = f"{text} {MOBILE_SUBTITLE_TAIL}"
    return wrap_to_content_width(text, layout, layout["body_fontsize"])


def build_note(breaks: list[float], layout: dict) -> str:
    """Compose the Note row: the two source discontinuities and what each product is."""
    text = (
        f"Note: The curves step down slightly at age {breaks[0]:.0f}, where height starts being measured standing "
        f"up rather than lying down, and at age {breaks[1]:.0f}, where WHO's standards for under-fives give way to "
        "its reference for older children. The under-fives standards show how children grow in good conditions; the "
        "reference for older children describes how an earlier sample did grow."
    )
    return wrap_to_content_width(text, layout, layout["footer_fontsize"])


def create_visualization(tb: Table, source_citation: str, breaks: list[float], layout: dict) -> plt.Figure:
    """Build one version of the two-panel growth-curve chart.

    Layout notes:
    - One panel per sex, sharing a y-axis, nested percentile bands deepening where they overlap
    - Median drawn solid on top; the other sex's median repeated as a faint dashed line
    - The -2 SD stunting threshold is labeled on the line rather than in the legend
    - No spines; light horizontal gridlines carry the height reading
    - Axis limits, ticks and footnote ages all derived from the data
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    body_fontsize = layout["body_fontsize"]
    age_max = float(tb["age_years"].max())
    height_max = float(tb["height_percentile_99_9"].max())

    medians = {}
    for sex, tb_sex in tb.groupby("sex", observed=True):
        tb_sex = tb_sex.sort_values("age_days")
        medians[sex] = (tb_sex["age_years"].to_numpy(), tb_sex[MEDIAN_COLUMN].to_numpy())

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
        for spine in ax.spines.values():
            spine.set_visible(False)

        # --- nested percentile bands, widest first ---
        # gid becomes the SVG element id, so Figma shows named layers instead of "Path 41".
        # Mirrors grapher, which stamps its own SVG nodes with makeFigmaId().
        slug = sex.lower()
        for lower, upper, weight, label in BANDS:
            ax.fill_between(
                age,
                tb_sex[lower].to_numpy(),
                tb_sex[upper].to_numpy(),
                facecolor=tint(color, weight),
                linewidth=0,
                zorder=2,
                gid=f"{slug}__{label.lower().replace(' ', '-').replace('%', '')}",
            )

        # --- the other sex's median, so the crossover stays visible in both panels ---
        # Drawn in that sex's own color rather than grey, which saves labelling it: the panel titles
        # carry the same two colors, so the dashed blue line in the Boys panel reads as the girls'.
        for other_sex, (other_age, other_median) in medians.items():
            if other_sex == sex:
                continue
            ax.plot(
                other_age,
                other_median,
                color=palette[PANEL_COLOR_INDEX[other_sex]],
                linewidth=1.2,
                linestyle=(0, (4, 3)),
                zorder=6,
                gid=f"{slug}__median-other-sex",
            )

        # --- stunting threshold; named in the encoding diagram where there is one, and otherwise
        # under the line around mid-childhood, where the panel is empty (at the right-hand end the
        # bands and medians all converge) ---
        stunting = tb_sex[STUNTING_COLUMN].to_numpy()
        ax.plot(
            age,
            stunting,
            color=REFERENCE_LINE_COLOR,
            linestyle=":",
            linewidth=0.8,
            zorder=4,
            gid=f"{slug}__stunting-threshold",
        )
        # --- percentile lines on top of the bands ---
        for column, line_width in QUANTILE_LINES:
            values = tb_sex[column].to_numpy()
            ax.plot(age, values, color=color, linewidth=line_width, zorder=5, gid=f"{slug}__{column[-3:]}")

        if layout["diagram"] == "panel" and ax is axes[0]:
            draw_encoding_diagram(ax, body_fontsize - 2.5)

        # --- panel title, in the panel's own color ---
        ax.text(
            0.2, height_max, sex, fontsize=body_fontsize + 6, color=color, ha="left", va="top", gid=f"{slug}__label"
        )

        ax.set_xlim(-0.45, age_max + 0.45)
        ax.set_ylim(38, height_max + 4)
        ticks = layout["age_ticks"]
        ax.set_xticks(ticks)
        ax.set_xticklabels(["Birth" if tick == 0 else str(tick) for tick in ticks])
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f} cm"))
        ax.tick_params(axis="both", length=0, labelsize=body_fontsize, labelcolor=TEXT_COLOR)
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

    subtitle = build_subtitle(tb, layout)
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
    chart_top_px = subtitle_bottom_px + DIAGRAM_CHART_GAP * px(body_fontsize)

    if layout["diagram"] == "header":
        # A 217px-wide mobile panel cannot hold the diagram -- the two band labels alone are wider
        # than the panel -- but this row is the full 508px content width, which fits it with room to
        # spare. Keeping the same explanatory device in both versions matters more than the vertical
        # cost, since the pair gets published together.
        diagram_axes = fig.add_axes(
            (
                fx(margin_px),
                fy(subtitle_bottom_px + HEADER_DIAGRAM_HEIGHT),
                1 - 2 * fx(margin_px),
                HEADER_DIAGRAM_HEIGHT / height_px,
            )
        )
        diagram_axes.set_axis_off()
        # No background patch: the SVG is saved transparent so the Figma template shows through.
        diagram_axes.patch.set_visible(False)
        draw_encoding_diagram(
            diagram_axes,
            body_fontsize - 2.5,
            left=0.386,
            right=0.642,
            middle=0.675,
            outer_half=0.315,
            label_gap=0.087,
        )
        chart_top_px = subtitle_bottom_px + HEADER_DIAGRAM_HEIGHT

    # --- footer, in the slots the static-chart templates define ---
    # Desktop: Note -> Data source -> tagline and license sharing one row, left and right.
    # Mobile: Data source -> license only, which is all that template has room for.
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
        chart_bottom_px = layout["chart_bottom_y"]

    fig.text(
        fx(margin_px),
        fy(layout["source_y"]),
        f"Data source: {source_citation}",
        ha="left",
        va="top",
        fontsize=footer_fontsize,
        color="#888888",
        gid="data-source",
    )

    # Desktop puts the tagline on its own row with the license right-aligned beside it; mobile
    # has no tagline, so there the license shares the Data source row.
    if layout["full_footer"]:
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
        license = f"Licensed under CC-BY by the author {AUTHOR}"
    else:
        license = "CC BY"
    fig.text(
        fx(width_px - margin_px),
        fy(layout["footer_y"]),
        license,
        ha="right",
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

    # Drop clipping everywhere so labels that sit outside the axes survive into the SVG
    # and Figma receives whole shapes rather than cropped ones.
    for artist in fig.findobj():
        artist.set_clip_on(False)

    return fig
