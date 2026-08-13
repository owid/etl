"""Recreate the 'Expected height of boys and girls' growth-curve chart.

Each panel shows nested percentile bands from the WHO growth reference standards, the median, and
the -2 SD stunting threshold.

Neither panel repeats the other sex's median. The two medians run within a few millimetres of each
other from birth to about age 9, so a second line traces the panel's own median for two thirds of the
range -- the same doubling that splitting the sexes into panels was meant to remove. The subtitle
states the crossover instead, and it can be read off the two panels at a shared gridline.

An encoding diagram names each part of the chart -- see `draw_encoding_diagram`. There is no legend
in either version.

Two versions are emitted, following the static-chart templates:

- desktop, 850x638: panels side by side, diagram inside the Boys panel, footer carrying Note, Data
  source, the OurWorldinData.org tagline and the license line.
- mobile, 540x824: panels side by side in the portrait frame, diagram in the header, footer reduced
  to Data source plus the license, which is all that template has room for. It has no Note slot, so
  the caveat that the two age ranges rest on different foundations sits in the subtitle instead --
  see MOBILE_SUBTITLE. Its panels are 217px wide, which is why it carries five age ticks to the
  desktop's six, and why the diagram cannot sit inside a panel.

Both layouts put their panels side by side rather than stacked. Stacked in the portrait frame each
panel is a 2:1 landscape box, about 222px of height for a 165 cm range, and the adolescent growth
spurt is not visible in it; side by side gives each panel 2.4x the vertical resolution.

Replaces the hand-drawn 'Expected Healthy Growth Curves for Boys and Girls' image used on the
human-height topic page and the stunting-definition article.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this step
fixes is the structure: which text slots exist, in what order, and which share a row.

Figma
-----
Page `20260812 Expected height of boys and girls, from birth to age 19 (Pablo A)` in `Charts (2026)`
(file key `s6Sv60bakebRRW2TxsMQbF`), frames `expected-height-boys-girls` and
`expected-height-boys-girls-mobile`, each beside a reference copy of this step's own render.

The frames take their prose, fonts, logo and background from the static-chart templates and only the
plot and the encoding diagram from here. To carry a data update through, re-run this step and replace
the `chart` group in each frame; these are the steps done by hand around it:

1. Rescale the import by 100/96 -- matplotlib declares points and Figma imports at 96px per inch,
   while this figure is built at 100 template px per inch.
2. Delete the import's wrapper frame (it carries a white fill) and its `patch_1`, `title`,
   `subtitle`, `note`, `data-source`, `tagline` and `license` groups. The template's own slots carry
   those strings, so they are duplicated otherwise.
3. Restyle the in-plot labels to Lato at 22/14/12, then re-anchor each on its mark: y ticks by their
   right edge, x ticks by their centre except the last by its right edge, `Almost all children` by
   its right edge, the median and `8 in 10 children` by their left, the stunting label by its centre.
4. Centre the group in the band between the header's bottom and the footer's first row.
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

# Neutral grey for the encoding diagram's bands and median. Grey is what marks the diagram as a
# key rather than as data.
DIAGRAM_COLOR = "#666666"

# Nested percentile bands, drawn widest first, as (lower column, upper column, how far the fill is
# blended towards white, layer name). Each band is a flat tint, not a translucent fill: an alpha fill
# composites onto whatever is behind it, and the SVG is saved transparent for the Figma template to
# supply the background. A tint renders the same on any backdrop and gives Figma one flat fill each.
BANDS = [
    ("height_percentile_0_1", "height_percentile_99_9", 0.90, "almost-all-children"),
    ("height_percentile_10", "height_percentile_90", 0.74, "8-in-10-children"),
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
# side-by-side panels enough height to read. Font sizes are derived from each slot's height in the
# template: a template px is 0.72pt, and a line of text occupies about 1.8x its point size.
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
        "y_label_space": 58,
        "x_label_space": 52,
    },
}

# The mobile template has no Note slot, and the standards-vs-reference caveat is about what the
# chart claims rather than about a visual artifact, so it rides in the subtitle instead -- without it
# the older half of the age range reads as an optimal-growth standard. Its two-line slot at the
# template's type size is about 114 characters, which is the whole budget for both sentences, so
# mobile states the crossover more briefly than desktop does.
MOBILE_SUBTITLE = (
    "Girls are taller than boys between ages {crossover_start:.0f} and {crossover_end:.0f}. "
    "Ages 0–{splice:.0f} show healthy growth; {splice:.0f}–{age_max:.0f}, how an earlier sample grew."
)

# A template pixel in points: the figure is 100 template px per inch and there are 72 points
# to the inch, so one pixel is 0.72pt. Used to convert the templates' geometry for text
# measurement, which matplotlib does in points.
POINTS_PER_PIXEL = 0.72

# Template pixels per inch. The figure is sized so that one template pixel is one hundredth
# of an inch, which keeps the saved image at the template's proportions.
PIXELS_PER_INCH = 100

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
    """Cite the producers behind the chart, from the origins on the median indicator.

    Follows grapher's own footer convention of `producer (year)`, so the two WHO products cite as one
    producer carrying both release years rather than as two separate data products.

    Returned without a label, so the caller supplies the template's "Data source:" slot name.
    """
    years: dict[str, list[str]] = {}
    for origin in tb[MEDIAN_COLUMN].metadata.origins:
        year = origin.date_published.split("-")[0] if origin.date_published else ""
        seen = years.setdefault(origin.producer, [])
        if year and year not in seen:
            seen.append(year)
    return "; ".join(f"{producer} ({'; '.join(sorted(ys))})" for producer, ys in years.items())


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
    left: float = 0.34,
    right: float = 0.60,
    middle: float = 0.24,
    outer_half: float = 0.105,
    rise: float = 0.12,
    label_gap: float = 0.03,
) -> None:
    """Draw a miniature growth curve carrying the encoding, with each part named beside it.

    Shaped like the chart it explains rather than as a flat block: a rising median with the two bands
    widening around it and the -2 SD line running below, so the reader recognises the marks by their
    shape and not only by their colour. It is a schematic, not a data slice -- the bands are drawn
    wider than the real ones so the four labelled marks separate at the curve's right-hand end, where
    the labels attach.

    Grey, so it reads as a key rather than as a third sex; the tints and line styles are the chart's.

    Where each label goes, and why:

    - Each band gets a square bracket at the curve's right end, spanning the band's full height there.
      A band is a range, and a tick at its boundary would read as naming the boundary. The brackets
      nest outwards and their labels attach to the top cap, which is what keeps the inner label from
      having to cross the outer bracket -- the panel is too narrow for it to clear.
    - The median's label sits at the line's left end, and the stunting label below the curve with a
      short leader. A leader is drawn only where a label cannot sit against the thing it names.

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
    # The inner band is the middle 80%, the outer the middle 99.8%, so it is about 0.42 as tall.
    inner = outer * 0.42
    # -2 SD inside the schematic: the outer band's edge is the 99.9th percentile, at about z = 3.09,
    # so 2 SD sits at 2/3.09 of the half-width. That ratio is what puts the stunting line inside the
    # outer band and below the inner one, as it is in the data.
    minus_2sd = median - outer * 2 / 3.09

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
        color=REFERENCE_LINE_COLOR,
        linestyle=":",
        linewidth=0.8,
        transform=ax.transAxes,
        zorder=8,
        gid="diagram__stunting-threshold",
    )

    # Nested brackets at the curve's right end: the big one around the whole band, the small one
    # around the middle 80%. Each label sits immediately right of its own bracket, with no leader --
    # "Almost all children" level with the big bracket's top arm, "8 in 10 children" level with the
    # small bracket's middle. The big bracket is the *nearer* of the two, which is what lets both
    # labels sit against their own bracket: the top label then clears the small bracket entirely, and
    # the middle label starts to the right of both.
    for half_end, bracket_x, label_x, at_top, name, text in (
        (outer[-1], right + 0.020, right + 0.035, True, "almost-all", "Almost all children"),
        (inner[-1], right + 0.050, right + 0.065, False, "8-in-10", "8 in 10 children"),
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

    # The stunting label sits below the curve, where there is room for one line, with a leader
    # dropping from the dotted line at the curve's midpoint.
    mid = len(t) // 2
    label_y = float((median - outer).min()) - label_gap
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


def build_subtitle(tb: Table, breaks: list[float], layout: dict) -> str:
    """Compose the subtitle, folding in the standards-vs-reference caveat on mobile.

    It carries what the shapes say together, not how to read them: the encoding diagram names each
    band where it is drawn, and repeating that here would cost the mobile template's whole slot.
    """
    crossover_start, crossover_end = find_crossover(tb)
    if layout["full_footer"]:
        text = (
            "Girls are taller than boys, on average, between the ages of about "
            f"{crossover_start:.0f} and {crossover_end:.0f}."
        )
    else:
        text = MOBILE_SUBTITLE.format(
            crossover_start=crossover_start,
            crossover_end=crossover_end,
            splice=breaks[1],
            age_max=float(tb["age_years"].max()),
        )
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
    - One panel per sex, sharing a y-axis, each with two nested percentile bands as flat tints
    - Median drawn solid on top of the bands
    - The median, the -2 SD stunting threshold and both bands are named in the encoding diagram
    - No spines; light horizontal gridlines carry the height reading
    - Axis limits, ticks and footnote ages all derived from the data
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    body_fontsize = layout["body_fontsize"]
    age_max = float(tb["age_years"].max())
    height_max = float(tb["height_percentile_99_9"].max())

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
            draw_encoding_diagram(ax, body_fontsize - DIAGRAM_FONTSIZE_DROP)

        # --- panel title, in the panel's own color ---
        ax.text(
            0.2, height_max, sex, fontsize=body_fontsize + 6, color=color, ha="left", va="top", gid=f"{slug}__label"
        )

        ax.set_xlim(-0.45, age_max + 0.45)
        ax.set_ylim(38, height_max + 4)
        ticks = layout["age_ticks"]
        ax.set_xticks(ticks)
        labels = ax.set_xticklabels(["Birth" if tick == 0 else str(tick) for tick in ticks])
        # The last tick label is right-anchored, as grapher anchors its outermost labels inwards:
        # centred, it crosses the frame's side margin, which is ink the templates keep clear. The
        # first label stays centred -- it overhangs only into the space reserved for the y labels,
        # and anchoring it left pushes it into the next tick.
        labels[-1].set_horizontalalignment("right")
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

    subtitle = build_subtitle(tb, breaks, layout)
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
        chart_top_px = diagram_top_px + HEADER_DIAGRAM_HEIGHT

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
        chart_bottom_px = layout["chart_bottom_y"] - (CHART_AREA_INSET if layout["diagram"] == "header" else 0)

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
