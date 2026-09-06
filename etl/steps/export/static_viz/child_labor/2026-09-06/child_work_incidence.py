"""Long-run chart of how common child work was, from the first census estimates to today.

Nine series, drawn from a dozen unrelated studies. Seven are national estimates of the share
of children recorded as working, each meeting a common standard — a census or administrative
source, roughly ages 10 to 15, any kind of work rather than one sector. Four of them are long
enough to draw as lines (Italy, England and Wales, the United States, Colombia) and three are
single observations (the Netherlands in 1849, Canada in 1891, France in 1896).

The remaining two are world estimates, drawn in grey to set them apart from the national
census figures, because neither is measured the same way:

- 1950-1995, ages 10-14, the ILO's own participation rates as reported by Basu (1999). The
  age band matches the country series, but these are modelled estimates rather than census
  counts, and Toniolo and Vecchi show they are far too high for at least one country on this
  chart: the ILO puts Italy at 10.9% in 1960 where the census-based series here reads 3.6%.
  That is a threefold overstatement, and it is why the note names the caveat rather than
  leaving the reader to assume the grey line is of a piece with the coloured ones.
- 2000-2024, ages 5-17, the ILO-UNICEF Global Estimates of Child Labour. A different age band
  and a narrower concept — "child labour" as the ILO defines it counts only work damaging to
  the child, where the historical censuses counted any recorded occupation.

Every series is labelled in place with its country, age band and sex, so no legend is needed
and the colours only have to distinguish rather than encode — which is what makes nine series
survivable on one frame. It also makes the mixed age bands and sexes legible at a glance,
which a legend keyed on country alone would hide.

The x range starts at 1825 rather than at the first observation (the Netherlands, 1849) so the
leftmost label has room, and ends at 2025 on a whole tick.

One frame only, at 850x638. A 540x540 mobile version was considered and dropped: nine labelled
series over two centuries does not survive the square frame legibly.

Colors, fonts, the logo and the background are deliberately not set here; those are applied in
Figma. What this step fixes is the data, the structure (which text slots exist, in what order)
and the proportions.

One deviation, measured and accepted rather than fixed: in the PNG the tagline and the license
share a row and **overlap by about 4px**. That is the typeface, not the layout. The two strings
measure 475 + 347 = 822px against 818px of content width in Liberation Sans, which is what this
step draws, and 464 + 338 = 803px in the Lato the template sets — a 15px clearance in the
deliverable. TEMPLATES.md's lever for a long license row is to drop the words "the authors",
which this already does, and it says never to shorten a name to make one fit. So the frame is
correct and the preview is 2.4% tight; do not "fix" it by shrinking the type or a name.

Figma
-----
Not yet placed. When it is, record here: the file key and page name, each frame's name and the
template node it was cloned from, the import mechanics (`upload_assets` + POST to the returned
`submitUrl`, never `createNodeFromSvg`; bin the wrapper frame; `rescale(100 / 96)`), which text
slots take which constant from this module and where the mixed weights fall, every colour as
its library style name and key, and the audit numbers a later run should expect.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.textpath import TextPath

from etl.helpers import PathFinder
from etl.static_viz import TEMPLATES, apply_svg_rcparams, export_frame, source_citation

paths = PathFinder(__file__)

# Lands in the SVG's `font-family` verbatim, so Figma renders the import in the template's own
# typeface on arrival. Lato is not a font this step can measure in — only one it can ask for.
#
# Liberation Sans is named ahead of the generic fallback in BOTH stacks on purpose. It is the
# metric-compatible Arial substitute, so it sets ~2.4% wider than Lato where matplotlib's DejaVu
# default sets ~15% wider — and 15% is enough that the footer's two rows, which the template fits
# side by side, overlap in the step's own render. Falling through to DejaVu does not break the
# deliverable (Figma re-renders in Lato either way); it breaks the PNG a human reviews.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "Liberation Sans", "sans-serif"]
# What this step measures and draws with. Same order after Lato, so measuring and drawing resolve
# to one face and the allowance below is chosen for the face actually used.
MEASURED_FONT_STACK = ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]

matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
apply_svg_rcparams()

# Drop the per-face misses for faces deliberately listed as alternatives, and nothing else —
# blanket silence would also take "Falling back to DejaVu Sans", which says a whole stack failed
# and every measurement has just moved ~15% against what gets drawn.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)

# No filter can protect the invariant the measurements rest on, so assert it.
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, (
    f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name} — every width in this "
    "step was measured in a face it will not be drawn in"
)

TEMPLATE = "horizontal"
AUTHOR = "Bertha Rohenkohl and Esteban Ortiz-Ospina"
# Copied verbatim from the template rather than paraphrased.
TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

TITLE = "Child work was once common, and has declined sharply"
SUBTITLE = (
    "Share of children recorded as working, from population censuses and other national records. "
    "Each series is labeled with the country, the age group and the sex it covers."
)
NOTE = (
    "These estimates come from a dozen different studies and are not strictly comparable: the age group, the sex "
    "covered and what counts as work all vary. Census counts also understate children's work, particularly unpaid "
    "work at home. The two world series in grey are ILO estimates rather than census counts; Toniolo and Vecchi find "
    "the 1950-1995 series far too high for Italy."
)

# Template slot sizes, in template pixels (TEMPLATES.md). The figure is built at 100 template px
# per inch, so a point size is `px * 0.72`.
PX_TO_PT = 0.72
TITLE_PX, SUBTITLE_PX, NOTE_PX, SOURCE_PX, FOOTER_PX = 25, 16, 12, 12, 11
TICK_PX, SERIES_LABEL_PX = 12, 13

# Header and footer rhythm (TEMPLATES.md). The header grows down from the title and the footer
# grows up from the frame's bottom margin, so both edges of the chart band are derived from the
# line counts rather than pinned to the template's placeholder positions.
ORIGIN_Y = 16
TITLE_LINE_PX, SUBTITLE_LINE_PX, NOTE_LINE_PX = 29, 19, 14
TITLE_SUBTITLE_GAP_PX = 6
NOTE_INK_BOTTOM_PX = 587  # Horizontal template
FOOTER_ROW_GAP_PX = 4
BAND_INSET_PX = 14
MARGIN_PX = 16

# Slot widths. The title is narrower than the content box because the logo sits beside it.
CONTENT_WIDTH_PX = TEMPLATES[TEMPLATE].width_px - 2 * MARGIN_PX
TITLE_WIDTH_PX = 737.84

# The step measures whatever face `findfont` resolves and the template sets Playfair Display and
# Lato, so every line count it predicts is an estimate. Wrapping early is not the safe choice — the
# footer rows are sized so the template just fits them — so give each slot the allowance it actually
# has, and keep the two directions separate so neither is applied backwards.
#
# The allowances are PER INSTALLED FONT (TEMPLATES.md's measured table): Lato sets 2.4% narrower
# than Arial but ~15% narrower than DejaVu Sans. Applying the Arial number on a machine without
# Arial wraps ~13% early — enough to break the footer onto a second line the template does not
# have, and to reserve chart space that the frame then does not need.
_ALLOWANCES = {  # drawn face -> (Lato / drawn, Playfair Display SemiBold / drawn)
    "arial": (0.976, 1.032),
    "dejavu": (0.850, 0.904),
}
# Liberation Sans is the metric-compatible Arial substitute, so it takes Arial's numbers.
_FACE_NAME = Path(_DRAWN_FACE).name.lower()
_FACE_KEY = "arial" if any(f in _FACE_NAME for f in ("arial", "liberation", "helvetica")) else "dejavu"
LATO_OVER_MEASURED, PLAYFAIR_OVER_MEASURED = _ALLOWANCES[_FACE_KEY]

# Grapher's axis treatment, so the static chart reads like our interactive ones.
GRID_COLOR = "#ddd"
GRID_DASHES = (0, (4, 4))
GRID_LINEWIDTH = 1.0
AXIS_COLOR = "#999"
TEXT_COLOR = "#5b5b5b"
TICK_LENGTH_PX = 5
Y_LABEL_PAD_PX = 6

X_TICKS = list(range(1825, 2026, 25))
Y_TICKS = list(range(0, 71, 10))

WORLD_COLOR = "#6e7581"  # the palette's own gray, for the two non-census world series

# One entry per series, in the order they are drawn. `palette` indexes seaborn's "deep" palette,
# which Figma rebinds to the library's Line and Slope Charts group; `None` means the world gray.
# `label` is the two-line direct label, `anchor` where it hangs (year, share) and `align` how.
SERIES = [
    dict(
        gid="italy",
        country="Italy",
        palette=0,
        label=["Italy", "all children, 10-14"],
        anchor=(1896, 62.5),
        align=("left", "bottom"),
    ),
    dict(
        gid="england-and-wales",
        country="England and Wales",
        palette=1,
        label=["England & Wales", "boys, 10-14"],
        anchor=(1849, 39.5),
        align=("left", "bottom"),
    ),
    dict(
        gid="united-states",
        country="United States",
        palette=2,
        label=["United States", "boys, 10-15"],
        anchor=(1933, 12),
        align=("left", "top"),
    ),
    dict(
        gid="colombia",
        country="Colombia",
        palette=3,
        label=["Colombia", "boys, 12-14"],
        anchor=(1957, 32.5),
        align=("left", "bottom"),
    ),
    dict(
        gid="netherlands",
        country="Netherlands",
        palette=4,
        label=["Netherlands, 1849", "ages 12-15"],
        anchor=(1853, 21.5),
        align=("left", "top"),
    ),
    dict(
        gid="canada",
        country="Canada",
        palette=5,
        label=["Canada, 1891", "boys, 10-14"],
        anchor=(1891, 37),
        align=("center", "bottom"),
        leader=(1891, 26.2),
    ),
    dict(
        gid="france",
        country="France",
        palette=6,
        label=["France, 1896", "ages 10-14"],
        anchor=(1901, 15),
        align=("left", "top"),
        leader=(1897, 19.2),
    ),
    dict(
        gid="world-10-14",
        country="World (ILO, ages 10-14)",
        palette=None,
        label=["World, ages 10-14", "ILO estimates"],
        anchor=(1978, 23),
        align=("left", "bottom"),
        dashes=(0, (4, 2.5)),
    ),
    dict(
        gid="world-5-17",
        country="World (ILO, ages 5-17)",
        palette=None,
        label=["World, ages 5-17", "ILO child labour estimates"],
        anchor=(2025, 6),
        align=("right", "top"),
        dashes=(0, (1.5, 2)),
    ),
]

SERIES_LINEWIDTH = 2.2
WORLD_LINEWIDTH = 1.6
MARKER_SIZE = 4.5
SINGLE_MARKER_SIZE = 7.5


def text_width_px(text: str, size_px: float, *, bold: bool = False) -> float:
    """Width of `text` in template pixels, measured in the face this step draws in."""
    prop = FontProperties(
        family=MEASURED_FONT_STACK, size=size_px * PX_TO_PT, weight="bold" if bold else "normal"
    )
    return TextPath((0, 0), text or " ", prop=prop).get_extents().width / PX_TO_PT


def wrap(text: str, size_px: float, width_px: float, *, allowance: float = 1.0) -> list[str]:
    """Greedily wrap `text` to `width_px`, measured rather than estimated from the font size."""
    budget = width_px / allowance
    lines, current = [], ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and text_width_px(candidate, size_px) > budget:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


def load_series(paths_: PathFinder) -> tuple[dict[str, tuple[np.ndarray, np.ndarray]], list]:
    """Read every series onto one (year, share) footing, and collect the columns to cite."""
    ds_chart = paths_.load_dataset("child_work_incidence")
    ds_ilo = paths_.load_dataset("child_labor_report")
    ds_basu = paths_.load_dataset("child_labor__world_1950_1995__basu__1999")

    tb_chart = ds_chart.read("child_work_incidence")  # the grapher step renames the chart table to the step short name
    tb_ilo = ds_ilo.read("child_labor")
    tb_basu = ds_basu.read("child_labor__world_1950_1995__basu__1999")

    data: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for country, group in tb_chart.dropna(subset="share_comparable").groupby("country", observed=True):
        group = group.sort_values("year")
        data[str(country)] = (group["year"].to_numpy(), group["share_comparable"].to_numpy())

    basu_column = "child_labor__world__ilo_epeap__1950_1995"
    world_10_14 = tb_basu[tb_basu["country"] == "World"].sort_values("year")
    data["World (ILO, ages 10-14)"] = (
        world_10_14["year"].to_numpy(),
        world_10_14[basu_column].to_numpy(),
    )

    world_5_17 = tb_ilo[(tb_ilo["country"] == "World") & (tb_ilo["sex"] == "total") & (tb_ilo["age"] == "5-17")]
    world_5_17 = world_5_17.dropna(subset="share_child_labor").sort_values("year")
    data["World (ILO, ages 5-17)"] = (
        world_5_17["year"].to_numpy(),
        world_5_17["share_child_labor"].to_numpy(),
    )

    cited = [tb_chart["share_comparable"], tb_basu[basu_column], tb_ilo["share_child_labor"]]
    return data, cited


def sanity_check(data: dict[str, tuple[np.ndarray, np.ndarray]]) -> None:
    """Assert the claims the chart makes, not only its schema."""
    expected = {spec["country"] for spec in SERIES}
    assert set(data) == expected, f"Series mismatch: {sorted(set(data) ^ expected)}"

    for country, (years, shares) in data.items():
        assert len(years) > 0, f"{country} has no observations."
        assert np.all(np.diff(years) > 0), f"{country} years are not strictly increasing."
        assert np.all((shares >= 0) & (shares <= 100)), f"{country} has a share outside 0-100%."
        assert years.min() >= X_TICKS[0] and years.max() <= X_TICKS[-1], (
            f"{country} falls outside the drawn x range."
        )
        assert shares.max() <= Y_TICKS[-1], f"{country} exceeds the drawn y range."

    # The title says child work "was once common", which the chart has to show: the earliest
    # national estimates must sit far above the latest ones.
    earliest = max(shares[0] for country, (_, shares) in data.items() if not country.startswith("World"))
    assert earliest > 50, f"No early national estimate above 50% (highest is {earliest:.1f}%)."

    # The note's Italy claim is what justifies drawing the 1950-1995 world series in grey, so it
    # must keep holding against the data rather than being a remembered fact.
    world_years, world_shares = data["World (ILO, ages 10-14)"]
    italy_years, italy_shares = data["Italy"]
    world_1960 = world_shares[world_years == 1960][0]
    italy_1961 = italy_shares[italy_years == 1961][0]
    assert world_1960 > 3 * italy_1961, (
        f"The ILO 10-14 world series ({world_1960:.1f}% in 1960) is no longer far above Italy's "
        f"census estimate ({italy_1961:.1f}% in 1961) — re-check the note."
    )


def build(data: dict[str, tuple[np.ndarray, np.ndarray]], source: str):
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_style("ticks")
    sns.set_palette("deep")
    # seaborn's set_style replaces `font.sans-serif`, so put the emitted stack back.
    matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
    palette = sns.color_palette("deep")

    template = TEMPLATES[TEMPLATE]
    fig = plt.figure(figsize=template.figsize)
    fig.patch.set_facecolor("white")  # legible when the PNG is reviewed on a dark background

    # Wrap the text slots first: the chart band is derived from how many lines they take.
    # Two different wraps, and conflating them is what puts text outside the frame.
    #
    # What the step DRAWS has to fit the step's own frame in the step's own face, so it wraps with
    # no allowance. What the TEMPLATE will take is a different count, because Lato and Playfair set
    # narrower than the face here — and that count is what reserves the chart band. Reserve the
    # larger of the two, so a disagreement leaves a generous gap rather than an overlap.
    title_lines = wrap(TITLE, TITLE_PX, TITLE_WIDTH_PX)
    subtitle_lines = wrap(SUBTITLE, SUBTITLE_PX, CONTENT_WIDTH_PX)
    note_lines = wrap(f"Note: {NOTE}", NOTE_PX, CONTENT_WIDTH_PX)
    title_rows = max(len(title_lines), len(wrap(TITLE, TITLE_PX, TITLE_WIDTH_PX, allowance=PLAYFAIR_OVER_MEASURED)))
    subtitle_rows = max(
        len(subtitle_lines), len(wrap(SUBTITLE, SUBTITLE_PX, CONTENT_WIDTH_PX, allowance=LATO_OVER_MEASURED))
    )
    note_rows = max(len(note_lines), len(wrap(f"Note: {NOTE}", NOTE_PX, CONTENT_WIDTH_PX, allowance=LATO_OVER_MEASURED)))

    subtitle_y = ORIGIN_Y + title_rows * TITLE_LINE_PX + TITLE_SUBTITLE_GAP_PX
    band_top = subtitle_y + subtitle_rows * SUBTITLE_LINE_PX
    band_bottom = NOTE_INK_BOTTOM_PX - note_rows * NOTE_LINE_PX
    assert band_bottom - band_top > 300, "The text slots have left too little room for the chart."

    def px(x: float) -> float:
        return x / template.width_px

    def py(y: float) -> float:
        """Template y (from the top) to figure fraction (from the bottom)."""
        return 1 - y / template.height_px

    chart_top, chart_bottom = band_top + BAND_INSET_PX, band_bottom - BAND_INSET_PX

    # Tick labels are drawn OUTSIDE the axes rectangle, so the plot has to be inset from the band by
    # their own size or they land outside the frame entirely.
    y_label_px = max(text_width_px(f"{tick}%", TICK_PX) for tick in Y_TICKS) + Y_LABEL_PAD_PX
    x_label_px = TICK_PX * 1.4 + TICK_LENGTH_PX + 3
    plot_left = MARGIN_PX + y_label_px
    plot_bottom = chart_bottom - x_label_px
    ax = fig.add_axes(
        (
            px(plot_left),
            py(plot_bottom),
            px(CONTENT_WIDTH_PX - y_label_px),
            (plot_bottom - chart_top) / template.height_px,
        )
    )
    ax.patch.set_visible(False)
    plot_height_px = plot_bottom - chart_top

    # ── Axes, following grapher ────────────────────────────────────────────────
    ax.set_xlim(X_TICKS[0], X_TICKS[-1])
    ax.set_ylim(Y_TICKS[0], Y_TICKS[-1])
    ax.set_xticks(X_TICKS)
    ax.set_yticks(Y_TICKS)
    ax.set_yticklabels([f"{t}%" for t in Y_TICKS])
    ax.set_xticklabels([str(t) for t in X_TICKS])

    ax.yaxis.grid(True, color=GRID_COLOR, linestyle=GRID_DASHES, linewidth=GRID_LINEWIDTH)
    for gridline in ax.yaxis.get_gridlines():
        gridline.set_gid("horizontal-grid-lines")
    # The baseline draws this one solid, as the axis line; a dashed stroke over it breaks it up.
    ax.yaxis.get_gridlines()[0].set_visible(False)
    ax.xaxis.grid(False)

    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS_COLOR)
    ax.spines["bottom"].set_linewidth(1.0)
    ax.spines["bottom"].set_gid("horizontal-axis__line")

    ax.tick_params(
        axis="x",
        length=TICK_LENGTH_PX * PX_TO_PT,
        width=1.0,
        color=AXIS_COLOR,
        labelsize=TICK_PX * PX_TO_PT,
        labelcolor=TEXT_COLOR,
        pad=3,
    )
    ax.tick_params(axis="y", length=0, labelsize=TICK_PX * PX_TO_PT, labelcolor=TEXT_COLOR, pad=4)
    # Grapher anchors the outermost x tick labels inwards so they stay inside the plot.
    ax.get_xticklabels()[0].set_horizontalalignment("left")
    ax.get_xticklabels()[-1].set_horizontalalignment("right")

    # ── Series ─────────────────────────────────────────────────────────────────
    for spec in SERIES:
        years, shares = data[spec["country"]]
        is_world = spec["palette"] is None
        color = WORLD_COLOR if is_world else palette[spec["palette"]]
        single_point = len(years) == 1

        if single_point:
            ax.plot(
                years,
                shares,
                marker="D",
                markersize=SINGLE_MARKER_SIZE,
                linestyle="none",
                color=color,
                gid=f"datapoints__{spec['gid']}",
            )
        else:
            # `dashes=None` is not the same as "solid" to matplotlib, so only pass it when set.
            dash_kwargs = {"dashes": spec["dashes"][1]} if "dashes" in spec else {}
            ax.plot(
                years,
                shares,
                color=color,
                linewidth=WORLD_LINEWIDTH if is_world else SERIES_LINEWIDTH,
                solid_capstyle="round",
                gid=f"line__{spec['gid']}",
                **dash_kwargs,
            )
            ax.plot(
                years,
                shares,
                marker="o",
                markersize=MARKER_SIZE,
                linestyle="none",
                color=color,
                gid=f"datapoints__{spec['gid']}",
            )

        # A leader only where the label cannot sit next to the mark it names.
        if "leader" in spec:
            lx, ly = spec["leader"]
            ax.plot(
                [lx, spec["anchor"][0]],
                [ly, spec["anchor"][1]],
                color=color,
                linewidth=0.8,
                gid=f"leader__{spec['gid']}",
            )

        ax_x, ax_y = spec["anchor"]
        ha, va = spec["align"]
        # One call per line: a multi-line `ax.text` gets no `text-anchor` at all, so its lines arrive
        # in Figma as independent left-anchored boxes that lose their centring on each other.
        # The country name is line 0 and sits on top whichever way the block hangs: measuring down
        # from the anchor when the block sits below it, and up from the last line when it sits above.
        line_step = (SERIES_LABEL_PX * 1.25) * (Y_TICKS[-1] / plot_height_px)
        for i, line in enumerate(spec["label"]):
            if va == "top":
                y = ax_y - i * line_step
            else:
                y = ax_y + (len(spec["label"]) - 1 - i) * line_step
            ax.text(
                ax_x,
                y,
                line,
                color=color,
                fontsize=SERIES_LABEL_PX * PX_TO_PT,
                fontweight="bold" if i == 0 else "normal",
                ha=ha,
                va=va,
                gid=f"label__{spec['gid']}__{i}",
            )

    # ── Template text slots ────────────────────────────────────────────────────
    for i, line in enumerate(title_lines):
        fig.text(
            px(MARGIN_PX),
            py(ORIGIN_Y + (i + 0.78) * TITLE_LINE_PX),
            line,
            fontsize=TITLE_PX * PX_TO_PT,
            color="#2d2e2d",
            va="baseline",
            gid=f"title__{i}",
        )
    for i, line in enumerate(subtitle_lines):
        fig.text(
            px(MARGIN_PX),
            py(subtitle_y + (i + 0.78) * SUBTITLE_LINE_PX),
            line,
            fontsize=SUBTITLE_PX * PX_TO_PT,
            color=TEXT_COLOR,
            va="baseline",
            gid=f"subtitle__{i}",
        )
    for i, line in enumerate(note_lines):
        fig.text(
            px(MARGIN_PX),
            py(band_bottom + (i + 0.78) * NOTE_LINE_PX),
            line,
            fontsize=NOTE_PX * PX_TO_PT,
            color="#858585",
            va="baseline",
            gid=f"note__{i}",
        )
    # The citation is built from the origins on the plotted columns, so it grows with the data and
    # has to wrap rather than run off the frame.
    source_lines = wrap(f"Data source: {source}", SOURCE_PX, CONTENT_WIDTH_PX)
    source_y = NOTE_INK_BOTTOM_PX + NOTE_LINE_PX + FOOTER_ROW_GAP_PX
    for i, line in enumerate(source_lines):
        fig.text(
            px(MARGIN_PX),
            py(source_y + i * NOTE_LINE_PX),
            line,
            fontsize=SOURCE_PX * PX_TO_PT,
            color="#858585",
            va="baseline",
            gid=f"data-source__{i}",
        )
    footer_y = source_y + (len(source_lines) - 1) * NOTE_LINE_PX + FOOTER_PX + FOOTER_ROW_GAP_PX + 2
    fig.text(
        px(MARGIN_PX),
        py(footer_y),
        TAGLINE,
        fontsize=FOOTER_PX * PX_TO_PT,
        color="#858585",
        va="baseline",
        gid="tagline",
    )
    # The tagline's own width is the check on the Lato allowance: the template sets this exact
    # string in the slot it reports as 467px wide, so a converted width far from that means the
    # allowance for this machine's face is wrong and every wrap above is suspect.
    tagline_px = text_width_px(TAGLINE, FOOTER_PX) * LATO_OVER_MEASURED
    assert abs(tagline_px - 467) < 25, (
        f"The tagline converts to {tagline_px:.0f}px against the template's 467px, so the Lato "
        f"allowance for {Path(_DRAWN_FACE).name} is wrong and every wrapped slot is suspect."
    )

    # Tagline and license share one row, left- and right-aligned inside the content box.
    license_text = f"Licensed under CC-BY by {AUTHOR}"
    license_px = text_width_px(license_text, FOOTER_PX) * LATO_OVER_MEASURED
    budget_px = CONTENT_WIDTH_PX - tagline_px - 12
    assert license_px < budget_px, (
        f"The license row is {license_px:.0f}px against a {budget_px:.0f}px budget and will print "
        "over the tagline. Shorten the phrasing, never a name."
    )
    fig.text(
        px(TEMPLATES[TEMPLATE].width_px - MARGIN_PX),
        py(footer_y),
        license_text,
        fontsize=FOOTER_PX * PX_TO_PT,
        color="#858585",
        ha="right",
        va="baseline",
        gid="license",
    )

    return fig


def run() -> None:
    data, cited = load_series(paths)
    sanity_check(data)
    fig = build(data, source_citation(*cited))
    export_frame(paths, fig, paths.short_name, template=TEMPLATE)
