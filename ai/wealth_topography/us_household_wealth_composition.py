"""US household assets and debt, as a multiple of national income (1930-2022).

Recreates the GC Wealth Project's "Wealth Topography / Mountains of assets" chart for the United
States in the OWID static-chart template: a diverging stacked area with the two asset categories
stacked above zero and household debt mirrored below it.

The source chart also carries a net-wealth line riding on top of the stack. It is deliberately
**not** drawn here: net wealth is the exact sum of the three bands (verified below to 0.01), so the
line re-states information the stack already shows, and it crosses the top band for most of the
range. The subtitle names the gap between the two sides as net wealth instead.

One-off, so it is a script rather than an `export://static_viz` step
-------------------------------------------------------------------
The underlying series are **not** in the ETL catalog. Our `wid/*` datasets are the *distributional*
World Inequality Database tables (wealth shares by percentile); these four are the aggregate
household balance-sheet series (`p-hn-agg-*`), which no step ingests. Bringing them in would be a
`/create-dataset` job, and this viz was commissioned as a one-off, so the data is committed next to
this script as the Tableau export it arrived as.

Two consequences worth knowing, both deviations from `/create-static-viz`'s normal shape:

- **There is no `paths.load_dataset`, no DAG entry and no `PathFinder`.** `_Out` below is a minimal
  stand-in that reuses `PathFinder.export_fig` unchanged, so the save discipline (reproducible
  metadata, opaque PNG / transparent SVG) is the shared one rather than a copy.
- **`Data source:` is typed, not derived.** `etl.static_viz.source_citation` reads
  `col.metadata.origins`, and a bare CSV has none. `DATA_SOURCE` therefore has to be re-checked by
  hand if the data is ever refreshed -- the one string in this file that cannot go stale loudly.

If these series are ever ingested, this script should be replaced by a real
`export://static_viz/...` step and the citation derived.

Data
----
`data/topography_download_data.csv`, downloaded from
https://wealthproject.gc.cuny.edu/wealth-topography/mountains-of-assets/#countryview
(United States, "Ratio to National Income", national currency adjusted for inflation).

It is a Tableau export: **UTF-16, tab-separated**, with an `index()` column and a trailing empty
column. Every string cell carries padding whitespace. `load()` handles all of it.

Four `Concept_` values, 93 years each, no missing values:

| Concept                                                 | Varcode              | Range         | Drawn as        |
|---------------------------------------------------------|----------------------|---------------|-----------------|
| Housing & Land                                          | `p-hn-agg-nfahou-ga` |  0.69 to 2.47 | lower band      |
| Financial Assets & Fixed Capital of Personal Businesses | `p-hn-agg-nnhass-ga` |  1.93 to 5.40 | upper band      |
| Debt                                                    | `p-hn-agg-fliabi-lb` | -1.30 to -0.18| band below zero |
| Net Wealth                                              | `p-hn-agg-netwea-na` |  2.43 to 6.67 | **not drawn**   |

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this
script fixes is the structure: which text slots exist, in what order, and which share a row.

Layout
------
One frame, `Static Chart Template_Horizontal` (850x638, node `5332:75`). A time series spanning 93
years wants the wide frame; there is no mobile version yet (see the open items in the handover).

Geometry follows TEMPLATES.md's derived rhythm rather than the template's fixed slot y values,
because those are pinned for a two-line title and a two-line subtitle and every string here is
measured, not assumed.

Axis treatment is grapher's, read from `/create-static-viz`'s table: dashed `4,4` `#ddd` gridlines,
`#5b5b5b` tick labels, bold axis title, 5px `#999` x tick marks hanging below the axis, outermost
tick labels anchored inwards, and no y-axis line. Two lines are solid `#999` rather than dashed:
the plot's own baseline (it *is* the axis line) and the **zero line**, which on a diverging chart is
the semantic divide between owning and owing.

Figma handoff
-------------
Not yet imported. When it is, `/create-figma-chart`'s local-SVG route applies; the layer names this
script emits are:

| gid                        | What                                      |
|----------------------------|-------------------------------------------|
| `housing-land__band`       | lower asset band                          |
| `financial-business__band` | upper asset band                          |
| `debt__band`               | band below zero                           |
| `<slug>__label`            | the in-band direct label for each         |
| `zero__line`               | the solid divide at 0                     |
| `title` / `subtitle`       | header slots (delete after import)        |
| `note` / `data-source`     | footer slots (delete after import)        |
| `tagline` / `license`      | footer slots (delete after import)        |
| `y-axis__title`            | `Multiple of national income`             |

Bands are directly labeled inside themselves, so there is no legend to remove. `place_band_label`
asserts each label clears its band's *interpolated* edges over the label's own x span, so a label
cannot end up sitting on a curve after a wording change.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath

from etl.helpers import PathFinder
from etl.static_viz import TEMPLATES, apply_svg_rcparams, export_frame

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

HERE = Path(__file__).parent
CSV = HERE / "data" / "topography_download_data.csv"
SHORT_NAME = "us_household_wealth_composition"

TEMPLATE = "horizontal"

# --- Text ---------------------------------------------------------------------------------------
# The title's claim is asserted against the data in `check()`, so a data refresh that breaks it
# fails the render instead of shipping a stale number. The break is explicit: auto-wrapping this
# string at the title slot's width leaves a near-full first line and two words on the second.
TITLE = "American households' assets have nearly doubled\nrelative to national income since 1980"
SUBTITLE = (
    "Assets are shown above the line and debt below it, each as a multiple of US national income. "
    "The gap between them is net wealth."
)
NOTE = (
    "Covers households and non-profit institutions serving households. “Financial assets and "
    "business capital” combines financial assets with the fixed capital of personal businesses. "
    "Each series is divided by national income for the same year, so the ratio is unaffected by "
    "inflation."
)
# Typed, not derived -- see the module docstring. Grapher's footer convention is `producer (year)`;
# the GC Wealth Project re-presents World Inequality Database balance-sheet series, so both are
# named. Warehouse documentation is v1.2, December 2024.
DATA_SOURCE = "GC Wealth Project (2024), based on the World Inequality Database"
TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."
# The templates ship `Licensed under CC-BY by the author [Name of author]`. Whether CC-BY is
# correct here is an OPEN QUESTION: the GC Wealth Project states only "©2023 CUNY Graduate
# Center" and no reuse terms, and WID's own terms have not been checked. Flagged in the handover.
AUTHOR = "Bertha Rohenkohl"
# Runs, not one string: `CC-BY` and the author's name are bold in the template. Every space rides on
# the *end* of its run -- `TextPath` measures ink, so a leading space adds nothing to a run's
# measured advance while matplotlib still draws it, and the next run then starts a space too far
# left (this is what put `by the authorBertha` in the first render).
LICENSE_RUNS_FULL = [("Licensed under ", "normal"), ("CC-BY ", "bold"), ("by the author ", "normal"), (AUTHOR, "bold")]
# The documented fallback when the row is too tight: the phrasing gives, never the name.
LICENSE_RUNS_SHORT = [("Licensed under ", "normal"), ("CC-BY ", "bold"), ("by ", "normal"), (AUTHOR, "bold")]

# --- Series -------------------------------------------------------------------------------------
HOUSING = "Housing & Land"
FINANCIAL = "Financial Assets & Fixed Capital of Personal Businesses"
DEBT = "Debt"
NET_WEALTH = "Net Wealth"

# Source category names are the producer's; these are the chart's. The financial category's full
# name is 55 characters and by far the longest -- it is shortened here and spelled out in the NOTE,
# which is the "shorten only the longest label" rule. The others are unchanged.
DISPLAY_NAME = {
    HOUSING: "Housing and land",
    FINANCIAL: "Financial assets and business capital",
    DEBT: "Debt",
}
SLUG = {HOUSING: "housing-land", FINANCIAL: "financial-business", DEBT: "debt"}

# seaborn "deep" positions rather than pinned hexes, so the render moves with the shared palette.
# Figma rebinds these to [Chart Colors] library styles on import; the assignment echoes the
# source chart (housing green, financial warm, debt blue) so the two are comparable side by side.
PALETTE_INDEX = {HOUSING: 2, FINANCIAL: 1, DEBT: 0}

Y_STEP = 1.0
Y_AXIS_TITLE = "Multiple of national income"
# Decades, plus the final data year so the axis says where the series ends. 2010 -> 2022 is a wider
# gap than the rest, so it cannot crowd its neighbour; pinning `xlim` to the outermost ticks then
# puts the end tick marks at the ends of the axis line, which is what closes it (grapher's rule).
X_TICKS = [1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2022]

# --- Template geometry (TEMPLATES.md) -----------------------------------------------------------
PX_PER_PT = 0.72  # a template pixel is 0.72pt; these figures are built at 100 template px / inch
MARGIN_PX = 16
ORIGIN_Y_PX = 16
TITLE_LINE_PX = 29
SUBTITLE_LINE_PX = 19
HEADER_GAP_PX = 6
NOTE_LINE_PX = 14
NOTE_INK_BOTTOM_PX = 587  # Horizontal template
FOOTER_ROW_GAP_PX = 4
BAND_INSET_PX = 14

TITLE_SIZE_PX = 25
SUBTITLE_SIZE_PX = 16
NOTE_SIZE_PX = 12
FOOTER_SIZE_PX = 11
TICK_SIZE_PX = 14
AXIS_TITLE_SIZE_PX = 14
BAND_LABEL_SIZE_PX = 14

# The title node is sized narrower than the content box to clear the logo (737.84 against 818).
TITLE_SLOT_PX = 737.84

# Grapher's palette, from `/create-static-viz`'s table.
GRID_COLOR = "#ddd"
GRID_DASH = (0, (4, 4))
TICK_COLOR = "#999"
TEXT_COLOR = "#5b5b5b"
TITLE_INK = "#2d2e2d"
FOOTER_INK = "#858585"

X_TICK_MARK_PX = 5
X_TICK_PAD_PX = 4

# A step measuring in DejaVu predicts the template's Lato and Playfair line counts imperfectly, and
# the error does not point one way (TEMPLATES.md). Two named constants so neither is applied
# backwards. Do NOT pad these "to be safe" -- wrapping early breaks a footer row onto a second line
# the frame does not have.
LATO_NARROWER = {16: 0.008, 12: 0.016, 11: 0.024}  # Lato is this much narrower than our measure
PLAYFAIR_WIDER = 0.032  # Playfair Display SemiBold is this much wider


# --- Text measurement ---------------------------------------------------------------------------
def text_width_px(text: str, size_px: float, weight: str = "normal") -> float:
    """Width of `text` in template px, measured as ink.

    `TextPath` measures ink, so a *leading* space contributes nothing while matplotlib still draws
    it -- which is why runs laid out by summed advance must keep their space on the end of the
    previous run. A sentinel recovers a trailing space here.
    """
    if not text:
        return 0.0
    prop = FontProperties(size=size_px * PX_PER_PT, weight=weight)
    if text != text.rstrip():
        # Measure "<text>|" and "|" and subtract, so a trailing space keeps its advance.
        full = TextPath((0, 0), text + "|", prop=prop).get_extents().width
        sentinel = TextPath((0, 0), "|", prop=prop).get_extents().width
        return (full - sentinel) / PX_PER_PT
    return TextPath((0, 0), text, prop=prop).get_extents().width / PX_PER_PT


def runs_width_px(runs: list[tuple[str, str]], size_px: float) -> float:
    """Total advance of a row of mixed-weight runs."""
    return sum(text_width_px(text, size_px, weight) for text, weight in runs)


def cap_height_px(size_px: float, weight: str = "normal") -> float:
    """Cap height in template px, for placing a row's text on an explicit baseline."""
    prop = FontProperties(size=size_px * PX_PER_PT, weight=weight)
    return TextPath((0, 0), "0", prop=prop).get_extents().ymax / PX_PER_PT


def wrap(
    text: str,
    slot_px: float,
    size_px: float,
    *,
    serif: bool = False,
    weight: str = "normal",
    first_indent_px: float = 0.0,
) -> list[str]:
    """Greedily wrap `text` to `slot_px`, correcting for the template's font.

    `first_indent_px` shortens the *first* line's budget, for a row whose bold label
    (`Note: `, `Data source: `) is prepended after wrapping. Without it the label's own width is
    added to an already-full line and the row overruns the content box -- which is invisible in the
    wrap and obvious in the render.
    """
    if serif:
        budget = slot_px / (1.0 + PLAYFAIR_WIDER)
    else:
        budget = slot_px * (1.0 + LATO_NARROWER.get(int(size_px), 0.0))
    lines: list[str] = []
    current = ""
    for word in text.split():
        trial = f"{current} {word}".strip()
        allowed = budget - (first_indent_px if not lines else 0.0)
        if current and text_width_px(trial, size_px, weight) > allowed:
            lines.append(current)
            current = word
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


# --- Data ---------------------------------------------------------------------------------------
def load() -> pd.DataFrame:
    """Read the Tableau export and return one column per concept, indexed by year."""
    df = pd.read_csv(CSV, encoding="utf-16", sep="\t")
    df.columns = [c.strip() for c in df.columns]
    for col in df.select_dtypes("object"):
        df[col] = df[col].str.strip()

    # One country, one unit, one sector -- assert rather than filter, so a re-download that
    # silently includes another country fails here instead of being averaged into the chart by
    # `pivot_table`.
    assert set(df["Country_"]) == {"United States"}, sorted(set(df["Country_"]))
    assert set(df["Unit (Topography)"]) == {"Ratio to National Income"}
    assert set(df["Sector_"]) == {"Households & NPISH"}
    assert set(df["Concept_"]) == {HOUSING, FINANCIAL, DEBT, NET_WEALTH}, sorted(set(df["Concept_"]))

    wide = df.pivot_table(index="Year", columns="Concept_", values="Value_Topography")
    wide.columns.name = None
    return wide.sort_index()


def check(data: pd.DataFrame) -> None:
    """Assert the claims this chart makes, not only its schema."""
    years = data.index.to_numpy()
    assert np.array_equal(years, np.arange(years[0], years[-1] + 1)), "years are not contiguous"
    assert (years[0], years[-1]) == (1930, 2022), (years[0], years[-1])
    assert not data.isna().to_numpy().any(), "unexpected missing values"

    # Signs: the chart's whole structure is assets above zero and debt below it.
    assert (data[DEBT] < 0).all(), "Debt is expected to be negative throughout"
    assert (data[[HOUSING, FINANCIAL, NET_WEALTH]] > 0).to_numpy().all(), "assets expected positive"

    # The accounting identity that lets the net-wealth line be dropped: it is the sum of the bands,
    # so the gap between the two sides of the chart *is* net wealth, as the subtitle claims.
    residual = (data[NET_WEALTH] - data[[HOUSING, FINANCIAL, DEBT]].sum(axis=1)).abs()
    assert residual.max() <= 0.011, f"net wealth is not the sum of the components (max {residual.max()})"

    # The title's claim. `nearly doubled` has to hold for the final year, not only at the peak:
    # the ratio is 1.98 at the 2021 peak and 1.91 in 2022, so "nearly" is the honest word and the
    # claim cannot be falsified by reading the last point instead of the highest one.
    assets = data[HOUSING] + data[FINANCIAL]
    ratio_final = assets.loc[2022] / assets.loc[1980]
    ratio_peak = assets.max() / assets.loc[1980]
    assert 1.85 <= ratio_final <= 2.0, f"assets 1980->2022 ratio is {ratio_final:.3f}, title says 'nearly doubled'"
    assert ratio_peak <= 2.05, f"assets peak/1980 ratio is {ratio_peak:.3f}; title may understate"


# --- Colors -------------------------------------------------------------------------------------
def relative_luminance(rgb: tuple[float, float, float]) -> float:
    channels = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]


def contrast_ratio(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    la, lb = relative_luminance(a), relative_luminance(b)
    lighter, darker = max(la, lb), min(la, lb)
    return (lighter + 0.05) / (darker + 0.05)


def label_ink(fill: tuple[float, float, float]) -> tuple[str, float]:
    """Pick white or near-black for a label sitting on `fill`, whichever reads better.

    Guidance is "white text over dark fills" but "on a pale band the inside label goes dark" --
    contrast decides, so it is computed rather than assumed. These labels are 14 template px
    regular, below the 3:1 large-text allowance, so 4.5:1 is the bar. The chosen ratio is reported
    at render time; Figma rebinds the fills, so whoever does that has to re-check it.
    """
    dark = to_rgb(TITLE_INK)
    white = (1.0, 1.0, 1.0)
    on_white = contrast_ratio(fill, white)
    on_dark = contrast_ratio(fill, dark)
    return ("#ffffff", on_white) if on_white >= on_dark else (TITLE_INK, on_dark)


# --- Layout -------------------------------------------------------------------------------------
class Geometry:
    """Derived template geometry, in template px with y measured from the top edge."""

    def __init__(self, title_lines: int, subtitle_lines: int, note_lines: int, template: str) -> None:
        tpl = TEMPLATES[template]
        self.width = tpl.width_px
        self.height = tpl.height_px
        self.content_left = MARGIN_PX
        self.content_right = self.width - MARGIN_PX

        # Header grows down from the title; the logo is a sibling and contributes no height.
        self.title_y = ORIGIN_Y_PX
        title_row = title_lines * TITLE_LINE_PX
        self.subtitle_y = ORIGIN_Y_PX + title_row + HEADER_GAP_PX
        self.band_top = self.subtitle_y + subtitle_lines * SUBTITLE_LINE_PX

        # Footer grows up from the Note's ink bottom.
        self.note_y = NOTE_INK_BOTTOM_PX - note_lines * NOTE_LINE_PX
        self.band_bottom = self.note_y
        self.source_y = NOTE_INK_BOTTOM_PX + FOOTER_ROW_GAP_PX
        self.footer_last_y = self.source_y + NOTE_LINE_PX + FOOTER_ROW_GAP_PX

    def fig_x(self, px: float) -> float:
        return px / self.width

    def fig_y(self, px: float) -> float:
        """Figure fraction from a top-measured px position."""
        return 1.0 - px / self.height


def build() -> tuple[plt.Figure, dict[str, float]]:
    data = load()
    check(data)

    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    title_lines = [ln for chunk in TITLE.split("\n") for ln in wrap(chunk, TITLE_SLOT_PX, TITLE_SIZE_PX, serif=True)]
    geo_probe = Geometry(len(title_lines), 1, 1, TEMPLATE)
    content_px = geo_probe.content_right - geo_probe.content_left
    subtitle_lines = wrap(SUBTITLE, content_px, SUBTITLE_SIZE_PX)
    note_lines = wrap(
        NOTE,
        content_px,
        NOTE_SIZE_PX,
        first_indent_px=text_width_px("Note: ", NOTE_SIZE_PX, "bold"),
    )
    geo = Geometry(len(title_lines), len(subtitle_lines), len(note_lines), TEMPLATE)

    # Nothing may overrun the content box. Cheap to assert, and it is the failure mode a wording
    # change reintroduces silently -- the wrap still succeeds, the render just runs off the edge.
    for label, lines, size in (
        ("subtitle", subtitle_lines, SUBTITLE_SIZE_PX),
        ("note", note_lines, NOTE_SIZE_PX),
    ):
        for line in lines:
            indent = text_width_px("Note: ", NOTE_SIZE_PX, "bold") if label == "note" and line is lines[0] else 0.0
            width = text_width_px(line, size) + indent
            assert width <= content_px, f"{label} line overruns content box by {width - content_px:.0f}px: {line!r}"
    source_px = runs_width_px([("Data source: ", "bold"), (DATA_SOURCE, "normal")], NOTE_SIZE_PX)
    assert source_px <= content_px, f"data source row overruns by {source_px - content_px:.0f}px"

    fig = plt.figure(figsize=TEMPLATES[TEMPLATE].figsize)
    fig.patch.set_facecolor("white")  # legible when the PNG is reviewed on a dark background

    # --- Plot box -------------------------------------------------------------------------------
    years = data.index.to_numpy()
    housing = data[HOUSING].to_numpy()
    financial = data[FINANCIAL].to_numpy()
    debt = data[DEBT].to_numpy()
    assets = housing + financial

    # Snap the value axis out to whole gridline steps, so the extreme gridlines land on the plot's
    # edges and there is only ever one line at each edge.
    y_low = np.floor(debt.min() / Y_STEP) * Y_STEP
    y_high = np.ceil(assets.max() / Y_STEP) * Y_STEP
    y_ticks = np.arange(y_low, y_high + Y_STEP / 2, Y_STEP)

    tick_label_px = max(text_width_px(f"{t:g}", TICK_SIZE_PX) for t in y_ticks)
    plot_left = geo.content_left + tick_label_px + 6
    plot_right = geo.content_right
    # The y-axis title sits horizontally above the axis, left-aligned on the content box.
    axis_title_row = AXIS_TITLE_SIZE_PX + HEADER_GAP_PX
    plot_top = geo.band_top + BAND_INSET_PX + axis_title_row
    # x tick marks hang below the axis line, then their labels.
    plot_bottom = geo.band_bottom - BAND_INSET_PX - (X_TICK_MARK_PX + X_TICK_PAD_PX + TICK_SIZE_PX)

    ax = fig.add_axes(
        (
            geo.fig_x(plot_left),
            geo.fig_y(plot_bottom),
            (plot_right - plot_left) / geo.width,
            (plot_bottom - plot_top) / geo.height,
        )
    )
    ax.patch.set_visible(False)  # the template supplies the background

    # --- Bands ----------------------------------------------------------------------------------
    fills = {name: palette[i] for name, i in PALETTE_INDEX.items()}
    ax.fill_between(years, 0, housing, facecolor=fills[HOUSING], linewidth=0, gid=f"{SLUG[HOUSING]}__band")
    ax.fill_between(years, housing, assets, facecolor=fills[FINANCIAL], linewidth=0, gid=f"{SLUG[FINANCIAL]}__band")
    ax.fill_between(years, debt, 0, facecolor=fills[DEBT], linewidth=0, gid=f"{SLUG[DEBT]}__band")

    # --- Axes -----------------------------------------------------------------------------------
    ax.set_xlim(X_TICKS[0], X_TICKS[-1])
    ax.set_ylim(y_ticks[0], y_ticks[-1])
    ax.set_yticks(y_ticks)
    ax.set_xticks(X_TICKS)
    ax.set_yticklabels([f"{t:g}" for t in y_ticks])
    ax.set_xticklabels([str(t) for t in X_TICKS])

    for spine in ax.spines.values():
        spine.set_visible(False)

    # Dashed gridlines, except at the two solid lines drawn below.
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=GRID_COLOR, linestyle=GRID_DASH, linewidth=1)
    ax.xaxis.grid(False)
    for gridline, value in zip(ax.yaxis.get_gridlines(), y_ticks):
        if value in (y_ticks[0], 0.0):
            gridline.set_visible(False)

    # The plot's baseline is the axis line; the zero line is the semantic divide on a diverging
    # chart. Both solid in the tick color, so neither is broken up by a dashed stroke laid over it.
    ax.axhline(y_ticks[0], color=TICK_COLOR, linewidth=1, zorder=2.5, gid="baseline__line")
    ax.axhline(0.0, color=TICK_COLOR, linewidth=1, zorder=2.5, gid="zero__line")

    ax.tick_params(axis="y", length=0, pad=6, labelsize=TICK_SIZE_PX * PX_PER_PT, colors=TEXT_COLOR)
    ax.tick_params(
        axis="x",
        length=X_TICK_MARK_PX * PX_PER_PT,
        width=1,
        color=TICK_COLOR,
        pad=X_TICK_PAD_PX,
        labelsize=TICK_SIZE_PX * PX_PER_PT,
        labelcolor=TEXT_COLOR,
    )
    # Outermost tick labels anchored inwards, so neither overhangs the content box.
    ax.get_xticklabels()[0].set_horizontalalignment("left")
    ax.get_xticklabels()[-1].set_horizontalalignment("right")

    fig.text(
        geo.fig_x(geo.content_left),
        geo.fig_y(geo.band_top + BAND_INSET_PX + AXIS_TITLE_SIZE_PX),
        Y_AXIS_TITLE,
        fontsize=AXIS_TITLE_SIZE_PX * PX_PER_PT,
        fontweight="bold",
        color=TEXT_COLOR,
        ha="left",
        va="baseline",
        gid="y-axis__title",
    )

    # --- Direct labels inside the bands ---------------------------------------------------------
    contrasts = place_band_labels(ax, years, housing, assets, debt, fills)

    # --- Template text slots --------------------------------------------------------------------
    draw_text_slots(fig, geo, title_lines, subtitle_lines, note_lines)

    return fig, contrasts


def place_band_labels(
    ax: plt.Axes,
    years: np.ndarray,
    housing: np.ndarray,
    assets: np.ndarray,
    debt: np.ndarray,
    fills: dict[str, tuple[float, float, float]],
) -> dict[str, float]:
    """Label each band inside itself, at an x where the band has room for the label.

    Bands, not a legend: three categories is well inside what direct labeling handles, and it
    removes the colour-matching step entirely.

    The anchor x values are chosen per band and asserted, never assumed. Each band's edges are
    *interpolated over the label's own x span* -- a band can be thick at a label's centre and thin
    at its end, so testing one x answers the wrong question.
    """
    # Where each label sits, and why:
    #   financial  1958 -- the calm mid-century stretch, band ~2.3 deep and its top edge flat
    #   housing    1958 -- directly below it, so the two asset labels read as one stack
    #   debt       2006 -- the band is deepest here (-1.19 to -1.30 through 2006-2011)
    anchors = {FINANCIAL: 1958, HOUSING: 1958, DEBT: 2009}
    # Every label is centred in its band. The "a very tall band takes its label near the top" rule
    # does not apply here: the financial band's top edge is jagged, so a label wide enough to span
    # 1943-1973 has to clear the 1943 wartime trough and lands mid-band regardless -- and there is
    # no annotation wanting the middle. Centring all three is one rule instead of two.
    edges = {
        FINANCIAL: (housing, assets),
        HOUSING: (np.zeros_like(housing), housing),
        DEBT: (debt, np.zeros_like(debt)),
    }

    contrasts: dict[str, float] = {}
    for name, x_anchor in anchors.items():
        lower, upper = edges[name]
        text = DISPLAY_NAME[name]
        width_px = text_width_px(text, BAND_LABEL_SIZE_PX, weight="bold")

        # Convert the label's width into data (year) units via the axes' own box.
        bbox = ax.get_window_extent()
        years_per_px = (ax.get_xlim()[1] - ax.get_xlim()[0]) / bbox.width
        # `bbox` is in display px at the figure's dpi; the figure is 100 template px / inch.
        px_per_template_px = bbox.width / (ax.get_position().width * ax.figure.get_figwidth() * 100)
        half_span = 0.5 * width_px * px_per_template_px * years_per_px

        x0, x1 = x_anchor - half_span, x_anchor + half_span
        assert years[0] <= x0 and x1 <= years[-1], f"{name} label runs off the plot"

        span = np.linspace(x0, x1, 40)
        lower_at = np.interp(span, years, lower)
        upper_at = np.interp(span, years, upper)
        thickness = float(np.min(upper_at - lower_at))

        units_per_px = (ax.get_ylim()[1] - ax.get_ylim()[0]) / bbox.height
        label_units = BAND_LABEL_SIZE_PX * px_per_template_px * units_per_px
        cap_units = cap_height_px(BAND_LABEL_SIZE_PX, "bold") * px_per_template_px * units_per_px

        # Place the label in the interval that clears BOTH edges across its whole x span, not at
        # the mean of the band's midpoints. A band's midpoint drifts, so the mean sits the text
        # near the top edge wherever the band is thinnest -- on the financial band that put
        # "Financial assets..." within a few px of the 1943 wartime trough while every
        # thickness check still passed, because thickness is not the question being asked.
        pad_units = label_units * 0.30
        feasible_low = float(np.max(lower_at)) + pad_units
        feasible_high = float(np.min(upper_at)) - pad_units
        assert feasible_high - feasible_low >= cap_units, (
            f"{name} band leaves {feasible_high - feasible_low:.2f} units clear over the label's "
            f"span at x={x_anchor} (thinnest point {thickness:.2f}), label cap height is "
            f"{cap_units:.2f}. Move the anchor or label this band outside."
        )

        ink, ratio = label_ink(fills[name])
        contrasts[name] = ratio

        # An explicit baseline half a cap-height below the centre of that interval, not
        # `va="center"`: centring uses the font's whole line box, which reserves room for
        # descenders these labels mostly do not use, and sits the text visibly high in its band.
        y = (feasible_low + feasible_high) / 2.0 - cap_units / 2.0

        # Centred on its band: `text-anchor: middle` survives the font swap in Figma, a
        # left-anchored run does not.
        ax.text(
            x_anchor,
            y,
            text,
            fontsize=BAND_LABEL_SIZE_PX * PX_PER_PT,
            fontweight="bold",
            color=ink,
            ha="center",
            va="baseline",
            zorder=3,
            gid=f"{SLUG[name]}__label",
        )
    return contrasts


def draw_text_slots(
    fig: plt.Figure,
    geo: Geometry,
    title_lines: list[str],
    subtitle_lines: list[str],
    note_lines: list[str],
) -> None:
    """Fill the template's slots, in its order, with its labels and at its sizes.

    Drawn at the template's own sizes rather than sizes that merely look right: the band is correct
    for the frame, so a smaller subtitle here would end higher and show a hole the frame does not
    have. The import deletes all of these -- the template carries its own copies.
    """
    left = geo.fig_x(geo.content_left)

    for i, line in enumerate(title_lines):
        fig.text(
            left,
            geo.fig_y(geo.title_y + (i + 1) * TITLE_LINE_PX - 7),
            line,
            fontsize=TITLE_SIZE_PX * PX_PER_PT,
            color=TITLE_INK,
            ha="left",
            va="baseline",
            gid="title" if i == 0 else f"title__line{i + 1}",
        )

    for i, line in enumerate(subtitle_lines):
        fig.text(
            left,
            geo.fig_y(geo.subtitle_y + (i + 1) * SUBTITLE_LINE_PX - 5),
            line,
            fontsize=SUBTITLE_SIZE_PX * PX_PER_PT,
            color=TEXT_COLOR,
            ha="left",
            va="baseline",
            gid="subtitle" if i == 0 else f"subtitle__line{i + 1}",
        )

    # `Note:` and `Data source:` are bold labels on a regular-weight body; matplotlib has no rich
    # text, so each row is laid out as runs by summed advance. The space rides with the label,
    # because a run may not begin with one.
    for i, line in enumerate(note_lines):
        runs = [("Note: ", "bold"), (line, "normal")] if i == 0 else [(line, "normal")]
        draw_runs(
            fig,
            geo,
            runs,
            geo.note_y + (i + 1) * NOTE_LINE_PX - 4,
            NOTE_SIZE_PX,
            FOOTER_INK,
            f"note__line{i + 1}" if i else "note",
        )

    draw_runs(
        fig,
        geo,
        [("Data source: ", "bold"), (DATA_SOURCE, "normal")],
        geo.source_y + NOTE_LINE_PX - 4,
        NOTE_SIZE_PX,
        FOOTER_INK,
        "data-source",
    )

    # Tagline and license share the last row, left- and right-aligned. They compete for one content
    # width, so pick the phrasing that fits and assert the result -- an overrun here prints the
    # license on top of the tagline, which is exactly what the first render did.
    content_px = geo.content_right - geo.content_left
    tagline_px = text_width_px(TAGLINE, FOOTER_SIZE_PX)
    gap_px = 12
    license_runs = LICENSE_RUNS_FULL
    license_px = runs_width_px(license_runs, FOOTER_SIZE_PX)
    if tagline_px + gap_px + license_px > content_px:
        license_runs = LICENSE_RUNS_SHORT
        license_px = runs_width_px(license_runs, FOOTER_SIZE_PX)
    assert tagline_px + gap_px + license_px <= content_px, (
        f"footer row overruns: tagline {tagline_px:.0f}px + license {license_px:.0f}px "
        f"> {content_px:.0f}px. Shorten the phrasing, never the name."
    )

    fig.text(
        left,
        geo.fig_y(geo.footer_last_y + FOOTER_SIZE_PX),
        TAGLINE,
        fontsize=FOOTER_SIZE_PX * PX_PER_PT,
        color=FOOTER_INK,
        ha="left",
        va="baseline",
        gid="tagline",
    )
    draw_runs(
        fig,
        geo,
        license_runs,
        geo.footer_last_y + FOOTER_SIZE_PX,
        FOOTER_SIZE_PX,
        FOOTER_INK,
        "license",
        start_px=geo.content_right - license_px,
    )


def draw_runs(
    fig: plt.Figure,
    geo: Geometry,
    runs: list[tuple[str, str]],
    baseline_px: float,
    size_px: float,
    color: str,
    gid: str,
    start_px: float | None = None,
) -> None:
    """Lay out mixed-weight runs on one baseline, advancing by measured ink width."""
    cursor = geo.content_left if start_px is None else start_px
    for i, (text, weight) in enumerate(runs):
        if not text:
            continue
        fig.text(
            geo.fig_x(cursor),
            geo.fig_y(baseline_px),
            text.rstrip(),  # SVG centres trimmed ink; the space belongs to the layout
            fontsize=size_px * PX_PER_PT,
            fontweight=weight,
            color=color,
            ha="left",
            va="baseline",
            gid=gid if i == 0 else f"{gid}__run{i + 1}",
        )
        cursor += text_width_px(text, size_px, weight)


# --- Export -------------------------------------------------------------------------------------
class _Out:
    """Minimal `PathFinder` stand-in, so `export_frame` can be reused outside an ETL step.

    Reuses `PathFinder.export_fig` itself rather than copying it, which keeps the reproducible
    metadata (matplotlib's version stamp stripped, so a byte diff means the picture changed) and
    the PNG/SVG save discipline identical to a real static_viz step's.
    """

    _REPRODUCIBLE_METADATA = PathFinder._REPRODUCIBLE_METADATA
    export_fig = PathFinder.export_fig

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.log = logging.getLogger("static_viz")


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    fig, contrasts = build()
    export_frame(_Out(HERE), fig, SHORT_NAME, template=TEMPLATE)
    for name, ratio in sorted(contrasts.items()):
        logging.getLogger("static_viz").info(
            "label contrast on %s: %.2f:1 %s", DISPLAY_NAME[name], ratio, "OK" if ratio >= 4.5 else "BELOW 4.5:1"
        )
    plt.close(fig)


if __name__ == "__main__":
    run()
