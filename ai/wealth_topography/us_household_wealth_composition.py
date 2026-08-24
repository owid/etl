"""US household assets and debt, as a multiple of national income (1930-2022).

Recreates the GC Wealth Project's "Wealth Topography / Mountains of assets" chart for the United
States in the OWID static-chart templates: a diverging stacked area with the two asset categories
stacked above zero and household debt mirrored below it.

The source chart also carries a net-wealth line riding on top of the stack. It is deliberately
**not** drawn here: net wealth is the exact sum of the three bands (asserted to 0.01), so the line
re-states information the stack already shows, and it crosses the top band for most of the range.
The title calls the three bands the components of net wealth and the subtitle names the gap between
the two sides as net wealth, which is what makes the identity the chart's actual claim.

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

| Concept                                                 | Varcode              | Range          | Drawn as        |
|---------------------------------------------------------|----------------------|----------------|-----------------|
| Housing & Land                                          | `p-hn-agg-nfahou-ga` |  0.69 to 2.47  | lower band      |
| Financial Assets & Fixed Capital of Personal Businesses | `p-hn-agg-nnhass-ga` |  1.93 to 5.40  | upper band      |
| Debt                                                    | `p-hn-agg-fliabi-lb` | -1.30 to -0.18 | band below zero |
| Net Wealth                                              | `p-hn-agg-netwea-na` |  2.43 to 6.67  | **not drawn**   |

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this
script fixes is the structure: which text slots exist, in what order, and which share a row.

Layouts
-------
Two frames, both from `LAYOUTS`:

| Frame                | Template                             | Size    |
|----------------------|--------------------------------------|---------|
| desktop (unsuffixed) | `Static Chart Template_Horizontal`   | 850x638 |
| `_mobile_square`     | `Static Chart Template_Mobile (ex 1)`| 540x540 |

**Mobile is the *square* template, not the 540x824 portrait one**, and that is a measured choice
rather than a default. A 93-year time series needs a landscape plot: after the same reserves, the
square frame gives a 480x278 plot (aspect **1.73**) and the portrait frame 480x562 (aspect **0.85**),
which vertically exaggerates every wiggle in the series. The portrait template's own note in
TEMPLATES.md says it is for "two panels side by side"; this chart is one panel.

Geometry follows TEMPLATES.md's derived rhythm rather than the templates' fixed slot y values,
because those are pinned for a two-line title and a two-line subtitle and every string here is
measured, not assumed.

**What mobile drops, and why.** The square template has no `Note:` row and no tagline, so the
desktop note has nowhere to go. All three of its sentences are dropped, and the subtitle is
**identical** on both frames -- which is what the pairing rule wants, since the two versions should
differ only where the template forces it.

That is a judgement, so here is the test it was made against: a caveat about *what the chart claims*
may not be dropped (it would have to move into the subtitle, which mobile does have), while a gloss
on a label that is already accurate may. All three sentences are the latter. The sector scope
(households plus non-profit institutions serving them) is the closest call, because it qualifies
whose wealth this is -- but NPISH is conventionally part of the household sector in national
accounts and is small beside it, so "household net wealth" does not become an over-claim without the
sentence. It was drafted into the mobile subtitle and then taken back out: it cost a fourth subtitle
line, and a fourth line of technical detail is worse value than the chart height it spends. Stated
here rather than dropped silently, which is the part that would actually have been wrong.

Axis treatment is grapher's, read from `/create-static-viz`'s table: dashed `4,4` `#ddd` gridlines,
`#5b5b5b` tick labels, bold axis title, 5px `#999` x tick marks hanging below the axis, outermost
tick labels anchored inwards, and no y-axis line. Two lines are solid `#999` rather than dashed:
the plot's own baseline (it *is* the axis line) and the **zero line**, which on a diverging chart is
the semantic divide between owning and owing. The x tick set is **per layout** -- decades fit the
850-wide frame, but at 480px they leave 18px between labels, so the narrow frame takes every 20
years. Both sets keep 1930 and 2022 as the outermost ticks, which is what lets `xlim` pin to them.

Figma handoff
-------------
Target: `Charts (2026)`, key `s6Sv60bakebRRW2TxsMQbF`, page
`20260824 Components of household net wealth in the US (Bertha)` at the top of the dated block
(insert after the `-----------` divider page at index 8, not at a counted index).

| Frame                                     | Cloned from                        | Size    |
|-------------------------------------------|------------------------------------|---------|
| `us-household-net-wealth-components`      | `5332:75` Horizontal               | 850x638 |
| `us-household-net-wealth-components-mobile` | `24590:20` Mobile (example 1)    | 540x540 |

Import with `upload_assets` + POST to the returned `submitUrl`; never `createNodeFromSvg`, which
caps at 50k characters. Then bin the wrapper FRAME (it carries a white fill that would cover the
template's cream background, and `resize()` on it rewraps every text node), and rescale by
`clone.width / imported.width` -- matplotlib declares the root in points, Figma imports at 96px per
inch, and this figure is built at 100 template px per inch, so the factor is `100/96` and the
self-correcting form is the ratio.

Delete these from the working copy after import -- the template carries its own slots: `patch_1`,
`title`, `subtitle`, `note`, `data-source`, `tagline`, `license` (plus their `__line*`/`__run*`
siblings).

Layer names this script emits:

| gid                        | What                                      |
|----------------------------|-------------------------------------------|
| `housing-land__band`       | lower asset band                          |
| `financial-business__band` | upper asset band                          |
| `debt__band`               | band below zero                           |
| `<slug>__label`            | the in-band direct label for each         |
| `zero__line`               | the solid divide at 0                     |
| `baseline__line`           | the plot's own axis line                  |
| `y-axis__title`            | `Multiple of national income`             |

Bands are directly labeled inside themselves, so there is no legend to remove. `place_band_labels`
asserts each label clears its band's *interpolated* edges over the label's own x span, so a label
cannot end up sitting on a curve after a wording change.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import NamedTuple

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

# --- Text ---------------------------------------------------------------------------------------
# Descriptive rather than a story claim, and durable: it carries no year, so it cannot be falsified
# by a data refresh. The three bands *are* the components of net wealth -- `check()` asserts exactly
# that identity, which is what makes this title accurate with the net-wealth line dropped.
#
# `US` rather than `United States` so it fits the desktop title slot on one line (587px measured
# against a 715px Playfair-corrected budget in the 737.84px slot); the full name misses by 7.5px and
# auto-wrap then orphans the single word "States". The style guide rules on US/UK without periods.
# On the 428px mobile slot the same string takes two lines -- template-forced, so both frames keep
# the same words.
TITLE = "Components of household net wealth in the US"
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
# Typed, not derived -- see the module docstring. Grapher's footer convention is `producer (year)`
# joined by "; ", which is what this uses: the GC Wealth Project re-presents World Inequality
# Database balance-sheet series, so both producers are named. Warehouse documentation is v1.2,
# December 2024; WID's release year for this vintage is unknown, and `source_citation` omits a year
# it does not have rather than guessing one, so this does too.
#
# An earlier draft read "GC Wealth Project (2024), based on the World Inequality Database". It fit
# desktop's 12px footer and overran mobile's 14px one by 63px. The fix is the standard join rather
# than a shortened producer name -- "based on the" was editorial prose, not part of anyone's name,
# and producer names never give to make a line fit.
DATA_SOURCE = "GC Wealth Project (2024); World Inequality Database"
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

# --- Template geometry (TEMPLATES.md) -----------------------------------------------------------
PX_PER_PT = 0.72  # a template pixel is 0.72pt; these figures are built at 100 template px / inch
MARGIN_PX = 16
ORIGIN_Y_PX = 16
TITLE_LINE_PX = 29
SUBTITLE_LINE_PX = 19
HEADER_GAP_PX = 6
BAND_INSET_PX = 14

TITLE_SIZE_PX = 25
SUBTITLE_SIZE_PX = 16
TICK_SIZE_PX = 14
AXIS_TITLE_SIZE_PX = 14
BAND_LABEL_SIZE_PX = 14

# Grapher's palette, from `/create-static-viz`'s table.
GRID_COLOR = "#ddd"
GRID_DASH = (0, (4, 4))
TICK_COLOR = "#999"
TEXT_COLOR = "#5b5b5b"
TITLE_INK = "#2d2e2d"
FOOTER_INK = "#858585"

X_TICK_MARK_PX = 5
X_TICK_PAD_PX = 4

# A step measuring in DejaVu predicts the templates' Lato and Playfair line counts imperfectly, and
# the error does not point one way (TEMPLATES.md). Two named constants so neither is applied
# backwards. Do NOT pad these "to be safe" -- wrapping early breaks a footer row onto a second line
# the frame does not have.
LATO_NARROWER = {16: 0.008, 14: 0.012, 12: 0.016, 11: 0.024}
PLAYFAIR_WIDER = 0.032  # Playfair Display SemiBold is this much wider


class Layout(NamedTuple):
    """One emitted frame: its template, its per-layout text and its tick set.

    The two layouts differ only where the template forces it -- frame, tick count, which footer
    rows exist, and the subtitle that has to absorb the missing `Note:`. Everything else is shared,
    because two different explanatory devices would read as two different charts.
    """

    template: str
    suffix: str
    title_slot_px: float
    subtitle: str
    note: str | None
    has_tagline: bool
    footer_size_px: float
    footer_line_px: float
    # Desktop: the `Note:` ink bottom, from which the band grows upward. Mobile: the footer's own y,
    # which is its first row's ink and therefore the band's bottom directly.
    footer_anchor_px: float
    footer_row_gap_px: float
    x_ticks: tuple[int, ...]
    band_label_anchors: dict[str, int]


# Decades fit the 850-wide frame; at 480px they leave only ~18px between 4-digit labels, so the
# narrow frame takes every 20 years. Both sets keep 1930 and 2022 as the outermost ticks, so
# `set_xlim(first, last)` puts the end tick marks at the ends of the axis line and closes it.
LAYOUTS = (
    Layout(
        template="horizontal",
        suffix="",
        title_slot_px=737.84,  # sized narrower than the content box to clear the logo
        subtitle=SUBTITLE,
        note=NOTE,
        has_tagline=True,
        footer_size_px=11,
        footer_line_px=14,
        footer_anchor_px=587.0,
        footer_row_gap_px=4,
        x_ticks=(1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2022),
        band_label_anchors={FINANCIAL: 1958, HOUSING: 1958, DEBT: 2009},
    ),
    Layout(
        template="mobile-square",
        suffix="_mobile_square",
        title_slot_px=428.0,  # the logo sits beside the title at x=460
        subtitle=SUBTITLE,
        note=None,
        has_tagline=False,
        footer_size_px=14,
        footer_line_px=14,
        footer_anchor_px=486.0,
        footer_row_gap_px=7,  # mobile's two rows sit 21px apart: 14px of line plus 7
        x_ticks=(1930, 1950, 1970, 1990, 2010, 2022),
        band_label_anchors={FINANCIAL: 1960, HOUSING: 1958, DEBT: 2009},
    ),
)


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
    """Assert the claims the chart makes, not only the schema."""
    years = data.index.to_numpy()
    assert np.array_equal(years, np.arange(years[0], years[-1] + 1)), "years are not contiguous"
    assert (years[0], years[-1]) == (1930, 2022), (years[0], years[-1])
    assert not data.isna().to_numpy().any(), "unexpected missing values"

    # Signs: the chart's whole structure is assets above zero and debt below it.
    assert (data[DEBT] < 0).all(), "Debt is expected to be negative throughout"
    assert (data[[HOUSING, FINANCIAL, NET_WEALTH]] > 0).to_numpy().all(), "assets expected positive"

    # The title's own claim: these three bands ARE the components of net wealth. Net wealth is the
    # exact sum of them, which is what lets the source chart's net-wealth line be dropped and the
    # subtitle call the gap between the two sides net wealth.
    residual = (data[NET_WEALTH] - data[[HOUSING, FINANCIAL, DEBT]].sum(axis=1)).abs()
    assert residual.max() <= 0.011, f"net wealth is not the sum of the components (max {residual.max()})"

    # An earlier draft titled this "assets have nearly doubled since 1980" and asserted that ratio
    # here; the assertion went with the title rather than being left behind to guard a claim nothing
    # makes any more. For the record it is 1.98 at the 2021 peak and 1.91 in 2022 -- if a story
    # title is ever wanted again, that is the number to re-assert.
    #
    # What does still need asserting is that every band stays thick enough to hold a label.
    assert (data[HOUSING] > 0.5).all(), "housing band thinner than expected; check label placement"
    assert (data[FINANCIAL] > 1.5).all(), "financial band thinner than expected; check label placement"


# --- Colors -------------------------------------------------------------------------------------
def relative_luminance(rgb: tuple[float, float, float]) -> float:
    channels = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]


def contrast_ratio(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    la, lb = relative_luminance(a), relative_luminance(b)
    return (max(la, lb) + 0.05) / (min(la, lb) + 0.05)


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

    def __init__(self, layout: Layout, title_lines: int, subtitle_lines: int, note_lines: int) -> None:
        tpl = TEMPLATES[layout.template]
        self.width = tpl.width_px
        self.height = tpl.height_px
        self.content_left = MARGIN_PX
        self.content_right = self.width - MARGIN_PX

        # Header grows down from the title; the logo is a sibling and contributes no height.
        self.title_y = ORIGIN_Y_PX
        self.subtitle_y = ORIGIN_Y_PX + title_lines * TITLE_LINE_PX + HEADER_GAP_PX
        self.band_top = self.subtitle_y + subtitle_lines * SUBTITLE_LINE_PX

        line = layout.footer_line_px
        gap = layout.footer_row_gap_px
        if layout.note is None:
            # Mobile: the footer's own y IS its first row's ink, and that row is `Data source:`.
            self.note_y = None
            self.band_bottom = layout.footer_anchor_px
            self.source_y = layout.footer_anchor_px
        else:
            # Desktop: the footer grows up from the `Note:` ink bottom.
            self.note_y = layout.footer_anchor_px - note_lines * line
            self.band_bottom = self.note_y
            self.source_y = layout.footer_anchor_px + gap
        self.last_row_y = self.source_y + line + gap

    def fig_x(self, px: float) -> float:
        return px / self.width

    def fig_y(self, px: float) -> float:
        """Figure fraction from a top-measured px position."""
        return 1.0 - px / self.height


def build(layout: Layout, data: pd.DataFrame) -> tuple[plt.Figure, dict[str, float]]:
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    title_lines = [
        line for chunk in TITLE.split("\n") for line in wrap(chunk, layout.title_slot_px, TITLE_SIZE_PX, serif=True)
    ]
    probe = Geometry(layout, len(title_lines), 1, 1)
    content_px = probe.content_right - probe.content_left
    subtitle_lines = wrap(layout.subtitle, content_px, SUBTITLE_SIZE_PX)
    note_indent = text_width_px("Note: ", layout.footer_line_px - 2, "bold") if layout.note else 0.0
    note_size = layout.footer_line_px - 2 if layout.template == "horizontal" else layout.footer_size_px
    note_lines = wrap(layout.note, content_px, note_size, first_indent_px=note_indent) if layout.note else []
    geo = Geometry(layout, len(title_lines), len(subtitle_lines), len(note_lines))

    # Nothing may overrun the content box. Cheap to assert, and it is the failure mode a wording
    # change reintroduces silently -- the wrap still succeeds, the render just runs off the edge.
    for label, lines, size, indent in (
        ("subtitle", subtitle_lines, SUBTITLE_SIZE_PX, 0.0),
        ("note", note_lines, note_size, note_indent),
    ):
        for i, line in enumerate(lines):
            width = text_width_px(line, size) + (indent if i == 0 else 0.0)
            assert width <= content_px, (
                f"[{layout.template}] {label} line overruns content box by {width - content_px:.0f}px: {line!r}"
            )
    source_px = runs_width_px([("Data source: ", "bold"), (DATA_SOURCE, "normal")], note_size)
    assert source_px <= content_px, f"[{layout.template}] data source row overruns by {source_px - content_px:.0f}px"

    fig = plt.figure(figsize=TEMPLATES[layout.template].figsize)
    fig.patch.set_facecolor("white")  # legible when the PNG is reviewed on a dark background

    years = data.index.to_numpy()
    housing = data[HOUSING].to_numpy()
    financial = data[FINANCIAL].to_numpy()
    debt = data[DEBT].to_numpy()
    assets = housing + financial

    # Snap the value axis out to whole gridline steps, so the extreme gridlines land on the plot's
    # edges and there is only ever one line at each edge.
    y_ticks = np.arange(
        np.floor(debt.min() / Y_STEP) * Y_STEP,
        np.ceil(assets.max() / Y_STEP) * Y_STEP + Y_STEP / 2,
        Y_STEP,
    )

    tick_label_px = max(text_width_px(f"{t:g}", TICK_SIZE_PX) for t in y_ticks)
    plot_left = geo.content_left + tick_label_px + 6
    plot_right = geo.content_right
    # The y-axis title sits horizontally above the axis, left-aligned on the content box.
    plot_top = geo.band_top + BAND_INSET_PX + AXIS_TITLE_SIZE_PX + HEADER_GAP_PX
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

    fills = {name: palette[i] for name, i in PALETTE_INDEX.items()}
    ax.fill_between(years, 0, housing, facecolor=fills[HOUSING], linewidth=0, gid=f"{SLUG[HOUSING]}__band")
    ax.fill_between(years, housing, assets, facecolor=fills[FINANCIAL], linewidth=0, gid=f"{SLUG[FINANCIAL]}__band")
    ax.fill_between(years, debt, 0, facecolor=fills[DEBT], linewidth=0, gid=f"{SLUG[DEBT]}__band")

    ax.set_xlim(layout.x_ticks[0], layout.x_ticks[-1])
    ax.set_ylim(y_ticks[0], y_ticks[-1])
    ax.set_yticks(y_ticks)
    ax.set_xticks(list(layout.x_ticks))
    ax.set_yticklabels([f"{t:g}" for t in y_ticks])
    ax.set_xticklabels([str(t) for t in layout.x_ticks])

    for spine in ax.spines.values():
        spine.set_visible(False)

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

    contrasts = place_band_labels(ax, layout, years, housing, assets, debt, fills)
    draw_text_slots(fig, geo, layout, title_lines, subtitle_lines, note_lines, note_size)
    return fig, contrasts


def place_band_labels(
    ax: plt.Axes,
    layout: Layout,
    years: np.ndarray,
    housing: np.ndarray,
    assets: np.ndarray,
    debt: np.ndarray,
    fills: dict[str, tuple[float, float, float]],
) -> dict[str, float]:
    """Label each band inside itself, at an x where the band has room for the label.

    Bands, not a legend: three categories is well inside what direct labeling handles, and it
    removes the colour-matching step entirely.

    The anchor x values are per layout and asserted, never assumed. Each band's edges are
    *interpolated over the label's own x span* -- a band can be thick at a label's centre and thin
    at its end, so testing one x answers the wrong question.
    """
    edges = {
        FINANCIAL: (housing, assets),
        HOUSING: (np.zeros_like(housing), housing),
        DEBT: (debt, np.zeros_like(debt)),
    }
    contrasts: dict[str, float] = {}
    for name, x_anchor in layout.band_label_anchors.items():
        lower, upper = edges[name]
        text = DISPLAY_NAME[name]
        width_px = text_width_px(text, BAND_LABEL_SIZE_PX, weight="bold")

        bbox = ax.get_window_extent()
        years_per_px = (ax.get_xlim()[1] - ax.get_xlim()[0]) / bbox.width
        # `bbox` is in display px at the figure's dpi; the figure is 100 template px / inch.
        px_per_template_px = bbox.width / (ax.get_position().width * ax.figure.get_figwidth() * 100)
        half_span = 0.5 * width_px * px_per_template_px * years_per_px

        x0, x1 = x_anchor - half_span, x_anchor + half_span
        assert years[0] <= x0 and x1 <= years[-1], (
            f"[{layout.template}] {name} label runs off the plot ({x0:.0f}..{x1:.0f})"
        )

        span = np.linspace(x0, x1, 40)
        lower_at = np.interp(span, years, lower)
        upper_at = np.interp(span, years, upper)

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
            f"[{layout.template}] {name} band leaves {feasible_high - feasible_low:.2f} units clear "
            f"over the label's span at x={x_anchor}, label cap height is {cap_units:.2f}. "
            "Move the anchor or label this band outside."
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
    layout: Layout,
    title_lines: list[str],
    subtitle_lines: list[str],
    note_lines: list[str],
    note_size: float,
) -> None:
    """Fill the template's slots, in its order, with its labels and at its sizes.

    Drawn at the templates' own sizes rather than sizes that merely look right: the band is correct
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
    if geo.note_y is not None:
        for i, line in enumerate(note_lines):
            runs = [("Note: ", "bold"), (line, "normal")] if i == 0 else [(line, "normal")]
            draw_runs(
                fig,
                geo,
                runs,
                geo.note_y + (i + 1) * layout.footer_line_px - 4,
                note_size,
                FOOTER_INK,
                f"note__line{i + 1}" if i else "note",
            )

    draw_runs(
        fig,
        geo,
        [("Data source: ", "bold"), (DATA_SOURCE, "normal")],
        geo.source_y + layout.footer_line_px - 4,
        note_size,
        FOOTER_INK,
        "data-source",
    )

    # Desktop shares the last row between the tagline (left) and the license (right); mobile has no
    # tagline and gives the license its own full-width row.
    content_px = geo.content_right - geo.content_left
    license_runs = LICENSE_RUNS_FULL
    license_px = runs_width_px(license_runs, layout.footer_size_px)
    license_baseline = geo.last_row_y + layout.footer_size_px

    if layout.has_tagline:
        tagline_px = text_width_px(TAGLINE, layout.footer_size_px)
        gap_px = 12
        if tagline_px + gap_px + license_px > content_px:
            license_runs = LICENSE_RUNS_SHORT
            license_px = runs_width_px(license_runs, layout.footer_size_px)
        assert tagline_px + gap_px + license_px <= content_px, (
            f"footer row overruns: tagline {tagline_px:.0f}px + license {license_px:.0f}px "
            f"> {content_px:.0f}px. Shorten the phrasing, never the name."
        )
        fig.text(
            left,
            geo.fig_y(license_baseline),
            TAGLINE,
            fontsize=layout.footer_size_px * PX_PER_PT,
            color=FOOTER_INK,
            ha="left",
            va="baseline",
            gid="tagline",
        )
        start_px = geo.content_right - license_px
    else:
        # Mobile stacks its rows full width and left-aligned, so the license has 508px to itself.
        assert license_px <= content_px, f"license row overruns by {license_px - content_px:.0f}px"
        start_px = geo.content_left

    draw_runs(fig, geo, license_runs, license_baseline, layout.footer_size_px, FOOTER_INK, "license", start_px=start_px)


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
    log = logging.getLogger("static_viz")
    data = load()
    check(data)
    for layout in LAYOUTS:
        fig, contrasts = build(layout, data)
        export_frame(_Out(HERE), fig, f"{SHORT_NAME}{layout.suffix}", template=layout.template)
        for name, ratio in sorted(contrasts.items()):
            log.info(
                "  [%s] label contrast on %s: %.2f:1 %s",
                layout.template,
                DISPLAY_NAME[name],
                ratio,
                "OK" if ratio >= 4.5 else "BELOW 4.5:1",
            )
        plt.close(fig)


if __name__ == "__main__":
    run()
