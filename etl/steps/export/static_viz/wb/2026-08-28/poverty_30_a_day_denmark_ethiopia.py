"""Recreate the 'Share of the population living above and below $30 a day' comparison chart.

Two rows, one per country, each a 100%-stacked bar split into the share of the population living
below $30 a day (left segment) and above it (right segment) -- the same left-to-right order in both
rows, so a reader can compare across rows without re-reading which colour means what.

$30 a day (2021 international-$, i.e. adjusted for inflation and for differences in the cost of
living between countries) is one of the higher lines the World Bank's Poverty and Inequality
Platform (PIP) reports; it approximates the income level common in today's high-income countries.

Each country is shown at its own most recent *survey-based* estimate -- PIP also extends every
series to the present with growth-based nowcasts, which this step deliberately excludes (see
`load_latest_survey_estimates`), so the two countries' reference years differ, and both years are
named in the Note.

Denmark's above-the-line share is large enough to carry the sentence inside its own segment;
Ethiopia's is a sliver of a percent, so the true proportional width barely renders -- the chart
still draws it at true scale (nothing here rounds it away, unlike a plain "more than 99%" label with
no counterpart), but its own numeric label is dropped rather than squeezed outside the bar: there is
no free space on either side of a 100%-stacked row to put a label without it running off the frame,
and the sentence in the wide segment already states the complementary share.

Replaces the earlier hand-drawn two-row bar graphic used for the same comparison.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this step
fixes is the structure: which text slots exist, in what order, and which share a row.

Figma
-----
Not yet placed in the Charts file. Once it is, record here (see `/create-static-viz` skill's Step 8):
file key, page name and order, frame name and node id, the import mechanics, every text slot's
content, every colour's library name and key, and the fit -- so a later session can redo the handoff
from this file alone.
"""

import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.static_viz import PIXELS_PER_INCH, apply_svg_rcparams, export_frame, source_citation

# Figma-editable text, deterministic ids. Must run before any figure is created.
apply_svg_rcparams()

paths = PathFinder(__file__)

# World Bank PIP encodes a poverty line as its value in cents of the international dollar it is
# quoted in. "3000" is $30.00 -- see POVLINES_DICT in the garden step this reads from.
POVERTY_LINE_CODE = "3000"
POVERTY_LINE_DOLLARS = 30

# 2021 is PIP's current price-reference year (it also still reports the older 2017 vintage for
# comparability with earlier publications). "income or consumption" is PIP's harmonized welfare
# concept -- the one comparable across countries that measure welfare differently.
PPP_VERSION = 2021
WELFARE_TYPE = "income or consumption"

COUNTRIES = ["Denmark", "Ethiopia"]

# PIP's "consolidated" rows are anchored to an actual household survey; "intra/extrapolated" rows
# fill the years between (and after) surveys using national-accounts growth rates. Restricting to
# consolidated rows is what keeps this chart a reported estimate rather than a nowcast -- seven
# countries had their PIP line halved by a nowcast produced from stale growth assumptions in 2025,
# which is the failure mode this guards against.
SURVEY_TABLE_MARKER = "consolidated"

TITLE = "Share of the population living above and below $30 a day"

SUBTITLE = (
    f"Share of the population estimated to live below and above ${POVERTY_LINE_DOLLARS} a day, "
    f"measured in {PPP_VERSION} international-$, which adjusts for inflation and for differences "
    "in the cost of living between countries."
)

# Credited per the /create-static-viz skill's rule: the design is unchanged from the original, only
# the underlying data is refreshed, so both the original author and the refresher are named.
AUTHORS = ["Max Roser", "Daniel Bachler"]

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world’s largest problems."

# The "Static Chart Template_Horizontal" frame, 850x638 -- see TEMPLATES.md in the
# /create-static-viz skill for the full geometry this step lays out against.
WIDTH_PX, HEIGHT_PX = 850, 638
MARGIN_PX = 16
TITLE_Y = 16
CHART_BOTTOM_Y = 559  # Note row's top
SOURCE_Y = 591
FOOTER_Y = 609

# Sized empirically against matplotlib's own default font rather than the template's documented
# sizes (TEMPLATES.md's 25/16/12pt): those are Playfair Display / Lato sizes, which run
# meaningfully narrower than DejaVu Sans at the same point size, so reproducing them verbatim here
# overflows the frame (see who/2026-08-07/height_for_age.py, which sizes down the same way).
TITLE_FONTSIZE = 20
BODY_FONTSIZE = 14
FOOTER_FONTSIZE = 7.5
IN_BAR_FONTSIZE = 12
VALUE_LABEL_FONTSIZE = 11

TITLE_SUBTITLE_GAP = 6  # template px, the header's own auto-layout gap
BAND_INSET = 14  # template px, breathing room inside the chart band
POINTS_PER_PIXEL = 0.72  # a template px is 0.72pt

ROW_GAP_FRAC = 0.35  # gap between the two bars, as a fraction of one bar's height


def run() -> None:
    """Load the World Bank PIP poverty data and render the chart."""
    tb = load_latest_survey_estimates()
    paths.log.info(tb[["country", "year", "headcount_ratio", "headcount_ratio_above"]].to_string(index=False))

    sanity_check(tb)

    citation = source_citation(tb["headcount_ratio"])
    paths.log.info(f"Source citation: {citation}")

    fig = create_visualization(tb, citation)

    export_frame(paths, fig, paths.short_name, template="horizontal")

    plt.close(fig)


def load_latest_survey_estimates() -> Table:
    """Return, for each country in COUNTRIES, its most recent survey-based $30/day estimate.

    Restricted to `table` rows carrying SURVEY_TABLE_MARKER, so a country's series never resolves
    to a growth-nowcasted year -- see the module docstring.
    """
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("poverty")

    tb = tb[
        (tb["poverty_line"] == POVERTY_LINE_CODE)
        & (tb["ppp_version"] == PPP_VERSION)
        & (tb["welfare_type"] == WELFARE_TYPE)
        & (tb["table"].str.contains(SURVEY_TABLE_MARKER, case=False, na=False))
    ]
    tb = tb[tb["country"].isin(COUNTRIES)]

    missing = set(COUNTRIES) - set(tb["country"])
    assert not missing, f"No survey-based ${POVERTY_LINE_DOLLARS}/day estimate found for: {sorted(missing)}"

    tb = tb.sort_values("year").groupby("country", as_index=False, observed=True).tail(1)
    tb = tb.sort_values("country").reset_index(drop=True)

    assert len(tb) == len(COUNTRIES), "Expected exactly one row per country"
    return tb


def sanity_check(tb: Table) -> None:
    below = tb.set_index("country")["headcount_ratio"]
    above = tb.set_index("country")["headcount_ratio_above"]
    for country in COUNTRIES:
        total = below[country] + above[country]
        assert abs(total - 100) < 0.5, f"{country}: below + above = {total:.2f}, expected ~100"
    assert (below >= 0).all() and (above >= 0).all(), "Negative share found"


def text_width_pt(text: str, fontsize: float) -> float:
    """Measured width of `text` set at `fontsize` points, in points."""
    return TextPath((0, 0), text, prop=FontProperties(size=fontsize)).get_extents().width


def wrap_to_width(text: str, fontsize: float, max_width_px: float) -> list[str]:
    """Greedily wrap text into lines that fit `max_width_px` (template px) at `fontsize`.

    Wraps against the *measured* width of the rendered glyphs rather than a character-count
    estimate, which systematically under-fills (see GOTCHAS.md in the /create-static-viz skill).
    """
    max_points = max_width_px * POINTS_PER_PIXEL
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
    return lines


def wrap_to_content_width(text: str, fontsize: float) -> str:
    """Wrap text to fill the template's content width (frame width minus both margins)."""
    return "\n".join(wrap_to_width(text, fontsize, WIDTH_PX - 2 * MARGIN_PX))


def format_share(pct: float) -> str:
    """Format a share for display, rounding extreme tails to a qualitative-but-precise form.

    A share under 0.5% would round to "0%" at one decimal, which reads as a data error rather than
    as a genuinely tiny share -- so those get "less than 0.5%" / "more than 99.5%" instead of a
    misleading round number. Everything else gets one decimal.
    """
    if pct < 0.5:
        return "less than 0.5%"
    if pct > 99.5:
        return "more than 99.5%"
    return f"{pct:.1f}%"


def create_visualization(tb: Table, citation: str) -> plt.Figure:
    """Build the two-row 100%-stacked bar chart.

    Layout notes:
    - One row per country, each split into [below $30/day, above $30/day], in that fixed
      left-to-right order for both rows so colour position, not just colour, tells the two apart.
    - A value label sits inside a segment when it measures wide enough to hold one; otherwise the
      label sits just to the right of the bar, since the only segments this narrow are the last one
      in the row.
    - The sentence describing the majority segment is drawn inside it, in white, left-aligned a
      small inset from the segment's own left edge.
    - Bars, gaps and font sizes are all derived from the frame's own geometry, not hardcoded pixel
      guesses -- see the position derivation below.
    """
    sns.set_style("ticks")
    palette = sns.color_palette("deep")
    color_above = palette[2]  # placeholder -- Figma assigns the real teal
    color_below = palette[4]  # placeholder -- Figma assigns the real maroon

    fig = plt.figure(figsize=(WIDTH_PX / PIXELS_PER_INCH, HEIGHT_PX / PIXELS_PER_INCH))
    fig.patch.set_facecolor("white")  # legible when reviewed on a dark background; dropped on SVG save

    def fx(x_px: float) -> float:
        return x_px / WIDTH_PX

    def fy(y_px: float) -> float:
        return 1 - y_px / HEIGHT_PX

    def line_px(points: float) -> float:
        """A line of text at this point size, in template pixels."""
        return 1.3 * points / POINTS_PER_PIXEL

    content_width_px = WIDTH_PX - 2 * MARGIN_PX

    # --- header ---
    subtitle = wrap_to_content_width(SUBTITLE, BODY_FONTSIZE)
    subtitle_lines = subtitle.count("\n") + 1
    subtitle_y = TITLE_Y + line_px(TITLE_FONTSIZE) + TITLE_SUBTITLE_GAP
    fig.text(
        fx(MARGIN_PX), fy(TITLE_Y), TITLE, ha="left", va="top", fontsize=TITLE_FONTSIZE, color="#111111", gid="title"
    )
    fig.text(
        fx(MARGIN_PX),
        fy(subtitle_y),
        subtitle,
        ha="left",
        va="top",
        fontsize=BODY_FONTSIZE,
        color="#555555",
        gid="subtitle",
    )
    band_top_px = subtitle_y + subtitle_lines * line_px(BODY_FONTSIZE) + BAND_INSET

    # --- footer ---
    note_text = (
        "Note: Each country is shown at its own most recent household-survey estimate, which are "
        f"not the same year: {int(tb.set_index('country').loc['Denmark', 'year'])} for Denmark and "
        f"{int(tb.set_index('country').loc['Ethiopia', 'year'])} for Ethiopia."
    )
    note = wrap_to_content_width(note_text, FOOTER_FONTSIZE)
    note_lines = note.count("\n") + 1
    # The note grows upwards from its template row, so a longer note eats into the chart band
    # rather than running off the bottom of the frame.
    note_top_px = CHART_BOTTOM_Y - (note_lines - 1) * line_px(FOOTER_FONTSIZE)
    fig.text(
        fx(MARGIN_PX),
        fy(note_top_px),
        note,
        ha="left",
        va="top",
        fontsize=FOOTER_FONTSIZE,
        color="#858585",
        gid="note",
    )
    fig.text(
        fx(MARGIN_PX),
        fy(SOURCE_Y),
        f"Data source: {citation}",
        ha="left",
        va="top",
        fontsize=FOOTER_FONTSIZE,
        color="#858585",
        gid="data-source",
    )
    fig.text(
        fx(MARGIN_PX),
        fy(FOOTER_Y),
        TAGLINE,
        ha="left",
        va="top",
        fontsize=FOOTER_FONTSIZE,
        color="#858585",
        gid="tagline",
    )
    # Two names measure 233pt at 11pt Lato, well inside the 263px/197pt license slot once "the
    # authors" is dropped -- see TEMPLATES.md's fitting rule for a multi-author license line.
    license_text = f"Licensed under CC-BY by {' and '.join(AUTHORS)}"
    fig.text(
        fx(WIDTH_PX - MARGIN_PX),
        fy(FOOTER_Y),
        license_text,
        ha="right",
        va="top",
        fontsize=FOOTER_FONTSIZE,
        color="#858585",
        gid="license",
    )

    band_bottom_px = note_top_px - BAND_INSET

    # --- chart: two 100%-stacked rows ---
    ax = fig.add_axes(
        (
            fx(MARGIN_PX),
            fy(band_bottom_px),
            content_width_px / WIDTH_PX,
            (band_bottom_px - band_top_px) / HEIGHT_PX,
        )
    )
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 1)
    ax.set_axis_off()
    ax.patch.set_visible(False)  # keeps the SVG transparent; the template supplies the background

    n_rows = len(COUNTRIES)
    row_height = 1 / (n_rows + (n_rows - 1) * ROW_GAP_FRAC)
    row_step = row_height * (1 + ROW_GAP_FRAC)

    for i, country in enumerate(COUNTRIES):
        row = tb.set_index("country").loc[country]
        below_pct, above_pct = float(row["headcount_ratio"]), float(row["headcount_ratio_above"])
        row_top = 1 - i * row_step
        row_bottom = row_top - row_height
        row_center = (row_top + row_bottom) / 2

        segments = [("below", 0, below_pct, color_below), ("above", below_pct, 100, color_above)]
        for name, start, end, color in segments:
            ax.axhspan(
                row_bottom,
                row_top,
                xmin=start / 100,
                xmax=end / 100,
                facecolor=color,
                edgecolor="none",
                gid=f"{country.lower()}__{name}",
            )

        # The wider segment carries the sentence, drawn inside it in white; the narrower one gets a
        # plain value label, inside the segment if it is wide enough to hold one, otherwise just to
        # the segment's right -- the only segment this narrow is always the row's last one.
        wide_name = "below" if below_pct >= above_pct else "above"
        wide_pct = below_pct if wide_name == "below" else above_pct
        narrow_pct = above_pct if wide_name == "below" else below_pct
        wide_start = 0 if wide_name == "below" else below_pct
        narrow_start = below_pct if wide_name == "below" else 0

        direction = "less" if wide_name == "below" else "more"
        sentence = f"{format_share(wide_pct)} of {country}’s population is living on {direction} than ${POVERTY_LINE_DOLLARS} per day"
        # Wrapped to the segment's own pixel width (with a small inset), never to the full row --
        # the segment is what has to visually contain the sentence. One `ax.text` call per line: a
        # single call with embedded newlines gets no `text-anchor` in the emitted SVG (see
        # WRITING-THE-STEP.md's anchor rule), which is moot for `ha="left"` here but kept for
        # consistency with the rest of the skill's guidance.
        inset_px = 8
        sentence_lines = wrap_to_width(sentence, IN_BAR_FONTSIZE, wide_pct / 100 * content_width_px - 2 * inset_px)
        line_height_data = line_px(IN_BAR_FONTSIZE) / (band_bottom_px - band_top_px)
        block_top = row_center + (len(sentence_lines) - 1) * line_height_data / 2
        for line_i, line in enumerate(sentence_lines):
            ax.text(
                wide_start + inset_px / content_width_px * 100,
                block_top - line_i * line_height_data,
                line,
                ha="left",
                va="center",
                fontsize=IN_BAR_FONTSIZE,
                color="white",
                fontweight="bold",
                gid=f"{country.lower()}__label",
            )

        # A segment narrower than its own label at this font size gets no label at all, rather than
        # one squeezed outside it -- there is no free space on either side of a 100%-stacked bar to
        # put an outside label without it running off the frame, and the sentence in the wide
        # segment already states the narrow share's complement. Measured against the label's own
        # rendered width, not assumed.
        label_text = format_share(narrow_pct)
        label_width_axes_frac = text_width_pt(label_text, VALUE_LABEL_FONTSIZE) / POINTS_PER_PIXEL / content_width_px
        fits_inside = narrow_pct / 100 > label_width_axes_frac + 0.02
        if fits_inside:
            ax.text(
                narrow_start + narrow_pct / 2,
                row_center,
                label_text,
                ha="center",
                va="center",
                fontsize=VALUE_LABEL_FONTSIZE,
                color="white",
                fontweight="bold",
                gid=f"{country.lower()}__value-label",
            )

    return fig
