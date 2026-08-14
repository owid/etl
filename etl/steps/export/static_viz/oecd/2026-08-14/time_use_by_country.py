"""Recreate the 'How do people spend their time?' stacked-bar chart from the OECD Time Use Database.

One row per country, each splitting the 1440 minutes of a day into ten activity groups, sorted by
time spent on paid work. A column right of the bars totals the leisure groups, as the original
chart by Esteban Ortiz-Ospina did. The groups come precomputed from the garden step
(`time_use_chart_groups`), where the regrouping of the OECD's detailed activities is documented
and asserted; this step only lays them out.

On desktop, category names sit above the bars, staggered over tiers, each centred on its segment
in the top row with a leader line dropping to it from the upper tiers. Values are written inside
segments wide enough to hold them, sleep as hours and minutes. Survey years differ by country
(1999-2024), so each country label carries its year — the one deliberate addition over the
original, whose surveys spanned a narrower window.

Two versions are emitted, following the static-chart templates:

- desktop, 850x1095 (vertical template): all footer rows; the Note carries the survey-year span
  and the age-of-reference exceptions.
- mobile, 540x824: no Note slot, so the age caveat and the survey-year reference fold into the
  subtitle; in-bar values appear only where they fit at the same type size. The positional
  header cannot fit at this width — measured, not assumed: there is no x at which the
  seeing-friends leader clears every placement of the TV label — so the same names flow as
  wrapped lines above the chart instead, in bar order and category colors, without leaders.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this
step fixes is the data, the structure (which text slots exist, in what order), the proportions,
and the row layout. Segment colors are seaborn "deep" positions so the chart moves with the shared
palette; the three leisure groups are one hue at three tints, as the original drew them.
"""

import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties
from matplotlib.textpath import TextPath
from owid.catalog import Table

from etl.helpers import PathFinder

# Use non-path text so SVGs stay editable in Figma
matplotlib.rcParams["svg.fonttype"] = "none"
# Set deterministic hash for reproducible SVG output
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)

MINUTES_PER_DAY = 1440

# The ten groups, in bar order. Colors are seaborn "deep" positions, or tints of one, arranged
# so groups from the same top-level OECD category share a hue family: reds for paid work or
# study, grays for personal care (sleep, other personal care, eating), earth tones for unpaid
# work, blues for leisure. `tier` staggers the header labels: tier 0 sits just above the top bar
# (wide segments whose name fits directly over them), higher tiers stack upwards and drop a
# leader line onto their segment. `dx` nudges a label along the bar, in minutes of the top row,
# where centring it would collide with a neighbour or with a leader. `sublabel` is a smaller
# second line; `label2` a same-style second line (for names too wide for the gaps between
# leaders on one line). The arrangement is tuned against the current top row and re-checked by
# eye after every data update, per the skill's render-and-look loop.
GROUPS = [
    {"column": "paid_work", "label": "Paid work", "color": ("deep", 3), "tier": 0, "dx": 0},
    {
        "column": "education",
        "label": "Education",
        "sublabel": "In school & study",
        "color": ("tint", 3, 0.45),
        "tier": 1,
        "dx": 0,
    },
    {"column": "sleep", "label": "Sleep", "color": ("deep", 7), "tier": 0, "dx": 0},
    {
        "column": "other_unpaid_work",
        "label": "Other unpaid work",
        "sublabel": "Care work, volunteering",
        "color": ("deep", 5),
        "tier": 4,
        "dx": 0,
    },
    {
        "column": "housework_and_shopping",
        "label": "Housework",
        "label2": "& shopping",
        "color": ("tint", 5, 0.45),
        "tier": 0,
        "dx": 10,
    },
    {
        "column": "personal_care",
        "label": "Personal care",
        "color": ("tint", 7, 0.4),
        "tier": 2,
        "dx": -25,
    },
    {
        "column": "eating_and_drinking",
        "label": "Eating & drinking",
        "color": ("tint", 7, 0.65),
        "tier": 3,
        "dx": 0,
        "leader_dx": -40,
    },
    {"column": "tv_and_radio", "label": "TV & Radio", "color": ("deep", 0), "tier": 0, "dx": -40},
    {
        "column": "seeing_friends",
        "label": "Seeing friends",
        "color": ("tint", 0, 0.35),
        "tier": 2,
        "dx": 0,
    },
    {
        "column": "other_leisure",
        "label": "Other leisure",
        "color": ("tint", 0, 0.6),
        "tier": 1,
        "dx": 15,
    },
]

TOTAL_LEISURE_COLUMN = "total_leisure"

TITLE = "How do people spend their time?"

# Credited on the license line: the original chart's author, and this refresh's.
AUTHORS = "Esteban Ortiz-Ospina and Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"
LEADER_COLOR = "#999999"
# In-bar values are white on saturated fills and dark on light tints; the switch is on luminance.
DARK_VALUE_COLOR = "#444444"
LUMINANCE_THRESHOLD = 0.55

# A template pixel in points (100 template px per inch over 72 points per inch).
POINTS_PER_PIXEL = 0.72

# Template pixels per inch: the figure is sized so the saved image keeps the template proportions.
PIXELS_PER_INCH = 100

# Gap between the title block and the subtitle, calibrated so a two-line title puts the subtitle
# at the templates' own y=80.
TITLE_SUBTITLE_GAP = 6

# Vertical rhythm between the subtitle and the header tiers, in text lines.
SUBTITLE_GAP = 0.8

# Horizontal breathing room, in template pixels: between a country label and its bar, between the
# bars and the total-leisure column, and inside a segment around its value.
COUNTRY_LABEL_PAD = 8
TOTAL_COLUMN_GAP = 10
VALUE_PAD = 3

# Header geometry, in template pixels: the height of one tier of labels (kept above one line of
# header text, so adjacent tiers cannot bleed into each other), and the gap leader lines keep
# from labels and bars.
TIER_HEIGHT = 18
LEADER_GAP = 3

# Flowed-header geometry (mobile), in template pixels: the gap between names on a line, the
# extra leading between lines, and how much wider bold text measures than the regular-weight
# TextPath measurement.
FLOW_GAP = 16
FLOW_LINE_PAD = 4
BOLD_FACTOR = 1.06

# Bars fill this share of a row's pitch.
BAR_FRACTION = 0.8

# The two layouts, taken from the static-chart template frames. Geometry is in template pixels,
# y measured from the top edge as Figma reports it. `full_footer` separates desktop (Note and
# tagline rows) from mobile (neither). Row positions come from "Static Chart Template_Vertical"
# (850x1095) and "Static Chart Template_Mobile (example 2)" (540x824).
LAYOUTS = {
    "time_use_by_country": {
        "size": (850, 1095),
        "margin": 16,
        "title_y": 16,
        "chart_bottom_y": 997,
        "note_y": 1013,
        "source_y": 1046,
        "footer_y": 1066,
        "full_footer": True,
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 7.75,
        "country_fontsize": 9,
        "value_fontsize": 8.75,
        "header_fontsize": 9.5,
        "subheader_fontsize": 8.25,
        # Width reserved for the total-leisure column, in template pixels.
        "total_column_px": 74,
        "with_mins_suffix": True,
        "header_mode": "tiers",
    },
    "time_use_by_country_mobile": {
        "size": (540, 824),
        "margin": 16,
        "title_y": 16,
        # The template's chart area ends at the source row (y=770); a small inset keeps the last
        # bar off it.
        "chart_bottom_y": 758,
        "note_y": None,
        "source_y": 770,
        "footer_y": 791,
        "full_footer": False,
        "title_fontsize": 16,
        "body_fontsize": 10.5,
        "footer_fontsize": 8.75,
        "country_fontsize": 7.5,
        "value_fontsize": 7.5,
        "header_fontsize": 8,
        "subheader_fontsize": 7,
        "total_column_px": 46,
        "with_mins_suffix": False,
        # At this width the positional header cannot avoid its own leaders — measured, not
        # assumed: the seeing-friends leader has no x that clears any placement of the TV label —
        # so the same names flow as wrapped lines instead, in bar order and category colors.
        "header_mode": "flow",
    },
}

# Both layouts share the original chart's subtitle; mobile appends what its missing Note slot
# would have carried — the age caveat is about what the chart claims, so it cannot be dropped.
SUBTITLE = "Averages of minutes per day from time-use diaries for people between 15 and 64."
MOBILE_NOTE = "Ages differ in a few countries, and each country's survey year is shown in brackets."


def run() -> None:
    """Load data, render and save both versions of the chart."""
    tb, ages = load_chart_groups()
    paths.log.info(f"Loaded {len(tb)} countries, surveys {tb['year'].min()}-{tb['year'].max()}")

    source_citation = build_source_citation(tb)
    paths.log.info(f"Source citation: {source_citation}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(tb, ages, source_citation, layout)
        # No bbox_inches="tight": cropping to content would change the proportions the template
        # fixes. The PNG keeps an opaque canvas (it is what a human reviews, often on a dark
        # editor background); the SVG is saved transparent so the Figma template's background,
        # logo and text are not covered by matplotlib's white figure patch.
        paths.export_fig(fig, short_name, ["png"], dpi=300)
        paths.export_fig(fig, short_name, ["svg"], transparent=True)
        plt.close(fig)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_chart_groups() -> tuple[Table, dict[str, str]]:
    """Load the precomputed chart groups (total population), sorted by paid work.

    Returns the table plus the age-of-reference exceptions (country -> age range) for the note.
    """
    ds = paths.load_dataset("time_use")
    tb = ds.read("time_use_chart_groups")
    tb = tb[tb["sex"] == "total"].drop(columns=["sex"]).sort_values("paid_work", ascending=False)

    detail = ds.read("time_use")
    detail = detail[detail["sex"] == "total"]
    ages = {
        str(row["country"]): str(row["age_of_reference"])
        for _, row in detail.iterrows()
        if str(row["age_of_reference"]) != "15-64"
    }

    group_columns = [group["column"] for group in GROUPS]
    assert not set(group_columns + [TOTAL_LEISURE_COLUMN]) - set(tb.columns), "Chart group columns changed."
    assert len(tb) >= 35, "Country coverage shrank."
    assert tb["country"].is_unique, "One row per country expected."
    # The groups partition the day (asserted strictly in garden; re-checked here at the source's
    # own rounding tolerance so a broken load cannot draw bars that misrepresent shares).
    assert ((tb[group_columns].sum(axis=1) - MINUTES_PER_DAY).abs() < 2.0).all(), "Rows do not sum to 24 hours."
    assert set(ages) == {"Australia", "China", "Lithuania"}, "Age-of-reference exceptions changed at the source."

    return tb, ages


def build_source_citation(tb: Table) -> str:
    """Cite the producer behind the chart from the origins, as `producer (year)`."""
    years: dict[str, list[str]] = {}
    for origin in tb["paid_work"].metadata.origins:
        year = origin.date_published.split("-")[0] if origin.date_published else ""
        seen = years.setdefault(origin.producer, [])
        if year and year not in seen:
            seen.append(year)
    return "; ".join(f"{producer} ({'; '.join(sorted(ys))})" for producer, ys in years.items())


# ---------------------------------------------------------------------------
# Text and color helpers
# ---------------------------------------------------------------------------


def tint(color, weight: float) -> tuple[float, float, float]:
    """Blend a color towards white. weight=0 keeps it, weight=1 turns it white."""
    r, g, b = to_rgb(color)
    return (r + (1 - r) * weight, g + (1 - g) * weight, b + (1 - b) * weight)


def resolve_color(spec: tuple, palette) -> tuple[float, float, float]:
    """Resolve a ("deep", i) palette position or ("tint", i, w) blend into an RGB color."""
    if spec[0] == "deep":
        return palette[spec[1]]
    return tint(palette[spec[1]], spec[2])


def luminance(color) -> float:
    """Relative luminance, for choosing white or dark text on a fill."""
    r, g, b = to_rgb(color)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def header_text_color(spec: tuple, palette) -> tuple[float, float, float]:
    """A pale group's own fill is unreadable as text on the light background, so its header
    label keeps the hue at a much shallower tint."""
    if spec[0] == "tint":
        return tint(palette[spec[1]], spec[2] * 0.4)
    return palette[spec[1]]


def text_width_px(text: str, fontsize: float) -> float:
    """Measured width of rendered text, in template pixels."""
    if not text.strip():
        return 0.0
    points = TextPath((0, 0), text, prop=FontProperties(size=fontsize)).get_extents().width
    return points / POINTS_PER_PIXEL


def wrap_to_width(text: str, max_px: float, fontsize: float) -> str:
    """Wrap text greedily against measured glyph widths, in template pixels."""
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and text_width_px(candidate, fontsize) > max_px:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\n".join(lines)


def format_sleep(minutes: float) -> list[str]:
    """Sleep value candidates, longest first: '8 hours 28 mins', '8h 28m', '508'."""
    hours, mins = divmod(round(minutes), 60)
    hours_word = "hour" if hours == 1 else "hours"
    if mins == 0:
        return [f"{hours} {hours_word}", f"{hours}h", f"{round(minutes)}"]
    mins_word = "min" if mins == 1 else "mins"
    return [f"{hours} {hours_word} {mins} {mins_word}", f"{hours}h {mins}m", f"{round(minutes)}"]


def value_candidates(column: str, minutes: float, with_suffix: bool) -> list[str]:
    """In-bar label candidates for a segment, longest first."""
    if column == "sleep":
        return format_sleep(minutes)
    if column == "paid_work" and with_suffix:
        return [f"{round(minutes)} mins", f"{round(minutes)}"]
    return [f"{round(minutes)}"]


def fit_text(candidates: list[str], available_px: float, fontsize: float) -> str | None:
    """The longest candidate that fits the available width, or None."""
    for candidate in candidates:
        if text_width_px(candidate, fontsize) + 2 * VALUE_PAD <= available_px:
            return candidate
    return None


def slugify(name: str) -> str:
    """Layer-panel-friendly id: lowercase, hyphens for spaces and underscores."""
    return name.lower().replace(" & ", "-").replace(" ", "-").replace("_", "-")


def flow_header_lines(layout: dict, available_px: float) -> list[list[tuple[float, str, str, str]]]:
    """Arrange the group names into flowed lines for the narrow frame.

    Returns one list per line of (x offset in px, name, parenthetical sublabel, column).
    """
    lines: list[list[tuple[float, str, str, str]]] = [[]]
    x = 0.0
    for group in GROUPS:
        main = group["label"] + (f" {group['label2']}" if group.get("label2") else "")
        sub = f" ({group['sublabel'].lower()})" if group.get("sublabel") else ""
        width = text_width_px(main, layout["header_fontsize"]) * BOLD_FACTOR + text_width_px(
            sub, layout["subheader_fontsize"]
        )
        if x > 0 and x + width > available_px:
            lines.append([])
            x = 0.0
        lines[-1].append((x, main, sub, group["column"]))
        x += width + FLOW_GAP
    return lines


def build_note(tb: Table, ages: dict[str, str], layout: dict) -> str:
    """Compose the Note row: survey-year span and the age-of-reference exceptions, from the data."""
    described = {
        "15 and more": "15 and over",
        "15-74": "15 to 74",
        "20-64": "20 to 64",
    }
    exceptions = ", ".join(f"{country} ({described.get(age, age)})" for country, age in sorted(ages.items()))
    text = (
        f"Note: Each country's most recent time-use survey is shown, with its year in brackets; "
        f"survey years range from {tb['year'].min()} to {tb['year'].max()}. "
        f"Estimates cover people aged 15 to 64, except in {exceptions}."
    )
    max_px = layout["size"][0] - 2 * layout["margin"]
    wrapped = wrap_to_width(text, max_px, layout["footer_fontsize"])
    assert wrapped.count("\n") + 1 <= 2, "Note exceeds the template's two-line slot."
    return wrapped


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def create_visualization(tb: Table, ages: dict[str, str], source_citation: str, layout: dict) -> plt.Figure:
    """Build one version of the stacked-bar chart.

    Layout notes:
    - One axes carries the bars; x is minutes of the day (0-1440) plus a text column for total
      leisure, y is one unit per country row, row 0 at the top. The category header and the
      country labels are drawn outside the axes box (clipping is off).
    - Values are written inside segments wide enough to hold them at the layout's type size.
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    palette = sns.color_palette("deep")

    width_px, height_px = layout["size"]
    margin_px = layout["margin"]
    body_fontsize = layout["body_fontsize"]

    def fx(x_px: float) -> float:
        return x_px / width_px

    def fy(y_px: float) -> float:
        return 1 - y_px / height_px

    def line_px(points: float) -> float:
        """A line of text at this point size, in template pixels."""
        return 1.3 * points / POINTS_PER_PIXEL

    fig = plt.figure(figsize=(width_px / PIXELS_PER_INCH, height_px / PIXELS_PER_INCH))
    fig.patch.set_facecolor("white")

    # --- header: title, then subtitle directly beneath it ---
    title = wrap_to_width(TITLE, width_px - 2 * margin_px, layout["title_fontsize"])
    title_lines = title.count("\n") + 1
    subtitle_y = layout["title_y"] + title_lines * line_px(layout["title_fontsize"]) + TITLE_SUBTITLE_GAP

    subtitle = SUBTITLE if layout["full_footer"] else f"{SUBTITLE} {MOBILE_NOTE}"
    subtitle = wrap_to_width(subtitle, width_px - 2 * margin_px, body_fontsize)
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

    # --- footer, in the slots the static-chart templates define ---
    footer_fontsize = layout["footer_fontsize"]
    if layout["full_footer"]:
        fig.text(
            fx(margin_px),
            fy(layout["note_y"]),
            build_note(tb, ages, layout),
            ha="left",
            va="top",
            fontsize=footer_fontsize,
            color=MUTED_COLOR,
            gid="note",
        )
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
    # Desktop's tagline and license share one row, so each wraps within its template slot width
    # (467px and 263px) rather than running into the other. Mobile stacks its rows full-width.
    shares_tagline_row = layout["full_footer"]
    if shares_tagline_row:
        fig.text(
            fx(margin_px),
            fy(layout["footer_y"]),
            wrap_to_width(TAGLINE, 467, footer_fontsize),
            ha="left",
            va="top",
            fontsize=footer_fontsize,
            color="#888888",
            gid="tagline",
        )
    license_text = f"Licensed under CC-BY by the authors {AUTHORS}"
    if shares_tagline_row:
        license_text = wrap_to_width(license_text, 263, footer_fontsize)
    fig.text(
        fx(width_px - margin_px if shares_tagline_row else margin_px),
        fy(layout["footer_y"]),
        license_text,
        ha="right" if shares_tagline_row else "left",
        va="top",
        fontsize=footer_fontsize,
        color="#888888",
        gid="license",
    )

    # --- the chart band: the category header, then the bar rows, down to the chart bottom ---
    subtitle_bottom_px = subtitle_y + subtitle_lines * line_px(body_fontsize) + SUBTITLE_GAP * line_px(body_fontsize)
    if layout["header_mode"] == "tiers":
        n_tiers = max(group["tier"] for group in GROUPS) + 1
        # Tier 0's label line, one TIER_HEIGHT per higher tier, plus the sublabel line that hangs
        # below the tiered labels into the leader zone.
        header_px = line_px(layout["header_fontsize"]) + n_tiers * TIER_HEIGHT + line_px(layout["subheader_fontsize"])
        flow_lines = None
    else:
        flow_lines = flow_header_lines(layout, width_px - 2 * margin_px)
        flow_line_px = line_px(layout["header_fontsize"]) + FLOW_LINE_PAD
        # The flowed lines, plus a band for the two-line total-leisure column header.
        header_px = len(flow_lines) * flow_line_px + 2 * line_px(layout["header_fontsize"]) + LEADER_GAP
    chart_top_px = subtitle_bottom_px + header_px
    chart_bottom_px = layout["chart_bottom_y"]

    # Country labels sit left of the bars; measure the widest to size the reserve.
    country_labels = [f"{country} ({year})" for country, year in zip(tb["country"].tolist(), tb["year"].tolist())]
    country_space_px = (
        max(text_width_px(label, layout["country_fontsize"]) for label in country_labels) + COUNTRY_LABEL_PAD
    )

    plot_left_px = margin_px + country_space_px
    plot_right_px = width_px - margin_px
    plot_width_px = plot_right_px - plot_left_px
    bar_width_px = plot_width_px - layout["total_column_px"]
    px_per_min = bar_width_px / MINUTES_PER_DAY

    n_rows = len(tb)
    row_pitch_px = (chart_bottom_px - chart_top_px) / n_rows

    ax = fig.add_axes(
        (
            fx(plot_left_px),
            fy(chart_bottom_px),
            plot_width_px / width_px,
            (chart_bottom_px - chart_top_px) / height_px,
        )
    )
    ax.set_axis_off()
    ax.patch.set_visible(False)
    ax.set_xlim(0, plot_width_px / px_per_min)
    ax.set_ylim(n_rows, 0)  # row 0 at the top; the header lives above it, at negative y

    group_colors = {group["column"]: resolve_color(group["color"], palette) for group in GROUPS}

    # --- bars, one row per country ---
    for row in range(n_rows):
        country_row = tb.iloc[row]
        slug = slugify(str(country_row["country"]))
        y_center = row + 0.5

        ax.text(
            -COUNTRY_LABEL_PAD / px_per_min,
            y_center,
            country_labels[row],
            ha="right",
            va="center",
            fontsize=layout["country_fontsize"],
            color=TEXT_COLOR,
            gid=f"{slug}__label",
        )

        left = 0.0
        for group in GROUPS:
            column = group["column"]
            minutes = float(country_row[column])
            color = group_colors[column]
            ax.barh(
                y_center,
                minutes,
                left=left,
                height=BAR_FRACTION,
                color=color,
                linewidth=0,
                gid=f"{slug}__{slugify(column)}",
            )
            label = fit_text(
                value_candidates(column, minutes, layout["with_mins_suffix"]),
                minutes * px_per_min,
                layout["value_fontsize"],
            )
            if label:
                ax.text(
                    left + minutes / 2,
                    y_center,
                    label,
                    ha="center",
                    va="center",
                    fontsize=layout["value_fontsize"],
                    color="white" if luminance(color) < LUMINANCE_THRESHOLD else DARK_VALUE_COLOR,
                    gid=f"{slug}__{slugify(column)}-value",
                )
            left += minutes

        total_leisure = round(float(country_row[TOTAL_LEISURE_COLUMN]))
        total_label = f"{total_leisure} mins" if layout["with_mins_suffix"] else f"{total_leisure}"
        ax.text(
            MINUTES_PER_DAY + TOTAL_COLUMN_GAP / px_per_min,
            y_center,
            total_label,
            ha="left",
            va="center",
            fontsize=layout["value_fontsize"],
            color=TEXT_COLOR,
            gid=f"{slug}__total-leisure",
        )

    # --- category header: staggered labels above the top row, leaders dropping to segments ---
    def rows_above(px_above: float) -> float:
        """A height above the axes' top edge, in row units (negative y on this axes)."""
        return -px_above / row_pitch_px

    if flow_lines is not None:
        # Flowed header (narrow frame): the same names in the same order and colors, wrapped as
        # lines between the subtitle and the bars, without positional leaders.
        flow_line_px = line_px(layout["header_fontsize"]) + FLOW_LINE_PAD
        for line_index, line in enumerate(flow_lines):
            y_px = subtitle_bottom_px + line_index * flow_line_px
            for x_offset, main, sub, column in line:
                spec = next(group["color"] for group in GROUPS if group["column"] == column)
                fig.text(
                    fx(margin_px + x_offset),
                    fy(y_px),
                    main,
                    ha="left",
                    va="top",
                    fontsize=layout["header_fontsize"],
                    fontweight="bold",
                    color=header_text_color(spec, palette),
                    gid=f"header__{slugify(column)}",
                )
                if sub:
                    fig.text(
                        fx(margin_px + x_offset + text_width_px(main, layout["header_fontsize"]) * BOLD_FACTOR),
                        fy(y_px + 1),
                        sub,
                        ha="left",
                        va="top",
                        fontsize=layout["subheader_fontsize"],
                        color=header_text_color(spec, palette),
                        gid=f"header__{slugify(column)}-sublabel",
                    )

    top_row = tb.iloc[0]
    cumulative = 0.0
    for group in GROUPS if flow_lines is None else []:
        minutes = float(top_row[group["column"]])
        segment_mid = cumulative + minutes / 2
        cumulative += minutes

        color = header_text_color(group["color"], palette)
        slug = slugify(group["column"])
        sublabel = group.get("sublabel")
        label2 = group.get("label2")
        second_line_px = (
            line_px(layout["subheader_fontsize"]) if sublabel else line_px(layout["header_fontsize"]) if label2 else 0.0
        )
        label_bottom_px = LEADER_GAP + group["tier"] * TIER_HEIGHT + second_line_px

        # Paid work is the first, widest segment; its name is left-aligned at the bars' start, as
        # in the original chart. Everything else centres on its segment (plus any nudge).
        at_start = group["column"] == "paid_work"
        x = 0.0 if at_start else segment_mid + group["dx"]

        ax.text(
            x,
            rows_above(label_bottom_px),
            group["label"],
            ha="left" if at_start else "center",
            va="bottom",
            fontsize=layout["header_fontsize"],
            fontweight="bold",
            color=color,
            gid=f"header__{slug}",
        )
        if sublabel or label2:
            ax.text(
                x,
                rows_above(label_bottom_px),
                sublabel or label2,
                ha="left" if at_start else "center",
                va="top",
                fontsize=layout["subheader_fontsize"] if sublabel else layout["header_fontsize"],
                fontweight="normal" if sublabel else "bold",
                color=color,
                gid=f"header__{slug}-sublabel",
            )
        if group["tier"] > 0:
            leader_top_px = label_bottom_px - second_line_px - LEADER_GAP
            # A leader may drop off-centre within its segment (leader_dx) where the centre would
            # run through a lower tier's label.
            leader_x = segment_mid + group.get("leader_dx", 0)
            ax.plot(
                [leader_x, leader_x],
                [rows_above(leader_top_px), rows_above(1.0)],
                color=LEADER_COLOR,
                linewidth=0.7,
                solid_capstyle="butt",
                gid=f"header__{slug}-leader",
            )

    # Total-leisure column header, over its own column, at the height of tier 0.
    ax.text(
        MINUTES_PER_DAY + TOTAL_COLUMN_GAP / px_per_min,
        rows_above(LEADER_GAP),
        "Total\nleisure",
        ha="left",
        va="bottom",
        fontsize=layout["header_fontsize"],
        fontweight="bold",
        color="#333333",
        gid="header__total-leisure",
    )

    # Drop clipping everywhere so labels outside the axes survive into the SVG whole.
    for artist in fig.findobj():
        artist.set_clip_on(False)

    return fig
