"""Recreate the 'How do people spend their time?' stacked-bar chart from the OECD Time Use Database.

One row per country, each splitting the 1440 minutes of a day into ten activity groups, sorted by
time spent on paid work. A column right of the bars totals the leisure groups, as the original
chart by Esteban Ortiz-Ospina did. The groups come precomputed from the garden step
(`time_use_chart_groups`), where the regrouping of the OECD's detailed activities is documented
and asserted; this step only lays them out.

The groups are shown under the OECD's own four top-level categories, which is the one structural
change from the original chart: the ten groups are ordered so that each category's members are
adjacent, and a bracket above the bars spans each category. That costs a reordering — unpaid work
now sits right of personal care rather than left of it — and it is what lets a reader see that
sleep, eating and personal care are one thing. Flag it at design review rather than letting it
pass as polish.

Values are written inside segments wide enough to hold them, sleep as hours and minutes. Survey
years differ by country (1999-2024), so each country label carries its year — the original's
surveys spanned a narrower window and it named none of them.

Two versions are emitted, following the static-chart templates:

- desktop, 850x1095 (vertical template): group names sit above the bars, staggered over tiers and
  centred on their segment in the top row, with a leader line dropping to the segment from the
  upper tiers. `solve_header_layout` places them; it never hand-places a label, so the header
  survives a data update that changes the top row's proportions. All footer rows are present, and
  the Note carries the survey-year span and the age-of-reference exceptions.
- mobile, 540x824: no Note slot, so the age caveat and the survey-year reference fold into the
  subtitle. The positional header cannot fit at this width — measured, not assumed: the
  seeing-friends segment is 2px wide there — so the same names are listed by category above the
  chart, in bar order and category colors. Both versions therefore show the same hierarchy, one
  positionally and one as a grouped list.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this
step fixes is the data, the structure (which text slots exist, in what order), the proportions,
and the row layout. Segment colors are seaborn "deep" positions, one hue family per top-level
category, so the chart moves with the shared palette.
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

# The OECD's top-level categories, in bar order. Each carries a seaborn "deep" palette position;
# its member groups are that hue at decreasing saturation, so a family reads as one category.
# `Unpaid work & other` is named for what it holds: the OECD's unpaid-work category plus its small
# "other" category (religious and civic activities, and uncategorized time), which garden folds
# into the same group.
CATEGORIES = [
    {"name": "Paid work or study", "color": ("deep", 3), "columns": ["paid_work", "education"]},
    {
        "name": "Personal care",
        "color": ("deep", 7),
        "columns": ["sleep", "personal_care", "eating_and_drinking"],
    },
    {
        "name": "Unpaid work & other",
        "color": ("deep", 5),
        "columns": ["other_unpaid_work", "housework_and_shopping"],
    },
    {
        "name": "Leisure",
        "color": ("deep", 0),
        "columns": ["tv_and_radio", "seeing_friends", "other_leisure"],
    },
]

# The ten groups, in bar order, which must match the categories' column order above (asserted).
# `sublabel` is a smaller second line. The group under "Personal care" is named "Other personal
# care" rather than "Personal care": the category bracket above it already carries that name, and
# two identical names at different levels would read as the same thing.
GROUPS = [
    {"column": "paid_work", "label": "Paid work", "color": ("deep", 3)},
    {
        "column": "education",
        "label": "Education",
        "sublabel": "In school & study",
        "color": ("tint", 3, 0.45),
    },
    {"column": "sleep", "label": "Sleep", "color": ("deep", 7)},
    {"column": "personal_care", "label": "Other personal care", "color": ("tint", 7, 0.4)},
    {"column": "eating_and_drinking", "label": "Eating & drinking", "color": ("tint", 7, 0.65)},
    {
        "column": "other_unpaid_work",
        "label": "Other unpaid work",
        "sublabel": "Care work, volunteering",
        "color": ("deep", 5),
    },
    {"column": "housework_and_shopping", "label": "Housework & shopping", "color": ("tint", 5, 0.45)},
    {"column": "tv_and_radio", "label": "TV & Radio", "color": ("deep", 0)},
    {"column": "seeing_friends", "label": "Seeing friends", "color": ("tint", 0, 0.35)},
    {"column": "other_leisure", "label": "Other leisure", "color": ("tint", 0, 0.6)},
]

TOTAL_LEISURE_COLUMN = "total_leisure"

TITLE = "How do people spend their time?"

# Credited on the license line: the original chart's author, and this refresh's.
AUTHORS = "Esteban Ortiz-Ospina and Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"
LEADER_COLOR = "#999999"
CATEGORY_RULE_COLOR = "#bbbbbb"
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

# Vertical rhythm between the subtitle and the header, in text lines.
SUBTITLE_GAP = 0.8

# Horizontal breathing room, in template pixels: between a country label and its bar, between the
# bars and the total-leisure column, and inside a segment around its value.
COUNTRY_LABEL_PAD = 8
TOTAL_COLUMN_GAP = 10
VALUE_PAD = 3

# Positional header geometry, in template pixels. A tier holds one line of header text; a
# two-line label occupies two tiers, its first line on top. `HEADER_NUDGES` are the horizontal
# offsets the solver may apply, smallest first, when a label does not fit centred on its segment.
TIER_HEIGHT = 15
LEADER_GAP = 3
HEADER_MIN_GAP = 8
LEADER_CLEARANCE = 4
HEADER_NUDGES = [0, 5, -5, 10, -10, 15, -15, 20, -20, 26, -26, 32, -32, 40, -40, 48, -48, 56, -56]
MAX_TIERS = 8
# A label on tier 0 carries no leader, so it may only drift as far as its own segment: past that
# it would name the wrong bar. Labels on higher tiers have a leader pointing back at the truth.
TIER_ZERO_DRIFT = 12

# Category bracket: the gap above the tallest group label, the bracket's end ticks, and the gap
# between the bracket and its name.
CATEGORY_GAP = 7
CATEGORY_TICK = 4
CATEGORY_LABEL_GAP = 3

# Listed header geometry (mobile), in template pixels: the gap between categories on a line and
# the extra leading between lines.
FLOW_GAP = 14
FLOW_LINE_PAD = 4

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
        "header_mode": "positional",
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
        "header_mode": "listed",
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
    # The category brackets span contiguous runs of segments, which only holds if the bar order
    # is the categories' column order concatenated.
    assert group_columns == [column for category in CATEGORIES for column in category["columns"]], (
        "Bar order no longer matches the category grouping, so a bracket would span the wrong segments."
    )
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
# Visualization
# ---------------------------------------------------------------------------


def create_visualization(tb: Table, ages: dict[str, str], source_citation: str, layout: dict) -> plt.Figure:
    """Build one version of the stacked-bar chart.

    Layout notes:
    - One axes carries the bars; x is minutes of the day (0-1440) plus a text column for total
      leisure, y is one unit per country row, row 0 at the top. The header and the country
      labels are drawn outside the axes box (clipping is off).
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
    draw_footer(fig, tb, ages, source_citation, layout, fx, fy)

    # --- the chart band: the category header, then the bar rows ---
    subtitle_bottom_px = subtitle_y + subtitle_lines * line_px(body_fontsize) + SUBTITLE_GAP * line_px(body_fontsize)

    country_labels = [f"{country} ({year})" for country, year in zip(tb["country"].tolist(), tb["year"].tolist())]
    country_space_px = (
        max(text_width_px(label, layout["country_fontsize"]) for label in country_labels) + COUNTRY_LABEL_PAD
    )

    plot_left_px = margin_px + country_space_px
    plot_width_px = (width_px - margin_px) - plot_left_px
    bar_width_px = plot_width_px - layout["total_column_px"]
    px_per_min = bar_width_px / MINUTES_PER_DAY

    # Segment spans in the top row, which is what the positional header labels attach to.
    spans = segment_spans(tb.iloc[0], px_per_min)

    if layout["header_mode"] == "positional":
        placements = solve_header_layout(spans, bar_width_px, layout)
        category_rows = max(placement["row"] for placement in solve_category_layout(spans, layout))
        header_px = category_rule_px(placements) + CATEGORY_LABEL_GAP + category_rows * TIER_HEIGHT
        header_px += line_px(layout["header_fontsize"])
        listed_lines = None
    else:
        placements = None
        listed_lines = layout_listed_header(layout, width_px - 2 * margin_px)
        header_px = len(listed_lines) * (line_px(layout["header_fontsize"]) + FLOW_LINE_PAD)
        # Room for the two-line "Total leisure" column header, which sits above the first bar.
        header_px += 2 * line_px(layout["header_fontsize"]) + LEADER_GAP

    chart_top_px = subtitle_bottom_px + header_px
    chart_bottom_px = layout["chart_bottom_y"]

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

    def rows_above(px_above: float) -> float:
        """A height above the axes' top edge, in row units (negative y on this axes)."""
        return -px_above / row_pitch_px

    group_colors = {group["column"]: resolve_color(group["color"], palette) for group in GROUPS}

    draw_bars(ax, tb, country_labels, group_colors, px_per_min, layout)

    if placements is not None:
        draw_positional_header(ax, placements, spans, group_colors, palette, px_per_min, rows_above, layout)
    else:
        draw_listed_header(fig, listed_lines, palette, subtitle_bottom_px, margin_px, layout, fx, fy)

    # Total-leisure column header, over its own column.
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


def draw_bars(ax, tb: Table, country_labels: list[str], group_colors: dict, px_per_min: float, layout: dict) -> None:
    """One stacked row per country: the country label, the ten segments, and the leisure total."""
    for row in range(len(tb)):
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


def draw_positional_header(
    ax, placements: list[dict], spans: dict, group_colors: dict, palette, px_per_min: float, rows_above, layout: dict
) -> None:
    """Draw the solved group labels with their leaders, and the category brackets above them."""
    for placement in placements:
        group = placement["group"]
        slug = slugify(group["column"])
        color = header_text_color(group["color"], palette)
        # A block reads downwards: its first line sits on the topmost tier it reserves, each
        # further line one tier lower, so the block occupies exactly the tiers the solver
        # reserved for it and nothing spills into a neighbour's row.
        for index, (text, fontsize, bold) in enumerate(placement["lines"]):
            tier = placement["tier"] + placement["height"] - 1 - index
            ax.text(
                placement["center"] / px_per_min,
                rows_above(LEADER_GAP + tier * TIER_HEIGHT),
                text,
                ha="center",
                va="bottom",
                fontsize=fontsize,
                fontweight="bold" if bold else "normal",
                color=color,
                gid=f"header__{slug}" if index == 0 else f"header__{slug}-line{index}",
            )
        if placement["tier"] > 0:
            leader_x = placement["leader_x"]
            ax.plot(
                [leader_x / px_per_min, leader_x / px_per_min],
                [rows_above(LEADER_GAP + placement["tier"] * TIER_HEIGHT - LEADER_GAP), rows_above(1.0)],
                color=LEADER_COLOR,
                linewidth=0.7,
                solid_capstyle="butt",
                gid=f"header__{slug}-leader",
            )

    # --- category brackets, above the tallest label ---
    rule_px = category_rule_px(placements)
    for placement in solve_category_layout(spans, layout):
        slug = slugify(placement["name"])
        start, end = placement["bracket"]
        label_px = rule_px + CATEGORY_LABEL_GAP + placement["row"] * TIER_HEIGHT
        # A bracket, not a plain rule: the end ticks turn down towards the segments they enclose,
        # so the span reads as "these bars" rather than as a divider.
        ax.plot(
            [start / px_per_min, start / px_per_min, end / px_per_min, end / px_per_min],
            [
                rows_above(rule_px - CATEGORY_TICK),
                rows_above(rule_px),
                rows_above(rule_px),
                rows_above(rule_px - CATEGORY_TICK),
            ],
            color=CATEGORY_RULE_COLOR,
            linewidth=0.8,
            solid_capstyle="butt",
            gid=f"category__{slug}-bracket",
        )
        if placement["row"]:
            # A name on an upper row needs a stem back to its own bracket, or it reads as the
            # neighbour's.
            ax.plot(
                [placement["center"] / px_per_min] * 2,
                [rows_above(label_px), rows_above(rule_px)],
                color=CATEGORY_RULE_COLOR,
                linewidth=0.8,
                solid_capstyle="butt",
                gid=f"category__{slug}-stem",
            )
        ax.text(
            placement["center"] / px_per_min,
            rows_above(label_px),
            placement["name"],
            ha="center",
            va="bottom",
            fontsize=layout["header_fontsize"],
            fontweight="bold",
            color=header_text_color(placement["color"], palette),
            gid=f"category__{slug}",
        )


def draw_listed_header(fig, listed_lines, palette, top_px: float, margin_px: float, layout: dict, fx, fy) -> None:
    """Draw the category-grouped name list used where the frame is too narrow to label in place."""
    for index, line in enumerate(listed_lines):
        y_px = top_px + index * (line_px(layout["header_fontsize"]) + FLOW_LINE_PAD)
        for x_offset, text, spec, bold, gid in line:
            fig.text(
                fx(margin_px + x_offset),
                fy(y_px),
                text,
                ha="left",
                va="top",
                fontsize=layout["header_fontsize"],
                fontweight="bold" if bold else "normal",
                color=header_text_color(spec, palette),
                gid=gid,
            )


def draw_footer(fig, tb: Table, ages: dict[str, str], source_citation: str, layout: dict, fx, fy) -> None:
    """Fill the template's footer slots: Note, Data source, tagline and license."""
    width_px = layout["size"][0]
    margin_px = layout["margin"]
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


# ---------------------------------------------------------------------------
# Header layout
# ---------------------------------------------------------------------------


def segment_spans(row, px_per_min: float) -> dict[str, tuple[float, float]]:
    """Each group's (start, end) in template pixels, for one row of the chart."""
    spans = {}
    cumulative = 0.0
    for group in GROUPS:
        minutes = float(row[group["column"]])
        spans[group["column"]] = (cumulative * px_per_min, (cumulative + minutes) * px_per_min)
        cumulative += minutes
    return spans


def solve_header_layout(spans: dict, bar_width_px: float, layout: dict) -> list[dict]:
    """Place every group label on a tier above the bars, without collisions.

    Labels are placed in bar order, each at the lowest tier where it fits. A label may be split
    over two lines and nudged sideways; a label above tier 0 drops a leader to its segment. Three
    things must hold, and are checked at placement rather than assumed:

    1. Two labels on the same tier keep `HEADER_MIN_GAP` between them.
    2. A leader does not pass through a label on a tier below its own.
    3. A label does not sit on top of a leader already dropped from a tier above it.

    Solving this rather than hand-placing each label is what lets the header survive a data update
    that reorders the top row or changes its proportions — with 35 countries and ten groups, the
    top row's segment widths are not stable between OECD releases.
    """
    # tier -> spans occupied by labels, and the leaders already dropped (x, bottom-most tier).
    occupied: dict[int, list[tuple[float, float]]] = {}
    leaders: list[tuple[float, int]] = []
    placements: list[dict] = []

    for index, group in enumerate(GROUPS):
        start, end = spans[group["column"]]
        mid = (start + end) / 2
        unplaced = [other["column"] for other in GROUPS[index + 1 :]]
        placement = None
        for tier in range(MAX_TIERS):
            for lines in label_variants(group, layout, end - start):
                width = block_width(lines)
                if width > bar_width_px:
                    continue
                height = len(lines)
                drift = TIER_ZERO_DRIFT + (end - start) / 2 if tier == 0 else max(HEADER_NUDGES)
                for nudge in HEADER_NUDGES:
                    if abs(nudge) > drift:
                        continue
                    center = min(max(mid + nudge, width / 2), bar_width_px - width / 2)
                    label_span = (center - width / 2, center + width / 2)
                    tiers = range(tier, tier + height)
                    if any(overlaps(label_span, other, HEADER_MIN_GAP) for t in tiers for other in occupied.get(t, [])):
                        continue
                    # A label that blankets a not-yet-placed group's segment leaves that group's
                    # leader nowhere to come down: every x it could use is under this label.
                    # Groups already placed are unaffected — their leaders run below this tier.
                    if any(covers(label_span, spans[column], LEADER_CLEARANCE) for column in unplaced):
                        continue
                    # A leader from above must not be covered by this label.
                    if any(
                        leader_tier > tier + height - 1 and inside(leader_x, label_span, LEADER_CLEARANCE)
                        for leader_x, leader_tier in leaders
                    ):
                        continue
                    leader_x = pick_leader_x(start, end, tier, occupied) if tier > 0 else None
                    if tier > 0 and leader_x is None:
                        continue
                    placement = {
                        "group": group,
                        "tier": tier,
                        "height": height,
                        "lines": lines,
                        "center": center,
                        "span": label_span,
                        "leader_x": leader_x,
                    }
                    break
                if placement:
                    break
            if placement:
                break
        assert placement, f"Could not place the header label for {group['column']} in {MAX_TIERS} tiers."

        for t in range(placement["tier"], placement["tier"] + placement["height"]):
            occupied.setdefault(t, []).append(placement["span"])
        if placement["leader_x"] is not None:
            leaders.append((placement["leader_x"], placement["tier"]))
        placements.append(placement)

    return placements


# Widths a header label may be wrapped to, in template pixels, when its natural width does not
# fit over its own segment. Narrower blocks take less of the crowded zone above the bars and leave
# corridors for their neighbours' leaders, at the cost of one tier each.
LABEL_WRAP_WIDTHS = [115, 90, 70]


def label_variants(group: dict, layout: dict, segment_width: float) -> list[list[tuple[str, float, bool]]]:
    """A group's label as blocks of (text, size, bold) lines, shortest block first.

    A label wider than the segment it names has to be threaded past its neighbours, and a wide
    one-liner both takes the room they need and blocks their leaders — so wrapped forms are kept
    as fallbacks. A sublabel wraps too: it is often the widest line of the block.
    """
    header_fontsize = layout["header_fontsize"]
    subheader_fontsize = layout["subheader_fontsize"]

    def block(max_px: float) -> list[tuple[str, float, bool]]:
        lines = [
            (line, header_fontsize, True)
            for line in wrap_to_width(group["label"], max_px, header_fontsize, bold=True).split("\n")
        ]
        if group.get("sublabel"):
            lines += [
                (line, subheader_fontsize, False)
                for line in wrap_to_width(group["sublabel"], max_px, subheader_fontsize).split("\n")
            ]
        return lines

    variants = [block(float("inf"))]
    for max_px in sorted(LABEL_WRAP_WIDTHS, reverse=True):
        candidate = block(max_px)
        if candidate not in variants:
            variants.append(candidate)

    # Shortest block first, narrowest to break ties: every extra line costs a tier, and a header
    # that grows upwards eats the chart. Narrower forms are a fallback for a crowded neighbourhood,
    # not a default — `segment_width` decides only how hard that fallback is likely to be needed.
    variants.sort(key=lambda lines: (len(lines), block_width(lines) > segment_width))
    return variants


def block_width(lines: list[tuple[str, float, bool]]) -> float:
    """The widest line of a label block, in template pixels."""
    return max(text_width_px(text, size, bold) for text, size, bold in lines)


def pick_leader_x(start: float, end: float, tier: int, occupied: dict) -> float | None:
    """An x inside the segment where a leader can reach it without crossing a lower label.

    Searched from the segment's middle outwards, so a leader stays as central as it can: the
    corridor left between two labels is often only a few pixels wide.
    """
    middle = (start + end) / 2
    steps = 12
    candidates = [middle]
    for index in range(1, steps + 1):
        offset = index * (end - start) / (2 * steps)
        candidates.extend([middle + offset, middle - offset])
    for candidate in candidates:
        if not start <= candidate <= end:
            continue
        if any(
            inside(candidate, other, LEADER_CLEARANCE) for lower in range(tier) for other in occupied.get(lower, [])
        ):
            continue
        return candidate
    return None


def category_rule_px(placements: list[dict]) -> float:
    """Height above the bars at which the category brackets are drawn, clear of every label."""
    n_tiers = max(placement["tier"] + placement["height"] for placement in placements)
    return LEADER_GAP + n_tiers * TIER_HEIGHT + CATEGORY_GAP


def solve_category_layout(spans: dict, layout: dict) -> list[dict]:
    """Place the category names over their brackets, stacking those that would collide.

    A category name is often wider than the run of bars it covers — "Unpaid work & other" spans
    67px of a desktop frame — so names are centred on their bracket, pulled inside the plot, and
    moved to a further row when the one below is taken.
    """
    right_edge = spans[GROUPS[-1]["column"]][1]
    placed: list[tuple[tuple[float, float], int]] = []
    placements = []
    for category in CATEGORIES:
        start = spans[category["columns"][0]][0]
        end = spans[category["columns"][-1]][1]
        width = text_width_px(category["name"], layout["header_fontsize"], bold=True)
        center = min(max((start + end) / 2, width / 2), right_edge - width / 2)
        label_span = (center - width / 2, center + width / 2)
        row = 0
        while any(other_row == row and overlaps(label_span, other, HEADER_MIN_GAP) for other, other_row in placed):
            row += 1
        placed.append((label_span, row))
        placements.append(
            {
                "name": category["name"],
                "color": category["color"],
                "bracket": (start, end),
                "center": center,
                "row": row,
            }
        )
    return placements


def layout_listed_header(layout: dict, available_px: float) -> list[list[tuple]]:
    """Wrap the category-grouped name list into lines of (x, text, color spec, bold, gid)."""
    lines: list[list[tuple]] = [[]]
    x = 0.0
    for category in CATEGORIES:
        runs = [(f"{category['name']}: ", category["color"], True, f"category__{slugify(category['name'])}")]
        members = [group for group in GROUPS if group["column"] in category["columns"]]
        for index, group in enumerate(members):
            text = group["label"] + (" · " if index < len(members) - 1 else "")
            runs.append((text, group["color"], False, f"header__{slugify(group['column'])}"))

        def advance(text: str, bold: bool) -> float:
            return text_advance_px(text, layout["header_fontsize"], bold)

        block_width = sum(advance(text, bold) for text, _, bold, _ in runs)
        if x > 0 and x + block_width > available_px:
            lines.append([])
            x = 0.0
        offset = x
        for text, spec, bold, gid in runs:
            lines[-1].append((offset, text, spec, bold, gid))
            offset += advance(text, bold)
        x = offset + FLOW_GAP
    return lines


# ---------------------------------------------------------------------------
# Text and color helpers
# ---------------------------------------------------------------------------


def overlaps(a: tuple[float, float], b: tuple[float, float], gap: float) -> bool:
    """Whether two spans come within `gap` of each other."""
    return a[0] - gap < b[1] and b[0] - gap < a[1]


def inside(x: float, span: tuple[float, float], clearance: float) -> bool:
    """Whether x falls within `clearance` of a span."""
    return span[0] - clearance <= x <= span[1] + clearance


def covers(outer: tuple[float, float], inner: tuple[float, float], clearance: float) -> bool:
    """Whether `outer` swallows `inner` whole, leaving no clear x on either side of it."""
    return outer[0] - clearance <= inner[0] and inner[1] <= outer[1] + clearance


def line_px(points: float) -> float:
    """A line of text at this point size, in template pixels."""
    return 1.3 * points / POINTS_PER_PIXEL


def tint(color, weight: float) -> tuple[float, float, float]:
    """Blend a color towards white. weight=0 keeps it, weight=1 turns it white."""
    r, g, b = to_rgb(color)
    return (r + (1 - r) * weight, g + (1 - g) * weight, b + (1 - b) * weight)


def resolve_color(spec: tuple, palette) -> tuple[float, float, float]:
    """Resolve a ("deep", i) palette position or ("tint", i, w) blend into an RGB color."""
    if spec[0] == "deep":
        return palette[spec[1]]
    return tint(palette[spec[1]], spec[2])


def header_text_color(spec: tuple, palette) -> tuple[float, float, float]:
    """A pale group's own fill is unreadable as text on the light background, so its header
    label keeps the hue at a much shallower tint."""
    if spec[0] == "tint":
        return tint(palette[spec[1]], spec[2] * 0.4)
    return palette[spec[1]]


def luminance(color) -> float:
    """Relative luminance, for choosing white or dark text on a fill."""
    r, g, b = to_rgb(color)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def text_width_px(text: str, fontsize: float, bold: bool = False) -> float:
    """Measured width of rendered text, in template pixels.

    Bold text is measured in the bold face rather than scaled from the regular one: the error in
    any single fudge factor grows with the length of the string, so a long bold run overruns what
    follows it while a short one looks fine.
    """
    if not text.strip():
        return 0.0
    prop = FontProperties(size=fontsize, weight="bold" if bold else "normal")
    points = TextPath((0, 0), text, prop=prop).get_extents().width
    return points / POINTS_PER_PIXEL


def text_advance_px(text: str, fontsize: float, bold: bool = False) -> float:
    """Width of text *including* any trailing space, in template pixels.

    `TextPath` measures ink, so a trailing space contributes nothing to its extents — laying runs
    out by that measurement butts each one against the last (`study:Paid work`). Measuring with a
    sentinel glyph on the end and subtracting it back gives the advance instead.
    """
    sentinel = "|"
    return text_width_px(text + sentinel, fontsize, bold) - text_width_px(sentinel, fontsize, bold)


def wrap_to_width(text: str, max_px: float, fontsize: float, bold: bool = False) -> str:
    """Wrap text greedily against measured glyph widths, in template pixels."""
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if current and text_width_px(candidate, fontsize, bold) > max_px:
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
    return name.lower().replace(" & ", "-").replace(" ", "-").replace("_", "-").replace(":", "")


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
