"""Recreate the 'How do people spend their time?' stacked-bar chart from the OECD Time Use Database.

One row per country, each splitting the 1440 minutes of a day into ten activity groups, ranked by
`SORT_BY` — time spent on paid work, as the original chart by Esteban Ortiz-Ospina ranked it. A
column right of the bars totals the leisure groups, as the original did too. The groups come precomputed from the garden step
(`time_use_chart_groups`), where the regrouping of the OECD's detailed activities is documented
and asserted; this step only lays them out.

The groups are shown under the OECD's own four top-level categories, which is the one structural
change from the original chart: the ten groups are ordered so that each category's members are
adjacent, and a bracket above the bars spans each category. That costs a reordering — unpaid work
now sits right of personal care rather than left of it — and it is what lets a reader see that
sleep, eating and personal care are one thing. Flag it at design review rather than letting it
pass as polish.

Within a category the named activities come first, carrying the deep hue and then tints of it, and
a residual "other" group always comes last — asserted in `load_chart_groups`, since a leftover
bucket sitting mid-category reads as a thing in its own right. So: sleep, eating and drinking,
then other personal care; housework and shopping, then other unpaid work.

Values are written inside segments wide enough to hold them, sleep as hours and minutes — but only
for groups that fit on most rows (`VALUE_LABEL_COVERAGE`), so no group carries numbers on just a few
countries. That drops education and seeing friends here, and two more groups on mobile. Survey
years differ by country (1999-2024), so each country label carries its year — the original's
surveys spanned a narrower window and it named none of them.

The whole header sits above the bars, in two layers that both point at the top row: four category
brackets span their runs of segments, and inside each bracket its own member names, stacked one per
line, so every name stands over the bars it belongs to. The blocks hang from their brackets,
top-aligned, so they line up however deep each one had to go — three lines here.

Two things keep that band shallow. Inside a bracket the category's name is already given, so the
residual buckets are just "Other" rather than "Other personal care" and the like
(`label_in_context`); and a name still wider than its span is wrapped to it, which at 68px only
"Housework & shopping" needs. Centring each block in its own span is what makes the width binding —
the names cannot borrow the empty width over sleep without ceasing to be inside their bracket — so
`blocks_collide` asserts that no block reaches into its neighbour's, since wrapping cannot save a
single word wider than its span.

`LAYOUTS` assigns each half a side. `category_side` takes "above" or "below"; `group_labels` takes
"bracketed" (the layout above), "below_flow" (one list in bar order under the bars, its line breaks
evened out), or "below_listed" / "listed_above" (the names grouped under their category names, as
mobile lists them). Labelling each group over its own segment instead, with a leader back to it, was
tried and dropped: the top row is Japan's, whose "Seeing friends" is the dataset's narrowest segment
at 4px, and threading ten names past each other there needed elbow leaders, a search over placement
orders, and a budget for how many leaders may cross — for a header no easier to read than this one.

Two versions are emitted, following the static-chart templates:

- desktop, 850x1095 (vertical template): category brackets above with each category's member names
  stacked inside its bracket, all footer rows present. The Note carries the survey-year span, the
  age-of-reference exceptions, and what the two groups whose names do not say it themselves contain.
- mobile, 540x824: no Note slot, so the age caveat and the survey-year reference fold into the
  subtitle. Its bars are too narrow to bracket — "Unpaid work & other" spans 38px there — so both
  halves of the header become one grouped list above the chart. Both versions name the same
  groups in the same order and colors, so the pair reads as one chart.

Colors, fonts and the logo are deliberately not set here; those are applied in Figma. What this
step fixes is the data, the structure (which text slots exist, in what order), the proportions,
and the row layout. Segment colors are seaborn "deep" positions, one hue family per top-level
category, so the chart moves with the shared palette.
"""

import itertools

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
        "columns": ["sleep", "eating_and_drinking", "personal_care"],
    },
    {
        "name": "Unpaid work & other",
        "color": ("deep", 5),
        "columns": ["housework_and_shopping", "other_unpaid_work"],
    },
    {
        "name": "Leisure",
        "color": ("deep", 0),
        "columns": ["tv_and_radio", "seeing_friends", "other_leisure"],
    },
]

# The ten groups, in bar order, which must match the categories' column order above (asserted).
# `contents` says what a group holds where its name does not; it is written into the Note rather
# than under the name, because a second line over a 25px segment costs several tiers of wrapping
# (see `place_header_labels`) and the Note has room for the same words. `label` is the name for a
# group standing on its own; wherever its category's name is beside it, the three residual buckets
# shorten to "Other" (see `label_in_context`), which is why they are named "Other <category>" here
# rather than repeating the category outright.
GROUPS = [
    {"column": "paid_work", "label": "Paid work", "color": ("deep", 3)},
    {
        "column": "education",
        "label": "Education",
        "contents": "time in school and studying",
        "color": ("tint", 3, 0.45),
    },
    {"column": "sleep", "label": "Sleep", "color": ("deep", 7)},
    {"column": "eating_and_drinking", "label": "Eating & drinking", "color": ("tint", 7, 0.4)},
    {"column": "personal_care", "label": "Other personal care", "color": ("tint", 7, 0.65)},
    {"column": "housework_and_shopping", "label": "Housework & shopping", "color": ("deep", 5)},
    {
        "column": "other_unpaid_work",
        "label": "Other unpaid work",
        "contents": "care work and volunteering",
        "color": ("tint", 5, 0.45),
    },
    {"column": "tv_and_radio", "label": "TV & Radio", "color": ("deep", 0)},
    {"column": "seeing_friends", "label": "Seeing friends", "color": ("tint", 0, 0.35)},
    {"column": "other_leisure", "label": "Other leisure", "color": ("tint", 0, 0.6)},
]

TOTAL_LEISURE_COLUMN = "total_leisure"

# A group's values are drawn only where this share of countries can hold one; otherwise none of them
# are. A number on a handful of rows reads as a fact about those countries rather than as the rest
# being too narrow to print — education fitted on 1 of 35 rows, and that one number said nothing.
VALUE_LABEL_COVERAGE = 0.75

# Countries are ranked by this group or category, most minutes at the top. A GROUPS column ranks by
# that group alone; a CATEGORIES name ranks by the sum of its groups. Asserted in load_chart_groups.
# Paid work is the original chart's ranking, and the one column education cannot distort: education
# time is depressed wherever the survey's age floor excludes teenagers (Lithuania, 20-64).
SORT_BY = "paid_work"

TITLE = "How do people spend their time?"

# Credited on the license line: the original chart's author, and this refresh's.
AUTHORS = "Esteban Ortiz-Ospina and Pablo Arriagada"

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

TEXT_COLOR = "#5b5b5b"
MUTED_COLOR = "#777777"
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

# Header geometry, in template pixels. A tier holds one line of header text, which is what a wrapped
# category name stacks in; `LEADER_GAP` is the clearance between the header and the bars, and
# `HEADER_MIN_GAP` the least space two neighbouring names may leave between them.
TIER_HEIGHT = 15
LEADER_GAP = 3
HEADER_MIN_GAP = 8

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
        # Width reserved for the total-leisure column, in template pixels.
        "total_column_px": 74,
        "with_mins_suffix": True,
        # Where each half of the header goes. The category brackets span the top row from above, and
        # each category's own member names are stacked inside its bracket, one per line
        # ("bracketed") — so the header reads category, then its members, then the data, with every
        # name over the run of bars it belongs to.
        # Alternatives for the names: "below_flow" (one list in bar order under the bars) or
        # "below_listed" (grouped under their category names, as mobile lists them).
        "category_side": "above",
        "group_labels": "bracketed",
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
        "total_column_px": 46,
        "with_mins_suffix": False,
        "category_side": "listed_above",
        "group_labels": "listed_above",
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
    """Load the precomputed chart groups (total population), ranked by `SORT_BY`.

    Returns the table plus the age-of-reference exceptions (country -> age range) for the note.
    """
    ds = paths.load_dataset("time_use")
    tb = ds.read("time_use_chart_groups")
    tb = tb[tb["sex"] == "total"].drop(columns=["sex"])

    group_columns = [group["column"] for group in GROUPS]
    assert not set(group_columns + [TOTAL_LEISURE_COLUMN]) - set(tb.columns), "Chart group columns changed."
    # A category name ranks by the sum of its groups; a group column ranks by itself.
    sort_columns = next((list(c["columns"]) for c in CATEGORIES if c["name"] == SORT_BY), [SORT_BY])
    assert not set(sort_columns) - set(group_columns), f"{SORT_BY} is not a group or category of the chart."
    # Stable, so tied countries (Norway and New Zealand at 241, Belgium and Greece at 194) keep the
    # source's order instead of swapping between runs and churning the SVG.
    tb = tb.loc[tb[sort_columns].sum(axis=1).sort_values(ascending=False, kind="stable").index]

    detail = ds.read("time_use")
    detail = detail[detail["sex"] == "total"]
    ages = {
        str(row["country"]): str(row["age_of_reference"])
        for _, row in detail.iterrows()
        if str(row["age_of_reference"]) != "15-64"
    }

    assert len(tb) >= 35, "Country coverage shrank."
    assert tb["country"].is_unique, "One row per country expected."
    # The category brackets span contiguous runs of segments, which only holds if the bar order
    # is the categories' column order concatenated.
    assert group_columns == [column for category in CATEGORIES for column in category["columns"]], (
        "Bar order no longer matches the category grouping, so a bracket would span the wrong segments."
    )
    # A residual "other" group is whatever its category has left over, so it belongs at that
    # category's far end rather than between two named activities.
    for category in CATEGORIES:
        labels = [next(g["label"] for g in GROUPS if g["column"] == column) for column in category["columns"]]
        residual = [index for index, label in enumerate(labels) if label.startswith("Other ")]
        assert all(index == len(labels) - 1 for index in residual), (
            f"An 'other' group is not last within {category['name']}: {labels}"
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

    # Category brackets attach to the row they touch: the top row above the bars. Group labels
    # placed below attach to the bottom row, for the same reason.
    top_spans = segment_spans(tb.iloc[0], px_per_min)
    bottom_spans = segment_spans(tb.iloc[-1], px_per_min)

    # The name list, for the layouts that use one. Drawn in the band above the bars it shares that
    # band with the two-line "Total leisure" column header, so it stops short of that column; drawn
    # below the chart it has the whole frame.
    listed_lines: list[list[tuple]] = []
    listed_px = 0.0
    if layout["group_labels"] in ("listed_above", "below_listed", "below_flow"):
        listed_available_px = width_px - 2 * margin_px
        if layout["group_labels"] == "listed_above":
            listed_available_px -= layout["total_column_px"] + TOTAL_COLUMN_GAP
        if layout["group_labels"] == "below_flow":
            listed_lines = layout_flowed_names(layout, listed_available_px)
        else:
            listed_lines = layout_listed_header(layout, listed_available_px)
        listed_px = len(listed_lines) * (line_px(layout["header_fontsize"]) + FLOW_LINE_PAD)
        # `layout_listed_header` keeps a category and its members together on one line, so a single
        # block wider than what it was given still overruns — into the total-leisure header, where
        # the two would be printed over each other.
        widest_px = max(
            offset + text_advance_px(text, layout["header_fontsize"], bold)
            for line in listed_lines
            for offset, text, _, bold, _ in line
        )
        assert widest_px <= listed_available_px, (
            f"A line of the listed header is {widest_px:.0f}px wide, over the {listed_available_px:.0f}px it "
            f"has: it would run into the total-leisure column."
        )

    # Which side each half of the header is drawn on. Both halves point at the row they touch, so a
    # half placed above works off the top row's segments and one placed below off the bottom row's.
    category_at = {"above": "above", "below": "below", "listed_above": "above"}[layout["category_side"]]
    group_at = {"below_listed": "below", "below_flow": "below"}.get(layout["group_labels"], "above")
    assert layout["group_labels"] in {"bracketed", "below_flow", "below_listed", "listed_above"}, (
        f"Unknown group_labels {layout['group_labels']!r}."
    )

    category_placements = None
    if layout["category_side"] in ("above", "below"):
        category_placements = solve_category_layout(top_spans if category_at == "above" else bottom_spans, layout)

    # The member names drawn inside each bracket sit between the rule and the bars, so the rule moves
    # out by their height: category first, its own members under it, then the data.
    bracketed_blocks = None
    bracketed_px = 0.0
    if layout["group_labels"] == "bracketed":
        bracketed_blocks = layout_bracketed_names(top_spans if category_at == "above" else bottom_spans, layout)
        collision = blocks_collide(bracketed_blocks, layout)
        assert not collision, (
            f"The names inside the {collision} brackets would touch. Their category spans this row too "
            f"narrowly to hold them — list the names beyond the chart instead ('below_listed')."
        )
        deepest = max(len(block["lines"]) for block in bracketed_blocks)
        bracketed_px = deepest * line_px(layout["header_fontsize"])
    category_base_px = CATEGORY_GAP + (LEADER_GAP + bracketed_px if bracketed_blocks else 0.0)

    def band_px(side: str) -> float:
        """The room the header needs on one side of the bars."""
        room = 0.0
        if category_at == side and category_placements is not None:
            # The tallest name decides the band: its row, plus however many lines it wrapped onto.
            tallest = max(
                placement["row"] * TIER_HEIGHT + len(placement["lines"]) * line_px(layout["header_fontsize"])
                for placement in category_placements
            )
            room = max(room, category_base_px + CATEGORY_TICK + CATEGORY_LABEL_GAP + tallest)
        if group_at == side and layout["group_labels"] in ("below_listed", "below_flow"):
            room = max(room, LEADER_GAP + listed_px)
        if side == "above" and "listed_above" in (layout["category_side"], layout["group_labels"]):
            room = max(room, listed_px)
        return room

    # Room above for whichever half goes there, and never less than the two-line "Total leisure"
    # column header, which sits above the first bar.
    header_px = max(band_px("above"), 2 * line_px(layout["header_fontsize"]) + LEADER_GAP)
    below_px = band_px("below")

    chart_top_px = subtitle_bottom_px + header_px
    chart_bottom_px = layout["chart_bottom_y"] - below_px

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

    def rows_below(px_below: float) -> float:
        """A depth below the axes' bottom edge, in row units."""
        return n_rows + px_below / row_pitch_px

    group_colors = {group["column"]: resolve_color(group["color"], palette) for group in GROUPS}

    draw_bars(ax, tb, country_labels, group_colors, px_per_min, layout, value_label_columns(tb, px_per_min, layout))

    if category_placements is not None:
        rows_out = rows_above if category_at == "above" else rows_below
        draw_category_brackets(
            ax, category_placements, palette, px_per_min, rows_out, layout, category_at, category_base_px
        )
    if bracketed_blocks is not None:
        rows_out = rows_above if category_at == "above" else rows_below
        draw_bracketed_names(ax, bracketed_blocks, palette, px_per_min, rows_out, layout, bracketed_px)
    if layout["group_labels"] == "listed_above":
        draw_listed_header(fig, listed_lines, palette, subtitle_bottom_px, margin_px, layout, fx, fy)
    elif layout["group_labels"] in ("below_listed", "below_flow"):
        draw_listed_header(fig, listed_lines, palette, chart_bottom_px + LEADER_GAP, margin_px, layout, fx, fy)

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


def value_label_columns(tb: Table, px_per_min: float, layout: dict) -> set[str]:
    """The groups whose value is drawn: those that fit on at least `VALUE_LABEL_COVERAGE` of rows.

    Per frame, since the mobile bars are little over half as wide.
    """
    labelled = set()
    for group in GROUPS:
        column = group["column"]
        fits = sum(
            1
            for minutes in tb[column].astype(float)
            if fit_text(
                value_candidates(column, minutes, layout["with_mins_suffix"]),
                minutes * px_per_min,
                layout["value_fontsize"],
            )
        )
        if fits >= VALUE_LABEL_COVERAGE * len(tb):
            labelled.add(column)
    return labelled


def draw_bars(
    ax,
    tb: Table,
    country_labels: list[str],
    group_colors: dict,
    px_per_min: float,
    layout: dict,
    value_columns: set[str],
) -> None:
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
            label = (
                fit_text(
                    value_candidates(column, minutes, layout["with_mins_suffix"]),
                    minutes * px_per_min,
                    layout["value_fontsize"],
                )
                if column in value_columns
                else None
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


def draw_category_brackets(
    ax, category_placements: list[dict], palette, px_per_min: float, rows_out, layout, side: str, base_px: float
) -> None:
    """Bracket each category's run of bars, with its name beside the bracket.

    Mirrors like the group labels: `rows_out` measures distance out from the bars, so the end ticks
    turn back towards the segments they enclose on either side, and only the name's stacking flips.
    `base_px` is how far out the bracket rule sits, which grows when the group names are listed
    between the brackets and the bars.
    """
    rule_px = base_px
    for placement in category_placements:
        slug = slugify(placement["name"])
        start, end = placement["bracket"]
        label_px = rule_px + CATEGORY_LABEL_GAP + placement["row"] * TIER_HEIGHT
        # A bracket, not a plain rule: the end ticks turn back towards the segments they enclose, so
        # the span reads as "these bars" rather than as a divider.
        ax.plot(
            [start / px_per_min, start / px_per_min, end / px_per_min, end / px_per_min],
            [
                rows_out(rule_px - CATEGORY_TICK),
                rows_out(rule_px),
                rows_out(rule_px),
                rows_out(rule_px - CATEGORY_TICK),
            ],
            color=CATEGORY_RULE_COLOR,
            linewidth=0.8,
            solid_capstyle="butt",
            gid=f"category__{slug}-bracket",
        )
        if placement["row"]:
            # A name on an outer row needs a stem back to its own bracket, or it reads as the
            # neighbour's.
            ax.plot(
                [placement["center"] / px_per_min] * 2,
                [rows_out(label_px), rows_out(rule_px)],
                color=CATEGORY_RULE_COLOR,
                linewidth=0.8,
                solid_capstyle="butt",
                gid=f"category__{slug}-stem",
            )
        # Lines stack away from the bracket, so the name reads top-down beside it.
        for index, line in enumerate(placement["lines"]):
            offset = len(placement["lines"]) - 1 - index if side == "above" else index
            ax.text(
                placement["center"] / px_per_min,
                rows_out(label_px + offset * line_px(layout["header_fontsize"])),
                line,
                ha="center",
                va="bottom" if side == "above" else "top",
                fontsize=layout["header_fontsize"],
                fontweight="bold",
                color=header_text_color(placement["color"], palette),
                gid=f"category__{slug}" if index == 0 else f"category__{slug}-line{index}",
            )


def draw_bracketed_names(
    ax, blocks: list[dict], palette, px_per_min: float, rows_out, layout: dict, height_px: float
) -> None:
    """Draw each category's member names inside its bracket, centred and reading top-down.

    Every block starts at the same distance out from the bars — directly under the bracket rule — so
    the four of them line up however many lines each one needed.
    """
    fontsize = layout["header_fontsize"]
    for block in blocks:
        start, end = block["span"]
        centre = (start + end) / 2
        for index, line in enumerate(block["lines"]):
            width = sum(text_advance_px(text, fontsize) for text, _ in line)
            offset = centre - width / 2
            for text, group in line:
                ax.text(
                    offset / px_per_min,
                    rows_out(LEADER_GAP + height_px - index * line_px(fontsize)),
                    text,
                    ha="left",
                    va="top",
                    fontsize=fontsize,
                    color=header_text_color(group["color"], palette),
                    gid=f"header__{slugify(group['column'])}",
                )
                offset += text_advance_px(text, fontsize)


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


# Widths a header label may be wrapped to, in template pixels, when its natural width does not
# fit over its own segment. Narrower blocks take less of the crowded zone above the bars and leave
# corridors for their neighbours' leaders, at the cost of one tier each.
LABEL_WRAP_WIDTHS = [115, 90, 70]


def solve_category_layout(spans: dict, layout: dict) -> list[dict]:
    """Place the category names over their brackets, wrapping before stacking.

    A category name is often wider than the run of bars it covers — "Unpaid work & other" spans
    67px of a desktop frame — so a name that would crowd its neighbour wraps onto a second line
    first, and only takes a row of its own if wrapping is not enough. Wrapping keeps every name
    beside its own bracket; stacking pushes one away from the bars and needs a stem to explain
    which bracket it belongs to.

    A name is only allowed a wide form if the narrowest form of every category still to its right
    would still fit beside it. Without that one-step lookahead the widest name takes the room and
    its neighbour is the one that gets bumped, which is the wrong way round: the wide name is the
    one that should give.
    """
    right_edge = spans[GROUPS[-1]["column"]][1]

    def geometry(category: dict, lines: list[str]) -> tuple[float, tuple[float, float]]:
        start = spans[category["columns"][0]][0]
        end = spans[category["columns"][-1]][1]
        width = max(text_width_px(line, layout["header_fontsize"], bold=True) for line in lines)
        center = min(max((start + end) / 2, width / 2), right_edge - width / 2)
        return center, (center - width / 2, center + width / 2)

    placed: list[tuple[tuple[float, float], int]] = []
    placements = []
    for index, category in enumerate(CATEGORIES):
        variants = category_variants(category["name"], layout)
        remaining = CATEGORIES[index + 1 :]
        placement = None
        for row in range(3):
            for lines in variants:
                center, label_span = geometry(category, lines)
                if any(other_row == row and overlaps(label_span, other, HEADER_MIN_GAP) for other, other_row in placed):
                    continue
                # Would this form leave a later category nowhere to sit on this row?
                if row == 0 and any(
                    overlaps(
                        label_span, geometry(later, category_variants(later["name"], layout)[-1])[1], HEADER_MIN_GAP
                    )
                    for later in remaining
                ):
                    continue
                placement = {"lines": lines, "center": center, "row": row}
                break
            if placement:
                break
        assert placement, f"Could not place the category name {category['name']}."

        placed.append(((placement["center"] - 0.5, placement["center"] + 0.5), placement["row"]))
        placed[-1] = (geometry(category, placement["lines"])[1], placement["row"])
        placements.append(
            {
                "name": category["name"],
                "lines": placement["lines"],
                "color": category["color"],
                "bracket": (spans[category["columns"][0]][0], spans[category["columns"][-1]][1]),
                "center": placement["center"],
                "row": placement["row"],
            }
        )
    return placements


def category_variants(name: str, layout: dict) -> list[list[str]]:
    """A category name on one line, then wrapped over two — widest form first."""
    variants = [[name]]
    words = name.split()
    if len(words) > 1:
        best = min(
            range(1, len(words)),
            key=lambda i: abs(
                text_width_px(" ".join(words[:i]), layout["header_fontsize"], bold=True)
                - text_width_px(" ".join(words[i:]), layout["header_fontsize"], bold=True)
            ),
        )
        variants.append([" ".join(words[:best]), " ".join(words[best:])])
    return variants


def label_in_context(group: dict) -> str:
    """A group's name for use where its category's name is already beside it.

    The residual buckets become just "Other": "Other unpaid work" set inside the "Unpaid work &
    other" bracket repeats the bracket, and at 68px wraps onto three lines to do it. Where a group
    stands on its own — a flat list, or a label over its own segment — `label` is used instead.
    """
    return "Other" if group["label"].startswith("Other ") else group["label"]


def layout_bracketed_names(spans: dict, layout: dict) -> list[dict]:
    """Each category's member names laid out inside its own bracket's span, one per line.

    All four brackets stack their names, rather than setting the ones that would fit on a single line
    horizontally: a mixture reads as four different treatments, and only the two widest brackets
    could have taken a row anyway. A name wider than its span is wrapped to it, so a block never
    spills into the neighbouring category's; `blocks_collide` checks what is left.

    Returns one block per category: lines of (text, group) runs, and the span to centre them in.
    """
    fontsize = layout["header_fontsize"]
    blocks = []
    for category in CATEGORIES:
        start = spans[category["columns"][0]][0]
        end = spans[category["columns"][-1]][1]
        members = [group for group in GROUPS if group["column"] in category["columns"]]
        lines = [
            [(text, group)]
            for group in members
            for text in wrap_to_width(label_in_context(group), end - start, fontsize).split("\n")
        ]
        blocks.append({"name": category["name"], "lines": lines, "span": (start, end)})
    return blocks


def blocks_collide(blocks: list[dict], layout: dict) -> str | None:
    """The first pair of neighbouring name blocks that touch, or None if they all clear each other."""
    extents = []
    for block in blocks:
        start, end = block["span"]
        widest = max(
            sum(text_advance_px(text, layout["header_fontsize"]) for text, _ in line) for line in block["lines"]
        )
        centre = (start + end) / 2
        extents.append((block["name"], (centre - widest / 2, centre + widest / 2)))
    for (left_name, left), (right_name, right) in zip(extents, extents[1:]):
        if overlaps(left, right, HEADER_MIN_GAP):
            return f"{left_name} and {right_name}"
    return None


def layout_flowed_names(layout: dict, available_px: float) -> list[list[tuple]]:
    """Wrap the group names, in bar order, into lines of (x, text, color spec, bold, gid).

    One name after another for as long as the width allows, then onto the next line — so a wide frame
    lists them in a row, a narrow one ends up with a column, and there is no second layout to keep in
    step. The category names are not repeated: the brackets above the bars carry them, and the colors
    tie each name back to its own segment.
    """
    runs = [(group["label"] + (" · " if index < len(GROUPS) - 1 else ""), group) for index, group in enumerate(GROUPS)]
    widths = [text_advance_px(text, layout["header_fontsize"], False) for text, _ in runs]

    # How many lines the names need, packed as tightly as the width allows.
    lines_needed = 1
    x = 0.0
    for width in widths:
        if x > 0 and x + width > available_px:
            lines_needed += 1
            x = 0.0
        x += width

    # Then the breaks that make those lines as even as possible. Tight packing leaves the last line
    # holding one or two names under a full one, which reads as a mistake rather than as a list;
    # minimising the widest line spreads them out instead. Ten names over at most a few lines, so
    # this is a few dozen combinations.
    def widest(points: tuple[int, ...]) -> float:
        return max(sum(widths[start:end]) for start, end in zip((0, *points), (*points, len(widths))))

    breaks = min(itertools.combinations(range(1, len(widths)), lines_needed - 1), key=widest)
    assert widest(breaks) <= available_px, "The flowed name list does not fit the frame at any break."

    lines: list[list[tuple]] = [[]]
    x = 0.0
    for index, ((text, group), width) in enumerate(zip(runs, widths)):
        if index in breaks:
            lines.append([])
            x = 0.0
        lines[-1].append((x, text, group["color"], False, f"header__{slugify(group['column'])}"))
        x += width

    # A separator that ends a line has nothing to separate it from.
    for line in lines:
        offset, text, spec, bold, gid = line[-1]
        line[-1] = (offset, text.rstrip(" ·"), spec, bold, gid)
    return lines


def layout_listed_header(layout: dict, available_px: float) -> list[list[tuple]]:
    """Wrap the category-grouped name list into lines of (x, text, color spec, bold, gid)."""
    lines: list[list[tuple]] = [[]]
    x = 0.0
    for category in CATEGORIES:
        runs = [(f"{category['name']}: ", category["color"], True, f"category__{slugify(category['name'])}")]
        members = [group for group in GROUPS if group["column"] in category["columns"]]
        for index, group in enumerate(members):
            text = label_in_context(group) + (" · " if index < len(members) - 1 else "")
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
    # What the groups whose names do not say it themselves contain, in bar order.
    contents = "; ".join(
        f"{group['label'].lower()} covers {group['contents']}" for group in GROUPS if group.get("contents")
    )
    text = (
        f"Note: Each country's most recent time-use survey is shown, with its year in brackets; "
        f"survey years range from {tb['year'].min()} to {tb['year'].max()}. "
        f"Estimates cover people aged 15 to 64, except in {exceptions}. "
        f"{contents[0].upper()}{contents[1:]}."
    )
    max_px = layout["size"][0] - 2 * layout["margin"]
    wrapped = wrap_to_width(text, max_px, layout["footer_fontsize"])
    assert wrapped.count("\n") + 1 <= 2, "Note exceeds the template's two-line slot."
    return wrapped
