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
#
# Seaborn on purpose: OWID's own palette and fonts are applied in Figma, where the Chart colors
# library and Lato actually live. Setting them here would also be unreproducible — matplotlib on this
# machine has neither Lato nor Playfair Display, so a step that asked for them would silently fall
# back and emit different type depending on which machine built it.
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
    {"column": "eating_and_drinking", "label": "Eating & drinking", "color": ("tint", 7, 0.3)},
    {"column": "personal_care", "label": "Other personal care", "color": ("tint", 7, 0.68)},
    {"column": "housework_and_shopping", "label": "Housework & shopping", "color": ("deep", 5)},
    {
        "column": "other_unpaid_work",
        "label": "Other unpaid work",
        "contents": "care work and volunteering",
        "color": ("tint", 5, 0.45),
    },
    {"column": "tv_and_radio", "label": "TV & Radio", "color": ("deep", 0)},
    {"column": "seeing_friends", "label": "Seeing friends", "color": ("tint", 0, 0.42)},
    {"column": "other_leisure", "label": "Other leisure", "color": ("tint", 0, 0.78)},
]

TOTAL_LEISURE_COLUMN = "total_leisure"

# Between two names in a listed header. Drawn as its own run in `MUTED_COLOR` rather than appended to
# the name before it, so it neither takes that name's color nor fades out after a pale tint. The space
# that precedes it belongs to the name's run: `TextPath` measures ink, so a *leading* space is lost
# from a run's advance (a trailing one is recovered by the sentinel in `text_advance_px`), and a run
# starting with a space would draw one space further right than the layout accounted for.
SEPARATOR = "· "

# A group's values are drawn only where this share of countries can hold one; otherwise none of them
# are. A number on a handful of rows reads as a fact about those countries rather than as the rest
# being too narrow to print — education fitted on 1 of 35 rows, and that one number said nothing.
VALUE_LABEL_COVERAGE = 0.75

# Countries are ranked by this group or category, most minutes at the top. A GROUPS column ranks by
# that group alone; a CATEGORIES name ranks by the sum of its groups. Asserted in load_chart_groups.
# The alternative chart draws the four categories as single segments and nothing below them. The case
# for it is in `ai/time_use_comparability/`: the residual buckets *inside* each category vary two- to
# threefold across countries (other unpaid work runs 39-132 minutes, other leisure 78-179) while the
# categories themselves vary far less (personal care is 665 +/- 31), so most of what the ten-segment
# split resolves at that level is where each survey drew its coding lines rather than how people
# differ. Aggregating takes the mean coefficient of variation across the segments from 0.22 to 0.12.
#
# `contents` is deliberately terse here: all four are aggregates, so the Note has to say what each one
# holds, and it has three lines to do it in.
#
# These four name Chart colors library colors outright rather than the seaborn placeholders the ten-group
# version uses: the reason for placeholders is that the fonts cannot be reproduced here, and colors can,
# so a render that shows the real ones is simply a truer preview of the frame. Figma still binds each to
# its library style — the hex is the preview, the binding is the artifact.
#
# Why not the detailed chart's Coral, Denim, Copper and Dark Olive Green: both of that set's problems
# appear only once ten segments become four. Its neighbours merge in grayscale without the tints that
# used to sit between them (1.21:1 and 1.20:1 against a 1.6:1 floor), and Coral and Copper are the two
# most saturated entries in the palette, which is loud across segments this wide.
#
# Two constraints shape what is left, and the second one is easy to miss:
#   - Touching fills need a grayscale seam of 1.6:1, which forces the stack to alternate light and dark.
#     No all-light set exists: everything light in the palette sits at L 60-65, where two of them have
#     no seam at all.
#   - A category's color is also the color its legend name and member list are printed in, at 11px on
#     cream. Camel, Turquoise and Light Teal — the colors a reader would call soft — measure under 3:1
#     as text. The library's answer is its "Line and Slope Charts" group, a darker variant of each light
#     fill for thin marks; every one of them measures exactly 4.4:1 on this cream, which is the tier to
#     hold text to. So a light fill is fine as long as its name is set in that variant, which is the
#     three-part color spec below.
# The Default Palette's first four — Denim, Rusty Orange, Camel, Light Teal — with the middle two
# swapped so that touching segments differ in lightness. The library's own order puts two same-lightness
# pairs side by side (Denim beside Rusty Orange at 1.14:1, Camel beside Light Teal at 1.08:1, against a
# 1.6:1 floor), which reads fine in color and vanishes in black and white; swapping costs no color and
# takes the opaque seams to 1.86 / 2.12 / 2.28. Which category carries which color is free — the CVD
# numbers are identical for any arrangement of the same four.
#
# THREE ACCEPTED DEVIATIONS, all measured at the opacity the bars are actually drawn with (0.8 over the
# canvas, `SEGMENT_ALPHA`), because that composite is what a reader sees:
#   - Seams come out 1.57 / 1.86 / 1.95. The first is 2% under the floor: compositing lightens Denim and
#     Camel unevenly and closes the gap that is 1.86 opaque. Swapping Camel and Light Teal (Denim, Light
#     Teal, Rusty Orange, Camel) is the arrangement that clears all three at 0.8 — 1.66 / 1.95 / 1.86.
#   - In-bar labels: the best available color measures 3.74:1 on Denim and 4.30:1 on Rusty Orange, under
#     the 4.5:1 these labels need. This is the cost of the opacity, not of the palette — at full opacity
#     both clear it (5.46 and 6.21 in white). Grapher can afford 0.8 because it puts no labels inside its
#     stacked segments; this chart does.
#   - Camel against Light Teal: dE 19.2 opaque, 15.3 composited, against a floor of 20 — the palette's
#     warm-versus-light-green weak spot, made worse by everything moving towards the canvas. Fixing this
#     one needs a different color, not a different order.
MAIN_CATEGORY_GROUPS = [
    {
        "column": "main_paid_work_or_study",
        "members": ["Paid work", "Commuting", "School or classes", "Homework"],
        "compact": True,
        "label": "Paid work or study",
        # Denim, legible as its own text at 5.3:1.
        "color": ("hex", "#4c6a9c"),
        "as_hours": True,
    },
    {
        "column": "main_personal_care",
        "members": ["Sleep", "Eating & drinking", "Other personal care"],
        "label": "Personal care",
        # Camel, with Camel* for the name: the fill itself measures 2.8:1 as text.
        "color": ("hex", "#bc8e5a", "#996d39"),
        "as_hours": True,
        # Compact like the rest, though it has room for the spelled-out form. One category set
        # differently from its neighbours reads as a difference in kind, and the only difference is
        # that this segment is wider.
        "compact": True,
    },
    {
        "column": "main_unpaid_work_and_other",
        "members": ["Housework", "Childcare", "Shopping", "Volunteering", "Other"],
        "compact": True,
        "as_hours": True,
        "label": "Unpaid work & other",
        # Rusty Orange, legible as its own text at 6.0:1.
        "color": ("hex", "#b13507"),
    },
    {
        "column": "main_leisure",
        "members": ["TV & radio", "Seeing friends", "Sports", "Events", "Other leisure"],
        "compact": True,
        "as_hours": True,
        "label": "Leisure",
        # Light Teal, with Light Teal* for the name: the fill itself measures 2.6:1 as text.
        "color": ("hex", "#58ac8c", "#2c8465"),
    },
]

# One category per segment, so the header machinery names each segment instead of bracketing a run.
MAIN_CATEGORIES = [
    {
        "name": group["label"],
        "color": group["color"],
        "columns": [group["column"]],
        # What the segment holds, listed under its name — the same names the detailed chart sets inside
        # each bracket, so a reader moving between the two versions recognises them.
        "members": group["members"],
    }
    for group in MAIN_CATEGORY_GROUPS
]

# Paid work is the original chart's ranking, and the one column education cannot distort: education
# time is depressed wherever the survey's age floor excludes teenagers (Lithuania, 20-64).
SORT_BY = "paid_work"

TITLE = "How do people spend their time?"

# Countries whose most recent survey predates this are left out. The source gives one survey per
# country, so this drops countries rather than years, and 2010 is where that costs least: six
# countries sit on 2010 itself, so the cut lands on a cluster instead of slicing mid-run, and it buys
# eleven years of recency for nine countries. It also happens to remove every age-of-reference
# exception — Australia (15+), China (15-74) and Lithuania (20-64) are all pre-2010 — so what is left
# is 15-to-64 throughout.
#
# What it costs is India and China, and with them most of the coverage outside high-income Europe.
# Worth knowing when weighing that: survey year does not tilt the ranking. The correlation between a
# country's survey year and any of the four categories is +0.09 at most (ai/time_use_comparability).
# Set to None to draw all 35.
EARLIEST_SURVEY_YEAR = 2010

# The row labels set the width of the column they sit in, so the two longest names are shortened to
# the forms the style guide sanctions (no periods). It buys the bars 15px on desktop and 13px on
# mobile, which is the gap between these two names and the next-longest, "New Zealand".
SHORT_COUNTRY_NAMES = {"United Kingdom": "UK", "United States": "US"}

# Credited on the license line: the original chart's author, and this refresh's. Named one by one
# because the line sets them in bold, as it does the license itself.
AUTHORS = ["Esteban Ortiz-Ospina", "Pablo Arriagada"]

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

# The greys the templates set on their own text slots, so a render reads like the frame it feeds.
# Every other colour here is a placeholder that Figma replaces.
TITLE_COLOR = "#2d2e2d"
TEXT_COLOR = "#5b5b5b"
FOOTER_COLOR = "#858585"
MUTED_COLOR = "#777777"
CATEGORY_RULE_COLOR = "#bbbbbb"
# Bar fills carry grapher's own default, GRAPHER_AREA_OPACITY_DEFAULT = 0.8 (GrapherConstants.ts), which
# both the stacked and the discrete bar chart apply to every bar; grapher leaves its labels at 1, and so
# does this. It changes every measured number, because what a reader sees is the fill composited over the
# canvas rather than the library hex — see `composite_on_background`, which is what the label colours and
# the recorded seams are measured against.
SEGMENT_ALPHA = 0.8

# In-bar values are white on saturated fills and dark on light ones, whichever reads better *measured*
# against the fill — a luminance cutoff picked white on Dark Orange at 4.48:1 where the dark scores
# 4.68:1, and the 4.5:1 bar is exactly what these labels have to clear. The dark is Text/Gray 100, the
# style the frame binds them to, so the render and the frame agree.
DARK_VALUE_COLOR = "#2d2e2d"

# The templates' own canvas, which the transparent SVG sits on. A category's name and member list are
# printed in that category's colour, so this is what they have to stand out against.
BACKGROUND_COLOR = "#fffbf5"
# The tier the Chart colors library itself targets: each of its "Line and Slope Charts" variants — the
# darker form of a light fill, meant for thin marks and text — measures exactly 4.4:1 on this cream.
# A fill lighter than that is darkened for text here, which is the same pairing, derived rather than
# hardcoded: the frame binds the library's variant, and this keeps the local render showing the same
# relationship instead of printing a name nobody could read.
TEXT_CONTRAST_MIN = 4.4

# A template pixel in points (100 template px per inch over 72 points per inch).
POINTS_PER_PIXEL = 0.72

# Template pixels per inch: the figure is sized so the saved image keeps the template proportions.
PIXELS_PER_INCH = 100

# Least clearance between the tagline and the license, which share the desktop footer's last row.
LICENSE_TAGLINE_GAP = 8

# Clearance between the plot's own ink and the template's text above and below it, in template px.
# The design asks for 12-16 on these frames; 14 is the middle of that.
BAND_INSET = 14

# The template sets its slots in Lato and Playfair Display, neither of which is installed here, so
# this step measures in DejaVu Sans (matplotlib's fallback) and wraps against the slot as that font
# sets it. DejaVu is the wider of the two rulers, measured in Figma on 2026-08-19 by setting this
# chart's own strings in the real faces at the real sizes: DejaVu runs 1.15x Lato Regular (four
# strings at 11, 12 and 16px, spread 1.150-1.159), 1.26x Lato Bold, and 1.10x Playfair Display
# SemiBold. So a line that fits the slot here fits it in the frame with room to spare.
#
# Which ruler applies depends on what is being asked, and one constant used to answer both:
#   - *Wrapping* text the step draws is judged in DejaVu, at the slot itself. Anything wider overruns
#     the margin of the PNG a reviewer looks at, and going the other way — letting a line use the Lato
#     room — hands Figma a baked line too wide for its box, which it re-wraps into a line the step
#     never drew. The cost is a wrap the frame would not have needed: one line of band.
#   - *Whether a design fits the template* is a fact about the frame, so it is judged in Lato, by
#     dividing out the ratio below. The footer's tagline-and-license row is the case that matters: it
#     needs 838px of an 818px row in DejaVu and 792px of it in Lato, so measuring it in the step's own
#     font rejects a row that sets correctly in the frame.
# (An earlier pair of 1.03/0.97 recorded the ratio as 2-3%, from a measurement I could not reproduce.
# These come from probe nodes set in the Charts file itself.)
DEJAVU_OVER_LATO = 1.15
DEJAVU_OVER_PLAYFAIR = 1.10

# A title line, in template px, in every one of these templates. Their titles set line height
# explicitly where every other slot leaves it automatic, which is why this is a constant and the
# other line heights are per-slot.
TEMPLATE_TITLE_LINE_PX = 29

# Horizontal breathing room, in template pixels: between a country label and its bar, between the
# bars and the total-leisure column, and inside a segment around its value.
COUNTRY_LABEL_PAD = 8
TOTAL_COLUMN_GAP = 10
VALUE_PAD = 3

# Most of the frame the total-leisure column may take before its unit is dropped to give the bars
# their room back.
TOTAL_COLUMN_MAX_SHARE = 0.12

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

# Extra air between a category's name and the list under it, where the name sits directly on its own
# list rather than above a bracket rule. In lines of that list: half a line is enough to read as a
# heading, and a full line pushes the name far enough from its own list to start looking detached
# again — which is the problem this gap exists to solve.
CATEGORY_NAME_GAP_LINES = 0.5

# Listed header geometry (mobile), in template pixels: the gap between categories on a line and
# the extra leading between lines.
FLOW_GAP = 14
FLOW_LINE_PAD = 4

# Bars fill this share of a row's pitch.
BAR_FRACTION = 0.8

# The two layouts, taken from the static-chart template frames. Geometry is in template pixels,
# y measured from the top edge as Figma reports it. `full_footer` separates desktop (Note and
# tagline rows) from mobile (neither). Row positions come from "Static Chart Template_Vertical"
# (850x1095) and "Static Chart Template_Mobile (example 2)" (540x824), re-measured 2026-08-17.
LAYOUTS = {
    "time_use_by_country": {
        "size": (850, 1095),
        "margin": 16.216,
        # Every text slot the template defines, at the size and line height the template gives it,
        # so this step draws the same text the frame will and the render previews the frame instead
        # of only its proportions. The plot's band comes from these too: the header is a top-anchored
        # auto-layout and the footer a bottom-anchored one, so the room between them moves a line at
        # a time as the slots wrap, and recording one case as a constant left dead space above the
        # plot whenever a slot came in shorter than it.
        "template_text": {
            # Header: a title row, a 6px auto-layout gap, then the subtitle. `origin_y` is where the
            # row starts — the header block's own top padding here, and the block's y on mobile.
            #
            # `logo_px` is the height the logo beside the title forces on that row, since the row
            # hugs whichever of the two is taller. It is 0 here because the frame takes the logo out
            # of the auto-layout instead: with a one-line title the logo would otherwise set the row's
            # height and drop the subtitle 12px, where every other page in the Charts file — all of
            # them two-line titles, taller than the logo — shows a 6px gap. On mobile it stays in the
            # flow, which is why that template's `logo_px` is its real height.
            #
            # Out of the flow, the logo hangs into the subtitle's own band — it runs to y=57.5 where
            # the subtitle's line box starts at 51.2 — so the subtitle has to stop short of it
            # sideways instead. `logo_reserve_px` is the logo's width plus a gap, and it is why the
            # subtitle wraps against less than its slot. A note used to record that this was "safe
            # because the subtitle ends at x=588", which held for the detailed chart's subtitle and
            # not for the four-category one: at 801px in Lato it ran 47px under the logo.
            "origin_y": 16.216,
            "logo_px": 0,
            "logo_reserve_px": 73.5,
            "title_slot_px": 737.84,
            "title_px": 25,
            "header_gap_px": 6,
            "subtitle_slot_px": 817.57,
            "subtitle_px": 16,
            "subtitle_line_px": 19,
            # Footer. Its rows are pinned to the frame's bottom margin, so the Note's ink bottom is
            # fixed and its top rises as it wraps.
            "note_px": 12,
            "note_line_px": 14,
            "note_slot_px": 818,
            "note_bottom_px": 1043.81,
            "source_y": 1047.81,
            "source_px": 12,
            "tagline_y": 1065.81,
            "tagline_slot_px": 467,
            "license_y": 1065.81,
            "license_px": 11,
        },
        "full_footer": True,
        "country_fontsize": 9,
        "value_fontsize": 8.75,
        "header_fontsize": 9.5,
        # Width reserved for the total-leisure column, in template pixels.
        "with_mins_suffix": True,
        # Where each half of the header goes. The category brackets span the top row from above, and
        # each category's own member names are stacked inside its bracket, one per line
        # ("bracketed") — so the header reads category, then its members, then the data, with every
        # name over the run of bars it belongs to.
        # Alternatives for the names: "below_flow" (one list in bar order under the bars) or
        # "below_listed" (grouped under their category names, as mobile lists them).
        "groups": GROUPS,
        "categories": CATEGORIES,
        "category_rule": True,
        "total_column": True,
        "category_side": "above",
        "group_labels": "bracketed",
    },
    "time_use_by_country_mobile": {
        "size": (540, 824),
        "margin": 16,
        "template_text": {
            # Mobile's header block carries no padding of its own and its logo is shorter. The logo
            # stays in the flow here — unlike the 850-wide frame — because this subtitle runs the full
            # 508px and its first line reaches x=493 against a logo starting at 476, so a header that
            # came up past the logo would print the subtitle under it. The cost is a 12px title-to-
            # subtitle gap where desktop gets 6.
            "origin_y": 16,
            "logo_px": 35.23,
            # In the flow, so the subtitle already sits below it and needs no side clearance.
            "logo_reserve_px": 0,
            "title_slot_px": 428,
            "title_px": 25,
            "header_gap_px": 6,
            "subtitle_slot_px": 508,
            "subtitle_px": 16,
            "subtitle_line_px": 19,
            # No Note row, so the Data source row's ink top bounds the band and nothing below the
            # plot reflows. Both footer rows are a size larger than the 850-wide pair's.
            "note_px": None,
            "footer_top_px": 770,
            "source_y": 770,
            "source_px": 14,
            "tagline_slot_px": None,
            "license_y": 791,
            "license_px": 14,
        },
        "full_footer": False,
        "country_fontsize": 7.5,
        "value_fontsize": 7.5,
        "header_fontsize": 8,
        "with_mins_suffix": False,
        "groups": GROUPS,
        "categories": CATEGORIES,
        "category_rule": True,
        "total_column": True,
        "category_side": "listed_above",
        "group_labels": "listed_above",
    },
}

# The alternative shares the desktop frame exactly — same template, same type, same row order — so that
# comparing the two charts is comparing the segmentation and nothing else.
LAYOUTS["time_use_by_country_main_categories"] = {
    **LAYOUTS["time_use_by_country"],
    "groups": MAIN_CATEGORY_GROUPS,
    "categories": MAIN_CATEGORIES,
    # Each category is one segment, so a bracket would only underline a name that already stands over
    # it, and there is no member layer left to list.
    "category_rule": False,
    "group_labels": "bracketed",
    # Leisure is a segment here and carries its own value, so the column would repeat it.
    "total_column": False,
    # The values are hours and minutes here, so the subtitle has to say so.
    # "Average", not "Averages of": measured in Lato, the longer opening put the line at 826px against
    # an 817.57px slot, so the frame wrapped a subtitle this step had drawn on one line. A slot filled
    # past ~97% is a coin flip between the two fonts — leave a few percent, or measure in Figma.
    # No age clause: it has to clear the logo hanging at the end of this line (see `logo_reserve_px`),
    # and the Note already says who the estimates cover.
    "subtitle": "Average hours and minutes per day, from time-use surveys run between {years}.",
    # With one segment per category, a name that overhangs its own bar reads as pointing at its
    # neighbour too — so wrap it rather than only wrapping to avoid a collision.
    "wrap_overhanging_names": True,
    # Hang each category's list off the bars rather than off the top of the band, so a short list does
    # not end two lines short of the plot.
    "names_bottom_aligned": True,
}

# Both layouts share the original chart's subtitle; mobile appends what its missing Note slot
# would have carried — the age caveat is about what the chart claims, so it cannot be dropped.
# `{years}` is filled from the data, so the window a reader is told about cannot drift from the window
# the chart draws — the Note repeated it before, but a subtitle-only reader saw no date at all.
SUBTITLE = "Average minutes per day, from time-use surveys run between {years}."
# Mobile has no Note row, so anything the desktop Note carries and mobile readers still need has to
# ride here — the ages among them, now that the subtitle no longer states them.
MOBILE_NOTE = "Estimates cover people aged 15 to 64, and each country's survey year is shown in brackets."


def run() -> None:
    """Load data, render and save both versions of the chart."""
    tb, ages = load_chart_groups()
    tb = add_main_category_totals(tb)
    paths.log.info(f"Loaded {len(tb)} countries, surveys {tb['year'].min()}-{tb['year'].max()}")

    source_citation = build_source_citation(tb)
    paths.log.info(f"Source citation: {source_citation}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(sort_rows(tb, layout), ages, source_citation, layout)
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


def sort_rows(tb: Table, layout: dict) -> Table:
    """Rank rows by the layout's own leading segment, descending.

    A chart whose first column is not in order reads as unsorted, and the four-category version leads
    with paid work *plus* study where the detailed one leads with paid work alone — so the two orders
    differ by a few places, and each chart is right about itself. Stable, so tied rows keep the
    source's order between runs.
    """
    column = layout["groups"][0]["column"]
    return tb.loc[tb[column].sort_values(ascending=False, kind="stable").index]


def add_main_category_totals(tb: Table) -> Table:
    """Add each top-level category's total as its own column, for the four-category chart.

    This is a chart-side sum of this step's own display groups, which is why it lives here rather than
    in garden: garden owns the mapping from the OECD's 30 activities into the ten groups, and
    `CATEGORIES` is how *this* step groups those ten. If the four-category version is the one that
    ships, promote the sums to garden so the aggregate is in the catalog too.
    """
    assert [group["label"] for group in MAIN_CATEGORY_GROUPS] == [category["name"] for category in CATEGORIES], (
        "The four-category groups must stay in the same order as CATEGORIES, since they are its totals."
    )
    for group, category in zip(MAIN_CATEGORY_GROUPS, CATEGORIES):
        tb[group["column"]] = tb[list(category["columns"])].sum(axis=1)
    totals = tb[[group["column"] for group in MAIN_CATEGORY_GROUPS]].sum(axis=1)
    assert totals.between(MINUTES_PER_DAY - 1, MINUTES_PER_DAY + 1).all(), (
        f"The four categories must still spend the whole day: got {totals.min():.1f}-{totals.max():.1f}."
    )
    return tb


def load_chart_groups() -> tuple[Table, dict[str, str]]:
    """Load the precomputed chart groups (total population), ranked by `SORT_BY`.

    Returns the table plus the age-of-reference exceptions (country -> age range) for the note.
    """
    ds = paths.load_dataset("time_use")
    tb = ds.read("time_use_chart_groups")
    tb = tb[tb["sex"] == "total"].drop(columns=["sex"])

    if EARLIEST_SURVEY_YEAR is not None:
        dropped = sorted(
            (str(row["country"]), int(row["year"])) for _, row in tb[tb["year"] < EARLIEST_SURVEY_YEAR].iterrows()
        )
        tb = tb[tb["year"] >= EARLIEST_SURVEY_YEAR]
        paths.log.info(
            f"Surveys before {EARLIEST_SURVEY_YEAR} left out: "
            + ", ".join(f"{country} ({year})" for country, year in dropped)
        )

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

    expected_countries = 26 if EARLIEST_SURVEY_YEAR == 2010 else 35
    assert len(tb) == expected_countries, f"Expected {expected_countries} countries, got {len(tb)}."
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
    # The source's three exceptions are all pre-2010 surveys, so a 2010 cutoff removes them; without a
    # cutoff all three are in. Either way, assert which ones survive rather than trusting the filter.
    ages = {country: age for country, age in ages.items() if country in set(tb["country"])}
    expected_ages: set[str] = (
        set()
        if EARLIEST_SURVEY_YEAR and EARLIEST_SURVEY_YEAR > 2006
        else {
            "Australia",
            "China",
            "Lithuania",
        }
    )
    assert set(ages) == expected_ages, f"Age-of-reference exceptions changed: {sorted(ages)}."

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
    template = layout["template_text"]

    def fx(x_px: float) -> float:
        return x_px / width_px

    def fy(y_px: float) -> float:
        return 1 - y_px / height_px

    fig = plt.figure(figsize=(width_px / PIXELS_PER_INCH, height_px / PIXELS_PER_INCH))
    fig.patch.set_facecolor("white")

    # --- header: the template's title row, then its subtitle ---
    title = wrap_to_slot(TITLE, template["title_slot_px"], template["title_px"])
    years = f"{tb['year'].min()} and {tb['year'].max()}"
    subtitle_text = layout.get("subtitle", SUBTITLE).format(years=years)
    if not layout["full_footer"]:
        subtitle_text = f"{subtitle_text} {MOBILE_NOTE}"
    # Where the logo hangs beside the subtitle rather than above it, the subtitle's own slot is that
    # much narrower — every line of it, not only the first, since a second line costs a line of band
    # and nothing else.
    subtitle_slot_px = template["subtitle_slot_px"] - template["logo_reserve_px"]
    subtitle = wrap_to_slot(subtitle_text, subtitle_slot_px, template["subtitle_px"])
    title_row_px = max(lines_in(title) * TEMPLATE_TITLE_LINE_PX, template["logo_px"])
    subtitle_y = template["origin_y"] + title_row_px + template["header_gap_px"]

    draw_slot(
        fig, fx(margin_px), fy(template["origin_y"]), title, template["title_px"], TEMPLATE_TITLE_LINE_PX, "title"
    )
    draw_slot(
        fig,
        fx(margin_px),
        fy(subtitle_y),
        subtitle,
        template["subtitle_px"],
        template["subtitle_line_px"],
        "subtitle",
        color=TEXT_COLOR,
    )

    # --- footer, in the slots the static-chart templates define ---
    note = build_note(tb, ages, layout) if layout["full_footer"] else None
    draw_footer(fig, note, source_citation, layout, fx, fy)

    # --- the chart band: the category header, then the bar rows ---
    country_labels = [
        f"{SHORT_COUNTRY_NAMES.get(country, country)} ({year})"
        for country, year in zip(tb["country"].tolist(), tb["year"].tolist())
    ]
    country_space_px = (
        max(text_width_px(label, layout["country_fontsize"]) for label in country_labels) + COUNTRY_LABEL_PAD
    )

    plot_left_px = margin_px + country_space_px
    plot_width_px = (width_px - margin_px) - plot_left_px
    total_column_px, total_with_mins = total_column(tb, layout) if layout["total_column"] else (0.0, False)
    bar_width_px = plot_width_px - total_column_px
    px_per_min = bar_width_px / MINUTES_PER_DAY

    # Category brackets attach to the row they touch: the top row above the bars. Group labels
    # placed below attach to the bottom row, for the same reason.
    top_spans = segment_spans(tb.iloc[0], px_per_min, layout["groups"])
    bottom_spans = segment_spans(tb.iloc[-1], px_per_min, layout["groups"])

    # The name list, for the layouts that use one. Drawn in the band above the bars it shares that
    # band with the two-line "Total leisure" column header, so it stops short of that column; drawn
    # below the chart it has the whole frame.
    listed_lines: list[list[tuple]] = []
    listed_px = 0.0
    if layout["group_labels"] in ("listed_above", "below_listed", "below_flow"):
        listed_available_px = width_px - 2 * margin_px
        if layout["group_labels"] == "listed_above":
            listed_available_px -= total_column_px + TOTAL_COLUMN_GAP
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
    assert layout["group_labels"] in {"bracketed", "below_flow", "below_listed", "listed_above", None}, (
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
            name_gap = (
                CATEGORY_NAME_GAP_LINES * line_px(layout["header_fontsize"])
                if layout.get("names_bottom_aligned")
                else 0.0
            )
            room = max(room, category_base_px + CATEGORY_TICK + CATEGORY_LABEL_GAP + name_gap + tallest)
        if group_at == side and layout["group_labels"] in ("below_listed", "below_flow"):
            room = max(room, LEADER_GAP + listed_px)
        if side == "above" and "listed_above" in (layout["category_side"], layout["group_labels"]):
            room = max(room, listed_px)
        return room

    # Room above for whichever half goes there, and never less than the two-line "Total leisure"
    # column header, which sits above the first bar — on a frame that has one.
    total_header_px = 2 * line_px(layout["header_fontsize"]) + LEADER_GAP if layout["total_column"] else 0.0
    header_px = max(band_px("above"), total_header_px)
    below_px = band_px("below")

    # The plot sits between the subtitle's ink and the footer's, inset by BAND_INSET at each end.
    # Both are ink rather than frame edges: the footer frame already starts 16px above its Note, so
    # insetting from the frame would inset twice.
    band_top = subtitle_y + lines_in(subtitle) * template["subtitle_line_px"]
    band_bottom = (
        template["note_bottom_px"] - lines_in(note) * template["note_line_px"]
        if note is not None
        else template["footer_top_px"]
    )
    content_top_px = band_top + BAND_INSET
    chart_top_px = content_top_px + header_px
    chart_bottom_px = band_bottom - BAND_INSET - below_px

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

    group_colors = {group["column"]: resolve_color(group["color"], palette) for group in layout["groups"]}

    draw_bars(
        ax,
        tb,
        country_labels,
        group_colors,
        px_per_min,
        row_pitch_px,
        total_column_px,
        total_with_mins,
        layout,
        value_label_columns(tb, px_per_min, layout),
    )

    if category_placements is not None:
        rows_out = rows_above if category_at == "above" else rows_below
        # Each category's own list depth, so a name can follow its list rather than sit at the band's top.
        block_depths = (
            {block["name"]: len(block["lines"]) * line_px(layout["header_fontsize"]) for block in bracketed_blocks}
            if bracketed_blocks and layout.get("names_bottom_aligned")
            else None
        )
        draw_category_brackets(
            ax,
            category_placements,
            palette,
            px_per_min,
            rows_out,
            layout,
            category_at,
            category_base_px,
            block_depths,
        )
    if bracketed_blocks is not None:
        rows_out = rows_above if category_at == "above" else rows_below
        draw_bracketed_names(ax, bracketed_blocks, palette, px_per_min, rows_out, layout, bracketed_px)
    if layout["group_labels"] == "listed_above":
        draw_listed_header(fig, listed_lines, palette, content_top_px, margin_px, layout, fx, fy)
    elif layout["group_labels"] in ("below_listed", "below_flow"):
        draw_listed_header(fig, listed_lines, palette, chart_bottom_px + LEADER_GAP, margin_px, layout, fx, fy)

    # Total-leisure column header, centred over the numbers it heads — which is the column minus the
    # gap that separates it from the bars, since the values are right-aligned on the content edge.
    #
    # One `ax.text` per line, rather than one call with a newline in it: matplotlib bakes a
    # multi-line label's alignment into each line's own offset and emits no `text-anchor` at all, so
    # the lines arrive in Figma as independent left-aligned boxes and lose both their centring on the
    # column and on each other the moment the font changes.
    values_centre_min = content_right_min(total_column_px, px_per_min) - (
        (total_column_px - TOTAL_COLUMN_GAP) / 2 / px_per_min
    )
    header_lines = ["Total", "leisure"] if layout["total_column"] else []
    for index, line in enumerate(header_lines):
        depth_px = LEADER_GAP + (len(header_lines) - 1 - index) * line_px(layout["header_fontsize"])
        ax.text(
            values_centre_min,
            rows_above(depth_px),
            line,
            ha="center",
            va="bottom",
            fontsize=layout["header_fontsize"],
            fontweight="bold",
            color="#333333",
            gid="header__total-leisure" if index == 0 else f"header__total-leisure-line{index}",
        )

    # Drop clipping everywhere so labels outside the axes survive into the SVG whole.
    for artist in fig.findobj():
        artist.set_clip_on(False)

    return fig


def value_label_columns(tb: Table, px_per_min: float, layout: dict) -> set[str]:
    """Which groups carry a value, and in which form — `{column: index into value_candidates}`.

    Per frame, since the mobile bars are little over half as wide.
    """
    labelled = {}
    for group in layout["groups"]:
        column = group["column"]
        forms = value_candidates(group, float(tb[column].iloc[0]), layout["with_mins_suffix"])
        # A column is one unit, so it takes one form: the longest that fits on `VALUE_LABEL_COVERAGE`
        # of the rows, used on all of them. Choosing per row instead reads as a mistake — three rows
        # saying "2h 42m" among thirty saying "2 hours 42 mins" looks like the label ran out of room,
        # which it did, but the reader sees an inconsistency rather than a constraint.
        #
        # A group marked `compact` starts one form down, so the search picks "4h 29m" over
        # "4 hours 29 mins" even where the long form would fit.
        for index in range(1 if group.get("compact") else 0, len(forms)):
            fits = sum(
                1
                for minutes in tb[column].astype(float)
                if fit_text(
                    value_candidates(group, minutes, layout["with_mins_suffix"])[index:],
                    minutes * px_per_min,
                    layout["value_fontsize"],
                )
            )
            if fits >= VALUE_LABEL_COVERAGE * len(tb):
                labelled[column] = index
                break
    return labelled


def draw_bars(
    ax,
    tb: Table,
    country_labels: list[str],
    group_colors: dict,
    px_per_min: float,
    row_pitch_px: float,
    total_column_px: float,
    total_with_mins: bool,
    layout: dict,
    value_columns: dict[str, int],
) -> None:
    """One stacked row per country: the country label, the ten segments, and the leisure total."""

    def baseline(fontsize: float) -> float:
        """Where to put a row's baseline so its ink is centred on the bar, in row units.

        `va="center"` centres the font's whole line box, which reserves room for descenders the
        digits never use — so a value label sits a tenth of a bar high, and text with descenders
        ("Japan") lands on a different baseline from text without ("269 mins"). Placing the baseline
        explicitly at half a cap-height below the bar's centre fixes both.
        """
        return 0.5 + cap_height_px(fontsize) / 2 / row_pitch_px

    for row in range(len(tb)):
        country_row = tb.iloc[row]
        slug = slugify(str(country_row["country"]))
        y_center = row + 0.5

        ax.text(
            -COUNTRY_LABEL_PAD / px_per_min,
            row + baseline(layout["country_fontsize"]),
            country_labels[row],
            ha="right",
            va="baseline",
            fontsize=layout["country_fontsize"],
            color=TEXT_COLOR,
            gid=f"{slug}__label",
        )

        left = 0.0
        for group in layout["groups"]:
            column = group["column"]
            minutes = float(country_row[column])
            color = group_colors[column]
            ax.barh(
                y_center,
                minutes,
                left=left,
                height=BAR_FRACTION,
                color=color,
                alpha=SEGMENT_ALPHA,
                linewidth=0,
                gid=f"{slug}__{slugify(column)}",
            )
            label = (
                fit_text(
                    value_candidates(group, minutes, layout["with_mins_suffix"])[value_columns[column] :],
                    minutes * px_per_min,
                    layout["value_fontsize"],
                )
                if column in value_columns
                else None
            )
            if label:
                ax.text(
                    left + minutes / 2,
                    row + baseline(layout["value_fontsize"]),
                    label,
                    ha="center",
                    va="baseline",
                    fontsize=layout["value_fontsize"],
                    color=value_label_color(composite_on_background(color)),
                    gid=f"{slug}__{slugify(column)}-value",
                )
            left += minutes

        if not layout["total_column"]:
            continue

        total_leisure = round(float(country_row[TOTAL_LEISURE_COLUMN]))
        total_label = f"{total_leisure} mins" if total_with_mins else f"{total_leisure}"
        ax.text(
            content_right_min(total_column_px, px_per_min),
            row + baseline(layout["value_fontsize"]),
            total_label,
            ha="right",
            va="baseline",
            fontsize=layout["value_fontsize"],
            color=TEXT_COLOR,
            gid=f"{slug}__total-leisure",
        )


def draw_category_brackets(
    ax,
    category_placements: list[dict],
    palette,
    px_per_min: float,
    rows_out,
    layout,
    side: str,
    base_px: float,
    block_depths: dict[str, float] | None = None,
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
        if not layout["category_rule"]:
            # Where each category is a single segment, its name already stands over the run it names
            # and a bracket only underlines it.
            depth_px = block_depths.get(placement["name"]) if block_depths else None
            draw_category_name(ax, placement, palette, px_per_min, rows_out, layout, side, label_px, depth_px)
            continue
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


def draw_category_name(
    ax, placement, palette, px_per_min, rows_out, layout, side: str, label_px: float, depth_px: float | None = None
) -> None:
    """A category's name, stacked away from the bars, with no rule under it.

    `depth_px` is how deep this category's own list reaches. Passed, the name sits directly above that
    list rather than at the band's top, so a short list keeps its heading and the four names end up
    bottom-aligned as a group.
    """
    if depth_px is not None:
        label_px = depth_px + CATEGORY_LABEL_GAP + CATEGORY_NAME_GAP_LINES * line_px(layout["header_fontsize"])
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
            gid=f"category__{slugify(placement['name'])}"
            if index == 0
            else f"category__{slugify(placement['name'])}-line{index}",
        )


def draw_bracketed_names(
    ax, blocks: list[dict], palette, px_per_min: float, rows_out, layout: dict, height_px: float
) -> None:
    """Draw each category's member names inside its bracket, centred and reading top-down.

    Every block starts at the same distance out from the bars — directly under the bracket rule — so
    the four of them line up however many lines each one needed.

    Each run is anchored on the centre of its own slot rather than on its left edge. It draws in the
    same place either way, but the anchor is what survives Figma: a left-anchored run re-rendered in
    the template's narrower font shrinks away from its centre and pushes nothing back, so a line of
    them creeps out of its bracket. One name per line is the normal case, and a middle anchor pins
    it to the bracket's centre exactly.
    """
    fontsize = layout["header_fontsize"]
    for block in blocks:
        start, end = block["span"]
        centre = (start + end) / 2
        # Top-aligned, every list starts level with the deepest one and a short list ends well short of
        # the bars — two lines of air under "Personal care" while "Leisure" reaches down to them.
        # Bottom-aligned, every list ends the same distance above the bars and the air moves under the
        # category name, where it reads as space beneath a heading instead of a detached list.
        depth_px = len(block["lines"]) * line_px(fontsize) if layout.get("names_bottom_aligned") else height_px
        for index, line in enumerate(block["lines"]):
            width = sum(text_advance_px(text, fontsize) for text, _ in line)
            offset = centre - width / 2
            for text, group in line:
                drawn, ink_px, step_px = place_run(text, fontsize)
                ax.text(
                    (offset + ink_px / 2) / px_per_min,
                    rows_out(LEADER_GAP + depth_px - index * line_px(fontsize)),
                    drawn,
                    ha="center",
                    va="top",
                    fontsize=fontsize,
                    color=header_text_color(group["color"], palette),
                    gid=f"header__{slugify(group['column'])}",
                )
                offset += step_px


def draw_listed_header(fig, listed_lines, palette, top_px: float, margin_px: float, layout: dict, fx, fy) -> None:
    """Draw the category-grouped name list used where the frame is too narrow to label in place.

    Runs are anchored on their own centres, for the reason `draw_bracketed_names` gives: left-anchored
    runs re-rendered in a narrower font leave the gaps between the names growing down the line.
    """
    for index, line in enumerate(listed_lines):
        y_px = top_px + index * (line_px(layout["header_fontsize"]) + FLOW_LINE_PAD)
        for x_offset, text, spec, bold, gid in line:
            drawn, ink_px, _ = place_run(text, layout["header_fontsize"], bold)
            fig.text(
                fx(margin_px + x_offset + ink_px / 2),
                fy(y_px),
                drawn,
                ha="center",
                va="top",
                fontsize=layout["header_fontsize"],
                fontweight="bold" if bold else "normal",
                color=header_text_color(spec, palette),
                gid=gid,
            )


def draw_footer(fig, note: str | None, source_citation: str, layout: dict, fx, fy) -> None:
    """Fill the template's footer slots: Note, Data source, tagline and license.

    The footer is bottom-anchored, so the rows below the Note keep their y whatever the Note does and
    the Note itself grows upward from a fixed ink bottom.
    """
    width_px = layout["size"][0]
    margin_px = layout["margin"]
    template = layout["template_text"]

    if note is not None:
        note_top = template["note_bottom_px"] - lines_in(note) * template["note_line_px"]
        draw_slot(
            fig, fx(margin_px), fy(note_top), note, template["note_px"], template["note_line_px"], "note", FOOTER_COLOR
        )
    draw_slot(
        fig,
        fx(margin_px),
        fy(template["source_y"]),
        f"Data source: {source_citation}",
        template["source_px"],
        None,
        "data-source",
        FOOTER_COLOR,
    )

    # Desktop's tagline and license share one row; mobile stacks its two rows full-width.
    shares_tagline_row = layout["full_footer"]
    row_px = template["license_px"]
    if shares_tagline_row:
        # Nothing wraps this row — the tagline's wording is fixed and a name is never shortened — so
        # instead the row is drawn a hair smaller when this step's font would collide the two, which
        # the template's narrower Lato would not. The assert is the real limit: past it the row does
        # not fit even once the template sets it, and the wording has to give.
        content_px = width_px - 2 * margin_px
        needed_px = (
            text_advance_px(TAGLINE, row_px * POINTS_PER_PIXEL)
            + LICENSE_TAGLINE_GAP
            + run_row_width(license_runs(), row_px)
        )
        assert fits_slot(needed_px, content_px), (
            f"The tagline and license need {needed_px:.0f}px of the footer's {content_px:.0f}px row. "
            f"Shorten the license's wording — never a name."
        )
        row_px *= min(1.0, content_px / needed_px)
        draw_slot(
            fig,
            fx(margin_px),
            fy(template["tagline_y"]),
            wrap_to_slot(TAGLINE, template["tagline_slot_px"], row_px),
            row_px,
            None,
            "tagline",
            FOOTER_COLOR,
        )
    # "by <names>", not the template's "by the author <name>": with two names those two words are
    # what pushes the line past the tagline it shares a row with. The license and the names it credits
    # are set in bold, as the templates set their own CC-BY, which is why the row is laid out as runs
    # rather than written as one string.
    draw_run_row(
        fig,
        license_runs(),
        row_px,
        margin_px if not shares_tagline_row else width_px - margin_px,
        template["license_y"],
        "license",
        fx,
        fy,
        align="right" if shares_tagline_row else "left",
    )


# ---------------------------------------------------------------------------
# Header layout
# ---------------------------------------------------------------------------


def total_column(tb: Table, layout: dict) -> tuple[float, bool]:
    """Width of the total-leisure column, and whether its values carry the unit.

    Only as wide as its widest value plus one gap: the column *is* the distance between those numbers
    and the bars they belong to, so slack in it reads as the numbers drifting off the chart. The unit
    is spelled out where that still leaves the column a small share of the frame, and dropped where it
    would not — on a narrow frame the bars need the room more than the reader needs "mins" repeated.
    """
    widest = max(round(float(value)) for value in tb[TOTAL_LEISURE_COLUMN])
    budget_px = TOTAL_COLUMN_MAX_SHARE * (layout["size"][0] - 2 * layout["margin"])
    for with_mins in (True, False):
        label = f"{widest} mins" if with_mins else f"{widest}"
        width_px = text_width_px(label, layout["value_fontsize"]) + TOTAL_COLUMN_GAP
        if width_px <= budget_px:
            return width_px, with_mins
    raise AssertionError(f"The total-leisure column needs more than {budget_px:.0f}px, the frame's share of it.")


def content_right_min(total_column_px: float, px_per_min: float) -> float:
    """The content box's right edge, in the axes' minute units.

    Everything in the frame lines up on two verticals: the content's left edge, where the subtitle and
    note start and where the widest country label begins, and its right edge, which is where the logo
    ends. The bars stop at 1440 minutes, so the total-leisure column past them is what has to reach
    this edge — right-aligned on it, rather than left-aligned at a fixed gap and stopping short.
    """
    return MINUTES_PER_DAY + total_column_px / px_per_min


def segment_spans(row, px_per_min: float, groups: list[dict]) -> dict[str, tuple[float, float]]:
    """Each group's (start, end) in template pixels, for one row of the chart."""
    spans = {}
    cumulative = 0.0
    for group in groups:
        minutes = float(row[group["column"]])
        spans[group["column"]] = (cumulative * px_per_min, (cumulative + minutes) * px_per_min)
        cumulative += minutes
    return spans


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
    right_edge = spans[layout["groups"][-1]["column"]][1]

    def geometry(category: dict, lines: list[str]) -> tuple[float, tuple[float, float]]:
        start = spans[category["columns"][0]][0]
        end = spans[category["columns"][-1]][1]
        width = max(text_width_px(line, layout["header_fontsize"], bold=True) for line in lines)
        center = min(max((start + end) / 2, width / 2), right_edge - width / 2)
        return center, (center - width / 2, center + width / 2)

    placed: list[tuple[tuple[float, float], int]] = []
    placements = []
    for index, category in enumerate(layout["categories"]):
        variants = category_variants(category["name"], layout)
        if layout.get("wrap_overhanging_names"):
            # Collision with a neighbour is not the only reason to wrap. Where each category is a
            # single segment, a name can clear its neighbours and still overhang its own bar by half
            # its length, which reads as pointing at both. Prefer any form that fits inside the span
            # it names; `sorted` is stable, so the widest-first order survives within each group.
            span_px = spans[category["columns"][-1]][1] - spans[category["columns"][0]][0]
            variants = sorted(
                variants,
                key=lambda lines: (
                    max(text_width_px(line, layout["header_fontsize"], bold=True) for line in lines) > span_px
                ),
            )
        remaining = layout["categories"][index + 1 :]
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
    for category in layout["categories"]:
        start = spans[category["columns"][0]][0]
        end = spans[category["columns"][-1]][1]
        if len(category["columns"]) == 1 and category.get("members"):
            # One segment per category: the members are names rather than groups, and they all take the
            # category's own color — there are no tints here to match them to.
            members = [
                {"column": slugify(name), "label": name, "color": category["color"]} for name in category["members"]
            ]
        else:
            members = [group for group in layout["groups"] if group["column"] in category["columns"]]
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
    # One item per group, each carrying the separator that follows it: the break search below needs a
    # width per item, and a line may not begin with punctuation. The separator is drawn as its own run.
    groups = layout["groups"]
    items = [(group, " " + SEPARATOR if index < len(groups) - 1 else "") for index, group in enumerate(groups)]
    widths = [text_advance_px(group["label"] + sep, layout["header_fontsize"], False) for group, sep in items]

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
    for index, ((group, sep), width) in enumerate(zip(items, widths)):
        if index in breaks:
            lines.append([])
            x = 0.0
        slug = slugify(group["column"])
        # The space before the separator rides with the name, so neither run begins with one.
        name = group["label"] + (" " if sep else "")
        lines[-1].append((x, name, group["color"], False, f"header__{slug}"))
        if sep:
            name_px = text_advance_px(name, layout["header_fontsize"], False)
            lines[-1].append((x + name_px, SEPARATOR, ("literal", MUTED_COLOR), False, f"header__{slug}-separator"))
        x += width

    # A separator that ends a line has nothing to separate it from.
    for line in lines:
        if line[-1][4].endswith("-separator"):
            line.pop()
    return lines


def layout_listed_header(layout: dict, available_px: float) -> list[list[tuple]]:
    """Wrap the category-grouped name list into lines of (x, text, color spec, bold, gid)."""
    lines: list[list[tuple]] = [[]]
    x = 0.0
    for category in layout["categories"]:
        runs = [(f"{category['name']}: ", category["color"], True, f"category__{slugify(category['name'])}")]
        members = [group for group in layout["groups"] if group["column"] in category["columns"]]
        for index, group in enumerate(members):
            slug = slugify(group["column"])
            last = index == len(members) - 1
            # The space before a separator rides with the name; see SEPARATOR on why a run may not
            # begin with one.
            runs.append((label_in_context(group) + ("" if last else " "), group["color"], False, f"header__{slug}"))
            if not last:
                # Its own run, in one neutral color: appended to the name before it, the separator
                # inherits that name's fill, so it changes color down the list and all but vanishes
                # after a pale tint. It is punctuation, not data.
                runs.append((SEPARATOR, ("literal", MUTED_COLOR), False, f"header__{slug}-separator"))

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
    """Resolve a color spec into RGB.

    ("deep", i) is a seaborn palette position and ("tint", i, w) a blend of one towards white — both
    placeholders for a color Figma sets. ("hex", fill) and ("hex", fill, text) name a library color
    outright, the second form pairing a light fill with the darker variant its text uses.
    """
    if spec[0] == "hex":
        return to_rgb(spec[1])
    if spec[0] == "deep":
        return palette[spec[1]]
    return tint(palette[spec[1]], spec[2])


def header_text_color(spec: tuple, palette) -> tuple[float, float, float]:
    """The text form of a fill: a pale group's own fill is unreadable as a name on the light
    background, so it keeps the hue at a much shallower tint.

    A ("literal", color) spec passes through unchanged, which is how punctuation between names avoids
    taking either name's color.
    """
    if spec[0] == "literal":
        return to_rgb(spec[1])
    if spec[0] == "hex":
        # A three-part spec names the library's own darker variant of this fill; two parts leave it to
        # be derived, which is the same rule applied by measurement.
        return to_rgb(spec[2]) if len(spec) > 2 else readable_on_background(spec[1])
    if spec[0] == "tint":
        return tint(palette[spec[1]], spec[2] * 0.4)
    return readable_on_background(palette[spec[1]])


def readable_on_background(color) -> tuple[float, float, float]:
    """The same hue, darkened just far enough to be read as text on the template's canvas.

    Returns the colour untouched when it already clears the bar, so this only bites on fills light
    enough to need it.
    """
    shaded = to_rgb(color)
    while contrast_ratio(shaded, BACKGROUND_COLOR) < TEXT_CONTRAST_MIN:
        shaded = shade(shaded, 0.02)
    return shaded


def shade(color, weight: float) -> tuple[float, float, float]:
    """Blend a color towards black. weight=0 keeps it, weight=1 turns it black."""
    r, g, b = to_rgb(color)
    return (r * (1 - weight), g * (1 - weight), b * (1 - weight))


def contrast_ratio(first, second) -> float:
    """WCAG contrast ratio between two colors."""
    light, dark = sorted((relative_luminance(first), relative_luminance(second)), reverse=True)
    return (light + 0.05) / (dark + 0.05)


def relative_luminance(color) -> float:
    """WCAG relative luminance: sRGB channels linearised, then weighted."""
    channels = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in to_rgb(color)]
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]


def value_label_color(fill) -> str:
    """White or dark for a value sitting on `fill`, whichever has the higher contrast against it."""
    return "white" if contrast_ratio("white", fill) >= contrast_ratio(DARK_VALUE_COLOR, fill) else DARK_VALUE_COLOR


def composite_on_background(color, alpha: float = SEGMENT_ALPHA) -> tuple[float, float, float]:
    """A fill as it lands on the canvas once its opacity is applied — the color a label sits on."""
    fill, canvas = to_rgb(color), to_rgb(BACKGROUND_COLOR)
    return tuple(alpha * f + (1 - alpha) * c for f, c in zip(fill, canvas))


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


def cap_height_px(fontsize: float) -> float:
    """Height of a digit's ink above the baseline, in template pixels."""
    prop = FontProperties(size=fontsize)
    return TextPath((0, 0), "0", prop=prop).get_extents().ymax / POINTS_PER_PIXEL


def draw_slot(
    fig, x, y, text: str, size_px: float, line_height_px: float | None, gid: str, color=TITLE_COLOR, ha="left"
):
    """Fill one of the template's text slots, at the size and line height the template gives it."""
    fig.text(
        x,
        y,
        text,
        ha=ha,
        va="top",
        fontsize=size_px * POINTS_PER_PIXEL,
        linespacing=(line_height_px / size_px) if line_height_px else 1.2,
        color=color,
        gid=gid,
    )


def license_runs() -> list[tuple[str, bool]]:
    """The license row, as (text, bold) runs: the license itself and each name it credits are bold.

    Every space rides at the *end* of a run, never at the start: a leading space is invisible to
    `TextPath`, so a run beginning with one gets laid out flush against the run before it.
    """
    runs: list[tuple[str, bool]] = [("Licensed under ", False), ("CC-BY ", True), ("by ", False)]
    for index, author in enumerate(AUTHORS):
        last = index == len(AUTHORS) - 1
        if index:
            runs.append(("and " if last else ", ", False))
        runs.append((author if last else f"{author} ", True))
    return runs


def run_row_width(runs: list[tuple[str, bool]], size_px: float) -> float:
    """Width of a row of runs: every run's step, but only the last one's ink."""
    fontsize = size_px * POINTS_PER_PIXEL
    placements = [place_run(text, fontsize, bold) for text, bold in runs]
    return sum(step for _, _, step in placements[:-1]) + placements[-1][1]


def fits_slot(measured_px: float, slot_px: float, ratio: float = DEJAVU_OVER_LATO) -> bool:
    """Whether text this step measures at `measured_px` fits `slot_px` once the template sets it.

    The question is about the frame, so the step's own width is divided by how much wider its font is
    than the template's.
    """
    return measured_px <= slot_px * ratio


def draw_run_row(
    fig, runs: list[tuple[str, bool]], size_px: float, x_px: float, y_px: float, gid: str, fx, fy, align="left"
) -> None:
    """Draw one row of mixed-weight runs, laid out left to right from `x_px` (or back from it).

    matplotlib has no rich text, so a row that changes weight mid-line is several text objects — which
    is also what Figma wants, since a single node whose weight varies has to be re-ranged by hand.
    """
    fontsize = size_px * POINTS_PER_PIXEL
    placements = [place_run(text, fontsize, bold) for text, bold in runs]
    cursor = x_px - run_row_width(runs, size_px) if align == "right" else x_px
    for index, (drawn, ink_px, step_px) in enumerate(placements):
        fig.text(
            fx(cursor + ink_px / 2),
            fy(y_px),
            drawn,
            ha="center",
            va="top",
            fontsize=fontsize,
            fontweight="bold" if runs[index][1] else "normal",
            color=FOOTER_COLOR,
            gid=f"{gid}-{index}",
        )
        cursor += step_px


def wrap_to_slot(text: str, slot_px: float, size_px: float) -> str:
    """Wrap text into one of the template's slots, at the template's own size.

    Judged in the step's own font against the slot itself, so a line that fits here fits the frame with
    room to spare — see the ratios above for why that is the safe direction.
    """
    return wrap_to_width(text, slot_px, size_px * POINTS_PER_PIXEL)


def lines_in(text: str) -> int:
    """Line count of already-wrapped text."""
    return text.count("\n") + 1


def place_run(text: str, fontsize: float, bold: bool = False) -> tuple[str, float, float]:
    """A run's drawn text, the width to centre it on, and the step to the next run.

    A run's trailing space belongs to the layout, not to its glyphs — so it is dropped from the text
    and kept in the step. Centring the space along with the ink puts the ink half a space off, which
    is invisible here but not in Figma: it trims trailing space out of a text box, so a separator dot
    ends up hugging the name after it.
    """
    drawn = text.strip()
    return drawn, text_advance_px(drawn, fontsize, bold), text_advance_px(text, fontsize, bold)


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


def format_hours(minutes: float) -> list[str]:
    """Hours-and-minutes candidates, longest first: '8 hours 28 mins', '8h 28m', '508'."""
    hours, mins = divmod(round(minutes), 60)
    hours_word = "hour" if hours == 1 else "hours"
    if mins == 0:
        return [f"{hours} {hours_word}", f"{hours}h", f"{round(minutes)}"]
    mins_word = "min" if mins == 1 else "mins"
    return [f"{hours} {hours_word} {mins} {mins_word}", f"{hours}h {mins}m", f"{round(minutes)}"]


def value_candidates(group: dict, minutes: float, with_suffix: bool) -> list[str]:
    """In-bar label candidates for a segment, longest first.

    A segment worth hours rather than minutes says so — sleep on the detailed chart, personal care on
    the four-category one — and the leftmost segment carries the unit for the row.
    """
    if group.get("as_hours") or group["column"] == "sleep":
        return format_hours(minutes)
    if with_suffix and group.get("unit_suffix", group["column"] == "paid_work"):
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
    ages_sentence = (
        f"Estimates cover people aged 15 to 64, except in {exceptions}. "
        if exceptions
        else "Estimates cover people aged 15 to 64. "
    )
    # What the groups whose names do not say it themselves contain, in bar order.
    contents = "; ".join(
        f"{group['label'].lower()} covers {group['contents']}" for group in layout["groups"] if group.get("contents")
    )
    text = (
        f"Note: Each country's most recent time-use survey is shown, with its year in brackets; "
        f"survey years range from {tb['year'].min()} to {tb['year'].max()}. " + ages_sentence
    )
    if contents:
        text += f"{contents[0].upper()}{contents[1:]}."
    else:
        # Where the legend names a category's activities, the Note's job is the caveat instead: not
        # every country reports every activity separately, and those minutes stay inside the category.
        text += (
            "Not every country reports each activity separately; where one does not, its minutes stay "
            "within the same category."
        )
    # Against the narrower of the Note's own slot and the content width — the slot is the template's,
    # but a mobile frame's content is narrower than it.
    template = layout["template_text"]
    content_px = layout["size"][0] - 2 * layout["margin"]
    wrapped = wrap_to_slot(text, min(template["note_slot_px"], content_px), template["note_px"])
    # The Note's slot is two lines tall, and the footer's auto-layout grows upward past that rather
    # than clipping — but every line it gains is a line the chart loses, so cap it. Three is what the
    # template's 12px takes for this Note where this step's older, smaller footer took two.
    assert lines_in(wrapped) <= 3, f"The Note wraps to {lines_in(wrapped)} lines in its slot — shorten it."
    return wrapped
