"""Draw the 'How do people spend their time?' chart from the OECD Time Use Database.

One row per country, splitting the 1440 minutes of a day into the OECD's four top-level categories —
paid work or study, personal care, unpaid work and other, leisure — ranked by the leading segment,
paid work or study, as the original chart by Esteban Ortiz-Ospina ranked it (see `sort_rows`).

**Four categories, not the source's ten display groups**, and that is the substantive decision in this
step: the ten resolve more than the harmonization supports. Measured across the source's countries, the
residual buckets *inside* each category vary two- to threefold (other unpaid work runs 39 minutes in
France to 132 in Ireland) while the categories themselves vary far less — personal care is 665 minutes
+/- 31. Aggregating takes the mean coefficient of variation across segments from 0.22 to 0.12, so most
of what the finer split resolves is where each survey drew its coding lines. The workings are in
`ai/time_use_comparability/`. A ten-group version was built and dropped; if the aggregation outlives
this step, move the sums into garden so the four categories are in the catalog.

**Each of the four IS one of the source's own top-level categories**, so nothing is aggregated here
beyond a single addition. Codes 1, 3 and 4 are read straight off garden's `time_use` table as
`paid_work_or_study`, `personal_care` and `leisure`; "Unpaid work & other" is code 2 plus code 5, the
source's small religious/civic/uncategorized bucket, which is a presentation choice and so belongs
here rather than in the catalog. A ten-display-group table used to sit in between and was summed back
up to these same numbers, which is a detour: it reconstructed the source's own totals to within
3e-05 minutes, and no chart used any of its 33 indicators.

**25 countries over 2010-2024**: the 26 garden publishes, less Mexico. The survey-year cutoff and the
reasoning behind it live beside `EARLIEST_SURVEY_YEAR` in `data://garden/oecd/2026-08-14/time_use`, and
this step does not filter by year again — a second filter would be dead code that reads as the live one.
Mexico is dropped here, not in garden, because it is a presentation call for one chart: the OECD flags its
estimates as not fully comparable, it is the only charted country so flagged, and it would otherwise rank
second on the leading segment with a footnote attached (see `NOT_COMPARABLE`).

Values are written inside segments wide enough to hold them, in hours and minutes, left-aligned at each
segment's start (the last segment's right-aligned at the bar's end), with a 1px gap of canvas between
segments. Survey years differ by country, so each country label carries its year — the original's
surveys spanned a narrower window and it named none of them. On desktop the name and the year take a
line each, the name in bold.

The desktop key sits above the bars and points at the top row: over each segment, the activities that
category holds as one run-in list, and under it, nearest the bars, the category's name in its colour.
Each block is aligned with the values under it — left at its segment's start, the last one right at the
bar's end — and its line breaks are the design team's (`key_lines`, `label_lines`). A block that clears
the subtitle sideways may rise beside it, which is what lets the four-line lists sit right of a
subtitle balanced over two shorter lines (`key_header_px`); `key_blocks_collide` asserts that no block
reaches into its neighbour's. The mobile frame is too narrow for that and keeps a row legend instead.

This layout is the design team's rework of the frame, replicated here: their palette, the key's order
and alignment, the two-line country labels, the segment gaps, the wider bar spacing and the taller
desktop frame.

Two frames are emitted: 850x1162 desktop, from the vertical static-chart template run 67px taller, and
a mobile frame from the mobile template, also run taller, so the wider bar spacing does not thin the
bars. Fonts and the logo come from the templates and are deliberately not set here. Colors *are* set here, unlike the retired
ten-group charts, which left them to Figma as seaborn placeholders: the reason for a placeholder is
that a font cannot be reproduced on this machine, and a color can, so naming the library colors makes
the render a true preview of the frame (see `MAIN_CATEGORY_GROUPS`). What this step fixes is the data,
the structure — which text slots exist, in what order — the proportions, and the row layout.

Figma handoff — a recipe, not a record
--------------------------------------
File `Charts (2026)`, key `s6Sv60bakebRRW2TxsMQbF`:
https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-

- Page **20260817 How do people spend their time? (Pablo A)**, `25524:5` —
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=25524-5
  Dated when the page was first placed, not when it was last refreshed, and it sits among the dated
  chart pages after the divider.
- Frame **`how-do-people-spend-their-time`**, `29442:11` (850x1162) —
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=29442-11
  A clone of `Static Chart Template_Vertical` (`5332:93`, 850x1095) run 67px taller with its footer
  re-pinned to the bottom margin, at x=1000, with its unstyled import parked at x=70 to the LEFT so the
  page reads original then edited.
- Frame **`how-do-people-spend-their-time-mobile`**, `29442:520` (540x884) —
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=29442-520
  A clone of `Static Chart Template_Mobile (example 2)` (`24590:32`, 540x824) run 60px taller, at
  x=2550, its own unstyled import parked at x=1930. The published 2023 image sits leftmost, at x=-860.

**The node ids are a convenience; the frame NAME is the durable join.** It is the kebab-case slug the
website exports the PNG by, so it is the one string shared by the layer panel, the exported file and
this step — and re-cloning a rebuilt template mints new ids, which is exactly when someone comes
looking. Search the file for the name before trusting an id.

**Re-verify the templates before cloning** — `/create-figma-chart`'s `scripts/verify_templates.js`,
one `use_figma` call, `ok`/`DRIFT` verdict. The design team edits these frames in place and has: the
2026-08 rebuild dropped the header wrappers' inner padding (`origin_y` 16.216 -> 16) and moved the logo
out of the title row into a sibling on *both* families, which is what `logo_px: 0` and the derived
70px header bottom in `template_text` now encode. A step laid out against the previous generation
still renders and still passes every contract check; it just no longer matches the frame.

**A frame whose GEOMETRY has not changed does not need re-importing — recolour it in place.** Prove
it first, and the proof is cheap: harvest the frame's named groups and every text run, and compare
against the freshly rendered SVG (the twelve template slots the restyle drops are the only expected
difference). Identical on both counts means the frame is the same render and only the paint differs,
which is a two-minute pass instead of the whole import-restyle-crop-snap sequence. That is how the D2
palette reached the desktop frame. Two things the pass has to get right: the SVG's `opacity` sits on
the imported LEAF rather than on the group that names it (set it on the group and it multiplies with
the leaf's own, halving the bar), and a bound style OVERRIDES the local fill, so a node whose target
is unbound must be unbound with `setFillStyleIdAsync("")` — this palette flips which in-bar labels are
white, and without that the flipped ones keep their `Text/Gray 100` binding and go on rendering grey
behind a dead local fill.

**Drive the recolour from the render, not from a table typed beside it.** Every group the step names
carries the fill the step chose, so parse the SVG for `gid -> (hex, opacity)` and look the library
style up by hex. Parse it as XML: a `<g id="...">(.*?)</g>` regex silently skips a group whose
predecessor nested one, which produced a plausible-looking table with 255 of 256 rows.

**Getting the SVG in.** `upload_assets` + POST the file (never `createNodeFromSvg`, which rasterizes
text). Two copies per frame where the reference copy earns its place: one to style, one to park. The
import lands on the file's *currently open page*, which is the **Cover** unless a `use_figma` call has
just set the page — so fetch it by the returned `placedOnNodeId`, `appendChild` it onto the target
page, and sweep the landing page afterwards. It arrives as a FRAME sized to the SVG canvas, which is
0.96x the template, so the rescale is exact and needs no bbox arithmetic: `frame.width / import.width`
(850/816 = 1.0417). Then drop the step's own copies of the template's slots by
prefix — `title`, `subtitle`, `note`, `data-source`, `tagline`, `license-*` — because the clone's
wrappers carry those, and a slot emitted as runs is `license-0 ... license-5`.

Then **delete matplotlib's figure patch** (`figure_1/patch_1`), which is frame-sized and carries
`fills: []`. It paints nothing, so the skill's restyle pass leaves it alone by design and no screenshot
can show it — but its bounding box is the whole canvas, which makes every box- and band-based check in
`verify_page.js` measure the artboard instead of the plot and report three failures that are not there.
Keep the guard the skill's pass uses: strip it only when nothing under it is painted.

**Pin every descendant's constraints to MIN/MIN before resizing the chart frame.** This frame KEEPS
its import frame, which is a deliberate departure from `/create-figma-chart`'s "bin the import frame"
— the frame is what makes the plot grabbable as a unit — and the crop below therefore resizes it. An
SVG import arrives with `SCALE/SCALE` on every descendant (269 of 269, measured), and `resize()`
applies them: it stretches each text box through its constraint and rewraps all 152 labels, "Paid work
or study" onto two lines, every country label split from its year, every value split from its unit. The
call succeeds, so nothing tells you; only a screenshot does. `rescale()` is safe and needs no pin — it
scales type with geometry by design.

**Crop the chart frame to the plot's own ink** once the restyle is done — `x`/`y` onto the ink, resize
to it, and shift the children back by the same offset, with clipping left off. The import arrives the
size of the SVG canvas, which is the artboard, and a frame that size has no box to show: hovering the
plot highlights a rectangle identical to the artboard's, so there is no way to see or grab the plot as
a unit, and `verify_page.js`'s box-alignment and gap rows measure the canvas and report negative
insets. Cropping moves nothing on the canvas — measured at max channel difference 0 across the frame. Then
**snap each side that lands within a pixel of the header's content column onto it**, moving the
children back so no ink shifts: a TEXT node's box carries its advance width rather than its glyphs, so
cropping to ink alone left this frame at 15.92..833.88 against a 16..834 column and `box-alignment`,
which measures to 0.05, failed on 0.08px of font metrics. Compute the target edge and set it, rather
than nudging the ink offsets and deriving the edge: a nudge in the wrong direction puts the left edge
at 15.83, which reads as a rounding artifact rather than as the sign error it is. Leave the parked unstyled copy at full
canvas size and untouched: it is there to be compared against the export.

**No font pass, and no anchor pass.** The SVG names Lato first (`EMITTED_FONT_STACK`), so the import
arrives as Lato Regular/Bold — verified 156/8 on arrival — and the passes that exist to change the
face and undo the drift it causes have nothing to do. Read the faces before assuming it: an import
that comes in as Inter means the stack did not survive to the file.

**Switch the import frame's fill ON**, to the clone's own canvas paint and its bound style.
An import arrives with a `SOLID` fill marked `visible: false`, and a frame with no visible fill is not a
hit target over its empty area — so hovering the plot highlights nothing and the chart is reachable only
from the layer panel, which is the second way these frames feel unlike the template. It costs no pixel:
with the chart at the bottom of the z-order the clone's identical cream sits beneath it.

**Put the chart at the BOTTOM of the frame's z-order** — `frame.insertChild(0, chart)`, not
`appendChild` — and this is a usability requirement rather than a visual one. The import is a frame the
size of the whole artboard, so appended last it covers the header and footer, and every double-click on
the subtitle or the Note descends into the import's nested groups instead of selecting the text. Below
them, a click over the subtitle lands on the header wrapper and the text is one double-click away, as it
is in the template, while bars and legend names still resolve to the chart. Nothing moves: the frame is
pixel-identical either way (checked, max channel difference 0), because the header and footer wrappers
carry no fill and the figure patch is already gone.

**Text slots.** Rebind the template's text STYLE where a slot has one — a single-face slot like the
subtitle carries `S:bd2b46c8f1ae73...`, which owns family, size and line height, and re-applying the
face by hand with `setRangeFontName`/`setRangeFontSize` leaves the text looking right and the binding
gone. `diff_against_template.js` is what catches it (`header[1] style (unbound) != template`); a
screenshot cannot. Use `setTextStyleIdAsync` and let the style supply all three. A multi-run slot like
the Note cannot be fully style-bound — a bolded range and a style binding are exclusive — so those
keep the per-run recipe: setting `characters` gives the WHOLE string the face of the old first
character, so re-apply **every** run's face, not only the bold ones — the runs that silently go wrong are the
*non-bold* ones. Three of the four footer rows shipped entirely bold from a pass that re-applied the
bold ranges and trusted the rest, and a screenshot does not show it; `/create-figma-chart`'s
`diff_against_template.js` does. The template's own faces are Bold(0-5) + Medium(5-6) + Regular for the
Note, Bold(0-12) + Regular for the source, Bold(0-18) + Medium for the tagline, and Medium with Bold on
`CC-BY` and each name for the licence:

| Slot | Fills with | Bold |
|---|---|---|
| Title | `TITLE` | whole line (Playfair Display SemiBold, from the template) |
| Subtitle | the layout's `subtitle`, `{years}` filled | none |
| `Note:` | `build_note` | the `Note:` label |
| `Data source:` | `build_source_citation` | the `Data source:` label |
| Tagline | `TAGLINE` | `OurWorldinData.org` |
| License | `license_runs()` | `CC-BY` and each author's name |

**In-plot restyle.** There is no font pass and no anchor pass: the SVG names Lato first
(`EMITTED_FONT_STACK`), so the import arrives in the template's face and nothing needs re-setting —
which also removes the drift the anchor pass existed to undo, since a face change moves a label by half
its width change. Read the faces on arrival before trusting that; an import in **Inter** means the
stack did not survive to the file, and then the old recipe applies (record each node's own
`textAlignHorizontal`, set every run to Lato, put each node back on its anchor — in that order, because
the middle step invalidates the first). The 25 country labels and the four member lists take
`Text/Gray 80`.

**Colors**, bound as library styles rather than pasted as hexes: `Default Palette/Dark Copper`,
`Midnight Blue`, `Dark Olive Green` and `Maroon`, for the bars, the legend chips and the category names
alike. Bars carry `SEGMENT_ALPHA` as *node* opacity, not paint opacity, so the binding survives; the
names are full strength. Value labels are white on all four, which is what `value_label_color` picks
measured against the composited fill at `SEGMENT_ALPHA`, and they stay unbound. Bind whatever hex the
SVG carries rather than a colour named here — the step measures it and this comment cannot.

**Re-import with `/create-figma-chart`'s `restyle_static_import.js`, one job per frame with
`inheritStyles: true` and `reflowLegend: false`, and `families: []`.** It binds every colour in the new
render to the library style the frame's current chart carries, which covers the fill-plus-darker-text
pairs above that the script's `families` model has no place for. Its report must come back with empty
`leftAsImported` and `orphanedBindings`: anything listed there is a style to rebind by hand. Keep
`reflowLegend` off: it re-lays a row of runs from the leftmost, which collides this step's legend runs
(doubled separators, overlapping names) where the step already spaced them by measured advance. Desktop
and mobile go in one call, since they share a page; it also replaces each frame's parked reference copy.
The template text slots are not touched by it, so a copy change still means editing them in the frame.
A render that CHANGES a colour cannot inherit: the old chart never bound the new hex, and the script
refuses. Run it with `inheritStyles: false` and `families: []` instead (country labels and lists go to
the body style through `bodyTextParent`), then bind each palette hex to its `Default Palette/*` style,
moving the SVG's paint opacity onto the node first, and the in-band Note to `Text/Gray 70`.

**Then rebuild the key as auto-layout containers**, which an SVG cannot carry, with
`/owid-staff:create-figma-chart`'s `scripts/group_text_blocks.js`, run after the restyle and again after
every re-import (it replaces `chart`, and the containers with it). Desktop, one block per category:

    {"name": "key__<slug>", "direction": "VERTICAL", "itemSpacing": 3, "align": "MIN", "anchorY": "bottom",
     "children": [{"merge": "key__<slug>-members", "lineHeight": 16}, {"merge": "category__<slug>", "lineHeight": null}]}

with `"align": "MAX"` for `leisure`, the right-aligned last block. Mobile, one `legend` block stacking a
row per category at the import's own pitch:

    {"name": "legend", "direction": "VERTICAL", "itemSpacing": "keep", "align": "MIN", "anchorY": "top",
     "children": [{"block": {"name": "legend__<column>", "direction": "HORIZONTAL", "itemSpacing": 5, "align": "CENTER",
       "children": [{"node": "legend__<column>-chip"}, {"block": {"name": "legend__<column>-labels",
         "direction": "HORIZONTAL", "itemSpacing": 8, "align": "CENTER",
         "children": [{"merge": "legend__<column>", "lineHeight": null}, {"merge": "legend__<column>-members", "lineHeight": null}]}}]}}]}

The nested row also fixes the name-to-list gap, which the import lays out in this step's wider font.

**The gate's standing findings**, each accepted rather than open: the template's own 11px tagline and
licence rows (`text-floor`, desktop); the desktop key rising beside the subtitle's shorter lines (`gap`
top ~3.5px, the design team's layout); the mobile Note drawn in the band (`gap` bottom ~4.4px); the
two-author licence run (`diff_against_template`, mobile). `color_audit.py --separated` on the four
composited fills fails under red-green colour-blindness (closest: Paid work against Unpaid work, dE 8.2
for protanopia). Every segment sits in a fixed position under its own key block, so position rather than
hue carries the category; the palette is the design team's call.
"""

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.colors import to_rgb
from matplotlib.font_manager import FontProperties, findfont
from matplotlib.patches import Rectangle
from matplotlib.textpath import TextPath
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.viz.static import TEMPLATES, apply_svg_rcparams, export_frame

# Non-path text so the SVG stays editable in Figma, and a fixed hash salt so it is reproducible.
# Both come from `etl.viz.static`, which this step used to set by hand with the same two values.
apply_svg_rcparams()

paths = PathFinder(__file__)

MINUTES_PER_DAY = 1440

# The OECD's top-level categories, in bar order. Each carries a seaborn "deep" palette position;
# its member groups are that hue at decreasing saturation, so a family reads as one category.
#
# Seaborn on purpose: OWID's own palette and fonts are applied in Figma, where the Chart colors
# library and Lato actually live. Setting them here would also be unreproducible — matplotlib on this
# machine has neither Lato nor Playfair Display, so a step that asked for them would silently fall
# back and emit different type depending on which machine built it.
# A group's values are drawn only where this share of countries can hold one; otherwise none of them
# are. A number on a handful of rows reads as a fact about those countries rather than as the rest
# being too narrow to print — education fitted on 1 of 35 rows, and that one number said nothing.
VALUE_LABEL_COVERAGE = 0.75

# The four drawn segments. Each is one of the source's own top-level categories, so what this chart
# resolves is what the OECD itself publishes at that level.
#
# A finer ten-group version was built and dropped, and the case against it is in
# `ai/time_use_comparability/`: the residual buckets *inside* each category vary two- to threefold
# across countries (other unpaid work runs 39-132 minutes, other leisure 78-179) while the categories
# themselves vary far less (personal care is 665 +/- 31), so most of what a ten-segment split resolves
# at that level is where each survey drew its coding lines rather than how people differ. Aggregating
# takes the mean coefficient of variation across the segments from 0.22 to 0.12.
#
# These four name Chart colors library colors outright rather than seaborn placeholders: the reason for a
# placeholder is that a font cannot be reproduced here and a color can, so a render that shows the real
# ones is simply a truer preview of the frame. Figma still binds each to its library style — the hex is
# the preview, the binding is the artifact.
#
# The palette is the design team's: Default Palette's Dark Copper, Midnight Blue, Dark Olive Green and
# Maroon, all dark enough to carry WHITE values on every segment and to set each category's name in its
# own fill. Two things follow from it:
#   - Neighbouring segments no longer differ in lightness (Midnight Blue against Dark Olive Green is
#     close in grayscale), so the boundary between them is carried by a 1px gap of canvas instead
#     (`SEGMENT_GAP_PX`). That is why `check_contrast` holds the seam floors only where segments touch.
#   - White on Dark Copper measures under WCAG's 4.5:1 for body text at `SEGMENT_ALPHA`; it is pinned in
#     `CONTRAST_ALLOWANCES`, so the shortfall is re-measured on every build rather than recorded here.
#
# Which category carries which colour still follows width: Personal care is 44% of every bar, so it takes
# the recessive Midnight Blue.
# `members` names what each category holds, in the source's own terms and ordered by the minutes each
# takes. Two of them are worth not "correcting" back to "Other": the OECD's 3.3 is labelled *personal,
# household, and medical services + travel related to personal care* — grooming and health, 61 min/day,
# 9% of personal care. Its 4.5 is the other way round: it stays "Hobbies & other", because per the
# country mappings on the workbook's activity sheet it holds games, hobbies, arts and crafts, reading and
# leisure travel, but also rest and relaxation (Japan, HETUS, Italy, Ireland) and computer and internet
# use (Canada, HETUS, Ireland, New Zealand) — "Hobbies" alone would mislabel it, and at 96 min/day it is
# the second-largest thing anyone does with their leisure. Unpaid work lists no "Other": its category
# name already says "& other", which is this chart's fold of the source's fifth top-level category
# (religious, civic and unclassified time, 18 min/day), and a second "Other" in the key read as the same
# bucket as the one under Leisure.
MAIN_CATEGORY_GROUPS = [
    {
        "column": "main_paid_work_or_study",
        # The source's own code 1.
        "source_columns": ["paid_work_or_study"],
        "members": ["Paid work", "Commuting", "School or classes", "Homework"],
        "compact": True,
        # "&", not the source's "or": the segment is the sum of the two, and it matches "Unpaid work & other".
        "label": "Paid work & study",
        # Dark Copper.
        "color": ("hex", "#9a5129"),
        "as_hours": True,
        # The desktop key's line breaks, from the design team's frame. Asserted to spell out
        # `run_in_list(members)`, so they cannot drift from the members.
        "key_lines": ["Paid work, commuting, school", "or classes, homework"],
    },
    {
        "column": "main_personal_care",
        # The source's own code 3.
        "source_columns": ["personal_care"],
        "members": ["Sleep", "Eating & drinking", "Grooming & health"],
        "label": "Personal care",
        # Midnight Blue, recessive, which is why it takes the widest segment.
        "color": ("hex", "#00295b"),
        "as_hours": True,
        "key_lines": ["Sleep, eating & drinking,", "grooming & health"],
        # Compact like the rest, though it has room for the spelled-out form. One category set
        # differently from its neighbours reads as a difference in kind, and the only difference is
        # that this segment is wider.
        "compact": True,
    },
    {
        "column": "main_unpaid_work_and_other",
        # Code 2 plus code 5 — the only addition this chart makes. Code 5 is religious and civic
        # activities and uncategorized time, 18 min/day, too small to read as its own segment and
        # closest in kind to unpaid work; folding it is a presentation call, which is why it is here.
        "source_columns": ["unpaid_work", "other_activities"],
        # "Household travel", not "Volunteering". Both are in here, but the list has to name the
        # biggest things, and across the charted countries household travel is a median 17 min/day
        # while volunteering is 3 — the list was naming the 7th-largest member and skipping the 4th.
        # `check_members_are_the_largest` asserts it. Spelled out, not "Travel": on its own it reads
        # as holidays, which the source counts under leisure.
        "members": ["Housework", "Childcare", "Shopping", "Household travel"],
        "compact": True,
        "as_hours": True,
        "label": "Unpaid work & other",
        # Dark Olive Green.
        "color": ("hex", "#18470f"),
        "key_lines": ["Housework,", "childcare,", "shopping,", "household travel"],
        # The narrowest segment, so its name takes two lines.
        "label_lines": ["Unpaid work", "& other"],
    },
    {
        "column": "main_leisure",
        # The source's own code 4.
        "source_columns": ["leisure"],
        "members": ["TV & radio", "Seeing friends", "Sports", "Events", "Hobbies & other"],
        "compact": True,
        "as_hours": True,
        "label": "Leisure",
        # Maroon.
        "color": ("hex", "#883039"),
        "key_lines": ["TV & radio,", "seeing friends,", "sports, events,", "hobbies & other"],
    },
]

# Which source sub-activity each name in the key stands for, and which ones the key does not name.
# A five-name list is a selection, and the invariant is that it selects the *biggest* members: the key
# named volunteering (a median 3.0 min/day) while leaving household travel (17.3) unnamed, which
# `check_members_are_the_largest` now makes impossible to ship.
# `care_for_household_members` is deliberately absent: it is the parent of child and adult care, and
# counting it would double the branch.
KEY_MEMBER_COLUMNS = {
    "Paid work & study": {
        "named": {
            "Paid work": "paid_work_all_jobs",
            "Commuting": "travel_to_and_from_work_or_study",
            "School or classes": "time_in_school_or_classes",
            "Homework": "research_and_homework",
        },
        "unnamed": ["job_search", "other_paid_work_or_study_related"],
    },
    "Personal care": {
        "named": {
            "Sleep": "sleeping",
            "Eating & drinking": "eating_and_drinking",
            "Grooming & health": "other_personal_care_services",
        },
        "unnamed": [],
    },
    "Unpaid work & other": {
        "named": {
            "Housework": "routine_housework",
            "Childcare": "child_care",
            "Shopping": "shopping",
            "Household travel": "travel_related_to_household_activities",
        },
        "unnamed": [
            "adult_care",
            "care_for_non_household_members",
            "volunteering",
            "other_unpaid_work_activities",
            "religious_spiritual_and_civic_activities",
            "other_uncategorized_activities",
        ],
    },
    "Leisure": {
        "named": {
            "TV & radio": "tv_or_radio_at_home",
            "Seeing friends": "visiting_or_entertaining_friends",
            "Sports": "sports",
            "Events": "participating_in_or_attending_events",
            "Hobbies & other": "other_leisure_activities",
        },
        "unnamed": [],
    },
}

# One category per segment, so the key names each segment.
MAIN_CATEGORIES = [
    {
        "name": group["label"],
        "color": group["color"],
        "columns": [group["column"]],  # one drawn segment per category
        # What the segment holds, listed over its name.
        "members": group["members"],
        "key_lines": group["key_lines"],
        "label_lines": group.get("label_lines", [group["label"]]),
    }
    for group in MAIN_CATEGORY_GROUPS
]

TITLE = "How do people spend their time?"
SUBTITLE = (
    "Average time spent in a 24-hour day by people aged 15 to 64. Based on each country's most recent "
    "time-use survey in the OECD data, from {first} to {last}."
)

# The row labels set the width of the column they sit in, so the two longest names are shortened to
# the forms the style guide sanctions (no periods). It buys the bars 15px on desktop and 13px on
# mobile, which is the gap between these two names and the next-longest, "New Zealand".
SHORT_COUNTRY_NAMES = {"United Kingdom": "UK", "United States": "US"}

# Countries the OECD flags as "not fully comparable, due to methodological differences" (the starred
# columns of the workbook's Total sheet), left off this chart. Mexico is the only charted one: the other
# stars are age-weighting notes, or Australia, Lithuania and China, whose surveys predate garden's cutoff.
NOT_COMPARABLE = ["Mexico"]

# Credited on the license line: the original chart's author, and this refresh's. Named one by one
# because the line sets them in bold, as it does the license itself.
AUTHORS = ["Esteban Ortiz-Ospina", "Pablo Arriagada"]

TAGLINE = "OurWorldinData.org — Research and data to make progress against the world's largest problems."

# The greys the templates set on their own text slots, so a render reads like the frame it feeds.
# Every other colour here is a placeholder that Figma replaces.
TITLE_COLOR = "#2d2e2d"
TEXT_COLOR = "#5b5b5b"
FOOTER_COLOR = "#858585"
# Grapher's own GRAPHER_AREA_OPACITY_DEFAULT = 0.8 (GrapherConstants.ts), which both the stacked and the
# discrete bar chart apply to every bar; grapher leaves its labels at 1, and so does this. It changes
# every measured number, because what a reader sees is the fill composited over the canvas rather than
# the library hex — see `composite_on_background`, which is what the label colours and the recorded
# seams are measured against.
#
# It costs white-on-Dark-Copper its 4.5:1, a deliberate trade rather than an oversight: matching
# grapher's stacked bars is worth more here than clearing it. The shortfall is pinned in
# `CONTRAST_ALLOWANCES`, so it is checked on every build instead of recorded in prose.
SEGMENT_ALPHA = 0.8

# In-bar values are white on saturated fills and dark on light ones, whichever reads better *measured*
# against the fill — a luminance cutoff picked white on Dark Orange at 4.48:1 where the dark scores
# 4.68:1, and the 4.5:1 bar is exactly what these labels have to clear. The dark is Text/Gray 100, the
# style the frame binds them to, so the render and the frame agree.
DARK_VALUE_COLOR = "#2d2e2d"

# The activities listed over each category name, in the body gray (`Text/Gray 80` in the frame) rather
# than the category's colour. With both in colour the name and its list differed only by size and
# weight, and the four names did not read as headings; the name keeps the colour that ties it to its bar.
MEMBER_TEXT_COLOR = TEXT_COLOR

# The floors `check_contrast` holds every color to, measured over the canvas at `SEGMENT_ALPHA`.
# A seam is a lightness gap between touching fills, so it is well below a text ratio; the value floor
# is WCAG AA for body text; the header floor is the tier the Chart colors library's own "Line and Slope
# Charts" variants land on, which is what makes them the answer for a light fill's text.
SEAM_MIN_RATIO = 1.6
VALUE_LABEL_MIN_RATIO = 4.5
HEADER_TEXT_MIN_RATIO = 4.4

# Red-green dichromacy, the two common forms, as linear-RGB matrices. The seam floor applies under
# these too: a boundary that exists only for a full-colour reader is not a boundary.
DICHROMACY_MATRICES = {
    "deuteranopia": ((0.625, 0.375, 0.0), (0.70, 0.30, 0.0), (0.0, 0.30, 0.70)),
    "protanopia": ((0.567, 0.433, 0.0), (0.558, 0.442, 0.0), (0.0, 0.242, 0.758)),
}

# The shortfalls this chart accepts, and the measured value each one is allowed to sit at.
# `check_contrast` treats every entry as a pin, not a waiver: a value that drops BELOW its allowance is
# a regression and fails, a shortfall with no entry fails, and a value that has climbed back over its
# floor fails too, so a fix cannot quietly leave a stale allowance behind.
#
# One entry: white values on Dark Copper, the design team's choice so that every segment carries white
# text (the dark alternative measures lower still, 3.49:1).
CONTRAST_ALLOWANCES = {
    ("value", "Paid work & study"): 3.90,
}
# Ratios are compared to their allowance at this tolerance, so a font or palette change that moves a
# number in the third decimal does not fail the build.
CONTRAST_TOLERANCE = 0.02

# The templates' own canvas, which the transparent SVG sits on. A category's name and member list are
# printed in that category's colour, so this is what they have to stand out against.
BACKGROUND_COLOR = "#fffbf5"
# The tier the Chart colors library itself targets: each of its "Line and Slope Charts" variants — the
# darker form of a light fill, meant for thin marks and text — measures exactly 4.4:1 on this cream.
# A fill lighter than that is darkened for text here, which is the same pairing, derived rather than
# hardcoded: the frame binds the library's variant, and this keeps the local render showing the same
# relationship instead of printing a name nobody could read.
# The current palette is dark enough that every category name is set in its own fill.
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

# Two font stacks, because two different things read them.
#
# `EMITTED_FONT_STACK` is what lands in the SVG's `font-family`, verbatim — matplotlib copies the rcParam
# out rather than writing the font it resolved. Naming **Lato** first therefore makes Figma render the
# import in the template's own typeface on arrival, which is worth more than it sounds: the frame no
# longer needs a font pass, and with no face change there is no label drift for an anchor pass to undo.
# Without it Figma resolves none of `Arial, Helvetica, DejaVu Sans` and substitutes Inter, which is wider
# — the parked copy of this chart overran its canvas by 39px that way.
#
# `MEASURED_FONT_STACK` is what this step measures and draws with, and it deliberately does NOT name
# Lato: Lato is not installed here, so asking for it would only make every `findfont` call log a miss
# before falling through to Arial. Measuring in the font actually resolved is what the slot allowances
# below are calibrated against — and getting that wrong is not academic. seaborn's `set_style` installs
# its own Arial-first stack, `FontProperties()` with no family resolves matplotlib's DejaVu-first
# default, the two are ~15% apart, and because the style was applied *after* the title and subtitle were
# wrapped this step measured in one font and drew in the other, which made the allowances look as though
# they needed to be 1.0.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "sans-serif"]
# Liberation Sans BEFORE DejaVu Sans, deliberately: matplotlib bundles DejaVu, so anything behind it is
# unreachable — and Liberation is the metric-compatible Arial substitute, the one face that keeps these
# allowances valid on a machine without Arial. DejaVu stays as the last named fallback, where it means
# "nothing Arial-shaped is installed" rather than "the second choice".
MEASURED_FONT_STACK = ["Arial", "Helvetica", "Liberation Sans", "DejaVu Sans", "sans-serif"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
# Drop the per-face misses for faces we deliberately name as alternatives, and NOTHING else.
#
# Both stacks list fallbacks that no single machine has all of: `Lato` is a request to whoever opens
# the SVG rather than to this machine, and `Liberation Sans` is the metric-compatible Arial substitute
# that matters on Linux and is absent on macOS. Drawing text with an explicit family list makes
# matplotlib build the whole fallback chain and warn once per missing face — measured at 724 warnings
# for one run of this step — so the noise is real and worth removing.
#
# Do NOT reach for `setLevel(logging.ERROR)` instead, which is what this step did first: `findfont`
# has a second warning site that fires only when a family list resolves to NOTHING
# (`... not found. Falling back to DejaVu Sans.`), and that one says every measurement here just moved
# ~15% against what gets drawn — the failure the two stacks exist to prevent. The filter keeps it.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: (
        "Falling back" in record.getMessage()
        or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
    )
)

# The filter above cannot catch the failure that matters, so assert it instead. Every slot allowance
# below is calibrated on ONE assumption — that this step measures in the same face it draws in — and
# two machines break it silently: one where every face of a stack is missing (matplotlib falls back to
# DejaVu, ~15% off, and the per-face misses are exactly the ones the filter drops), and one where Lato
# IS installed, which makes the emitted stack draw Lato while the measured stack still resolves Arial.
# Resolving both once turns either into a loud failure with somewhere to go.
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, (
    f"This step draws in {Path(_DRAWN_FACE).name} but measures in {Path(_MEASURED_FACE).name}, so every "
    f"slot allowance is calibrated against a font the figure does not use. Name the same face in both "
    f"stacks, or take the missing one out of MEASURED_FONT_STACK."
)

# The template sets its slots in Lato and Playfair Display, neither of which is installed here, so this
# step predicts their line counts from the font it does have. Both directions are measured, in Figma, at
# the same pixel size, and they do not point the same way (create-static-viz/TEMPLATES.md): against
# Arial, Lato sets a string 2.4% narrower at 11px and 0.8% narrower at 16px, while Playfair Display
# SemiBold is 3.2% *wider* at 25px. So a Lato slot has a little more room than this step measures and a
# serif slot a little less.
#
# The Lato figure takes the smaller of the two margins, which is the one that has to hold. Wrapping
# early is not the safe option it sounds like: the footer rows are sized so the template just fits them,
# and 6% early broke both onto second lines the frame does not have.
LATO_SLOT_SLACK = 1.008
PLAYFAIR_SLOT_SLACK = 0.968

# A row whose runs are mostly bold needs its own figure: the regular faces are within 1% of each other,
# but Arial Bold sets 6-7% wider than Lato Bold, so the license row measures 352.7px here against 331 in
# the frame. Measured on that row, since it is the one the footer's fit assert is about — and it is a
# property of the faces rather than of the string, so a re-worded row stays within a percent of it.
LATO_BOLD_SLOT_SLACK = 1.066

# A title line, in template px, in every one of these templates. Their titles set line height
# explicitly where every other slot leaves it automatic, which is why this is a constant and the
# other line heights are per-slot.
TEMPLATE_TITLE_LINE_PX = 29

# The gap the templates leave between a cropped subtitle's box and the logo, under a one-line title.
SUBTITLE_LOGO_GAP = 24

# Horizontal breathing room, in template pixels: between a country label and its bar, and the least room a
# value leaves at its segment's far end. The inset between a segment's edge and its value is per layout
# (`value_inset_px`).
COUNTRY_LABEL_PAD = 8
VALUE_PAD = 3

# The gap of canvas between neighbouring segments of a bar, in template pixels. It is what separates the
# categories now that the palette's neighbours no longer differ in lightness.
SEGMENT_GAP_PX = 1

# A desktop country label is two lines — the name in bold, the year under it — at this line height,
# centred on its bar.
COUNTRY_LINE_PX = 14

# The desktop key, in template pixels: the least space two neighbouring blocks may leave between them, the
# gap between a list and the category name under it, and the air between the name and the first bar's ink.
HEADER_MIN_GAP = 8
KEY_NAME_GAP = 3
KEY_BAR_GAP = 10

# The row legend, which is what a frame too narrow to name its own segments gets instead: the
# colour chip, the gap after it, the gap between a category's name and its member list, and the air
# between the block and the first bar. In template pixels.
LEGEND_CHIP_PX = 9
LEGEND_CHIP_GAP = 5
LEGEND_NAME_GAP = 8
LEGEND_BLOCK_GAP = 8

# Air between the last bar and a Note drawn inside the band, which is where a frame with no Note
# slot of its own has to put it.
NOTE_BAND_GAP = 10

# The Note's lead-in, set BOLD — the same shape the templates give their own footer: the placeholder's
# `Data source: ` is Lato Bold and the producer name Regular. It is named here because three places
# need to agree about it: the text `build_note` composes, the width its wrap has to leave for a bold
# run, and the run `draw_note` draws.
NOTE_LEAD = "Note:"

# Bars fill this share of a row's pitch: the design team's spacing, 25px bars on a 35px pitch on desktop
# where it was 25 on 31.5. The frames are run taller to make room (`height_px`), so the bars keep their
# thickness and only the air between them grows.
BAR_FRACTION = 0.716

# The two frames this step draws, cloned from "Static Chart Template_Vertical" (850x1095, node
# `5332:93`) and "Static Chart Template_Mobile (example 2)" (540x824, node `24590:32`) and measured
# with /create-figma-chart's `verify_templates.js`. Geometry is in template pixels, y from the top
# edge as Figma reports it — except the `*_fontsize` keys, which are POINTS, because they go
# straight to matplotlib. `line_px` converts between the two.
#
# The mobile frame differs in more than size, and both differences are in `template_text`: its
# footer is two rows (`Data source:` and the license, no Note and no tagline), and its content is
# 508px against the desktop's 818. The first is why the Note is drawn inside the band there; the
# second is why the key becomes a block of rows above the plot instead of names over the segments.
#
# Both frames run taller than their templates, by `*_EXTRA_PX`: the desktop by the 67px the design
# team's rework added, the mobile frame by enough to keep its bars at the thickness they had before the
# wider spacing. The footers are bottom-anchored, so every footer row moves down by that much.
DESKTOP_EXTRA_PX = 67
MOBILE_EXTRA_PX = 60
LAYOUTS = {
    "time_use_by_country": {
        "template": "vertical",
        "size": (TEMPLATES["vertical"].width_px, TEMPLATES["vertical"].height_px + DESKTOP_EXTRA_PX),
        # 16, not the 16.216 an earlier generation of these templates measured: the design team's
        # 2026-08 rebuild dropped the header wrappers' inner padding, and both frames' header nodes
        # now read 16..834 and 16..524 — margin 16, content 818 and 508. The stale figure cost 0.43px
        # of content width, which the Figma crop-snap absorbed inside its 1px tolerance and so never
        # failed a check. It does NOT remove the need for that snap, which was the guess: measured
        # after the change, the crop still lands at 15.39..834.09 and is nudged onto 16..834, because
        # a TEXT node box carries its advance width rather than its glyphs and the leftmost country
        # label overhangs the margin. Two separate sub-pixel effects, and only one of them was this.
        "margin": 16,
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
            # The design team rebuilt these headers (verified 2026-08-27 with create-figma-chart's
            # `verify_templates.js`): the wrapper's inner padding is gone, so `origin_y` is the frame
            # margin itself, and the logo is a SIBLING of the header on every static template rather
            # than a child of a title row. A sibling adds nothing to the header's height, which is why
            # `logo_px` is 0 and there is no `max(title, logo)` cap: a one-line title genuinely shrinks
            # the header by a line, taking this frame's band top from 82.5 to 70.
            #
            # The logo occupies a title line plus the gap (16 to 51), so under a one-line title the
            # subtitle starts level with its bottom edge, and the template crops the subtitle's box to
            # end 24px short of it (`subtitle_box_px`).
            "origin_y": 16,
            "logo_px": 0,
            "logo_x": 771,
            "title_slot_px": 737.84,
            "title_px": 25,
            "header_gap_px": 6,
            "subtitle_slot_px": 818,
            "subtitle_px": 16,
            "subtitle_line_px": 19,
            # Footer. Its wrapper is pinned by its TOP (`constraints.vertical: MIN`), so it grows
            # DOWNWARD as the Note wraps — not upward into the chart, which is what this step's own
            # layout does. The two agree while the Note is the template's own two lines, which is what
            # `build_note` caps it at; a third line would leave the frame's licence row 2px from the
            # bottom edge where the template puts 16. If a longer Note is ever wanted, re-pin the
            # footer in the frame (`footer.y = frame.height - margin - footer.height`) and let it eat
            # upward, and raise the cap here in the same change.
            "note_px": 12,
            "note_line_px": 14,
            "note_slot_px": 818,
            "note_bottom_px": 1043.81 + DESKTOP_EXTRA_PX,
            "source_y": 1047.81 + DESKTOP_EXTRA_PX,
            "source_px": 12,
            "tagline_y": 1065.81 + DESKTOP_EXTRA_PX,
            "tagline_slot_px": 467,
            "license_y": 1065.81 + DESKTOP_EXTRA_PX,
            "license_px": 11,
        },
        # In whole template pixels, like the template's own slots: the in-bar values at 13, the row
        # labels at 12, and in the key the run-in lists at 12 on a 16px line and the bold category names
        # at 14 on a 17px one — the design team's sizes.
        "country_font_px": 12,
        "value_font_px": 13,
        # The design team's inset between a segment's edge and its value, which the key shares, so each
        # key block lines up with the values under it.
        "value_inset_px": 8,
        "key_font_px": 12,
        "key_line_px": 16,
        "key_name_font_px": 14,
        "key_name_line_px": 17,
        # The country's name and its survey year on a line each.
        "country_two_lines": True,
        # Whether a value reads "4h 29m" rather than "4 hours 29 mins" when the long form does not fit.
        "with_mins_suffix": True,
        "groups": MAIN_CATEGORY_GROUPS,
        "categories": MAIN_CATEGORIES,
        # The key over the top row, each block aligned with the values under it.
        "group_labels": "aligned",
        # The age group first, since it is what a reader needs to interpret any bar, then the survey
        # scope, which is why the Note no longer carries it. "in a 24-hour day" earns its words: every bar
        # is the same length, and nothing else on the frame says that the length is a whole day. "from X
        # to Y", not "conducted between": the years are the ones the row labels show, and a survey labelled
        # 2010 (France, Spain, New Zealand) ran over 2009/10.
        "subtitle": SUBTITLE,
    },
    "time_use_by_country_mobile": {
        "template": "mobile",
        "size": (TEMPLATES["mobile"].width_px, TEMPLATES["mobile"].height_px + MOBILE_EXTRA_PX),
        "margin": 16,
        "template_text": {
            # The 540-wide set's title node is 428 wide against a 508 content box, which is the orphan
            # guard the design team builds in — so the title is measured against 428, not against the
            # content. It fits on one line there, which puts the subtitle at 51 rather than the
            # placeholder's 80, beside the logo — so the subtitle's box is cropped to clear it.
            "origin_y": 16,
            "logo_px": 0,
            "logo_x": 460,
            "title_slot_px": 428,
            "title_px": 25,
            "header_gap_px": 6,
            "subtitle_slot_px": 508,
            "subtitle_px": 16,
            "subtitle_line_px": 19,
            # This frame's footer is `Data source:` and the license, both full width at 14px, and
            # nothing else — no Note slot and no tagline. So `footer_top_px` is where the band stops,
            # `note_in_band` moves the Note into the band's foot, and the absence of `tagline_y` is
            # what tells `draw_footer` the license has a row to itself.
            "footer_top_px": 770 + MOBILE_EXTRA_PX,
            "source_y": 770 + MOBILE_EXTRA_PX,
            "source_px": 14,
            "license_y": 791 + MOBILE_EXTRA_PX,
            "license_px": 14,
            # Drawn inside the band, so a line costs a line of chart rather than the frame's bottom
            # margin — which is why the cap is four here and two on the desktop frame. At 12px the Note
            # takes four lines of 508px; it is the same text either way, and the caveats it carries are
            # the ones that can move a bar the reader is looking at.
            "note_px": 12,
            "note_line_px": 14,
            "note_slot_px": 508,
            "note_max_lines": 4,
            # Where the Note's own box ends, so it reads as a third footer row evenly spaced with the
            # other two — the same field the desktop frame uses, and measured the same way: on the
            # rendered frame, not derived. It has to be, because the two kinds of row do not agree
            # about their boxes. The template stacks 17px line boxes around 14px text with a 4px
            # `itemSpacing`, which renders as 8px of air between the source and licence rows; a Note
            # imported from this step arrives as ink-tight boxes whose last line's ink stops 8px short
            # of the box it is laid out in. So equal BOX gaps would render as 14px against 8px, and
            # this number is the one that puts the same 8px above the source row. To re-derive it after
            # a type-size change: measure the ink bands at the foot of the frame and shift by the
            # difference (`measure_footer_ink.py` in the session notes did exactly that). It moved
            # once already, by 2px, when the lead-in went bold: the Note is drawn as a one-line object
            # plus a block of the rest, and those two do not compose their vertical metrics the way a
            # single four-line block did.
            "note_bottom_px": 770 + MOBILE_EXTRA_PX,
        },
        # The same point sizes as the desktop frame, deliberately: on a frame 0.64x as wide they read
        # half again as large, which is the direction the template itself goes (its own source and
        # license rows are 14px against the desktop's 12 and 11). 98 of the 100 values fit their segments
        # at this scale; the two that do not are the narrowest unpaid-work segments (Japan, South Korea).
        "country_font_px": 12,
        "value_font_px": 12,
        # The desktop's 8px scaled to this frame's width, which keeps the values on the two narrowest
        # segments that the full inset would push out.
        "value_inset_px": 5,
        "legend_font_px": 13,
        "with_mins_suffix": True,
        "groups": MAIN_CATEGORY_GROUPS,
        "categories": MAIN_CATEGORIES,
        # A block of rows above the plot, not names attached to the segments. The desktop reading is
        # better and is why that frame keeps it, but it needs each segment to be wide enough to hold
        # its own name: here the narrowest category spans 56px against a 105px name, so all four would
        # overhang the bars they name by more than they cover. `draw_row_legend` says the rest.
        "group_labels": "rows",
        "note_in_band": True,
        # The same sentence as the desktop frame.
        "subtitle": SUBTITLE,
    },
}


def run() -> None:
    """Load data, render and save the chart."""
    check_contrast()
    check_key_lines()
    tb, ages = load_chart_groups()
    tb = add_main_category_totals(tb)
    paths.log.info(f"Loaded {len(tb)} countries, surveys {tb['year'].min()}-{tb['year'].max()}")

    source_citation = build_source_citation(tb)
    paths.log.info(f"Source citation: {source_citation}")

    for short_name, layout in LAYOUTS.items():
        fig = create_visualization(sort_rows(tb, layout), ages, source_citation, layout)
        # `export_frame` sweeps the clipping, saves the PNG opaque and the SVG transparent, and — the
        # reason to pass `template` — asserts the figure really is the size of the frame it names. That
        # check is what stands behind a hand-measured `size`: the registry carries the design team's
        # own numbers, so a typo in one is caught by the other. The filename suffix is the registry key
        # too, which is how `/create-static-viz`'s `verify_static_viz.py` knows which template to hold
        # `..._mobile.svg` against. Both frames run taller than their templates, so the height is passed.
        export_frame(paths, fig, short_name, template=layout["template"], height_px=layout["size"][1])
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
    """Build each drawn category from the source's own top-level columns.

    Three of the four are a straight rename; "Unpaid work & other" is the one addition. The day has to
    still close afterwards, which is what catches a source column read under the wrong name.
    """
    for group in MAIN_CATEGORY_GROUPS:
        tb[group["column"]] = tb[list(group["source_columns"])].sum(axis=1)
    totals = tb[[group["column"] for group in MAIN_CATEGORY_GROUPS]].sum(axis=1)
    assert totals.between(MINUTES_PER_DAY - 1, MINUTES_PER_DAY + 1).all(), (
        f"The four categories must spend the whole day: got {totals.min():.1f}-{totals.max():.1f}."
    )
    return tb


def load_chart_groups() -> tuple[Table, dict[str, str]]:
    """Load garden's `time_use` table (total population), unsorted — `sort_rows` ranks them.

    Returns the table plus the age-of-reference exceptions (country -> age range) for the note.
    """
    ds = paths.load_dataset("time_use")
    tb = ds.read("time_use")
    tb = tb[tb["sex"] == "total"].drop(columns=["sex"])

    source_columns = sorted({column for group in MAIN_CATEGORY_GROUPS for column in group["source_columns"]})
    assert not set(source_columns) - set(tb.columns), (
        f"Source columns changed in garden: missing {sorted(set(source_columns) - set(tb.columns))}."
    )
    # No sort here. `sort_rows` ranks by the layout's own leading segment and runs after this, so a sort
    # in this function only ever set an order that was thrown away.

    # What garden publishes, which this step takes as given. A layout tuned for these rows is not a
    # layout for 35: the row pitch, the value-label coverage and the header band all derive from the row
    # count, so a change to garden's cutoff should stop here to be looked at rather than be silently
    # redrawn.
    assert len(tb) == 26, f"Garden published {len(tb)} countries, not the 26 this layout is drawn for."
    missing = set(NOT_COMPARABLE) - set(tb["country"])
    assert not missing, f"Expected to leave out {sorted(missing)}, but garden no longer publishes them."
    tb = tb[~tb["country"].isin(NOT_COMPARABLE)].reset_index(drop=True)

    check_members_are_the_largest(tb)
    ages = {
        str(row["country"]): str(row["age_of_reference"])
        for _, row in tb.iterrows()
        if str(row["age_of_reference"]) != "15-64"
    }

    assert tb["country"].is_unique, "One row per country expected."
    # The source's five top-level categories partition the day (asserted strictly in garden; re-checked
    # here at the source's own rounding tolerance so a broken load cannot draw bars that misrepresent
    # shares).
    assert ((tb[source_columns].sum(axis=1) - MINUTES_PER_DAY).abs() < 2.0).all(), "Rows do not sum to 24 hours."
    # The source's three age-of-reference exceptions are all pre-2010 surveys, so garden's cutoff
    # removes them and everything here can say "aged 15 to 64" unqualified — garden asserts that too.
    # Assert it rather than assume it: `build_note` names the exceptions if any ever arrive, and this
    # is what says the subtitle would be wrong before the Note started covering for it.
    assert not ages, f"Garden published an age-of-reference exception the subtitle does not cover: {sorted(ages)}."

    return tb, ages


def build_source_citation(tb: Table) -> str:
    """Cite the data product behind the chart from the origins, as `title (year)`.

    The product's title rather than the producer: "OECD Time Use Database" names the dataset a reader
    would look for, where "OECD" alone names an organization with hundreds of them.
    """
    years: dict[str, list[str]] = {}
    for origin in tb[MAIN_CATEGORY_GROUPS[0]["source_columns"][0]].metadata.origins:
        year = origin.date_published.split("-")[0] if origin.date_published else ""
        seen = years.setdefault(origin.title or origin.producer, [])
        if year and year not in seen:
            seen.append(year)
    return "; ".join(f"{producer} ({'; '.join(sorted(ys))})" for producer, ys in years.items())


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def create_visualization(tb: Table, ages: dict[str, str], source_citation: str, layout: dict) -> plt.Figure:
    """Build one version of the stacked-bar chart.

    Layout notes:
    - One axes carries the bars; x is minutes of the day (0-1440), y is one unit per country row, row 0
      at the top. The key and the country labels are drawn outside the axes box (clipping is off).
    - Values are written inside segments wide enough to hold them at the layout's type size.
    """
    sns.set_style("ticks")
    sns.set_palette("deep")
    # After seaborn, not before: `set_style` replaces `font.sans-serif` with its own list, which is how
    # this step came to emit a stack it had not chosen.
    matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
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

    # Step 1: Header — the template's title row, then its subtitle.
    title = wrap_to_slot(TITLE, template["title_slot_px"], template["title_px"], PLAYFAIR_SLOT_SLACK)
    subtitle_text = layout["subtitle"].format(first=tb["year"].min(), last=tb["year"].max())
    subtitle = wrap_balanced(
        subtitle_text, subtitle_box_px(template, margin_px, lines_in(title)), template["subtitle_px"]
    )
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

    # Step 2: Footer, in the slots the static-chart templates define. A frame with no Note slot draws its
    # Note at the foot of the chart band instead, and the footer is told there is none.
    note = build_note(ages, layout)
    band_note = note if layout.get("note_in_band") else None
    draw_footer(fig, None if band_note else note, source_citation, layout, fx, fy)

    # Step 3: The plot's horizontal extent, set by the country label column.
    countries = [str(SHORT_COUNTRY_NAMES.get(country, country)) for country in tb["country"].tolist()]
    years = [str(year) for year in tb["year"].tolist()]
    country_font = font_pt(layout, "country_font_px")
    if layout.get("country_two_lines"):
        # The ADVANCE, not the ink: a text node's box spans its side bearings too, so a column sized to
        # the ink leaves the widest right-aligned label's box sticking past the frame margin.
        label_px = max(
            max(text_advance_px(name, country_font, bold=True), text_advance_px(year, country_font))
            for name, year in zip(countries, years)
        )
    else:
        label_px = max(
            run_row_width([(f"{name} ", True), (year, False)], layout["country_font_px"])
            for name, year in zip(countries, years)
        )
    plot_left_px = margin_px + label_px + COUNTRY_LABEL_PAD
    plot_width_px = (width_px - margin_px) - plot_left_px
    px_per_min = plot_width_px / MINUTES_PER_DAY

    # Step 4: The band between the subtitle's ink and the footer's, and the key's room in it.
    label_mode = layout["group_labels"]
    assert label_mode in {"aligned", "rows"}, f"Unknown group_labels {label_mode!r}."
    band_top = subtitle_y + lines_in(subtitle) * template["subtitle_line_px"]
    band_bottom = (
        template["note_bottom_px"] - lines_in(note) * template["note_line_px"]
        if band_note is None
        else template["footer_top_px"]
    )
    # The band's foot is BAND_INSET of air — unless a Note is drawn there, in which case the Note IS the
    # foot: it sits the footer's own row gap above the "Data source:" row, so the three read as three
    # evenly spaced footer rows, with NOTE_BAND_GAP of air above it holding it off the last bar.
    if band_note is None:
        note_top_px = None
        foot_px = BAND_INSET
    else:
        note_top_px = template["note_bottom_px"] - lines_in(band_note) * template["note_line_px"]
        foot_px = band_bottom - note_top_px + NOTE_BAND_GAP
    # Where the last bar's INK has to stop. A bar fills BAR_FRACTION of its row, centred, so it stops half
    # an inter-bar gap short of the axes' own bottom edge, and starts the same share below its top.
    ink_bottom_px = band_bottom - foot_px
    bar_head_rows = (1 - BAR_FRACTION) / 2
    n_rows = len(tb)

    key = []
    if label_mode == "aligned":
        key = key_blocks(segment_spans(tb.iloc[0], px_per_min, layout["groups"]), layout)
        collision = key_blocks_collide(key)
        assert not collision, f"The key blocks for {collision} would touch. Re-break their `key_lines`."
        # Where the first bar's ink has to start so every block fits above it, then the axes top that puts
        # it there: the pitch is solved from the axes top, so the two are one equation, not an iteration.
        subtitle_right_px = margin_px + max(
            text_width_px(line, template["subtitle_px"] * POINTS_PER_PIXEL) for line in subtitle.split("\n")
        )
        bars_top_px = key_header_px(key, plot_left_px, band_top, subtitle_right_px)
        share = bar_head_rows / (n_rows - bar_head_rows)
        chart_top_px = (bars_top_px - share * ink_bottom_px) / (1 - share)
    else:
        # The key is its own block above the plot, so the room it needs is the block plus the air under it.
        chart_top_px = band_top + BAND_INSET + legend_block_px(layout) + LEGEND_BLOCK_GAP
    row_pitch_px = (ink_bottom_px - chart_top_px) / (n_rows - bar_head_rows)
    chart_bottom_px = chart_top_px + n_rows * row_pitch_px
    paths.log.info(
        f"{layout['template']}: bars {BAR_FRACTION * row_pitch_px:.2f}px on a {row_pitch_px:.2f}px pitch, "
        f"first bar at {chart_top_px + bar_head_rows * row_pitch_px:.1f}px."
    )

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
    ax.set_ylim(n_rows, 0)  # row 0 at the top; the key lives above it

    # Step 5: The bars, their values and the country labels.
    group_colors = {group["column"]: resolve_color(group["color"], palette) for group in layout["groups"]}
    draw_bars(
        ax,
        tb,
        countries,
        years,
        group_colors,
        px_per_min,
        row_pitch_px,
        layout,
        value_label_columns(tb, px_per_min, layout),
    )

    # Step 6: The key.
    if label_mode == "rows":
        draw_row_legend(fig, layout, palette, band_top + BAND_INSET, fx, fy)
    else:
        draw_key(fig, key, plot_left_px, chart_top_px + bar_head_rows * row_pitch_px, layout, palette, fx, fy)

    if band_note is not None:
        assert note_top_px is not None
        # `chart__note`, not `note`: the Figma pass drops the step's copies of the template's own text
        # slots BY PREFIX, and `note` is one of them. On this frame the Note is not a duplicate of a
        # template slot — the frame has no Note slot, which is why it is drawn here — so a name that
        # collides with that list would have it deleted on import, silently and only on mobile. And
        # `<subject>__<role>`, so the re-import can join it to its library style by name.
        draw_note(fig, margin_px, note_top_px, band_note, template, "chart__note", fx, fy)

    # Clipping is swept in `export_frame`, on the way out, which is what lets the labels drawn
    # outside the axes box survive into the SVG whole.
    return fig


def subtitle_box_px(template: dict, margin_px: float, title_lines: int) -> float:
    """The subtitle's box width: cropped to end 24px short of the logo under a one-line title.

    A one-line title brings the subtitle up level with the logo's bottom edge, so the templates crop
    its box to clear it (`/owid-staff:create-figma-chart`, TEXTS.md). The title's line count is this
    step's own, which can disagree with Figma's on a title near one line; this one is far from it.
    """
    if title_lines == 1:
        return template["logo_x"] - SUBTITLE_LOGO_GAP - margin_px
    return template["subtitle_slot_px"]


def wrap_balanced(text: str, slot_px: float, size_px: float) -> str:
    """Wrap into the fewest lines the slot allows, then as evenly as those lines allow.

    The design team breaks the subtitle into lines of about equal length rather than filling the first
    one, which also leaves the desktop key room to rise beside its shorter lines. The breaks are
    explicit, so the frame's subtitle takes the same text with the same `\\n`.
    """
    fewest = lines_in(wrap_to_slot(text, slot_px, size_px))
    low, high = 0.0, slot_px
    for _ in range(30):
        mid = (low + high) / 2
        if lines_in(wrap_to_slot(text, mid, size_px)) <= fewest:
            high = mid
        else:
            low = mid
    return wrap_to_slot(text, high, size_px)


def value_label_columns(tb: Table, px_per_min: float, layout: dict) -> dict[str, int]:
    """Which groups carry a value, and in which form — `{column: index into value_candidates}`."""
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
                if value_label(group, minutes, index, segment_px(minutes, px_per_min), layout)
            )
            if fits >= VALUE_LABEL_COVERAGE * len(tb):
                labelled[column] = index
                break
    # A column that finds no form gets no labels at all, on every row, and nothing else in the step
    # notices — the chart just renders with a quarter of its numbers missing. It is the failure mode a
    # narrower frame produces, so it is the one to fail loudly on.
    missing = [group["label"] for group in layout["groups"] if group["column"] not in labelled]
    assert not missing, (
        f"No value form fits {VALUE_LABEL_COVERAGE:.0%} of the rows for {missing} at "
        f"{px_per_min:.3f}px per minute. Widen the plot — a shorter country label frees the column — "
        f"or drop `value_fontsize`."
    )
    return labelled


def segment_px(minutes: float, px_per_min: float) -> float:
    """A segment's drawn width: its share of the day, less the gap that separates it from the next."""
    return minutes * px_per_min - SEGMENT_GAP_PX


def draw_bars(
    ax,
    tb: Table,
    countries: list[str],
    years: list[str],
    group_colors: dict,
    px_per_min: float,
    row_pitch_px: float,
    layout: dict,
    value_columns: dict[str, int],
) -> None:
    """One stacked row per country: the country label and its four segments, a gap of canvas apart."""

    def baseline(fontsize: float, offset_px: float = 0.0) -> float:
        """Where to put a line's baseline so its ink is centred `offset_px` below the bar's centre.

        `va="center"` centres the font's whole line box, which reserves room for descenders the
        digits never use — so a value label sits a tenth of a bar high, and text with descenders
        ("Japan") lands on a different baseline from text without ("269 mins"). Placing the baseline
        explicitly at half a cap-height below the line's centre fixes both.
        """
        return 0.5 + (offset_px + cap_height_px(fontsize) / 2) / row_pitch_px

    country_font = font_pt(layout, "country_font_px")
    label_x = -COUNTRY_LABEL_PAD / px_per_min
    inset = layout["value_inset_px"]
    last_column = layout["groups"][-1]["column"]
    for row in range(len(tb)):
        country_row = tb.iloc[row]
        slug = slugify(str(country_row["country"]))

        # The country's name in bold and its survey year in regular: a line each on desktop, centred on
        # the bar, or one right-aligned line where the rows are too tight for two. No brackets on that line:
        # the weight separates the two, and right-aligned, the years already form a column.
        if layout.get("country_two_lines"):
            for text, offset, bold, gid in (
                (countries[row], -COUNTRY_LINE_PX / 2, True, f"{slug}__label"),
                (years[row], COUNTRY_LINE_PX / 2, False, f"{slug}__label-year"),
            ):
                ax.text(
                    label_x,
                    row + baseline(country_font, offset),
                    text,
                    ha="right",
                    va="baseline",
                    fontsize=country_font,
                    fontweight="bold" if bold else "normal",
                    color=TEXT_COLOR,
                    gid=gid,
                )
        else:
            name_run, year_run = f"{countries[row]} ", years[row]
            name, name_ink, name_step = place_run(name_run, country_font, bold=True)
            year, year_ink, _ = place_run(year_run, country_font)
            # Laid out left to right from where the row has to start, each run anchored on its ink
            # centre — the anchor that survives Figma's narrower face (see `draw_run_row`).
            start_px = -COUNTRY_LABEL_PAD - (name_step + year_ink)
            for text, centre_px, bold, gid in (
                (name, start_px + name_ink / 2, True, f"{slug}__label"),
                (year, start_px + name_step + year_ink / 2, False, f"{slug}__label-year"),
            ):
                ax.text(
                    centre_px / px_per_min,
                    row + baseline(country_font),
                    text,
                    ha="center",
                    va="baseline",
                    fontsize=country_font,
                    fontweight="bold" if bold else "normal",
                    color=TEXT_COLOR,
                    gid=gid,
                )

        left = 0.0
        for group in layout["groups"]:
            column = group["column"]
            minutes = float(country_row[column])
            color = group_colors[column]
            last = column == last_column
            # Every segment but the last stops short by the gap, so its start stays at its true minute.
            ax.barh(
                row + 0.5,
                minutes if last else segment_px(minutes, px_per_min) / px_per_min,
                left=left,
                height=BAR_FRACTION,
                color=color,
                alpha=SEGMENT_ALPHA,
                linewidth=0,
                gid=f"{slug}__{slugify(column)}",
            )
            label = (
                value_label(group, minutes, value_columns[column], segment_px(minutes, px_per_min), layout)
                if column in value_columns
                else None
            )
            if label:
                # Left at the segment's start, and the last one right at the bar's end, so the values line
                # up down each column with the key over them.
                ax.text(
                    left + minutes - inset / px_per_min if last else left + inset / px_per_min,
                    row + baseline(font_pt(layout, "value_font_px")),
                    label,
                    ha="right" if last else "left",
                    va="baseline",
                    fontsize=font_pt(layout, "value_font_px"),
                    color=value_label_color(composite_on_background(color)),
                    gid=f"{slug}__{slugify(column)}-value",
                )
            left += minutes


def key_blocks(spans: dict, layout: dict) -> list[dict]:
    """Measure the desktop key: per category, its list over its name, aligned with the values under it.

    Each block is anchored where the top row's value sits — left at its segment's start plus
    `value_inset_px`, and the last one right at the bar's end — and is as wide as its widest line.
    """
    plot_end = spans[layout["groups"][-1]["column"]][1]
    list_font, name_font = font_pt(layout, "key_font_px"), font_pt(layout, "key_name_font_px")
    blocks = []
    for index, category in enumerate(layout["categories"]):
        last = index == len(layout["categories"]) - 1
        start = spans[category["columns"][0]][0]
        anchor = plot_end - layout["value_inset_px"] if last else start + layout["value_inset_px"]
        width = max(
            *(text_advance_px(line, list_font) for line in category["key_lines"]),
            *(text_advance_px(line, name_font, bold=True) for line in category["label_lines"]),
        )
        blocks.append(
            {
                "category": category,
                "anchor": anchor,
                "align": "right" if last else "left",
                "extent": (anchor - width, anchor) if last else (anchor, anchor + width),
                # The block's box, from the bars up: the air over the bar, the name, the gap, the list.
                "height": KEY_BAR_GAP
                + len(category["label_lines"]) * layout["key_name_line_px"]
                + KEY_NAME_GAP
                + len(category["key_lines"]) * layout["key_line_px"],
            }
        )
    return blocks


def key_blocks_collide(blocks: list[dict]) -> str | None:
    """The first pair of neighbouring key blocks that touch, or None if they all clear each other."""
    for left, right in zip(blocks, blocks[1:]):
        if overlaps(left["extent"], right["extent"], HEADER_MIN_GAP):
            return f"{left['category']['name']} and {right['category']['name']}"
    return None


def key_header_px(blocks: list[dict], plot_left_px: float, band_top: float, subtitle_right_px: float) -> float:
    """Where the first bar's ink has to start so that every key block fits above it, in frame px.

    A block under the subtitle has to clear it by `BAND_INSET`, like any plot ink. One that clears the
    subtitle SIDEWAYS may rise to the header's bottom edge instead, beside the subtitle's shorter lines —
    which is where the design team put the two four-line lists.
    """
    needed = []
    for block in blocks:
        start, end = (plot_left_px + x for x in block["extent"])
        beside = start >= subtitle_right_px + HEADER_MIN_GAP
        needed.append((band_top if beside else band_top + BAND_INSET) + block["height"])
    return max(needed)


def draw_key(fig, blocks: list[dict], plot_left_px: float, bars_top_px: float, layout: dict, palette, fx, fy) -> None:
    """Draw the desktop key: each category's run-in list, and under it, nearest the bars, its name.

    One text node per line, each placed on its own baseline with its ink centred in its line box, as
    the bars' values are; the line breaks are the design team's.
    """
    list_font, name_font = font_pt(layout, "key_font_px"), font_pt(layout, "key_name_font_px")
    for block in blocks:
        category = block["category"]
        slug = slugify(category["name"])
        x = fx(plot_left_px + block["anchor"])
        name_top = bars_top_px - KEY_BAR_GAP - len(category["label_lines"]) * layout["key_name_line_px"]
        list_top = name_top - KEY_NAME_GAP - len(category["key_lines"]) * layout["key_line_px"]
        rows = [
            (
                line,
                list_top + index * layout["key_line_px"],
                layout["key_line_px"],
                list_font,
                False,
                f"key__{slug}-members",
            )
            for index, line in enumerate(category["key_lines"])
        ] + [
            (
                line,
                name_top + index * layout["key_name_line_px"],
                layout["key_name_line_px"],
                name_font,
                True,
                f"category__{slug}",
            )
            for index, line in enumerate(category["label_lines"])
        ]
        seen: dict[str, int] = {}
        for text, top, line, fontsize, bold, gid in rows:
            count = seen.get(gid, 0)
            seen[gid] = count + 1
            fig.text(
                x,
                fy(top + line / 2 + cap_height_px(fontsize) / 2),
                text,
                ha=block["align"],
                va="baseline",
                fontsize=fontsize,
                fontweight="bold" if bold else "normal",
                color=header_text_color(category["color"], palette) if bold else MEMBER_TEXT_COLOR,
                gid=gid if count == 0 else f"{gid}-line{count}",
            )


def check_key_lines() -> None:
    """Assert each category's key lines spell out its members, so a designer's breaks cannot drift."""
    for category in MAIN_CATEGORIES:
        assert " ".join(category["key_lines"]) == run_in_list(category["members"]), (
            f"The key lines for {category['name']} no longer spell out its members: re-break "
            f"{run_in_list(category['members'])!r}."
        )
        assert " ".join(category["label_lines"]) == category["name"], (
            f"The name lines for {category['name']} no longer spell out its name."
        )


def layout_row_legend(layout: dict) -> list[dict]:
    """Measure each legend row: the chip, the bold category name, then the members it holds.

    Asserted to fit the content width rather than wrapped. A key row that wraps stops reading as a
    row — the second line hangs under the members with nothing to tie it to its chip — so the fix is
    the wording, and a member name is the thing to shorten.
    """
    fontsize = font_pt(layout, "legend_font_px")
    content_px = layout["size"][0] - 2 * layout["margin"]
    rows = []
    for group in layout["groups"]:
        members = run_in_list(group["members"])
        width_px = (
            LEGEND_CHIP_PX
            + LEGEND_CHIP_GAP
            + text_advance_px(group["label"], fontsize, bold=True)
            + LEGEND_NAME_GAP
            + text_advance_px(members, fontsize)
        )
        # Against the regular-weight slack, though the name is bold: bold Lato is the *narrower* of the
        # two against this step's Arial, so the regular figure is the conservative one.
        assert fits_slot(width_px, content_px), (
            f"The {group['label']!r} key row needs {width_px:.0f}px of the frame's {content_px:.0f}px. "
            f"Shorten a member name."
        )
        rows.append({"group": group, "members": members})
    return rows


def run_in_list(members: list[str]) -> str:
    """The members as one comma-separated phrase, in sentence case: "Paid work, commuting, homework".

    The desktop key stacks one name per line, where each is a label and keeps its capital; inline, they
    are a list inside a phrase, which the style guide sets in sentence case. A word that opens with two
    capitals is an abbreviation ("TV & radio") and keeps them.
    """

    def lowered(name: str) -> str:
        return name if name[1:2].isupper() else name[:1].lower() + name[1:]

    return ", ".join([members[0], *(lowered(name) for name in members[1:])])


def legend_block_px(layout: dict) -> float:
    """How tall the row legend is, from its first row's INK: one line per category, less the top inset.

    Measured from the ink for the same reason the desktop header is: the band is inset by BAND_INSET
    from the subtitle, and reserving the first row's whole line box put the chip and text 3.5px further
    down, 17.5px of air where every other frame holds 14.
    """
    return len(layout["groups"]) * line_px(font_pt(layout, "legend_font_px")) - legend_ink_inset_px(layout)


def legend_ink_inset_px(layout: dict) -> float:
    """How far below the top of a key row's line box its ink starts: the taller of chip and cap height."""
    fontsize = font_pt(layout, "legend_font_px")
    return line_px(fontsize) / 2 - max(LEGEND_CHIP_PX, cap_height_px(fontsize)) / 2


def draw_row_legend(fig, layout: dict, palette, top_px: float, fx, fy) -> None:
    """The key as a block of rows above the plot: a colour chip, the category's name, what it holds.

    The desktop frame hangs each name over the run of bars it belongs to, which is the better reading
    and is why that frame keeps it — the key is where the eye already is. It needs the segment to be
    wide enough to hold the words, though, and this frame's 508px of content does not give it: the
    narrowest category spans 56px against a 105px name, so all four names would overhang the bars they
    name by more than they cover, and a name that overhangs points at its neighbour too. The chip
    carries the link to the bar instead, at the bar's own fill and opacity.

    Drawn in figure coordinates rather than on the axes, because unlike the desktop key it is
    positioned by the frame's margin rather than by any segment.
    """
    fontsize = font_pt(layout, "legend_font_px")
    width_px, height_px = layout["size"]
    line = line_px(fontsize)
    for index, row in enumerate(layout_row_legend(layout)):
        group = row["group"]
        # The block starts at its ink (see `legend_block_px`), so the rows start an inset higher.
        centre_px = top_px - legend_ink_inset_px(layout) + index * line + line / 2
        # Ink centred on the row, the way `draw_bars` centres a value on its bar: `va="center"` centres
        # the whole line box instead, which reserves room for descenders the chip does not have and
        # leaves the two a descender out of line.
        baseline_px = centre_px + cap_height_px(fontsize) / 2
        fig.add_artist(
            Rectangle(
                (fx(layout["margin"]), fy(centre_px + LEGEND_CHIP_PX / 2)),
                LEGEND_CHIP_PX / width_px,
                LEGEND_CHIP_PX / height_px,
                transform=fig.transFigure,
                facecolor=resolve_color(group["color"], palette),
                alpha=SEGMENT_ALPHA,
                linewidth=0,
                gid=f"legend__{slugify(group['column'])}-chip",
            )
        )
        cursor = layout["margin"] + LEGEND_CHIP_PX + LEGEND_CHIP_GAP
        color = header_text_color(group["color"], palette)
        # Name and members at one size, separated by weight and colour: the name in its category's
        # colour, the list in the body gray, as on the desktop frame. There the name is also larger,
        # because it is a heading on a line of its own above its list; inline, immediately before the
        # list, weight and colour are enough.
        for text, bold, gid in ((group["label"], True, ""), (row["members"], False, "-members")):
            drawn, ink_px, step_px = place_run(text, fontsize, bold)
            fig.text(
                fx(cursor + ink_px / 2),
                fy(baseline_px),
                drawn,
                ha="center",
                va="baseline",
                fontsize=fontsize,
                fontweight="bold" if bold else "normal",
                color=color if bold else MEMBER_TEXT_COLOR,
                gid=f"legend__{slugify(group['column'])}{gid}",
            )
            cursor += step_px + (LEGEND_NAME_GAP if bold else 0.0)


def draw_footer(fig, note: str | None, source_citation: str, layout: dict, fx, fy) -> None:
    """Fill the template's footer slots: Note, Data source, tagline and license.

    The footer is bottom-anchored, so the rows below the Note keep their y whatever the Note does and
    the Note itself grows upward from a fixed ink bottom.

    Two shapes, told apart by whether the template has a `tagline_y`: the 850-wide frames put the
    tagline and the license on one shared row, and the 540-wide ones give the license a full-width row
    of its own and carry no tagline at all.
    """
    width_px = layout["size"][0]
    margin_px = layout["margin"]
    template = layout["template_text"]
    content_px = width_px - 2 * margin_px

    if note is not None:
        note_top = template["note_bottom_px"] - lines_in(note) * template["note_line_px"]
        draw_note(fig, margin_px, note_top, note, template, "note", fx, fy)
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

    if "tagline_y" not in template:
        # Nothing shares this row, so there is nothing to shrink — only the fit to check.
        assert fits_slot(run_row_width(license_runs(), template["license_px"]), content_px), (
            f"The license row needs more than the frame's {content_px:.0f}px. The phrasing gives, never a name."
        )
        draw_run_row(fig, license_runs(), template["license_px"], margin_px, template["license_y"], "license", fx, fy)
        return

    # The tagline and the license share this row: the tagline left, the license right-aligned to the
    # content edge.
    row_px = template["license_px"]
    # Nothing wraps this row — the tagline's wording is fixed and a name is never shortened — so
    # instead the row is drawn a hair smaller when this step's font would collide the two, which
    # the template's narrower Lato would not. The assert is the real limit: past it the row does
    # not fit even once the template sets it, and the wording has to give.
    needed_px = (
        text_advance_px(TAGLINE, row_px * POINTS_PER_PIXEL)
        + LICENSE_TAGLINE_GAP
        + run_row_width(license_runs(), row_px)
    )
    assert fits_slot(needed_px, content_px, LATO_BOLD_SLOT_SLACK), (
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
        width_px - margin_px,
        template["license_y"],
        "license",
        fx,
        fy,
        align="right",
    )


# ---------------------------------------------------------------------------
# Key layout
# ---------------------------------------------------------------------------


def segment_spans(row, px_per_min: float, groups: list[dict]) -> dict[str, tuple[float, float]]:
    """Each group's (start, end) in template pixels, for one row of the chart."""
    spans = {}
    cumulative = 0.0
    for group in groups:
        minutes = float(row[group["column"]])
        spans[group["column"]] = (cumulative * px_per_min, (cumulative + minutes) * px_per_min)
        cumulative += minutes
    return spans


# ---------------------------------------------------------------------------
# Text and color helpers
# ---------------------------------------------------------------------------


def overlaps(a: tuple[float, float], b: tuple[float, float], gap: float) -> bool:
    """Whether two spans come within `gap` of each other."""
    return a[0] - gap < b[1] and b[0] - gap < a[1]


def font_pt(layout: dict, role: str) -> float:
    """One of the layout's type sizes, in points, from the whole template pixels it is declared in.

    Declared in pixels because that is the unit the templates state their own slots in — 25, 16, 14, 12
    and 11 — and because declaring them in points is what produced sizes like 12.153px, which no
    template would ask for and no designer would set. matplotlib wants points, so the conversion lives
    here rather than in the table.
    """
    return layout[role] * POINTS_PER_PIXEL


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


def check_contrast() -> None:
    """Check every contrast ratio this chart's colors have to hold, at the opacity it draws them.

    A ratio either clears its floor or matches a pin in `CONTRAST_ALLOWANCES`, and an allowance that
    is no longer needed fails too. Recorded numbers go stale and prose lies: the comment beside
    `SEGMENT_ALPHA` claimed for a while that 0.9 would fix the in-bar labels, and it does not.
    Measuring on every build is the only version of those numbers that cannot drift.
    """
    fills = [group["color"][1] for group in MAIN_CATEGORY_GROUPS]
    names = [group["label"] for group in MAIN_CATEGORY_GROUPS]
    composited = [composite_on_background(fill) for fill in fills]

    # Touching fills need a lightness gap, or the stack merges into one block in grayscale — and the
    # same gap has to survive dichromacy, which redistributes lightness. Segments separated by a gap of
    # canvas do not touch, so the gap carries the boundary and these floors do not apply.
    seams = list(zip(zip(composited, names), list(zip(composited, names))[1:])) if SEGMENT_GAP_PX == 0 else []
    for (fill, name), (next_fill, next_name) in seams:
        hold_contrast(
            ("seam", name, next_name),
            contrast_ratio(fill, next_fill),
            SEAM_MIN_RATIO,
            f"the {name} and {next_name} segments touch",
            "Reorder the palette, or raise SEGMENT_ALPHA",
        )
        worst = min(
            contrast_ratio(simulate_dichromacy(fill, kind), simulate_dichromacy(next_fill, kind))
            for kind in DICHROMACY_MATRICES
        )
        hold_contrast(
            ("cvd-seam", name, next_name),
            worst,
            SEAM_MIN_RATIO,
            f"the {name} and {next_name} seam under colour-blindness",
            "Reorder the palette, or pick a pair that differs in tone as well as hue",
        )
    # Values sit inside the segments, so they are body text on that fill.
    for fill, name in zip(composited, names):
        color = value_label_color(fill)
        hold_contrast(
            ("value", name),
            contrast_ratio(color, fill),
            VALUE_LABEL_MIN_RATIO,
            f"the value inside the {name} segment, in its best available color ({color})",
            "Raise SEGMENT_ALPHA, or draw this segment's values outside the bar",
        )
    # A category's name and member list are printed in its own color, on the cream canvas.
    for group in MAIN_CATEGORY_GROUPS:
        spec = group["color"]
        color = spec[2] if len(spec) > 2 else spec[1]
        hold_contrast(
            ("header", group["label"]),
            contrast_ratio(color, BACKGROUND_COLOR),
            HEADER_TEXT_MIN_RATIO,
            f"the {group['label']} name on the canvas",
            "Pair the fill with its Line and Slope Charts variant",
        )


def hold_contrast(key: tuple, ratio: float, floor: float, what: str, remedy: str) -> None:
    """One measured ratio against its floor, or against the shortfall this chart has accepted."""
    allowance = CONTRAST_ALLOWANCES.get(key)
    if ratio + CONTRAST_TOLERANCE >= floor:
        assert allowance is None, (
            f"CONTRAST_ALLOWANCES still pins {key} at {allowance:.2f}:1, but {what} now measures "
            f"{ratio:.2f}:1 and clears the {floor}:1 floor. Delete the allowance."
        )
        return
    assert allowance is not None, (
        f"At SEGMENT_ALPHA={SEGMENT_ALPHA}, {what} measures {ratio:.2f}:1, under the {floor}:1 floor. "
        f"{remedy} — or, if the shortfall is deliberate, pin it in CONTRAST_ALLOWANCES with the reason."
    )
    assert ratio + CONTRAST_TOLERANCE >= allowance, (
        f"{what.capitalize()} has regressed to {ratio:.2f}:1, below the {allowance:.2f}:1 this chart accepts. {remedy}."
    )


def check_members_are_the_largest(detail: Table) -> None:
    """Assert each category's key names its biggest members, not an arbitrary four or five of them.

    A key with fewer names than the category has members is a selection, and the only defensible
    selection is by size. Measured on the countries actually drawn, and on medians rather than means
    so one country's coding choice cannot promote a member into the key.
    """
    assert {category["name"] for category in MAIN_CATEGORIES} == set(KEY_MEMBER_COLUMNS), (
        "KEY_MEMBER_COLUMNS no longer covers exactly the drawn categories."
    )
    for category in MAIN_CATEGORIES:
        spec = KEY_MEMBER_COLUMNS[category["name"]]
        residual = {spec["residual"]} if "residual" in spec else set()
        assert set(category["members"]) == set(spec["named"]) | residual, (
            f"The names drawn for {category['name']} and the columns they map to have drifted: "
            f"{sorted(set(category['members']) ^ (set(spec['named']) | residual))}."
        )
        columns = list(spec["named"].values()) + list(spec["unnamed"])
        assert not set(columns) - set(detail.columns), f"Source columns changed under {category['name']}."
        medians = {column: detail[column].median() for column in columns}
        # A member no country reports has no median; it cannot outrank anything.
        smallest_named = min(
            ((name, medians[column]) for name, column in spec["named"].items() if pd.notna(medians[column])),
            key=lambda named: named[1],
        )
        for column in spec["unnamed"]:
            median = medians[column]
            assert pd.isna(median) or median <= smallest_named[1], (
                f"{category['name']} names '{smallest_named[0]}' at a median {smallest_named[1]:.1f} min/day "
                f"but leaves '{column}' unnamed at {median:.1f} — the key is naming a smaller member than "
                f"one it hides. Swap the name, or say why in KEY_MEMBER_COLUMNS."
            )


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


def simulate_dichromacy(color, kind: str) -> tuple[float, float, float]:
    """A color as a dichromat sees it — Vienot-Brettel-Mollon, applied in linear RGB.

    Here because a seam is a LIGHTNESS gap, and lightness is exactly what dichromacy redistributes: two
    fills can sit 2.3:1 apart in grayscale and 1.1:1 apart for a red-green colour-blind reader, which is
    the case this chart actually has. Nothing else in the step would notice.
    """
    matrix = DICHROMACY_MATRICES[kind]
    channels = [srgb_to_linear(c) for c in (color if not isinstance(color, str) else to_rgb(color))]
    out = [min(1.0, max(0.0, sum(m * c for m, c in zip(row, channels)))) for row in matrix]
    return tuple(linear_to_srgb(c) for c in out)


def srgb_to_linear(c: float) -> float:
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def linear_to_srgb(c: float) -> float:
    return c * 12.92 if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055


def composite_on_background(color, alpha: float | None = None) -> tuple[float, float, float]:
    """A fill as it lands on the canvas once its opacity is applied — the color a label sits on.

    `alpha` is read at call time, not captured as a default: a default argument would freeze
    `SEGMENT_ALPHA` at import, which both hides a later change to the constant and makes
    `check_contrast` untestable against any other opacity.
    """
    alpha = SEGMENT_ALPHA if alpha is None else alpha
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
    prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize, weight="bold" if bold else "normal")
    points = TextPath((0, 0), text, prop=prop).get_extents().width
    return points / POINTS_PER_PIXEL


def cap_height_px(fontsize: float) -> float:
    """Height of a digit's ink above the baseline, in template pixels."""
    prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize)
    return TextPath((0, 0), "0", prop=prop).get_extents().ymax / POINTS_PER_PIXEL


def draw_note(fig, x_px: float, y_px: float, text: str, template: dict, gid: str, fx, fy) -> None:
    """Draw a Note with its lead-in bold: `Note:` in bold, then the sentence in regular.

    matplotlib has no rich text, so the first line is two objects and the rest are one block. The
    lead-in is anchored on its own ink centre and the remainder on its LEFT edge, which is what
    survives Figma: a centred run re-rendered in the template's narrower Lato shrinks away from its
    centre, and doing that to the remainder would open a gap after the lead-in.
    """
    size_px = template["note_px"]
    line_height_px = template["note_line_px"]
    fontsize = size_px * POINTS_PER_PIXEL
    lines = text.split("\n")
    assert lines[0].startswith(NOTE_LEAD), f"The Note does not start with {NOTE_LEAD!r}: {lines[0][:40]!r}"

    drawn, ink_px, step_px = place_run(NOTE_LEAD + " ", fontsize, bold=True)
    fig.text(
        fx(x_px + ink_px / 2),
        fy(y_px),
        drawn,
        ha="center",
        va="top",
        fontsize=fontsize,
        fontweight="bold",
        color=FOOTER_COLOR,
        gid=gid,
    )
    fig.text(
        fx(x_px + step_px),
        fy(y_px),
        lines[0][len(NOTE_LEAD) :].lstrip(),
        ha="left",
        va="top",
        fontsize=fontsize,
        color=FOOTER_COLOR,
        gid=f"{gid}--rest",
    )
    if len(lines) > 1:
        draw_slot(
            fig,
            fx(x_px),
            fy(y_px + line_height_px),
            "\n".join(lines[1:]),
            size_px,
            line_height_px,
            f"{gid}--tail",
            FOOTER_COLOR,
        )


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


def fits_slot(measured_px: float, slot_px: float, ratio: float = LATO_SLOT_SLACK) -> bool:
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


def wrap_to_slot(text: str, slot_px: float, size_px: float, slack: float = LATO_SLOT_SLACK) -> str:
    """Wrap text into one of the template's slots, at the template's own size.

    `slack` converts the slot into this step's font: a Lato slot holds a little more than this step
    measures and a serif slot a little less, so the two faces pass different figures.
    """
    return wrap_to_width(text, slot_px * slack, size_px * POINTS_PER_PIXEL)


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

    Every drawn segment is worth hours rather than minutes, so all four set `as_hours`; `unit_suffix`
    puts the unit on the leftmost segment of a row where the values are minutes.
    """
    if group.get("as_hours"):
        return format_hours(minutes)
    if with_suffix and group.get("unit_suffix"):
        return [f"{round(minutes)} mins", f"{round(minutes)}"]
    return [f"{round(minutes)}"]


def value_label(group: dict, minutes: float, form: int, available_px: float, layout: dict) -> str | None:
    """A segment's label in its column's chosen form, or nothing where that form does not fit.

    One column, one form, and no shorter fallback: `format_hours`' last candidate is the bare minute
    count, and a row printing "162" among rows printing "2h 42m" does not read as a narrow bar — it
    reads as a different unit, with nothing on the frame to say which. The mobile frame has 388px of
    plot and two segments too narrow for the compact form, so it is the frame that made this visible;
    the rule is the one `value_label_columns` already states for choosing the form in the first place.
    Leaving those two unlabelled costs the reader nothing: the bar carries the value, and the row
    still visibly adds to a day.
    """
    candidates = value_candidates(group, minutes, layout["with_mins_suffix"])
    return fit_text(
        candidates[form : form + 1], available_px - layout["value_inset_px"], font_pt(layout, "value_font_px")
    )


def fit_text(candidates: list[str], available_px: float, fontsize: float) -> str | None:
    """The longest candidate that fits the available width once the frame sets it in Lato, or None."""
    for candidate in candidates:
        if fits_slot(text_width_px(candidate, fontsize) + VALUE_PAD, available_px):
            return candidate
    return None


def slugify(name: str) -> str:
    """Layer-panel-friendly id: lowercase, hyphens for spaces and underscores."""
    return name.lower().replace(" & ", "-").replace(" ", "-").replace("_", "-").replace(":", "")


def build_note(ages: dict[str, str], layout: dict) -> str:
    """Compose the Note row: the age-of-reference exceptions, if any, then what the averages cover."""
    described = {
        "15 and more": "15 and over",
        "15-74": "15 to 74",
        "20-64": "20 to 64",
    }
    exceptions = ", ".join(f"{country} ({described.get(age, age)})" for country, age in sorted(ages.items()))
    # Only the exceptions, because the subtitle already says 15 to 64 and the Note has two lines to
    # spend on things said nowhere else. With the 2010 cutoff there are no exceptions left, so this is
    # normally empty — it exists for a release that adds an older survey back.
    ages_sentence = f"Estimates cover people aged 15 to 64, except in {exceptions}. " if exceptions else ""
    # The survey scope is the subtitle's now ("each country's most recent time-use survey in the OECD
    # data" — "in the OECD data" because the OECD does not refresh a country the moment it runs a new
    # survey: Korea is shown at 2014 while Statistics Korea has published 2019 and 2024 rounds).
    text = f"{NOTE_LEAD} " + ages_sentence
    # The Note's two lines go to what a reader needs to read a bar right, and only that.
    #
    # First, what the averages are over. A reader who sees 2h 57m of paid work in Italy reads it as a
    # short working day; it is the average across everyone aged 15 to 64, working or not, and across all
    # seven days (the workbook: "both weekdays and weekends").
    #
    # Then the comparability caveat, in general terms. It used to name two countries. Mexico is now left
    # off the chart (`NOT_COMPARABLE`). Japan's non-commuting travel ("11. Moving (excluding commuting)")
    # is mapped to the source's 5.2 "other (no categories)", which this chart folds into "Unpaid work &
    # other" — not into unpaid work, as the old sentence said — and at 37 min/day that bucket sits with
    # Finland's and Turkey's 40, so no bar on the chart stands out for it. The workbook does not say
    # "harmonizes"; what its activity sheet shows is each survey's codes mapped onto common categories.
    text += (
        "Figures are averaged across everyone aged 15 to 64, including people who don't work or study, and "
        "across all days of the week. The OECD groups each country's survey activities into common "
        "categories, but differences between the surveys can still affect comparisons."
    )
    # Against the narrower of the Note's own slot and the content width — the slot is the template's,
    # but a mobile frame's content is narrower than it.
    template = layout["template_text"]
    content_px = layout["size"][0] - 2 * layout["margin"]
    # The lead-in is drawn bold and bold is wider, so its extra width comes off the wrap's budget
    # here rather than being discovered as an overhanging first line. Taken off every line, not just
    # the first: it is under 2px at this size, and both frames wrap to the same break points either
    # way, so the simpler rule costs nothing and cannot leave the first line short of room.
    note_pt = template["note_px"] * POINTS_PER_PIXEL
    bold_extra = text_advance_px(NOTE_LEAD, note_pt, bold=True) - text_advance_px(NOTE_LEAD, note_pt)
    slot_px = min(template["note_slot_px"], content_px) - bold_extra
    wrapped = wrap_to_slot(text, slot_px, template["note_px"])
    # Two lines on the desktop frame, whose footer grows DOWNWARD from a fixed top (see
    # `note_bottom_px`), so a third eats the template's bottom margin rather than the chart's band.
    # The mobile frame has no Note slot at all and draws this inside the band, where a line costs a
    # line of chart and nothing else — which is why its cap is its own number.
    cap = template.get("note_max_lines", 2)
    assert lines_in(wrapped) <= cap, (
        f"The Note wraps to {lines_in(wrapped)} lines and this frame holds {cap} — shorten it, or "
        f"re-pin the frame's footer and raise `note_max_lines` together."
    )
    return wrapped
