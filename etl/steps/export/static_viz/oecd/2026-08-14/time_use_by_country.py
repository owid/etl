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

**26 countries over 2010-2024**, which is what garden publishes rather than anything this step
selects: the survey-year cutoff and the reasoning behind it live beside `EARLIEST_SURVEY_YEAR` in
`data://garden/oecd/2026-08-14/time_use`. This step asserts what it received and does not filter — a
second filter here would be dead code that reads as the live one.

Values are written inside segments wide enough to hold them, in hours and minutes. Survey years differ
by country, so each country label carries its year — the original's surveys spanned a narrower window
and it named none of them.

The header sits above the bars and points at the top row: each category's name over the segment it
names, with the activities that category holds listed under it, one per line, bottom-aligned so every
list ends level with the plot and the air falls under the heading. A name wider than its own segment is
wrapped to it, and `blocks_collide` asserts that no block reaches into its neighbour's, since wrapping
cannot save a single word wider than its span. Labelling each activity over its own segment, with a
leader back to it, was tried and dropped: threading names past each other needed elbow leaders and a
crossing budget, for a header no easier to read.

Nothing draws each block's *extent*, only its centre, so how wide a category is has to be read off the
bars. A 1px rule under each name, spanning that category's own segment, was built and rendered (the
`bracket` span on every placement is what it needs) and is an open question for design rather than a
decision taken here: it does say which segment a block belongs to and how wide it is, but the rules
follow their own lists' depths, so four of them stagger across the band, and the two narrow ones very
nearly meet. The header lost its bracket rules on purpose when each category became a single segment,
and putting one back is that call being revisited.

One frame is emitted, 850x1095, following the vertical static-chart template. Fonts and the logo come
from that template and are deliberately not set here. Colors *are* set here, unlike the retired
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
- Frame **`how-do-people-spend-their-time`**, `26879:11` —
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=26879-11
  A clone of `Static Chart Template_Vertical` (`5332:93`, 850x1095), at x=1000, with its unstyled
  import parked at x=70 to the LEFT so the page reads original then edited.
- Frame **`how-do-people-spend-their-time-mobile`**, `27518:8` —
  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=27518-8
  A clone of `Static Chart Template_Mobile (example 2)` (`24590:32`, 540x824), at x=2550, its own
  unstyled import parked at x=1930. So the page reads as two original-then-edited pairs, left to
  right, and the mobile pair is far enough right that its reference copy clears the desktop frame.

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
the middle step invalidates the first). The 26 country labels take `Text/Gray 80`.

**Colors**, bound as library styles rather than pasted as hexes: `Default Palette/Denim`
`e1538d93...`, `Camel` `45161823...` with `Line and Slope Charts/Camel` `c17ca762...` for its name,
`Rusty Orange` `65bab597...`, and `Light Teal` `9a2854bc...` with `Line and Slope Charts/Light Teal`
`a07c1354...`. Bars carry `SEGMENT_ALPHA` as *node* opacity, not paint opacity, so the binding
survives. Value labels: white on Rusty Orange only, and `Text/Gray 100` on the other three, which is what
`value_label_color` picks measured against the composited fill at `SEGMENT_ALPHA`. This line used to
say white on Denim too, and the frame was styled that way; at 0.8 that is the WORSE of the two (dark
reaches 3.74:1 there and white less), so bind whatever hex the SVG carries rather than a colour named
here — the step measures it and this comment cannot.

**Paint this frame with its own pass, not `/create-figma-chart`'s `restyle_static_import.js`.** That
script is the right tool for a base-plus-tints chart and its font and anchor passes are the ones used
here, but its family model has no place for a fill paired with a darker text variant, which is the
whole point of the two light fills above. Its `reflowLegend` pass is also wrong for a legend like this
one — it re-lays a row of runs from the leftmost, which collided them (doubled separators, overlapping
names) where this step had already spaced them by measured advance.
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
from etl.static_viz import TEMPLATES, apply_svg_rcparams, export_frame

# Non-path text so the SVG stays editable in Figma, and a fixed hash salt so it is reproducible.
# Both come from `etl.static_viz`, which this step used to set by hand with the same two values.
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
# These four name Chart colors library colors outright rather than the seaborn placeholders the
# ten-group version used: the reason for a placeholder is that a font cannot be reproduced here and a
# color can, so a render that shows the real ones is simply a truer preview of the frame. Figma still
# binds each to its library style — the hex is the preview, the binding is the artifact.
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
# TWO ACCEPTED DEVIATIONS, both about colour-blindness and both re-measured on every build via
# `CONTRAST_ALLOWANCES`, so neither can go stale the way this comment once did. Everything the step
# holds a hard floor to — the grayscale seams, the in-bar labels, the legend text — this palette clears
# outright at 0.8, with no allowance:
#   - Light Teal beside Maroon: the grayscale seam is a healthy 2.34, but under dichromacy it falls to
#     1.13. That boundary is carried by HUE rather than tone for a colour-blind reader: the two simulate
#     to dE 32 apart, which is a clear difference, just not a difference in lightness. A grayscale print
#     is unaffected — that is the 2.34. The other two seams hold under simulation at 4.56 and 3.22.
#   - The closest pair under dichromacy is Camel against Maroon at dE 18.1, just under a 20 floor. They
#     sit at opposite ends of the bar and never touch, so no boundary depends on it; it is about telling
#     them apart across the chart, and 18 is close enough to 20 to accept rather than spend a colour on.
#
# What the palette was chosen against, in case it is revisited: of the 10,626 four-colour sets in the
# library, 72 clear every floor at 0.8 and only 9 also hold a seam under dichromacy — and none of those
# 9 separates as strongly as this one, which reaches dE 56 on its weakest touching pair against 21-42
# for them. The trade taken here is that one boundary leans on hue. `ai/palette-options/` has the
# renders, and the search is reproducible from the palette in owid-grapher's `CustomSchemes.ts`.
#
# Which category carries which colour is NOT free, and that is the other half of the decision: the four
# segments differ hugely in width, so the most advancing colour must not land on the widest. Personal
# care is 44% of every bar, so it takes the recessive Midnight Blue; Light Teal, which reads as the most
# forward of the four, sits on Unpaid work at 11%. Reversing those two measures identically and looks
# markedly busier.
# `members` names what each category holds, in the source's own terms and ordered by the minutes each
# takes. Two of them are worth not "correcting" back to "Other": the OECD's 3.3 is labelled *personal,
# household, and medical services + travel related to personal care* — grooming and health, 61 min/day,
# 9% of personal care — and its 4.5, though labelled "other leisure activities", is games, hobbies, arts
# and crafts, reading and leisure travel per the country mappings on the workbook's activity sheet, and
# at 96 min/day it is the second-largest thing anyone does with their leisure. Only unpaid work keeps an
# "Other", and that one is ours: it holds this chart's fold of the source's fifth top-level category
# (religious, civic and unclassified time, 18 min/day) along with household travel and the care items.
MAIN_CATEGORY_GROUPS = [
    {
        "column": "main_paid_work_or_study",
        # The source's own code 1.
        "source_columns": ["paid_work_or_study"],
        "members": ["Paid work", "Commuting", "School or classes", "Homework"],
        "compact": True,
        "label": "Paid work or study",
        # Camel, with the library's darker Camel for the name: the fill measures 2.8:1 as text.
        "color": ("hex", "#bc8e5a", "#996d39"),
        "as_hours": True,
    },
    {
        "column": "main_personal_care",
        # The source's own code 3.
        "source_columns": ["personal_care"],
        "members": ["Sleep", "Eating & drinking", "Grooming & health"],
        "label": "Personal care",
        # Midnight Blue, legible as its own text at 13.9:1 — and recessive, which is why it takes
        # the widest segment.
        "color": ("hex", "#00295b"),
        "as_hours": True,
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
        # "Travel", not "Volunteering". Both are in here, but a five-name list has to name the five
        # biggest things, and across the 26 charted countries household travel is a median 17.3
        # min/day reported by 25 of them while volunteering is 3.0 min — the list was naming the
        # 7th-largest member and skipping the 4th. `check_members_are_the_largest` asserts it.
        "members": ["Housework", "Childcare", "Shopping", "Travel", "Other"],
        "compact": True,
        "as_hours": True,
        "label": "Unpaid work & other",
        # Light Teal, with the library's darker Light Teal for the name: the fill measures 2.6:1
        # as text. On the narrowest segment on purpose.
        "color": ("hex", "#58ac8c", "#2c8465"),
    },
    {
        "column": "main_leisure",
        # The source's own code 4.
        "source_columns": ["leisure"],
        "members": ["TV & radio", "Seeing friends", "Sports", "Events", "Hobbies & other"],
        "compact": True,
        "as_hours": True,
        "label": "Leisure",
        # Maroon, legible as its own text at 8.1:1.
        "color": ("hex", "#883039"),
    },
]

# Which source sub-activity each name in the key stands for, and which ones the key does not name.
# A five-name list is a selection, and the invariant is that it selects the *biggest* members: the key
# named volunteering (a median 3.0 min/day) while leaving household travel (17.3, reported by 25 of the
# 26 charted countries) unnamed, which `check_members_are_the_largest` now makes impossible to ship.
# `care_for_household_members` is deliberately absent: it is the parent of child and adult care, and
# counting it would double the branch.
KEY_MEMBER_COLUMNS = {
    "Paid work or study": {
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
            "Travel": "travel_related_to_household_activities",
        },
        "unnamed": [
            "adult_care",
            "care_for_non_household_members",
            "volunteering",
            "other_unpaid_work_activities",
            "religious_spiritual_and_civic_activities",
            "other_uncategorized_activities",
        ],
        # Whatever the category has left over, so it is not compared against the named members.
        "residual": "Other",
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

# One category per segment, so the header machinery names each segment instead of bracketing a run.
MAIN_CATEGORIES = [
    {
        "name": group["label"],
        "color": group["color"],
        "columns": [group["column"]],  # one drawn segment per category
        # What the segment holds, listed under its name — the same names the detailed chart sets inside
        # each bracket, so a reader moving between the two versions recognises them.
        "members": group["members"],
    }
    for group in MAIN_CATEGORY_GROUPS
]

TITLE = "How do people spend their time?"

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
# Grapher's own GRAPHER_AREA_OPACITY_DEFAULT = 0.8 (GrapherConstants.ts), which both the stacked and the
# discrete bar chart apply to every bar; grapher leaves its labels at 1, and so does this. It changes
# every measured number, because what a reader sees is the fill composited over the canvas rather than
# the library hex — see `composite_on_background`, which is what the label colours and the recorded
# seams are measured against.
#
# It also costs two contrast floors, and that is a deliberate trade rather than an oversight: matching
# grapher's stacked bars is worth more here than clearing them. Both shortfalls are pinned in
# `CONTRAST_ALLOWANCES`, so they are checked on every build instead of recorded in prose — the comment
# they replace claimed 0.9 would fix the labels, and it does not (Denim's best colour there is white at
# 4.44:1, still short of 4.5). Only full opacity clears everything.
SEGMENT_ALPHA = 0.8

# In-bar values are white on saturated fills and dark on light ones, whichever reads better *measured*
# against the fill — a luminance cutoff picked white on Dark Orange at 4.48:1 where the dark scores
# 4.68:1, and the 4.5:1 bar is exactly what these labels have to clear. The dark is Text/Gray 100, the
# style the frame binds them to, so the render and the frame agree.
DARK_VALUE_COLOR = "#2d2e2d"

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
# Both entries are colour-blindness only — see the palette notes above. Nothing here is a shortfall in
# what a full-colour or grayscale reader sees.
CONTRAST_ALLOWANCES = {
    ("cvd-seam", "Unpaid work & other", "Leisure"): 1.13,
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
#
# ACCEPTED DEVIATION: measured in the frame, Camel* and Light Teal* come to 4.43:1 at 13.19px regular,
# which is 0.07 under WCAG's 4.5 for body text — so `/create-figma-chart`'s label-contrast row fails on
# the ten legend names set in them. Accepted because the variant IS the design system's answer for text
# in a category colour, and the alternatives are worse: darkening past it leaves the library, and setting
# those names in the body gray drops the colour that ties each name to its segment. The other three
# measure 5.29 (Denim), 6.02 (Rusty Orange) and 6.59 (the country labels in Text/Gray 80).
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

# Horizontal breathing room, in template pixels: between a country label and its bar, and inside a
# segment around its value.
COUNTRY_LABEL_PAD = 8
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

# The share of a category name's top line box that its ink does NOT fill, so the band can be inset
# from the name's ink rather than from its box. It is what the FRAME measures — Figma's box for an
# imported text node — and NOT `cap_height_px`, which is a digit's ink at 0.72 of the size and
# over-reclaims by 3.7px. Measured 4.44px on a 14px name, with `verify_page.js`'s `gap` row as the
# instrument: it read 18.44px of air against a 14px inset. Re-measure it if the name's size changes.
CATEGORY_NAME_BOX_SLACK = 0.317

# Extra air between a category's name and the list under it, where the name sits directly on its own
# list rather than above a bracket rule. In lines of that list: half a line is enough to read as a
# heading, and a full line pushes the name far enough from its own list to start looking detached
# again — which is the problem this gap exists to solve.
CATEGORY_NAME_GAP_LINES = 0.5

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

# Bars fill this share of a row's pitch.
BAR_FRACTION = 0.8

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
LAYOUTS = {
    "time_use_by_country": {
        "template": "vertical",
        "size": (TEMPLATES["vertical"].width_px, TEMPLATES["vertical"].height_px),
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
            # It also removes the collision the previous generation had. There the logo sat at y=22.3
            # and ran to 57.5, into the subtitle's own line box at 51.2, so a subtitle wide enough to
            # reach x=770 printed under it — which the four-category subtitle did, at 801px. Now the
            # logo occupies exactly a title line plus the gap (16 to 51, h=35), so the subtitle clears
            # it by construction and needs no width reserved.
            "origin_y": 16,
            "logo_px": 0,
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
            "note_bottom_px": 1043.81,
            "source_y": 1047.81,
            "source_px": 12,
            "tagline_y": 1065.81,
            "tagline_slot_px": 467,
            "license_y": 1065.81,
            "license_px": 11,
        },
        # In whole template pixels, like the template's own slots. The four sizes step down from the
        # template's smallest body text (its Note and source rows are 12): the category names at 14,
        # their member lists at 13, the row labels at 12 and the in-bar values at 12.
        "country_font_px": 12,
        "value_font_px": 12,
        "header_font_px": 13,
        # The bold category name, one pixel up on the member list under it. It is a heading over a list,
        # and at the same size the only thing separating the two is the weight — a pixel is enough to
        # make the hierarchy read without the name starting to compete with the title. It feeds the wrap
        # and collision solver as well as the drawing, so a name that no longer fits its own segment at
        # this size gets wrapped (or, if a single word overhangs, fails `place_header_labels`).
        "category_font_px": 14,
        # Whether a value reads "4h 29m" rather than "4 hours 29 mins" when the long form does not fit.
        "with_mins_suffix": True,
        # Where each half of the header goes. The category brackets span the top row from above, and
        # each category's own member names are stacked inside its bracket, one per line
        # ("bracketed") — so the header reads category, then its members, then the data, with every
        # name over the run of bars it belongs to.
        # Both keys are asserted single-valued below: the other placements went with the other charts.
        "category_side": "above",
        "groups": MAIN_CATEGORY_GROUPS,
        "categories": MAIN_CATEGORIES,
        "group_labels": "bracketed",
        # "in a 24-hour day" earns its words: every bar is the same length, and nothing else on the
        # frame says that the length is a whole day rather than a scale someone chose. It costs the
        # "time-use" in "time-use surveys", which the title and the source line both carry — the line
        # is full at 1117px of a 818px slot before Lato's slack, so anything longer wraps.
        "subtitle": "Average hours and minutes in a 24-hour day, from surveys run between {years}, for people aged 15 to 64.",
        # With one segment per category, a name that overhangs its own bar reads as pointing at its
        # neighbour too — so wrap it rather than only wrapping to avoid a collision.
        "wrap_overhanging_names": True,
        # Hang each category's list off the bars rather than off the top of the band, so a short list
        # does not end two lines short of the plot.
        "names_bottom_aligned": True,
    },
    "time_use_by_country_mobile": {
        "template": "mobile",
        "size": (TEMPLATES["mobile"].width_px, TEMPLATES["mobile"].height_px),
        "margin": 16,
        "template_text": {
            # The 540-wide set's title node is 428 wide against a 508 content box, which is the orphan
            # guard the design team builds in — so the title is measured against 428, not against the
            # content. It fits on one line there, which puts the subtitle at 51 rather than the
            # placeholder's 80 and the band top at 89 rather than 118: 29px of chart, for free.
            "origin_y": 16,
            "logo_px": 0,
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
            "footer_top_px": 770,
            "source_y": 770,
            "source_px": 14,
            "license_y": 791,
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
            "note_bottom_px": 770,
        },
        # The same point sizes as the desktop frame, deliberately: on a frame 0.64x as wide they read
        # half again as large, which is the direction the template itself goes (its own source and
        # license rows are 14px against the desktop's 12 and 11). Every value still fits its segment on
        # 26 of 26 rows at this scale — the country column takes 120px of the 508, leaving 388px of plot
        # at 0.27px per minute, and the narrowest segment on the narrowest row still holds "2h 42m".
        "country_font_px": 12,
        "value_font_px": 12,
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
        # The same sentence as the desktop frame. It wraps to two lines at 508px rather than one, which
        # the mobile subtitle slot is exactly tall enough for.
        "subtitle": "Average hours and minutes in a 24-hour day, from surveys run between {years}, for people aged 15 to 64.",
    },
}


def run() -> None:
    """Load data, render and save the chart."""
    check_contrast()
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
        # `..._mobile.svg` against.
        export_frame(paths, fig, short_name, template=layout["template"])
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

    check_members_are_the_largest(tb)
    ages = {
        str(row["country"]): str(row["age_of_reference"])
        for _, row in tb.iterrows()
        if str(row["age_of_reference"]) != "15-64"
    }

    # What garden publishes, which this step takes as given. A layout tuned for 26 rows is not a layout
    # for 35: the row pitch, the value-label coverage and the header band all derive from the row
    # count, so a change to garden's cutoff should stop here to be looked at rather than be silently
    # redrawn.
    assert len(tb) == 26, f"Garden published {len(tb)} countries, not the 26 this layout is drawn for."
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
    """Cite the producer behind the chart from the origins, as `producer (year)`."""
    years: dict[str, list[str]] = {}
    for origin in tb[MAIN_CATEGORY_GROUPS[0]["source_columns"][0]].metadata.origins:
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

    # --- header: the template's title row, then its subtitle ---
    title = wrap_to_slot(TITLE, template["title_slot_px"], template["title_px"], PLAYFAIR_SLOT_SLACK)
    years = f"{tb['year'].min()} and {tb['year'].max()}"
    subtitle_text = layout["subtitle"].format(years=years)
    subtitle = wrap_to_slot(subtitle_text, template["subtitle_slot_px"], template["subtitle_px"])
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
    # A frame with no Note slot draws its Note at the foot of the chart band instead, and the footer is
    # told there is none.
    note = build_note(tb, ages, layout)
    band_note = note if layout.get("note_in_band") else None
    draw_footer(fig, None if band_note else note, source_citation, layout, fx, fy)

    # --- the chart band: the category header, then the bar rows ---
    country_labels = [
        f"{SHORT_COUNTRY_NAMES.get(country, country)} ({year})"
        for country, year in zip(tb["country"].tolist(), tb["year"].tolist())
    ]
    country_space_px = (
        max(text_width_px(label, font_pt(layout, "country_font_px")) for label in country_labels) + COUNTRY_LABEL_PAD
    )

    plot_left_px = margin_px + country_space_px
    plot_width_px = (width_px - margin_px) - plot_left_px
    px_per_min = plot_width_px / MINUTES_PER_DAY

    # Where the key goes: attached to the segments ("bracketed") or in its own block above them
    # ("rows"). Attached, it points at the row it touches, which is the top one.
    label_mode = layout["group_labels"]
    assert label_mode in {"bracketed", "rows"}, f"Unknown group_labels {label_mode!r}."
    category_at = layout.get("category_side")
    category_placements: list[dict] | None = None
    bracketed_blocks: list[dict] = []
    category_base_px = 0.0

    if label_mode == "bracketed":
        # Both halves of the header point at the top row: the category name over its segment, and its
        # member names stacked under it.
        assert category_at == "above", f"Unknown category_side {category_at!r}."
        top_spans = segment_spans(tb.iloc[0], px_per_min, layout["groups"])
        category_placements = solve_category_layout(top_spans, layout)

        # The member names drawn inside each bracket sit between the rule and the bars, so the rule
        # moves out by their height: category first, its own members under it, then the data.
        bracketed_blocks = layout_bracketed_names(top_spans, layout)
        collision = blocks_collide(bracketed_blocks, layout)
        assert not collision, (
            f"The names inside the {collision} brackets would touch. Their category spans this row too "
            f"narrowly to hold them."
        )
        deepest = max(len(block["lines"]) for block in bracketed_blocks)
        category_base_px = LEADER_GAP + deepest * line_px(font_pt(layout, "header_font_px"))

    def band_px(side: str) -> float:
        """The room the header needs on one side of the bars.

        This has to be what the header *draws*, term for term, or the difference is air: measured in the
        frame, the topmost ink sat 29.35px inside a band inset by 14, because this still reserved the
        `CATEGORY_GAP`, `LEADER_GAP` and `CATEGORY_TICK` that the bracket rule needed — and the rule went
        when each category became a single segment. The terms below are exactly the ones
        `draw_bracketed_names` and `draw_category_name` use: the deepest member list, then the gap under
        the heading, then the tallest name.
        """
        room = 0.0
        if category_at == side and category_placements is not None:
            # The tallest name decides the band: its row, plus however many lines it wrapped onto —
            # measured to its INK, not to its line boxes. `draw_category_name` sets each line
            # `va="bottom"`, so the top of the topmost line box is empty; reserving the whole box put
            # the frame's topmost ink 4.44px inside a band meant to be inset by 14, which
            # `verify_page.js`'s `gap` row reads as 18.44.
            name_pt = font_pt(layout, "category_font_px")
            tallest = max(
                placement["row"] * TIER_HEIGHT
                + len(placement["lines"]) * line_px(name_pt)
                - CATEGORY_NAME_BOX_SLACK * layout["category_font_px"]
                for placement in category_placements
            )
            name_gap = (
                CATEGORY_NAME_GAP_LINES * line_px(font_pt(layout, "header_font_px"))
                if layout.get("names_bottom_aligned")
                else 0.0
            )
            room = max(room, category_base_px + CATEGORY_LABEL_GAP + name_gap + tallest)
        return room

    if label_mode == "rows":
        # The key is its own block, so the room it needs is the block plus the air under it.
        header_px = legend_block_px(layout) + LEGEND_BLOCK_GAP
        below_px = 0.0
    else:
        header_px = band_px("above")
        below_px = band_px("below")

    # The plot sits between the subtitle's ink and the footer's, inset by BAND_INSET at each end.
    # Both are ink rather than frame edges: the footer frame already starts 16px above its Note, so
    # insetting from the frame would inset twice.
    band_top = subtitle_y + lines_in(subtitle) * template["subtitle_line_px"]
    band_bottom = (
        template["note_bottom_px"] - lines_in(note) * template["note_line_px"]
        if band_note is None
        else template["footer_top_px"]
    )
    # A Note with no slot of its own comes out of the band, so the bars stop above it.
    # The band's foot is BAND_INSET of air — unless a Note is drawn there, in which case the Note IS
    # the foot: it sits the footer's own row gap above the "Data source:" row, so the three read as
    # three evenly spaced footer rows, with NOTE_BAND_GAP of air above it holding it off the last bar.
    if band_note is None:
        note_top_px = None
        foot_px = BAND_INSET
    else:
        note_top_px = template["note_bottom_px"] - lines_in(band_note) * template["note_line_px"]
        foot_px = band_bottom - note_top_px + NOTE_BAND_GAP
    content_top_px = band_top + BAND_INSET
    chart_top_px = content_top_px + header_px
    # Where the last bar's INK has to stop — the same "measure the ink" rule as the header above. A bar
    # fills BAR_FRACTION of its row, centred, so it stops half an inter-bar gap short of the axes' own
    # bottom edge; insetting the axes instead left 17.01px of visible air against the 14 the top now
    # holds, which is the asymmetry `verify_page.js` reported.
    ink_bottom_px = band_bottom - below_px - foot_px
    # That slack is a share of the row PITCH, and the pitch is what is being solved for — so it is one
    # equation rather than an iteration: `n_rows` pitches reach the axes edge and `n_rows - bar_foot`
    # of them reach the ink.
    bar_foot_rows = (1 - BAR_FRACTION) / 2
    n_rows = len(tb)
    row_pitch_px = (ink_bottom_px - chart_top_px) / (n_rows - bar_foot_rows)
    chart_bottom_px = chart_top_px + n_rows * row_pitch_px

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
        layout,
        value_label_columns(tb, px_per_min, layout),
    )

    if label_mode == "rows":
        draw_row_legend(fig, layout, palette, content_top_px, fx, fy)
    else:
        assert category_placements is not None
        rows_out = rows_above if category_at == "above" else rows_below
        # Each category's own list depth, so a name can follow its list rather than sit at the band's top.
        block_depths = (
            {
                block["name"]: len(block["lines"]) * line_px(font_pt(layout, "header_font_px"))
                for block in bracketed_blocks
            }
            if layout.get("names_bottom_aligned")
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
        draw_bracketed_names(
            ax, bracketed_blocks, palette, px_per_min, rows_above, layout, category_base_px - LEADER_GAP
        )

    if band_note is not None:
        assert note_top_px is not None
        # `chart-note`, not `note`: the Figma pass drops the step's copies of the template's own text
        # slots BY PREFIX, and `note` is one of them. On this frame the Note is not a duplicate of a
        # template slot — the frame has no Note slot, which is why it is drawn here — so a name that
        # collides with that list would have it deleted on import, silently and only on mobile.
        draw_note(fig, margin_px, note_top_px, band_note, template, "chart-note", fx, fy)

    # Clipping is swept in `export_frame`, on the way out, which is what lets the labels drawn
    # outside the axes box survive into the SVG whole.
    return fig


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
                if value_label(group, minutes, index, minutes * px_per_min, layout)
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


def draw_bars(
    ax,
    tb: Table,
    country_labels: list[str],
    group_colors: dict,
    px_per_min: float,
    row_pitch_px: float,
    layout: dict,
    value_columns: dict[str, int],
) -> None:
    """One stacked row per country: the country label and its four segments."""

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
            row + baseline(font_pt(layout, "country_font_px")),
            country_labels[row],
            ha="right",
            va="baseline",
            fontsize=font_pt(layout, "country_font_px"),
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
                value_label(group, minutes, value_columns[column], minutes * px_per_min, layout)
                if column in value_columns
                else None
            )
            if label:
                ax.text(
                    left + minutes / 2,
                    row + baseline(font_pt(layout, "value_font_px")),
                    label,
                    ha="center",
                    va="baseline",
                    fontsize=font_pt(layout, "value_font_px"),
                    color=value_label_color(composite_on_background(color)),
                    gid=f"{slug}__{slugify(column)}-value",
                )
            left += minutes


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
        # Each category is a single segment, so its name stands over the run it names on its own: no
        # bracket, no stem, no rule. (Both were drawn here while the ten-group version existed, where
        # a category spanned several segments and needed a span mark to group them.)
        label_px = rule_px + CATEGORY_LABEL_GAP + placement["row"] * TIER_HEIGHT
        depth_px = block_depths.get(placement["name"]) if block_depths else None
        draw_category_name(ax, placement, palette, px_per_min, rows_out, layout, side, label_px, depth_px)


def draw_category_name(
    ax, placement, palette, px_per_min, rows_out, layout, side: str, label_px: float, depth_px: float | None = None
) -> None:
    """A category's name, stacked away from the bars, with no rule under it.

    `depth_px` is how deep this category's own list reaches. Passed, the name sits directly above that
    list rather than at the band's top, so a short list keeps its heading and the four names end up
    bottom-aligned as a group.
    """
    if depth_px is not None:
        label_px = depth_px + CATEGORY_LABEL_GAP + CATEGORY_NAME_GAP_LINES * line_px(font_pt(layout, "header_font_px"))
    for index, line in enumerate(placement["lines"]):
        offset = len(placement["lines"]) - 1 - index if side == "above" else index
        ax.text(
            placement["center"] / px_per_min,
            rows_out(label_px + offset * line_px(font_pt(layout, "category_font_px"))),
            line,
            ha="center",
            va="bottom" if side == "above" else "top",
            fontsize=font_pt(layout, "category_font_px"),
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
    fontsize = font_pt(layout, "header_font_px")
    for block in blocks:
        start, end = block["span"]
        centre = (start + end) / 2
        # Top-aligned, every list starts level with the deepest one and a short list ends well short of
        # the bars — two lines of air under "Personal care" while "Leisure" reaches down to them.
        # Bottom-aligned, every list ends the same distance above the bars and the air moves under the
        # category name, where it reads as space beneath a heading instead of a detached list.
        #
        # Putting the four category NAMES on one baseline instead — bottom-aligned lists, one header
        # row above them — was tried and dropped. It does tidy the top of the band, where the names
        # otherwise land on four baselines up to 51px apart (the blank above the first line of text is
        # 26px over "Unpaid work & other" and 77px over "Personal care", the widest and so emptiest
        # column). But it re-opens the gap this bottom-alignment exists to close: with a 3-name list,
        # "Personal care" ends up two lines clear of "Sleep" and stops reading as its heading. It buys
        # no height either — `band_px` sizes the band from the deepest list plus the tallest name, which
        # neither choice changes, so it only ever moved names through space already reserved.
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
        members = ", ".join(group["members"])
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


def legend_block_px(layout: dict) -> float:
    """How tall the row legend is: one line per category."""
    return len(layout["groups"]) * line_px(font_pt(layout, "legend_font_px"))


def draw_row_legend(fig, layout: dict, palette, top_px: float, fx, fy) -> None:
    """The key as a block of rows above the plot: a colour chip, the category's name, what it holds.

    The desktop frame hangs each name over the run of bars it belongs to, which is the better reading
    and is why that frame keeps it — the key is where the eye already is. It needs the segment to be
    wide enough to hold the words, though, and this frame's 508px of content does not give it: the
    narrowest category spans 56px against a 105px name, so all four names would overhang the bars they
    name by more than they cover, and a name that overhangs points at its neighbour too. The chip
    carries the link to the bar instead, at the bar's own fill and opacity.

    Drawn in figure coordinates rather than on the axes, because unlike the bracketed header it is
    positioned by the frame's margin rather than by any segment.
    """
    fontsize = font_pt(layout, "legend_font_px")
    width_px, height_px = layout["size"]
    line = line_px(fontsize)
    for index, row in enumerate(layout_row_legend(layout)):
        group = row["group"]
        centre_px = top_px + index * line + line / 2
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
        # Name and members at one size, separated by weight alone. On the desktop frame the name is a
        # point larger, because there it is a heading on a line of its own above its list and the
        # weight by itself was not enough to make the hierarchy read. Inline, immediately before the
        # list, it is.
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
                color=color,
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
# Header layout
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
        width = max(text_width_px(line, font_pt(layout, "category_font_px"), bold=True) for line in lines)
        center = min(max((start + end) / 2, width / 2), right_edge - width / 2)
        return center, (center - width / 2, center + width / 2)

    placed: list[tuple[tuple[float, float], int]] = []
    placements = []
    for index, category in enumerate(layout["categories"]):
        variants = category_variants(category["name"], layout)
        if layout.get("wrap_overhanging_names"):
            # Collision with a neighbour is not the only reason to wrap. Where each category is a
            # single segment, a name can clear its neighbours and still overhang its own bar by half
            # its length, which reads as pointing at both. So rank the forms by how far each one
            # overhangs the span it names, least first; `sorted` is stable, so the widest-first order
            # survives among forms that overhang equally (every form that fits is 0).
            #
            # By how MUCH, not whether it fits: a boolean key ties whenever no form fits, and a tie
            # hands the row to the widest form, which is the worst one. "Unpaid work & other" over a
            # 79px segment is the case — one line overhangs by 63px, two by 8px — and it wrapped only
            # for as long as the two-line form happened to fit, which it stopped doing when this name
            # went up a pixel.
            span_px = spans[category["columns"][-1]][1] - spans[category["columns"][0]][0]
            variants = sorted(
                variants,
                key=lambda lines: max(
                    0.0,
                    max(text_width_px(line, font_pt(layout, "category_font_px"), bold=True) for line in lines)
                    - span_px,
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

        placed.append((geometry(category, placement["lines"])[1], placement["row"]))
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
    # Say which names still reach past the segment they name, and by how much. There is no threshold
    # worth asserting — "Unpaid work & other" legitimately overhangs its 79px segment by 8 at the size
    # this chart sets — but the one form that reads as pointing at two segments at once got chosen
    # silently before the ranking above was fixed, and a font change is exactly when that recurs.
    for placement in placements:
        start, end = placement["bracket"]
        widest = max(text_width_px(line, font_pt(layout, "category_font_px"), bold=True) for line in placement["lines"])
        if widest > end - start:
            paths.log.info(
                f"Category name overhangs its segment: {placement['name']} is {widest:.1f}px over a "
                f"{end - start:.1f}px span, on {len(placement['lines'])} line(s)."
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
                text_width_px(" ".join(words[:i]), font_pt(layout, "category_font_px"), bold=True)
                - text_width_px(" ".join(words[i:]), font_pt(layout, "category_font_px"), bold=True)
            ),
        )
        variants.append([" ".join(words[:best]), " ".join(words[best:])])
    return variants


def layout_bracketed_names(spans: dict, layout: dict) -> list[dict]:
    """Each category's member names laid out inside its own bracket's span, one per line.

    All four brackets stack their names, rather than setting the ones that would fit on a single line
    horizontally: a mixture reads as four different treatments, and only the two widest brackets
    could have taken a row anyway. A name wider than its span is wrapped to it, so a block never
    spills into the neighbouring category's; `blocks_collide` checks what is left.

    Returns one block per category: lines of (text, group) runs, and the span to centre them in.
    """
    fontsize = font_pt(layout, "header_font_px")
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
            for text in wrap_to_width(group["label"], end - start, fontsize).split("\n")
        ]
        blocks.append({"name": category["name"], "lines": lines, "span": (start, end)})
    return blocks


def blocks_collide(blocks: list[dict], layout: dict) -> str | None:
    """The first pair of neighbouring name blocks that touch, or None if they all clear each other."""
    extents = []
    for block in blocks:
        start, end = block["span"]
        widest = max(
            sum(text_advance_px(text, font_pt(layout, "header_font_px")) for text, _ in line) for line in block["lines"]
        )
        centre = (start + end) / 2
        extents.append((block["name"], (centre - widest / 2, centre + widest / 2)))
    for (left_name, left), (right_name, right) in zip(extents, extents[1:]):
        if overlaps(left, right, HEADER_MIN_GAP):
            return f"{left_name} and {right_name}"
    return None


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
    # same gap has to survive dichromacy, which redistributes lightness. Checking only the first would
    # have passed a boundary that falls to 1.13:1 for a red-green colour-blind reader.
    for (fill, name), (next_fill, next_name) in zip(zip(composited, names), list(zip(composited, names))[1:]):
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
    return fit_text(candidates[form : form + 1], available_px, font_pt(layout, "value_font_px"))


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
    # Only the exceptions, because the subtitle already says 15 to 64 and the Note has two lines to
    # spend on things said nowhere else. With the 2010 cutoff there are no exceptions left, so this is
    # normally empty — it exists for a release that adds an older survey back.
    ages_sentence = f"Estimates cover people aged 15 to 64, except in {exceptions}. " if exceptions else ""
    # "in the OECD database", not "most recent survey": the OECD does not refresh a country the moment
    # it runs a new survey, and Korea is the standing example — shown here at 2014 while Statistics
    # Korea has published 2019 and 2024 rounds. And state the cutoff as what the chart covers rather
    # than as countries it excludes: the reader is being told the scope of what they are looking at,
    # and a chart does not owe them a list of what is missing from it. It still rules out reading
    # "each country" as all 35 rather than the 26 garden publishes. The year comes off the data, not
    # off a constant here: garden owns the cutoff, and reading it back means the Note cannot claim a
    # span the rows do not have.
    text = "Note: The chart covers every country whose most recent survey in the OECD database is from "
    text += f"{int(tb['year'].min())} onwards; that survey's year is in brackets. "
    text += ages_sentence
    # The Note's two lines go to the caveats that can move a bar a reader is looking at, and only those.
    #
    # Which rules out the one this Note used to carry — that a country not reporting an activity
    # separately keeps those minutes inside the same category. True, and irrelevant here: each of the
    # four segments is one of the OECD's own top-level category totals, computed as a remainder that
    # comes out equal to the source's own number for all 26 countries (checked: the largest difference
    # is 1.5e-05 minutes, which is float32). So where a country drew its sub-activity coding lines
    # cannot change a value this chart draws. It belongs on the ten-group indicators, where the finer
    # split IS on show, and it is in their `description_key` in garden.
    #
    # What is left is the two countries whose top-level totals are not like the others'. Mexico's whole
    # instrument differs, which the OECD flags. Japan's category 5 absorbs its non-commuting travel,
    # which every other country reports inside personal care and leisure, so Japan's minutes move
    # BETWEEN drawn segments — the only caveat here that does.
    text += (
        "The OECD flags Mexico's estimates as not fully comparable, because it measures time use "
        "differently. In Japan, travel other than commuting counts as unpaid work."
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
