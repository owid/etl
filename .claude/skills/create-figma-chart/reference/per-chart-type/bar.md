# Bar / stacked bar charts

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

**One topic in the source, two pages in the archive — read the half you need.** The Cheat Sheets ship a single *“Bar/Stacked Bar Chart Guidelines”* sheet, which is why this subsection is not split. But the archive files them separately (Bar Charts `1:3`, 52 charts; Stacked Bar `1:6`, 24) and they diverge in practice, because a plain bar chart is about **ranking** and a stacked one about **composition** — almost nothing transfers. Below: shared bullets first, then *Ranked bars*, then *On **stacked** bars specifically*.
- **Spell out the unit in the chart area**, not only in the subtitle.
- Values to the right of the bars by default; inside the bars when space is tight (contrast + ≥12px rules).
- Highlight the entity the DI is about with a different bar color; mute the rest.
- Group bars meaningfully (delineate blocks) when it helps reading.
- Don't cram many entities into a limited height — stretch the frame vertically (GUIDELINES.md → Direct labeling).
- Legend: squares only; move it into empty chart space if there is some. On a **100% stacked** bar, prefer no legend at all — colored category labels above the reference row, tiered and wrapped, with a thin arrow onto any segment too narrow to sit under its own label (SKILL.md → Step 8). That is what the finished pages do, and it holds for six categories, not just three.

**Ranked bars — everything from here to the stacked-bar paragraph assumes a plain ranked chart.**

**Value labels go inside the bar when it is long enough and outside when it is not — decided per bar, within one chart.** `1:4005`, `1:4366`, `364:129`, `644:564`, `545:78`. And **spell the unit out on the longest bar only**, then use bare numbers: `1:4005` runs "23.5 deaths/100,000 people" on the top bar and `12.4`, `11.2` beneath. In a very long bar the label can absorb the whole sentence — `160:41`: "97% of all countries had universal voting rights".

- **A dashed separator plus a recolored top group marks "the ones that qualify"** — `169:1167`, `203:108`, `342:141`. A **dotted threshold line labeled in place** does the same job against a stated target — `676:351` ("UN's target: 0.7% of national income"), with the qualifying bars recolored and the title's "Four" in that color.
- **A tinted band behind a run of rows** highlights a group without recoloring its bars — `341:82`.
- **Bar color can encode the bar's own value**, with labeled dashed rules at the thresholds — `54:73`.
- **Or color bars by narrative role rather than category** — `137:341` (gray "before", orange pivot year, green "after", each group annotated in its own color).
- **Diverging change charts color by sign and put the signed value outside in the bar's color** — `584:45` (`+636 TWh` green, `−67` orange).
- **Butterfly bars around a central category column** are the house form for two quantities per entity: a colored header per side matching the title's colored words, a directional arrow, values inside the long bars and outside the short ones, and italics for non-numeric entries. `545:78`, `575:114`, `59:294`, `178:319` (pictograms as the central axis).
- **Two stacked panels sharing an x-axis, each with its own colored panel title**, is the answer to a second y-axis — `59:232`.
- **A two-bar comparison puts the value *above* each bar, in that bar's own color**, with a multi-line bold category label beneath and no axis at all — `243:29`, `246:31`, `513:177`, `178:347`. Four witnesses. It is the form for "X versus Y" where the ratio is the whole point; a shared axis would only invite reading precision the chart isn't making a claim about.
- **A dashed red box around one bar marks an exception**, with an asterisked footnote — `57:578`.
- **Flag icons beside country labels** on league tables — `21:447`, `80:944`, `149:15`, `258:1017`, `258:1551`, `531:703`, `163:511`.
- Taller frames carry long tables: 540×824 (`137:528`), 540×795 (`201:164`), 540×753 (`169:1167`).

On **stacked** bars specifically: label only the segment the title is about (`135:1424`, `330:323`); a category that never gets a header can be named inside the one bar where it is widest (`596:346`); totals sit outside the bar in dark gray past the last segment (`323:119`, `283:316`, `27:1027`); an ordinal category axis gets an explicit direction cue (`269:77`, "Richer countries →"); and one bar among plain ones may be stacked to break out the comparison (`596:405`). A 100%-stacked bar can also be turned on its side into a **labeled list** — `378:104`, one full-width row per category, name and value in white inside, an explanatory sentence to the right in that segment's color.

Exemplars: `364:129` (reference row), `169:1167` (braced ties + qualifying group), `545:78` (butterfly), `676:351` (threshold line), `687:203` (tiered headers over a reference row), `378:104` (stacked bar as a list).

## What a grapher bar-chart import needs fixing, measured

- **The value labels come in ~5px too high, because their box TOP is aligned to the bar's top.** On a
  discrete-bar import every `value-labels` child sat with its box top equal to its bar's top, so a 16px
  label inside a 27.4px row read 5.04–5.09px above centre on all nine rows. Grapher's *entity* labels
  are fine (−0.65px from the bar's centre, which is optical centring for cap height), so the fix is to
  align each value label to its **paired entity label**, not to the bar's geometric centre — matching
  the bar centre instead would leave the two texts in a row 0.65px apart, which is what a reader sees.
  Pair them by nearest vertical centre; the boxes are the same height, so it is a pure `y` move, the
  chart's own box does not change, and no refit is needed.
- **A stacked bar's value labels are NOT the same defect.** They measured −1.29px on all thirty, and a
  single consistent offset across every label is grapher centring deliberately, not a bug. Leave them.
  The tell is the *spread*: nine identical offsets of ~5.05 in the opposite direction to the entity
  labels' is a misalignment; thirty identical offsets of −1.29 that match the entity labels is a choice.
- **A single-entity stacked discrete bar is the wrong exemplar, not a geometry problem.** With one row,
  grapher draws the zero line at **1.54× the bar height** — the same ratio at every canvas size tested
  (imHeight 1000/700/520/420), so no re-export changes it, and the canvas saturates at 1010×505 so a
  shorter request returns the same SVG. The fix is the entity selection: with six countries the bars
  fill the plot and the proportion disappears. Reach for `country=` before reaching for the geometry.

## Shortening long entity names, and reclaiming the space properly

The names sit in a column on the left whose width is set by the **longest** one. That single fact
decides whether shortening is worth anything: renaming `United States` → `US` and `United Kingdom` →
`UK` on a chart whose widest name is `Dominican Republic` frees **0px** of plot, because the column does
not move. Measure before offering it — see [SKILL.md](../../SKILL.md) → Step 4 for the wording.

When it *is* worth doing, the re-layout is four steps and the third is the one that matters:

1. Rename, loading every segment's font first (`getStyledTextSegments(["fontName"])` — a text edit on
   an unloaded font throws), and switch a `NONE`/`HEIGHT` box to `WIDTH_AND_HEIGHT` so it hugs.
2. Take the new column width from the longest label and right-align every label to it. Keep grapher's
   own gap between the column and the zero line rather than inventing one.
3. Move the zero line, then **stretch the bars from it by one factor** —
   `newEnd = Z1 + (oldEnd − Z0) × (maxBarEnd − Z1) / (maxBarEnd − Z0)`. Every bar starts at the zero
   line and its length is proportional to its value, so scaling all of them from that shared origin
   preserves the mapping exactly: measured `maxRatioDrift: 0`. **Assert that**, because a bar chart
   whose lengths no longer match its numbers is the worst thing this skill could ship. And check first
   that the chart has no x-axis ticks — if it has, they must move with the bars or they will lie.
4. Move each value label by **its own bar's** end delta, so it keeps its gap. Never scale it.

The counter-intuitive part of step 3: most bar *ends* move LEFT while every bar gets LONGER, because the
origin moves left further than the bar grows. On the measured run the factor was 1.079 and India's bar
went 171.5 → 185.0px while its end went 312.5 → 299.2. Reading the ends alone looks like the chart
shrank; read the lengths.
