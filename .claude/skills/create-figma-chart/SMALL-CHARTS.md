# Small and pull charts — the 302-wide format

A compact chart image that sits inside an OWID article and points at a grapher view. Two flavors,
two templates, one export route. Read this alongside [GUIDELINES.md](GUIDELINES.md), which owns
everything shared with the larger formats — colors, annotations, per-chart-type conventions, the Good
Data Viz Checklist. This file only covers what is different at 302px.

**Last verified: 2026-08-13.** Re-verify the template geometry at the start of every run
(`get_metadata` on `798:54`); the design team edits these frames in place.

## The two vocabularies

The design team's names and the code's names diverged, and the code's older name has been deleted.
Get this straight before searching for anything:

| Design team says | gdoc block | Source row | Who supplies the context |
|---|---|---|---|
| "small chart" | a **`chart-rows`** row item | **none** | the block's own `kicker` / `title` / `source`, or the live grapher it drives |
| "pull chart" | a **`pull-chart`** block | **mandatory** | nothing — it stands alone |

- **`small-chart` is not a block type.** It was split into `chart-rows` + `pull-chart` and deleted
  (`b7c2e0f344`, 2026-04-03). Grepping for it finds nothing; don't conclude the feature is missing.
- **A "guided chart" is not one of these images.** `guided-chart` is a *live interactive* grapher
  whose surrounding prose carries links that mutate it in place
  (`site/gdocs/components/GuidedChart.tsx`). A `chart-rows` block placed **inside** a guided chart
  turns its thumbnails into buttons that drive that live chart — which is why the design file's
  section is titled "featured on the OWID website as guided and PULL charts". The image is the
  thumbnail; the guided chart is what it drives.
- The **"More views of this data"** heading is just `chart-rows`' default kicker
  (`ChartRows.tsx:82`).

Both render through `site/gdocs/components/ChartThumbnail.tsx`, which adds a 1px border and a
"Click to explore" hover overlay.

## Pull ≠ guided, and the source row is the symptom not the difference

The two templates differ by one text node, so it is tempting to clone either and add or remove a
source. Don't — **clone the right template**, because the reason there are two is editorial.

A **pull chart** can be dropped anywhere in an article, so it carries its own attribution and its
title and subtitle have to make sense with no surrounding narrative. A **chart-rows thumbnail** is
always framed by something that supplies the context, so its text can lean on that and its
attribution is already declared elsewhere.

The block schemas settle it rather than merely suggesting it:

- `EnrichedBlockChartRows` carries `kicker`, `title` **and `source`** at block level — attribution is
  declared once for the whole block, so a row image repeating it would duplicate it.
- `EnrichedBlockPullChart` is `{ type, align?, image, url, content }` — **no `source` field at all**.
  If the image doesn't carry the source, the pull chart has none.

So the pull chart's source is not a stylistic flourish and must not be dropped to buy 13px of plot;
and adding one to a `chart-rows` thumbnail duplicates the block's own line.

## The two templates

Page ` 📑 Templates` (`798:54`) of `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`, under the
heading `"SMALL" Charts (featured on the OWID website as guided and PULL charts)` (`25344:1235`).

| Template | Node | Ships at | Text slots |
|---|---|---|---|
| `small-chart-template-guided` | `25344:1357` | 302×233 | title `25344:1378`, optional subtitle `25344:1379` |
| `small-chart-template-pull` | `25344:1391` | 302×233 | title `25344:1396`, optional subtitle `25344:1397`, **source `25344:1398`** |

### The background is not where you expect it

Both templates ship their frame fill as **white with `visible: false`**, and paint the background from
a `Group > Group > Vector` — a white **302×233 rectangle** — instead. So:

- **Do not just delete that group.** It is the only thing painting the background; remove it and the
  frame is transparent, which shows up immediately as charts with no white card behind them.
- **But do not keep it either**, because it is a *fixed* 302×233 rectangle and this format's height is
  free. It under-covers a 272-tall frame and overhangs a 221-tall one.
- **Enable the frame's own fill and drop the vector group**: `frame.fills = frame.fills.map(f => ({...f, visible: true}))`.
  A fill follows the frame at any height, and it is what the designer's finished examples use — every
  one of them has the frame fill visible and no full-size background vector.

There is **no z-order hazard to avoid here.** `appendChild` puts the imported chart last, so it draws
above the background whatever the background is. The "an opaque background paints over the template"
warning belongs to the `static_viz` local-SVG route, where the opaque patch is *inside the imported
SVG* — a different problem with a different fix (SKILL.md Step 7).

### Measured spec

| Element | Font | Size | Color | Style |
|---|---|---|---|---|
| Title | Playfair Display **Bold** | 16px / 19px line height | `#2d2e2d` | `Data Insights/Title` |
| Subtitle (optional) | Lato Regular | 11px | `#5b5b5b` | `Data Insights/Subtitle` |
| Source (pull only) | Lato Regular | 11px | `#858585` | `Data Insights/Source` |
| In-plot entity labels | Lato **Bold** | 11–12px | the series color | bound palette style |
| In-plot value labels | Lato **Medium** | 11–12px | the series color, or `#2d2e2d` on a bar | — |
| Axis year labels | Lato Regular | 11px | `#5b5b5b` | `Data Insights/Subtitle` |

Geometry, all frame-local:

- Frame fill **white** — not the static templates' cream `#fffbf5`.
- Content `x=12`, width **278**. Side margins are 12px, not the 16px of the 540-wide frames.
- Header block `Frame 7` at `y=10`; title 19 + 2px gap + subtitle 13 = 34 tall, so
  `headerBottom = 44`. With no subtitle it is 19 tall and `headerBottom = 29`.
- The header block **hugs its own text width** (206–278px across the examples). Unlike the 540-wide
  templates, the plot may therefore legitimately rise into the space to its right — that is a design
  decision per chart, not a misfit.
- Source (pull) sits at `y = H − 23`, height 13, leaving a 10px bottom margin. In the 233-tall
  template that reads `y=210`, but **derive it from `H`** — see below.
- Band available to the chart: `44 → H − 10` (guided) or `44 → H − 23` (pull).

Palette fills are **bound library styles** (`Default Palette/Midnight Blue #00295B`,
`Default Palette/Rusty Orange #B13507` in the examples). GUIDELINES.md → Colors applies unchanged,
including the `color_audit.py` pass.

`Data Insights/Source` `#858585` is a paint style the rest of this skill doesn't otherwise use.

### The source string

Bare `Producer (Year)` — e.g. `Luxembourg Income Study (2026)`. **No `Data source:` prefix**: the
template's placeholder is `[Data source (YYYY)]`, a fill-in-the-blank, and the earlier version of it
spelled the rule out as *"an optional source, without the `Data source:`"*. This is the one place in
this skill where the source line is *not* the verbatim grapher footer string, so don't reuse the
Step 6 rule that forbids re-deriving it — take the producer and the release year from
`chart.citation` and drop the prefix.

## Reference renders

The design team's five worked examples lived on the Templates page and are expected to be deleted
once this route reproduces them. They are harvested into `assets/` so this file stands on its own —
**there are deliberately no live node ids in the run-time path.** They were drawn in matplotlib, so
they are the visual target, not an artifact of this pipeline; SKILL.md's *"pointed at a finished page
as the model"* mode applies for reading measurements off them, but not its re-render-and-diff half.

| Asset | Flavor | What it teaches |
|---|---|---|
| `assets/small-chart-example-line-two-series.png` | guided | two series, dots **and** value labels at both ends, entity label along each line, title + subtitle |
| `assets/small-chart-example-lines-indexed.png` | guided | three series of indexed % change, end dots and end values only, two-line title, no subtitle |
| `assets/small-chart-example-slope-thresholds.png` | guided | three bands between two time points, labels at both ends, shortest frame (221) |
| `assets/small-chart-example-bar.png` | guided | 7-row ranked bar, entity name over year in a left label column, tallest frame (272), no axis at all |
| `assets/small-chart-example-line-pull.png` | **pull** | the only pull example — line, four axis ticks with 4px tick marks, source row |

Two things to read off them rather than from prose: the axis treatment is **not** uniform (the pull
example carries four labeled ticks with 4px marks; the others carry only the first and last year on a
bare baseline), and the frame height moves with the content (221 / 233 / 234 / 272).

## Width is fixed; height is free

**302 is the only fixed dimension** — it is `.chart-rows__chart`'s 300px slot plus `ChartThumbnail`'s
1px border each side. The templates ship at 233 tall, but that is a starting point, not a target.

This inverts what every other template in this skill assumes. Elsewhere the frame is fixed and the
chart is fitted into a measured band (SKILL.md Steps 3 and 7). Here **the frame is an output**:

1. Decide the content — how many series or rows, axis or no axis. This is where a small chart is won;
   see GUIDELINES.md and the narrative the image serves.
2. Pick the plot height. Calibration from the examples: a 2–3 series line chart sits at 150–170px of
   plot; a ranked bar chart is `rows × ~30px` of row pitch.
3. `H = 44 (header) + gap + plotHeight + gap + (23 with a source row | 10 without)`.
4. Export at that height (below). No aspect-ratio arithmetic.
5. Resize the clone to `H`, and on a pull clone move the source row to `y = H − 23`.

A consequence worth stating because it will be reached for: **an aspect-ratio check on a small chart
is meaningless.** Only the width is a target. Don't add a `"small"` entry to
`create-static-viz/scripts/verify_static_viz.py`'s `TEMPLATE_RATIOS` — wrong pipeline, and a ratio
test on a free-height frame.

## Sourcing the view

The image points at a grapher view and, inside a guided chart, *drives the live chart to that view* —
so the geometry has to be an export of the view the `url:` selects, not an independently drawn chart
that is free to disagree with it.

| Input | SVG endpoint |
|---|---|
| Chart slug | `https://ourworldindata.org/grapher/<slug>.svg?<view params>` |
| MDim view | the same — the dimension params select the view |
| Narrative chart / unpublished draft | `https://ourworldindata.org/grapher/by-uuid/<configId>.svg` |
| **Explorer view** | `https://ourworldindata.org/explorers/<slug>.svg?<view params>` |

The explorer endpoint is `EXPLORER_DYNAMIC_THUMBNAIL_URL` (`settings/clientSettings.ts:40`) and is
new to this skill — the Step 1 table has no explorer row for any other format. Texts come from
`.metadata.json` with the same params, per SKILL.md Step 1.

**A missing view param fails silently, and it is the trap on this route.** Every one of these returns
HTTP 200 whatever you pass:

- An explorer requested with no view params came back with **two texts** — `1900`, `2020` — an axis
  and nothing else.
- An MDim slug with no params rendered its *default* view, which for `energy-mix` is a **map**
  (`No data`, `0 TWh`, … `20,000 TWh`).
- **A dimension takes one value, and an invalid set renders *nothing*.** `quantile=richest_1pct` gives
  one series; `quantile=richest_1pct~richest_0_1pct` and the comma form both return an **empty SVG —
  zero text nodes — at HTTP 200**, and a repeated `quantile=` param is last-wins. Nothing in the
  response says so. See the composition note below for what to do about it.
- **A `tab=` that the wrong slug can't honor degrades silently.** `tab=discrete-bar&time=latest` on
  one MDim came back as dots on a time axis — one point per country, no bars, no names — while the
  *same* params on the right slug produced a proper ranked bar with all seven names. The skill already
  notes that `tab=table` is silently ignored; treat every `tab=` as a request, and check the render
  rather than the URL before concluding anything about the route.

So carry the view's full param set, and before building anything **assert the text count and the
rendered tab against the view you asked for.**

> **Don't count texts with `grep -c`.** These SVGs are frequently a single line, so `grep -c '<text'`
> returns `1` for a chart with fourteen labels and reads exactly like the empty render you are testing
> for. Extract them instead:
>
> ```bash
> .venv/bin/python -c "import re,html,sys; s=open(sys.argv[1]).read(); \
>   print([html.unescape(re.sub('<[^>]*>','',m)).strip() for m in re.findall(r'<text.*?</text>',s,re.S)])" chart.svg
> ```
>
> A healthy 7-row bar thumbnail prints 14 strings — seven values and seven entity names.

## The export: `imType=thumbnail`

This is the same render mode the search results use
([`ourworldindata.org/search`](https://ourworldindata.org/search)), and the design of these charts is
deliberately close to those thumbnails. `constructPreviewUrl` (`site/search/searchUtils.tsx:301`)
builds it; `getThumbnailOptions` (`functions/_common/imageOptions.ts:70`) resolves it to
`variant: GrapherVariant.Thumbnail`, documented in grapher's own types as *"Simplified rendering,
suitable for thumbnails. Less noisy visualization, but should be understandable on its own"*
(`GrapherTypes.ts:851`). Dedicated renderers sit behind it: `LineChartThumbnail`,
`SlopeChartThumbnail`, `StackedAreaChartThumbnail`, `MarimekkoChartThumbnail`.

### It strips the furniture for you — but it does not label the series

Same chart, same size, two routes:

| Route | Texts emitted |
|---|---|
| `imType=uncaptioned` | 19 — `1880 · 1900 · 1920 · 1940 · 1960 · 1980 · 2000 · 2023`, `0 years` … `80 years`, `United States`, `China` |
| **`imType=thumbnail`** | **8** — `1880 · 2023`, `United States`, `China`, `79.3` |

So the y-axis, the interior year ticks and the legend are gone, and you get the first and last year
plus an end value. That much is dependable.

**The series labels are where it varies, so check them per chart rather than assuming.** Measured
across the five reference charts:

| Chart | Series labeled |
|---|---|
| Ranked discrete bar, 7 countries | **7 of 7**, each with its own observation year |
| Two-country line chart | 1 of 2 |
| Single-series line chart | 1 of 1 |
| Three-series MDim (levels) | 2 of 3 |
| The same MDim with `stackMode=relative` | **0 of 3** |

A discrete bar labels every row; a line chart labels some and drops the rest; `stackMode=relative`
suppresses them entirely. The last case is not collision avoidance you can tune out — the labels
stayed absent at `imFontSize` 12, 14, 15 and 16 and at frame heights of 180, 250 and 300px.

Two omissions are consistent across every type: **only end values are labeled**, never the values at
the start of a line (the reference charts label both ends), and an MDim's series names arrive in their
raw form (`World - Richest decile`) rather than the reference's `Richest decile`. Grapher also folds a
per-row year into the value rather than stacking it — `25.8% in 2022` where the reference sets
`Brazil` over `2022` in the label column.

So the thumbnail route reliably buys you the *stripping* — no legend to delete, no axis to remove, no
rescale. **Budget for re-adding the missing series labels and rewriting the raw names in Figma**;
GUIDELINES.md → Direct labeling has the placement rules and the reference renders in `assets/` show
the target. How much of that work there is depends on the chart type, so measure the export before
estimating.

### Request the final pixel size directly

`getThumbnailOptions` sets `staticBounds = Bounds(0, 0, imWidth / 4, imHeight / 4)`, so `imWidth`
gives you the SVG **canvas** width exactly: `imWidth=1208&imHeight=664` returns
`viewBox="0 0 302 166"`. Defaults with neither param are 1200×640 → a 300×160 SVG.

**But target the content width, not the frame width — grapher insets the drawing inside the canvas.**
Measured at `imFontSize=16`: a 302-wide canvas puts its ink at x 7.2 … 294.2, i.e. **~7.2px of padding
per side**, which lands outside the template's 12 … 290 content box at both ends. Asking for the frame
width and placing at `x=12` overflows the right margin; placing at `x=0` leaves a 7px margin where the
template wants 12.

So solve for the canvas whose *ink* is 278 wide:

```
imWidth  = 4 × (278 + 2 × 7.2)  ≈ 1170     # canvas 292.5 -> ink ~278
imHeight = 4 × plotHeight                  # whatever the height step above chose
```

Then, because `unwrap` leaves you a GROUP and a group's box hugs its contents, the imported chart
*is* its ink — set `chart.x = 12` and it lands on the content box. Verified across five charts:
widths came back 277.5, 277.3 and 277.0 against the 278 target, with right edges at 289.5, 289.3 and
289.0 against 290.

Two caveats. The 7.2px inset was measured at one font size, so **measure the import and expect one
correction** — the skill's standing advice for the default route applies here too. And a chart whose
ink does not fill the canvas comes back narrower regardless: on a discrete-bar MDim and a
single-series line chart the groups measured 235.9 and 233.7, because grapher reserved horizontal
space it then did not use. That is not a fit error to correct with a rescale (which would move the
font sizes off the ladder) — it is the export telling you the chart does not fill 278px.

Two rules from SKILL.md Step 3 **do not apply here**, and both would cost a re-export:

- **No renormalization and no aspect clamp.** `extractOptions` returns early for `imType=thumbnail`,
  so `MIN/MAX_ASPECT_RATIO` and the ~510k px² normalization never run. The standing rule that
  "`imWidth`/`imHeight` set the aspect ratio only" is true of the default and `uncaptioned` routes and
  false here.
- **No `rescale()` in Figma.** The import already lands at the content width, so every font size stays
  exactly where the export put it. Elsewhere this skill goes to some length to avoid a rescale; here
  it is free.

### The labeling policy — what gets a label, and what it says

The rule underneath every case: **name whatever distinguishes the series, and nothing else.** If the
series differ by entity, label the entity; if they differ by indicator, label the indicator's display
name; if there is only one series, name nothing and let the values carry it. The entity name on a
single-entity chart is pure overhead at 302px.

| Chart | What distinguishes the series | Label with | `imMinimal` |
|---|---|---|---|
| Several entities, one indicator | the entity | entity name, **bold, in the line's own color** | `0` |
| One entity, one indicator | nothing | no name at all — first and last **values** only | **`1`** |
| One entity, several indicators | the indicator | the indicator's **display name**, placed away from the values | **`1`** |
| Several entities *and* indicators | both | reconsider the chart — it is too much for 302px |

`imMinimal=1` is the mechanism for rows two and three, and it does exactly the right thing: it drops
the entity name **and** emits the first *and* last value per series. Verified —
`imMinimal=0` on a single-country line gives `1913 | 2024 | United States | 9.9%`, while `imMinimal=1`
gives `1913 | 2024 | 9.2% | 9.9%`. On the three-series MDim it replaces `World - Poorest decile` /
`World - Richest decile` with `$1.22 | $36.79 | $3 | $9.65 | $55.51`. The display names then get added
in Figma, positioned away from the value labels so the two roles stay legible.

**Always label the first and last value of each line**, not just the last. `imMinimal=1` gives both;
where you are on `imMinimal=0` (several entities) grapher labels only the end, so the start values are
added in Figma.

**Never carry grapher's `<name> - <indicator>` compound into the frame.** An MDim emits
`World - Richest decile`; the label should read `Richest decile`, since "World" is the only entity and
therefore distinguishes nothing.

### Value labels are centered on the mark they name

**This applies everywhere in this skill, not only at 302px** — bar values on their bars, end-of-line
values on their dots, legend labels on their swatches. Grapher positions text by baseline, so an
imported label sits high by construction and the drift is uniform, which makes it read as deliberate
rather than wrong. The recipe is in SKILL.md Step 7: `leadingTrim = "CAP_HEIGHT"` to shrink the line
box to the ink, then `label.y = markCenter − label.height / 2`. Step 8c's *Label alignment* check is
the gate.

### A ranked bar puts the year under the entity name, not after the value

Grapher folds a per-row year into the value — `25.8% in 2022` — which spends horizontal space on
repeated words and pushes the value column right. Split it: the **entity name** on one line with the
**year beneath it** in the label column (Lato Bold 11px `#2d2e2d` over Lato Regular 11px `#5b5b5b`,
right-aligned), and the bare value beside the bar. That is what the reference does, and the reclaimed
width goes to the bars.

### The y-axis minimum is not yours to set, and it matters here

A zero-based axis compresses a narrow series into a corner of the frame, which is much more visible at
302px than at 850. Measured on the Gini chart: pairing its labeled end dots with their values gives
−347.8 px per unit, which puts value 0 at y≈145.5 against a plot bottom of ~148 — i.e. **zero-based**,
with the whole 0.25–0.39 range squeezed into the top quarter. The reference uses a tight range and
fills the height.

**There is no URL parameter for this.** `GrapherQueryParams` is `country`, `focus`, `tab`, `overlay`,
`stackMode`, `zoomToSelection`, `xScale`, `yScale`, `time`, `region`, `endpointsOnly`, `facet`,
`uniformYAxis`, … — and `yScale` is linear-vs-log, not bounds. The minimum lives in the chart config
(`yAxis.min`), so it is **the chart author's to change**, exactly like sort order and entity selection
(SKILL.md Step 8b). Two routes: ask for the chart config to change, or point a draft chart with
`yAxis.min` set and export it through `by-uuid/<configId>.svg`. Do **not** fake it by cropping or
stretching the imported plot — the image would stop matching the view its `url:` navigates to.

### Comparing two values of one dimension: two exports, one frame

House practice for a chart like *"US income share: top 1% vs. top 0.1%"* is to export **each MDim view
separately** and combine them in one frame — the dimension cannot carry both values, so there is no
single view to ask for.

**The catch is the y-scale, and it is not a detail.** Each export auto-scales to its own series, so the
two arrive with different pixel-per-unit mappings and stacking them as-is misstates the gap between the
series. Measured on the designer's finished reference, the two series *are* on one shared scale: pair
each of the four labeled dots with its true value and every cross-series pair returns the same
**−6.8 px per percentage point** (−6.79, −6.76, −6.87, −6.85), which two independently scaled exports
would not produce.

So a faithful composition has to reconcile the scales, and the obvious fix is barred: a vertical-only
rescale ovals the dots and thickens the strokes unevenly, which SKILL.md forbids for exactly this
reason. Three honest options, in order of preference:

1. **Get a real two-series chart** — a standalone grapher chart or a narrative chart carrying both
   indicators. Then it is an ordinary single export and everything else on this page applies.
2. **Compose deliberately, and say so.** Place both exports, then reconcile by computing each series'
   true data range (from `.csv?...&csvType=filtered`) and mapping both onto a common scale before
   touching pixels. Record it as an accepted deviation — the image will not match either source view.
3. **Ship them as two separate small charts.** A `chart-rows` block has several rows; two charts each
   labeled with their own series is often clearer at 302px than one crowded composite.

Whichever you pick, `.csv?...&csvType=filtered` is how you check the values — and note the CSV returns
**every entity** unless `csvType=filtered` is present, so a naive `df[df.year == 1913]` silently reads
some other country.

### `imFontSize` is in rendered pixels, at 0.75×

`imFontSize` is a base; labels come out at **0.75 × the base**, in final rendered pixels. Measured at
302 wide on a two-series line chart and on a seven-row discrete bar:

| `imFontSize` | 14 (default) | **15** | **16** | 18 | 20 |
|---|---|---|---|---|---|
| line chart | 10px | **11px** | **12px** | 13px | 15px |
| discrete bar | 10.5px | 11.25px | **12px** | 13.5px | — |

So **`imFontSize=16` is the one to reach for** — it lands on 12px on both types, and 12px is the top
of the format's range. Use 15 when you want 11px on a line chart. The default of 14 renders at 10px,
below the format's floor, so **always pass `imFontSize`**.

Note the two types round differently — the bar chart keeps the fractional 11.25 where the line chart
reports a whole 11 — so a value other than 16 can leave you off the type ladder on one type and on it
on another. **Re-measure for a type you haven't done before** and record it here. Search itself drops
to `imFontSize: 12` for `WorldMap`, `DiscreteBar` and `StackedDiscreteBar`, on the grounds that
labels and legends are "too overpowering in thumbnail previews" at the default — worth trying if a
chart comes back crowded.

### `imMinimal` drops the entity names — it doesn't drop furniture

`imMinimal=1` returned `39.4 · 32 · 79.3` where `imMinimal=0` returned
`United States · China · 79.3`. Search can afford to lose the names because its own UI carries them.

**Default to `imMinimal=0`.** Reach for `1` only when the surrounding narrative already names the
entities and the values are the point. It is an editorial choice, so ask rather than assume.

## In Figma

Steps 5 and 7 of SKILL.md, reduced to what this format needs:

1. Clone `25344:1357` (guided) or `25344:1391` (pull) onto the page.
2. **Fix the background:** set the clone's own white fill to `visible: true`, then remove the
   `Group > Group > Vector` background rectangle (see above for why both halves are needed). That
   group is a **`GROUP`**, not a `FRAME` — `get_metadata` renders groups as `<frame …>`, so a filter
   on `type === "FRAME"` silently matches nothing and leaves it in place.
3. Resize the clone to `H`. On a pull clone, move the source row to `y = H − 23`.
4. Fill the text slots — title, optional subtitle, and the bare `Producer (Year)` source on a pull
   chart. Per SKILL.md Step 6, setting `characters` flattens mixed weights; these slots are
   single-weight, so nothing needs restoring.
5. `upload_assets` the SVG, unwrap the import frame (SKILL.md Step 5's `unwrap` helper), and place at
   `x = 12`, `y = 44 + gap`. **No rescale.**
6. Then the checks below.

A `chart-rows` block is 3–5 rows, so a run usually produces a **set**: one page, N frames laid out in
a row. Two rules for a set — the same entity keeps the same bound library color in every frame, and
heights may differ between frames, because the slot doesn't require them to match and forcing a
common height costs the tallest chart its room.

## Checks — the 302 numbers for Step 8c

Most of Step 8c carries over. These bars are different at this size, and reporting the 540-wide
figures here produces false failures:

| Check | 540-wide bar | **302-wide bar** |
|---|---|---|
| Text size floor | 12px | **11px** — the template's own subtitle, source and year labels are 11px. Not a deviation |
| Nothing in the margins | 16 … 524 | **12 … 290** |
| Box alignment | chart matches the header box | chart left edge at 12; **width need not match the header**, which hugs its text |
| Gap | 12–16px, equal top and bottom | not applicable as written — the band is `44 → H−10/−23` and the height was chosen to fit, so verify the chart sits inside the band with the margins above |
| Annotation block gap | 27px | scale to the frame; annotations are rare at this size |

Still fully in force: color-vision safety (`color_audit.py`), off-palette fills bound as library
styles, legend/direct-label agreement, label-on-fill contrast, the year or period being stated, and
every text claim being true of the indicator (`/adversarial-data-review`, `/check-metadata-style`,
`/check-metadata-typos`).

One new check for this route: **every label arrives twice.** Grapher draws each label as a white halo
plus the ink, so the export contains `United States` twice. A text edit that touches one copy leaves
the other behind, and a text-node count is double what the picture shows.

## Per chart type

GUIDELINES.md → Per chart type owns the conventions. What is specific to 302px:

- **Line** — the workhorse, and what the thumbnail renderer handles best. 1–3 series; 5 is the
  ceiling. Entity label along the line in Lato Bold, colored like it. Value labels at the ends only:
  both ends when the change over the period is the point, the end alone when the level is.
- **Slope / two time points** — labels at both ends, entity name beside the left end or along the
  band. Fits in the shortest frame of the set (221).
- **Ranked bar** — entity name (Lato Bold 11px `#2d2e2d`) over its year (Lato Regular 11px `#5b5b5b`)
  in a right-aligned left column, values to the right of each bar. **Drop the axis entirely** when
  every bar is labeled, per GUIDELINES.md. Height is `rows × ~30px`.
- **Stacked area / stacked bar** — labels inside the bands in white; check the label-on-fill contrast
  bar (4.5:1) at 11px, which is tighter than at 14.
- **Dumbbell** — entity names to the left, observation year below in 11px `#5b5b5b`.
- **Choropleth map** — **ask before building one.** A map loses most of its detail at 302px, and
  GUIDELINES.md → Colors keeps map ramps in grapher rather than in Figma.

When several series run close together, GUIDELINES.md's label-collision playbook applies; the two
moves that pay most at this size are shortening the longest entity name (`United Kingdom` → `UK`) and
moving a label to sit beside its end value rather than along the line.

## Delivery

The deliverable is a **PNG uploaded to Cloudflare Images**, referenced from the gdoc by bare
filename. `ACCEPTED_IMG_TYPES` (`adminSiteClient/imagesHelpers.ts:22`) has no `image/svg+xml`, so an
SVG is rejected.

1. **Name the frame after the file.** `<grapher-slug>-thumbnail` — `images.filename` is unique
   site-wide, and Figma uses the frame name as the export filename. Don't add a `-<W>x<H>` suffix:
   `appendImageSizeSuffix` reserves the `-{n}w` form for archival srcsets, and `htmlToEnriched.ts`
   strips `-1280x840`-style suffixes.
2. **Export at 3×.** The admin already has a Figma path —
   `GET /api/figma/image?fileId=<key>&nodeId=<node>` (`adminSiteServer/apiRoutes/figma.ts`) calls the
   Figma API at `scale: 3`, giving **906 × 3H**. This matters: `getSizes(302)` yields
   `[48, 100, 302]`, so the largest srcset candidate at 1× is 302w and a 2× display upscales it.
   Note `get_screenshot` **cannot** do this — `maxDimension` only ever downscales, and clamps at the
   node's natural size.
3. **Upload** via the admin (`POST /api/images`), then reference the filename in the block's `image:`
   field.

Side effect to expect: `ImagesIndexPage.tsx:56` buckets anything whose filename contains
`thumbnail` into the admin's featured-thumbnail filter.
