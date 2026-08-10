---
name: create-figma-chart
description: Turn an OWID grapher chart — given as a slug, a customized grapher link, an MDim view, an admin link, a narrative chart, or just a description — into a templated chart in the design team's yearly "Charts (YYYY)" Figma file. Exports the chart SVG, creates a new page named "YYYYMMDD Title (Author)", places the original chart and an adapted template side by side, replicates title/subtitle/data source/note in the template's styles, fits the chart into the template, proposes better labeling (direct line/bar labels instead of legends) and annotations with the file's curvy arrows, and names the final frame with the kebab-case slug used for the website PNG. Trigger when the user asks to "create a figma chart", "make a static chart in Figma", "prepare this chart for Instagram / as a data insight image", "put this grapher chart into the Charts file", or pastes a grapher/admin/narrative-chart link asking for a designed static version.
metadata:
  internal: true
---

# Create a templated Figma chart from a grapher chart

This skill takes any OWID grapher chart and produces a designed static version in the design team's yearly **Charts (YYYY)** Figma file, following the team's DI Charts Guidelines and the Good Data Viz Checklist.

**The defining principle:** the template is law. You adapt the chart's content *into* the template — you never restyle what the template provides (fonts, colors, spacing, logo, footer layout). Anything you add on top (annotations, direct labels, arrows) uses the file's shared text styles and the Chart colors library, nothing else.

**The single checkpoint rule:** the Charts file is a shared design file other people work in. Nothing is written to it before the user has seen the full proposal (page name, template choice, texts, planned label/annotation edits) and explicitly approved. Reading the file to check conventions needs no permission.

Read [GUIDELINES.md](GUIDELINES.md) (sibling file) before editing any chart — it distills the DI Charts Guidelines per chart type and the Good Data Viz Checklist.

## The yearly Charts file — node map (2026)

Each year gets a new file. For **2026** the file key is `s6Sv60bakebRRW2TxsMQbF` ([Charts (2026)](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-)). **If the current year is not 2026, ask the user for that year's file link and re-verify every node id below** (the templates page is named " 📑 Templates" — note the leading space).

| What | Node | Size | Notes |
|---|---|---|---|
| Templates page | `798:54` | — | all templates + instruction frames live here |
| InstagramPost_Template_English | `798:161` | 540×540 | footer: source + `OurWorldinData.org/[Topic]` + CC BY |
| InstagramPost_Template_Portrait_English | `6689:8` | 560×700 | footer includes a Note line |
| InstagramReel_template | `7336:8` | 616×1096 | has top/bottom no-go zones; contains a worked small-multiples example |
| DI_Template | `6799:1859` | 540×540 | one-line footer: source + CC BY |
| Static Chart Template_Mobile (example 1) | `24590:20` | 540×540 | |
| Static Chart Template_Mobile (example 2) | `24590:32` | 540×824 | taller variant — use when the chart needs vertical room |
| Static Chart Template_Horizontal | `5332:75` | 850×638 | footer: Note, Data source, OWID tagline, "Licensed under CC-BY by the author [Name]" |
| Static Chart Template_Vertical | `5332:93` | 850×1095 | |
| Curvy arrows | `798:773` | — | copy/paste into the chart; scaling rules in GUIDELINES.md |
| "No data" hashed-pattern instructions | `4162:5` | — | Hero Patterns **plugin** — manual step for the user |
| Flags | `2654:5` | — | Flags **plugin** — manual; US flags provided in the file |
| Animals | `5336:5` | — | chicken, rooster, turkey, fish, cow, egg-laying hen, pig |
| Good Data Viz Checklist | `20729:1027` | — | distilled in GUIDELINES.md |

Shared styles in the file: text styles `Data Insights/Title` (Playfair Display SemiBold 25) and `Data Insights/Subtitle` (Lato 16); color variables `Text/Gray 100` #2D2E2D, `Text/Gray 80` #5B5B5B, `Website/Text/Blue 100` #002147, `Instagram/Beige Background` #FBF9F3; plus the **Chart colors** library (see GUIDELINES.md → Colors).

The DI Charts Guidelines file (`8gxqkVmZ9x3MK3ky5oigrJ`) is the source of truth behind GUIDELINES.md — six pages: line `0:1`, stacked area `130:35045`, bar/stacked bar `130:35046`, slope `130:35047`, scatter `130:35048`, map `130:35049`. Re-read the relevant page if GUIDELINES.md seems stale.

## Inputs

- **A chart reference**, in any of the forms of the Step 1 table. If the user only describes the chart ("the life expectancy chart with just the US and China"), resolve candidates first and confirm.
- Optionally, **the DI/article text** the chart accompanies — the best source for annotation content. Ask for it if annotations are wanted and it exists.
- Everything else (formats, author, slug, topic link) is collected once in Step 2.

## Step 1 — Resolve the chart and gather its text

Get an SVG URL for the chart, whatever form the reference takes:

| Input | SVG URL |
|---|---|
| Slug or default grapher link | `https://ourworldindata.org/grapher/<slug>.svg` |
| Customized grapher link (query params) | insert `.svg` before the `?`, keep the query verbatim: `https://ourworldindata.org/grapher/<slug>.svg?country=USA~CHN&time=1990..latest` — `country`, `time`, `tab`, `stackMode`, `region`, `focus`, … are all honored, and slug redirects work |
| MDim view | same — the dimension params select the view: `.../energy-mix.svg?metric=per_capita&source=coal` |
| Admin link `/admin/charts/<id>/edit` | **`/admin/charts/<id>.svg` does not exist** (it returns the admin SPA shell). Resolve the chart's `configId` — `SELECT configId FROM charts WHERE id = <id>` on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use `https://ourworldindata.org/grapher/by-uuid/<configId>.svg`. Works for unpublished drafts too. |
| Narrative chart (**name**) | name → uuid via the unauthenticated map `https://admin.owid.io/api/narrative-chart-map`, then `https://ourworldindata.org/grapher/by-uuid/<uuid>.svg` |
| Narrative chart (**admin link with a numeric id**, `/admin/narrative-charts/<id>/edit`) | there is no id→uuid endpoint: the public map is keyed by name, and the Datasette mirror of `narrative_charts` lags behind production by days (it stopped at id 338 while 341 existed). Diff the live map against `select name from narrative_charts` to get the unmirrored names, then order them by uuid — they are **uuidv7, so lexical order is creation order** — and count up from the mirror's highest id. That gives a *candidate*, not an answer: ids have gaps where charts were deleted. **Always render the candidate and have the user confirm it before building.** |
| Description only | find candidates via site search (`https://ourworldindata.org/search?q=...`) or a Datasette title match; show the candidates and confirm before proceeding |

Then pull the chart's texts, which seed the template texts in Step 6. Read them from **`.metadata.json`**, not `.config.json`, and **keep the view's query params on the request** — `.../energy-mix.metadata.json?metric=per_capita&source=coal` resolves the selected MDim view exactly as the `.svg` request does:

| Template text | Where it comes from |
|---|---|
| Title | `chart.title` |
| Subtitle | `chart.subtitle`; when that's absent the chart is inheriting the indicator's `description_short` — take `columns[<column>].descriptionShort` |
| Note | `chart.note` (absent when the chart has none) |
| Data source | `chart.citation` — verbatim what grapher prints after `Data source:` |
| Topic page | **`.config.json` → `originUrl`** — the one field `.metadata.json` doesn't give (its `chart.originalChartUrl` is the grapher URL, not the topic page). Often null; fall back to asking the user in Step 2. |

`.config.json` is not a substitute: it never carries the source attribution, it carries `subtitle`/`note` only when a chart overrides them manually, and **for an MDim slug it 404s** — per-view configs aren't exposed under that path. Pass *every* dimension a view needs; a partial MDim param set returns an empty payload (`title: null`, no columns). If a text is ever in doubt, the rendered SVG is the tie-breaker — it shows exactly what the footer will say.

These texts also arrive **render-ready**: the endpoint unwraps grapher's detail-on-demand links across the whole payload, so a `description_short` written as `[lower secondary](#dod:lower-secondary-education)` reaches you as plain `lower secondary` — paste it as-is, and don't hand-strip anything. `.config.json` hands back the raw markup instead, which is one more reason not to take texts from it.

> **The one exception: `by-uuid` has no `.metadata.json`** — that route serves only `.config.json`, `.png` and `.svg`, so the request 404s. For a **narrative chart**, therefore, take the texts from `by-uuid/<uuid>.config.json`, which is complete for that case: a narrative chart stores its own `title`/`subtitle`/`note`/`originUrl` overrides rather than inheriting them. What it still won't give you is the source attribution — read that off the rendered SVG's footer instead, and re-derive nothing (see the producer-name rule below).

**Never shorten the producer's name to make it fit.** The footer string is the producer's official name — verify it against `rg "producer: .*<name>" snapshots/` if you're unsure — and "Food and Agriculture Organization of the United Nations" does not become "UN Food and Agriculture Organization" because the line is too long. When it overruns the CC BY text, wrap it (Step 7) rather than editing it.

**Check that every selected entity actually renders.** Grapher silently drops an entity whose data doesn't reach the displayed year, with no warning anywhere — a chart pinned to 2023 quietly showed ten of its eleven countries because one stopped at 2022, and the DI text still discussed the missing one. Compare the **effective** selection against the entity labels in the exported SVG, and if they differ, say so before building: the fix is the narrative chart's tolerance setting (or pinning the year), and it belongs to whoever owns the chart.

  "Effective" is the catch. `selectedEntityNames` in `.config.json` is the *saved* chart's selection, so it is the wrong baseline for exactly the inputs this skill takes most often:

  | Input | Baseline to compare the SVG against |
  |---|---|
  | Bare slug, or `by-uuid` | `selectedEntityNames` from `.config.json` |
  | Link carrying `country=` (or `focus=`) | the **URL's** list — it overrides the saved selection entirely |
  | MDim view | the URL's `country=` if present, otherwise render the view and treat its labels as the baseline |

  `life-expectancy.config.json` lists `World, Americas, Europe, Africa, Asia, Oceania`, but `life-expectancy.svg?country=USA~CHN` contains only `China` and `United States` — take the config as the baseline there and the check reports six entities missing and two unexpected, on a chart where nothing is wrong. And note the two sides speak different languages: `country=` takes **ISO codes** while the SVG prints **names**, so resolve the codes before comparing rather than diffing the strings.

## Step 2 — Ask the run options, all at once

One `AskUserQuestion` batch — don't drip-feed:

1. **Output format(s)** (multi-select — several deliverables from one run are normal). Constraint from the design team: **Instagram and DI images are always square/mobile**; a static chart (for the OWID website) can be desktop and/or mobile:
   - Instagram post (square 540×540) or portrait (560×700)
   - Data insight image (DI_Template, 540×540)
   - Static chart — mobile/square (540×540 or 540×824) and/or desktop (Horizontal 850×638 / Vertical 850×1095; Vertical when the chart needs height — rankings, long bar lists)
2. **Author** — goes into the page name and (static templates) the "Licensed under CC-BY by the author …" line. Default: the user.
3. **The DI's own title — or the claim the image is meant to make.** Ask for this whenever a DI or Instagram image is among the formats, and ask *independently of annotations*: grapher's descriptive title must not survive into those images (GUIDELINES.md → Titles), and the story is not yours to invent. If there's no title written yet, ask for the sentence the image supports and derive a candidate from it for approval in Step 4.
4. **Annotations** — should the chart carry annotations replicating what the accompanying text says? If yes, ask for that text (DI draft, article paragraph).
5. **Topic page** for the `OurWorldinData.org/[Topic]` footer line — default from the config's `originUrl`.
6. **Slug** for the final frame — short, kebab-case (`child-mortality-asia-decline`). It becomes the PNG filename when the frame is exported for the website. Propose one; let the user override.

## Step 3 — Export the SVGs

Two exports per format family: the **original** (placed on the page as the reference copy) and the **embed** (chart area only, placed inside the template).

```bash
DIR=/tmp/figma-chart && mkdir -p $DIR   # or the session scratchpad

# Original — desktop templates (850×600 default render)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&nocache" -o $DIR/original.svg
# Original — square/mobile templates (grapher's own square re-layout, 540×540)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=square&nocache" -o $DIR/original_square.svg

# Embed — chart area only, no grapher header/footer, at the template chart-area aspect ratio
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=uncaptioned&imWidth=<W>&imHeight=<H>&nocache" -o $DIR/embed.svg
```

`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes the SVG to ~510k px², so you cannot request a bigger SVG (irrelevant: it's a vector; you scale it in Figma). Aspect ratios that match the template chart areas (Step 7 table): square templates ≈ `imWidth=1000&imHeight=730`, Horizontal ≈ `imWidth=980&imHeight=525`, Vertical ≈ `imWidth=690&imHeight=745`. Sanity-check what came back:

```bash
head -c 300 $DIR/embed.svg   # expect <svg ... width="..." height="...">, no <html
```

> **Square charts, alternative route:** grapher's `imType=square` render re-lays out the chart for a square canvas (legend placement, font sizing tuned by the web team). When that layout is better than the uncaptioned crop — commonly for maps and charts with big legends — import the full square SVG instead and delete its header/footer layers in Figma after import. Offer both routes; pick per chart.

**Size the text at export time with `imFontSize` — scaling in Figma cannot fix it.** Grapher picks a base font for the canvas it renders (`max(10, height/25)`, so ~24 for the default uncaptioned export), and every label is derived from it — the segment values and country names land at about **0.75 × the base**. Placing that export at 508px wide shrinks all of it by the same factor, so a default export ends up with ~12px labels: legal, but on the floor of the 12px minimum. Ask for a bigger base instead — `imFontSize=28` gives ~13.5px labels and ~14px legend text in a 540 frame, which matches the template's own 14px source line. Check the export before importing:

```bash
grep -oE 'font-size="[0-9.]+"' chart.svg | sort | uniq -c | sort -rn | head -3
# multiply the most common value by (508 / the export's content width) to get the final size
```

Bigger text needs more room, so this trades against how much fits — see the axis rule in Step 8 and, failing that, the entity count.

Caveats: `?tab=table` is silently ignored (renders the default tab); `imSquareSize` affects PNG only; add `nocache` when re-exporting after a config change.

## Step 4 — Propose, then get the go-ahead

Before touching the file, show the user in one message: the page name **`YYYYMMDD <Title> (<Author>)`** (today's date, the *final* — possibly rewritten — title), the chosen template(s), every text that will go into the template, the labeling changes you propose (Step 8), and the annotations with their content. **Wait for explicit approval.** This is the single checkpoint; after it, iterate freely on the same page without re-asking.

## Step 5 — Create the page and place the pieces

> **Load the `figma-use` skill before any `use_figma` call** — hard prerequisite. It covers `loadFontAsync` before text edits and the other plugin-API rules.

1. **Enumerate pages with `use_figma`**, not `get_metadata` — the MCP page listing is unreliable (it returns only "Cover" for this file). Dated chart pages sort newest-first after the instructions/templates pages; insert the new page at the top of the dated block, matching the existing order:

```js
const pages = figma.root.children.map((p, i) => `${i}: ${p.name}`)
const page = figma.createPage()
page.name = "20260810 Child mortality in Asia (Pablo)"
// move it to the right index with figma.root.insertChild(index, page)
await figma.setCurrentPageAsync(page)
```

2. **Clone the template frame(s)** onto the new page: `(await figma.getNodeByIdAsync("<template-id>")).clone()`, then `page.appendChild(clone)` and position it.

3. **Import the SVGs with `upload_assets`** — never `createNodeFromSvg` (the `use_figma` code param caps at 50k chars; a grapher SVG is ~165 KB). `upload_assets` returns a single-use `submitUrl`; POST the file to it and keep the returned `placedOnNodeId`:

```bash
curl -s -X POST "<submitUrl>" -F "file=@$DIR/original.svg;type=image/svg+xml"
# → {"success":true, ..., "placedOnNodeId":"<id>"}
```

4. **Lay out the page**: the original chart on the left; the adapted template **to its right** (~100 px gap). If several formats were requested, keep one original and line the templates up to its right. Move imported nodes with `use_figma` (`page.appendChild(node)`, set `node.x/node.y`). This page-level parenting is for the **original** reference chart only — the embed gets reparented into the template clone in Step 7.

> **Imported SVGs arrive at their natural size** (850×600 / 540×540). Scale with `node.rescale(factor)` — never `resize()`, see Step 7.

**Bin the import frame.** `upload_assets` wraps the SVG in a FRAME that OWID's charts don't have and that causes two failures of its own: it carries a **white fill** that paints over the footer as soon as the frame overhangs, and `resize()` on it *stretches its children through their constraints* — which silently rewraps every text box in the chart, because grapher's exported labels are sized to their glyphs with no slack ("Brazil" becomes "Bra zil"). Move the frame's children into the template clone and delete the frame; a plain group is what the finished pages use:

```js
const kids = [...embed.children];
for (const k of kids) templateClone.appendChild(k);
embed.remove();
const chart = kids.length === 1 ? kids[0] : figma.group(kids, templateClone);
chart.name = "chart";
```

## Step 6 — Fill the template texts

Replace the lorem-ipsum text nodes in the cloned template. Source everything from the chart config (Step 1) and the user's answers (Step 2):

- **Title** — for a DI or Instagram image, start from the DI title collected in Step 2, not grapher's; otherwise suggest a more colloquial rewrite per GUIDELINES.md ("Death rate in the United States", not "Death rate, US"). Keep the user's final say. The page name uses this final title. Two or three lines is normal; check the line breaks and the year and highlight-color rules in GUIDELINES.md → Titles.
- **Subtitle** — the chart's subtitle, trimmed to what's necessary. When the chart shows a single year (or a narrow period the reader needs), append **`Data for <YYYY>.`** here.
- **Data source:** `Data source: ` + `chart.citation` from Step 1 — that field *is* grapher's own footer line, so don't re-derive a `<producer> (<year>)` string by hand, and don't abbreviate it to save space. A long producer name overruns the CC BY text at x=468. **Give the source its own full line and move CC BY to the row beneath it** — the source stays one unbroken line, which reads better than a wrap, and the template's own two-row footers (the Instagram ones) already use exactly this geometry:

  ```js
  source.textAutoResize = "WIDTH_AND_HEIGHT";   // one line, its natural width
  source.x = 0; source.y = 0;
  ccby.x = 0;  ccby.y = 20;                     // left-aligned under the source, template row pitch
  footer.resize(508, 36);
  footer.y = 524 - footer.height;               // grow upward; bottom margin stays 16px
  ```

  Then re-fit the chart into what's left (Step 7). Only if the source is too long even for a full line — beyond ~508px — wrap it with `textAutoResize = "HEIGHT"` at a width that breaks after the organization's name, and top-align CC BY with its first line. Either way CC BY is **left-aligned** once it has its own row — it only sits at x=468 while it shares the source's line.
- **Note:** only in templates that carry a Note line, and only if the chart has one worth keeping. **DI images normally carry no note at all** — drop it, or, when it's genuinely load-bearing for understanding the chart, fold it into the subtitle as a bolded second line (only if the subtitle isn't already crowded).
- **`OurWorldinData.org/[Topic]`** → the confirmed topic path (e.g. `OurWorldinData.org/child-mortality`).
- **CC BY** stays; static desktop templates also carry `Licensed under CC-BY by the author <Author>`.

Rules: replace `characters`, and leave the node's **base** styling alone — the fonts, sizes, colors, and positions are the template's, not yours. `await figma.loadFontAsync(node.fontName)` before each text edit. If you need a *new* text block the template doesn't have, **clone the nearest template text node and edit it** — that inherits the correct shared style without hunting style ids.

Two **range-level** exceptions the guidelines actively require, applied after the characters are in place and scoped to just those characters — never to the whole node:

- the title's highlight word → `setRangeFills`, in the exact color of the marks it names (GUIDELINES.md → Titles);
- a load-bearing note folded into the subtitle as a bolded second line → `setRangeFontName` to the family's bold weight, which needs its own `loadFontAsync` (GUIDELINES.md → Subtitles and notes).

Nothing else gets restyled.

## Step 7 — Fit the chart into the template

The chart spans the full content width, left-aligned with the title/subtitle/logo box, and sits in the band between the header and the footer with an even gap top and bottom.

**Measure that band; don't hardcode it.** The header's height depends on how many lines the title and subtitle take, so a fixed y is wrong as soon as the subtitle wraps — and centering inside a guessed band leaves a lopsided result (18px above, 6px below on the first run of this skill). Read the real edges instead:

```js
const headerBottom = header.y + header.height     // Frame 14: title + subtitle + logo
const gap = (footer.y - headerBottom - chart.height) / 2
chart.x = header.x
chart.y = headerBottom + gap
```

**How much gap is right: 14px, and 12–16 is the comfortable band.** That's what the finished pages and grapher itself converge on, measured in 540-wide frames — grapher's own square export leaves 13px above the plot and 14px below; recent DI pages in the file sit at 14/19, 15/14 and 7/15. Below ~10px it reads cramped and the legend starts to look like part of the subtitle; above ~20px you are wasting space the plot could use. When the chart comes out a few pixels too tall, spend the slack down to 12px a side **before** shrinking it — that is usually enough, and it keeps the full content width, which matters more than the last pixel of gap.

Side margins and the footer edge are the template's, not yours: content starts at the header's `x` and the footer's bottom stays where the template put it.

| Template | Content x / width | Header bottom → footer top (unwrapped subtitle) |
|---|---|---|
| 540-wide (IG square, DI, static mobile ex. 1) | x=16, w=508 | 118 → 508 (DI/static) or 488 (IG, 2-line footer) |
| Static mobile example 2 (540×824) | x=16, w=508 | 118 → 792 |
| IG portrait (560×700) | x=26, w=508 | 135 → 640 |
| Static Horizontal (850×638) | x=16, w=818 | 118 → 556 |
| Static Vertical (850×1095) | x=16, w=818 | 116 → 997 |

Verify against the actual clone with `get_metadata` (the templates evolve; the geometry above is a 2026 snapshot). These are **frame-local** coordinates, and `x`/`y` are relative to a node's parent — so append the embed to the template clone **before** positioning it. Left parented to the page (where Step 5 puts imported nodes), the same numbers land it near the page origin, on top of the reference chart:

**Anything you add to the chart aligns to the same box as the subtitle** — annotations, captions, notes all start at the content left edge and may run its full width. Aligning them to the bars' left edge instead leaves a ragged inner margin that reads as a mistake.

**But size them against the plot's own bounds, not the group's.** An annotation is a child of the chart group, so the moment it is wider than the plot it *becomes* the group's width — and the next `rescale(header.width / chart.width)` then scales the plot down to make room for it (a 508-wide group silently became 527). Measure the plot by walking the group and skipping the annotation nodes, size the annotations to that, and only then rescale:

```js
let left = Infinity, right = -Infinity
const walk = n => {
  if (n !== chart && n.type !== 'GROUP' && !annotationIds.has(n.id)) {
    left = Math.min(left, n.x); right = Math.max(right, n.x + n.width)
  }
  if (n.children) n.children.forEach(walk)
}
walk(chart)
for (const t of annotations) { t.x = left; t.resize(right - left, t.height) }
```

**Match the header box exactly — same left edge and same width.** A chart even a few pixels narrower than the title reads as a mistake. Scale off the header rather than off a constant, and do it *after* the frame is gone, so the group's bounding box is the plot's real extent and no export padding is baked into the width:

```js
const header = clone.children.find(c => c.name === "Frame 14")   // title + subtitle + logo
chart.rescale(header.width / chart.width)                        // never resize()
chart.x = header.x                                               // same left edge
chart.y = top + (bottom - top - chart.height) / 2                // centered between header and footer
```

`rescale()` on the group is safe; `resize()` on a frame is not (see Step 5). If the scaled chart overflows the vertical space, re-export at a flatter aspect ratio rather than squashing — **never stretch one axis** (it distorts dots, arrowheads, and text).

**After any scaling, let every text box re-hug its content.** Grapher's exported labels have no slack, so the smallest rounding makes them wrap. Sweep the chart once, preserving each label's anchor — the axis values are centered and the country names right-aligned, so keeping `x` alone would shift them:

```js
for (const t of chart.query('TEXT')) {
  const a = { x: t.x, y: t.y, w: t.width, h: t.height, align: t.textAlignHorizontal }
  t.textAutoResize = "WIDTH_AND_HEIGHT"                          // fonts loaded first
  if (a.align === "CENTER") t.x = a.x + (a.w - t.width) / 2
  else if (a.align === "RIGHT") t.x = a.x + a.w - t.width
  t.y = a.y + (a.h - t.height) / 2                               // keep the vertical center too
}
```

**Then verify the alignment against the marks, not against the old box.** Re-hugging changes a box's height as well as its width, so labels drift vertically — every value label on this skill's first run ended up 1.2px above its bar's center and every legend label 1.31px above its swatch. Individually invisible; as a set, the whole chart reads slightly high. It is worth an explicit pass, because nothing about it looks wrong in a node listing:

```js
for (const row of chart.query('[name=bars]').first().children) {
  const bar = row.query('VECTOR[name=bar]').first()
  const mid = bar.y + bar.height / 2
  for (const t of row.query('TEXT')) t.y -= (t.y + t.height / 2) - mid
}
// same for the legend: center each label on its swatch
```

Make this a habit rather than a reaction to someone noticing: **after any scale, re-hug or reflow, check that labels still center on the thing they label** — bar values on their bars, legend labels on their swatches, axis labels on their ticks.

## Step 8 — Improve the labeling and annotate

**Read [GUIDELINES.md](GUIDELINES.md) now if you haven't.** Browse 1–2 recent dated pages in the file (`get_screenshot`) to see how finished charts apply these conventions. The imported SVG is a fully editable vector tree — text nodes, line vectors, legend swatches are all addressable via `use_figma`.

The high-value edits to propose (include them in the Step 4 proposal):

- **Direct labels instead of legends and elbows.** Line charts: put the entity label at the end of its line, colored like the line, and delete the elbow/leader connectors; reclaim the freed right margin for the chart. Area/bar charts: label the series inside the chart area (white ≥12px text on dark fills) and delete the separate legend.

  **This is not a free win on a stacked chart — check that it beats the legend before proposing it.** Direct labeling works when every label can sit *on the mark it names*: over its own segment of the top bar (the pattern in [this DI](https://ourworldindata.org/data-insights/most-collected-waste-in-many-low--and-middle-income-countries-is-stored-in-open-dumps-or-is-burned), where colored category labels sit above the first row and the widest series is labelled in white inside the bar), or inside the widest segment of each category. Both need the segments to be wide enough, which in practice caps it at **three or four categories**. Beyond that the labels collide over the top bar, and spreading them evenly across the plot instead just yields a color-coded legend that is *harder* to read than the real one — the reader has lost the swatch and gained nothing. Six categories is past the line. When it doesn't fit, keep grapher's legend and say why; a conventional legend is not a failure to improve the chart.

  When it *does* fit, the reliable recipe is: for each category, find the row where its segment is widest, **clone that segment's existing value label** (the clone inherits the right font, size and — importantly — the black-on-light vs white-on-dark fill grapher already chose), set its characters to the category name, then center the `[name, 4px, value]` pair on the segment. To rebuild a legend you removed too eagerly: recolor the labels to `Text/Gray 80` #5B5B5B, add a 10×10 swatch in each category's own color 4px to their left, and lay them out in grapher's own split — as many as fit on the first row, the longest alone on the second.
- **Annotations replicating the accompanying text** (12–16px; 10–14px on maps): text color = the annotated object's color, `Text/Gray 80` #5B5B5B, or a mix; bold the key phrase; 2–3px **white outside stroke** instead of a background rectangle.
- **Arrows**: copy curvy arrows from node `798:773` — 1px stroke, arrowhead and line the same color as each other and consistent across the chart. Never scale a whole arrow (it distorts the head): Shift-resize the line segment only, then reposition the head. If a curvy arrow gets messy, use a straight thin line. **Maps: never curvy — straight 1px lines or values inside country shapes.**
- **Drop the axis and gridlines when every data point is already labelled.** The checklist says so outright, and it is the cheapest space you will ever find: deleting `horizontal-axis`, `vertical-grid-lines` and `vertical-zero-line` from the imported group frees ~25px — usually the difference between text at the 12px floor and text at a comfortable 13–14px. It applies most obviously to a **100% stacked bar**, where every bar spans 0–100% and the axis tells the reader nothing they can't read off the segment values. Don't do it where the reader still has to estimate: a line chart's y-axis, or any chart whose points are mostly unlabelled.
- **Dropping entities does not buy vertical space — it buys thicker bars.** Easy to get wrong: the export canvas is a fixed size, so grapher redistributes the freed rows into the remaining ones and the chart comes back exactly as tall. Measured: eleven countries and ten countries both returned a 346px chart, with the row pitch going from ~28 to ~31px. So cut entities to reduce clutter or to make bars more readable, never to make something fit. **The lever for fit is the export's aspect ratio** (`imWidth`/`imHeight`, which set the shape the layout is computed for) or removing furniture like the axis — not the entity list. Either way the selection belongs to the chart's author: surface it, don't decide it.
- **10×10 px dots** marking highlighted years, with the values written out for the first, last, and any mentioned data point (white-outlined dots on stacked areas; no outline elsewhere).
- **Flags** (`2654:5`) beside country labels/bars where they help; **animals** (`5336:5`) for livestock topics; both are copy/paste.
- **Colors**: only the file's Chart colors library, in the cheat-sheet order. **Audit them — never eyeball this.** A palette that looks fine can collapse for the ~8% of men with red-green deficiency, and the failure is invisible to you:

  ```bash
  .venv/bin/python .claude/skills/create-figma-chart/scripts/color_audit.py \
    '#bc8e5a,#883039,#6d3e91,#d73c50,#4c6a9c,#6e7581' \
    --names 'Poultry,Beef and buffalo,Sheep and goat,Pork,Fish and seafood,Other meats'
  ```

  It simulates deuteranopia, protanopia and tritanopia, reports the closest pairs as CIELAB ΔE (**under 20 fails, 20–30 is tight**), flags which pairs actually touch in the stack, checks white-vs-black label contrast on every fill, and measures the **grayscale seam** between each pair of touching fills (under **1.6:1** they merge when printed — two different hues at the same lightness pass every color check and still fail this one). Add `--suggest` (with `--keep` for the colors that carry meaning) to search the OWID palette for a safer set; it ranks by **hue variety first, then safety, then drift** from the colors already in use, because ranking on safety alone returns sets that are entirely blues and greens — technically separable, but the reader can no longer tell six categories apart at a glance — and among equally varied, equally safe palettes the one that moves the colors least is the one a designer reads as a fix rather than a different chart. Every suggestion it prints has also cleared the grayscale seam check, and it reports the seam alongside the ΔE so you can see it did: a palette can clear ΔE 20 comfortably and still have touching fills that merge in print, so the search picks the *order* as well as the colors. Where it can't help you is a failing seam between two colors you told it to keep — it says so rather than silently returning nothing. Constrain the roles as well when you search by hand (fish should stay blue, beef reddish): the unconstrained optimum is rarely the one to propose. Read the results with two cautions: **tritanopia is vanishingly rare**, so never repaint for it alone; and **swapping a single color usually doesn't help**, because the failures are independent — this chart's floor stayed at 9.2 whether you changed Pork or Sheep-and-goat, since a different pair took over each time. Colors live in the chart, so a repaint is a recommendation to its author, not an edit you make.

  **Apply the library *style*, not the hex.** A raw fill leaves the designer looking at `#B13507` with no way to tell whether it came from the palette; a bound style shows `Default Palette/Rusty Orange` in the Fill panel, and it updates if the library ever changes. Import each style by key and bind it — the color comes along, so never set `fills` as well:

  ```js
  const style = await figma.importStyleByKeyAsync("<style key>")   // from search_design_system
  await bar.setFillStyleIdAsync(style.id)                          // NOT bar.fills = [...]
  ```

  **Never map a group's children by index — pair them by geometry.** A node's position in `parent.children` is not its visual position, and sorting on `y` then `x` fails too: after a rescale, swatches on the same legend row differ in `y` by fractions of a pixel, so `a.y - b.y` never returns 0 and `x` is never consulted. Both mistakes recolored a legend that then disagreed with its own bars — the colors were all correct, just attached to the wrong words, which is worse than a wrong color because it silently misreads the chart. Match each **label** to the nearest swatch on its left, and drive the color from the label's text:

  ```js
  for (const lab of labels) {
    const sw = swatches.filter(s => Math.abs(s.y - lab.y) < 12 && s.x < lab.x)
                       .sort((a, b) => (lab.x - a.x) - (lab.x - b.x))[0]
    if (sw) await sw.setFillStyleIdAsync(style[spec[lab.characters]].id)
  }
  ```

  **Then assert it.** Compare each legend swatch's resolved fill against the fill of the segment it names, on one row, and report the mismatches — a legend keyed off text and verified against the bars cannot drift:

  ```js
  const bars = {}                       // segment name -> fill hex, from any one row
  for (const seg of chart.query('[name=Brazil]').first().children) { ... }
  // then for each label: swatchHex === bars[segmentNameFor(label.characters)]
  ```

  Get keys with `search_design_system` scoped to the `[Chart Colors] Library`, querying the color's name. Bind the legend swatches too, or the legend and the bars disagree about where their color came from. Text fills stay raw: the label color is a contrast decision (black or white on that fill), not a palette choice.

  **Build the candidate before recommending it.** Clone the finished frame, recolor the copy, and put it beside the original — the score says a set is *safe*, not that it is *good*. The top-scoring set for this chart (ΔE 26.2) turned poultry navy and fish denim, two blues at opposite ends of the stack, and made beef lime green next to olive-green pork: measurably safer and editorially worse, because a normal-vision reader now reads unrelated categories as related. Expect this — deuteranopia collapses the red-green axis, so safe six-category sets drift toward blues and greens. Offer the highest-scoring set that still makes sense, not the highest-scoring set, and let the author see both.

## Re-exporting after a change to the chart itself

Expect this to happen more than once per run. Anything that belongs to the **chart** — the category order, the entity selection, the tolerance, the colors, the year — is fixed in grapher or in the narrative chart by whoever owns it, and then re-exported. **Never patch it by moving vectors in Figma:** the image would then disagree with the interactive chart it accompanies, and the next re-export silently throws your edit away.

The swap is one scripted pass, and it is the same every time — worth doing as a single `use_figma` call rather than rebuilding by hand:

```js
oldChart.remove()
const kids = [...imported.children]                  // bin the upload's frame
for (const k of kids) clone.appendChild(k)
imported.remove()
const chart = kids.length === 1 ? kids[0] : figma.group(kids, clone)
chart.name = "chart"
for (const n of ["horizontal-axis", "vertical-grid-lines", "vertical-zero-line"])
  for (const x of chart.query(`[name=${n}]`).toArray()) x.remove()   // if they were dropped before
chart.rescale(header.width / chart.width)
// ... re-hug every TEXT, preserving its alignment anchor ...
chart.rescale(header.width / chart.width)            // re-hugging moves the bbox
const gap = (footer.y - (header.y + header.height) - chart.height) / 2
chart.x = header.x
chart.y = header.y + header.height + gap
```

Keep the export URL — same `imFontSize`, same `imType`, same params — so the only thing that changes is what the chart author changed. And re-check the category order and the entity list against what you were told changed: a reorder can move more than the category you asked about.

**Re-export the reference copy too.** The chart on the left of the page is the "before" of a before/after, so a stale one misrepresents the comparison — a reviewer reads a difference you didn't make, or misses one you did. Refresh it from the same source with its own params (`imType=square` for the square templates), replace it in place, and keep its layer name so the page reads the same. This is easy to forget precisely because nothing about the reference looks wrong on its own.

## Step 8b — Bring recommendations of your own

Fitting the chart into the template is the floor, not the job. Before the Step 4 proposal, look at the chart as an editor would and say what you would change. Read the data, not just the vectors — you have the CSV a `.csv` request away, and the values often make the case. **`by-uuid` has no `.csv`**, so for a narrative chart pull the data from its parent chart's slug instead (`grapher/<parent-slug>.csv?country=…&csvType=filtered&time=…`).

Worth looking for, roughly in order of how often it pays:

- **Does the sort serve the story?** A chart ordered by one series reads as a ranking of that series. If the point is variation rather than a ranking, or if the story leads with a different series, say so.
- **Aggregates sitting among countries.** "World", "European Union", income groups: mixed into a country list at their sorted position, a reader takes them for another country. Lift the row to the top and give it a small gap — ~8px, about a quarter of the row pitch — which says "not one of these" without adding any ink:

  ```js
  const rows = bars.children.slice().sort((a, b) => a.y - b.y)
  const pitch = rows[1].y - rows[0].y, topY = rows[0].y
  const agg = rows.find(r => r.name === "World"), i = rows.indexOf(agg)
  for (let j = 0; j < i; j++) rows[j].y = topY + (j + 1) * pitch   // everything above drops a slot
  agg.y = topY
  for (const r of rows) if (r !== agg) r.y += 8                    // the separation
  ```

  Reordering rows changes what the image shows relative to the interactive chart, so treat it as a trial and mirror it in the chart if it's kept.
- **Near-duplicate entities.** Two countries with near-identical profiles spend a row each to say one thing. Dropping one gives the rest thicker bars (not more space — see Step 7) — worth flagging even though it isn't your call.
- **Entities the accompanying text names.** Darkening just those labels (`Text/Gray 100` #2D2E2D against the default #5B5B5B) points the reader at them and costs no space — the fallback worth proposing when a chart is too full for annotations.
- **Wording the guidelines already cover** — "World" → "Global average", units spelled out in the chart area, a title that describes rather than tells (GUIDELINES.md → Titles).
- **Anything the checklist flags** that you can't fix yourself.

Split what you find in two, and be explicit about which is which:

- **Yours to do** — labeling, emphasis, spacing, annotation, anything living in the Figma page. Do it, and show it.
- **The chart author's** — sort order, entity selection, colors, tolerance, the year. Give them a short numbered list with the trade-off spelled out (what it costs, what it buys) and let them decide. Never apply these by editing vectors: the image would stop matching the interactive chart.

If you genuinely have nothing to suggest, say that instead of inventing something. A thin recommendation wastes more of the author's attention than none.

## Step 8c — The checks that must pass before you show it

Every one of these caught a real defect on this skill's first run, and none of them is visible by looking at the frame. Run them as a pass, and report the numbers rather than "looks fine".

| Check | How | Bar |
|---|---|---|
| Color-vision safety | `color_audit.py` | no pair under **ΔE 20** for deuteranopia or protanopia; tritanopia noted, never acted on alone |
| Grayscale survival | `color_audit.py` (grayscale seam section) | **adjacent** pairs above ~**1.6:1**; below that they merge in print |
| Off-palette fills | compare every fill against the library groups | every fill is a library color, **bound as a style** — grapher emits `#585c64` for residual categories, which is in no group |
| Legend agreement | pair swatch→label by geometry, compare against the bars | zero mismatches |
| Text size | read `fontSize` off every text node | nothing below **12px**; annotations on the named ladder |
| Label-on-fill contrast | `contrast(labelHex, barHex)` for every in-bar label | **4.5:1** at 13.5px regular — the 3:1 large-text allowance does not apply |
| Text hierarchy | list every distinct `fontSize` with what it belongs to, **and its rank** | title > subtitle ≥ annotations > supporting text ≥ labels. Sizes may vary inside the plot by rank; a lead annotation may *equal* the subtitle (Annotation XL 16) but nothing may exceed it, and same-rank items must share a size |
| Sizes are named styles | every size matches a style in the file | no arbitrary sizes left over from scaling the export (13.7, 16.8). Choose from the ladder by rank rather than by element type — see GUIDELINES.md → Subtitles and notes |
| Label alignment | compare each label's center against its mark | bar values centered on bars, legend labels on swatches |
| Box alignment | compare the chart's left/right against the header frame | identical to the subtitle box, to the pixel |
| Gap | `(footer.y - headerBottom - chart.height) / 2` | **12–16px**, equal top and bottom |

**Make label-centering part of the build, not a follow-up.** It regressed three times in one run — each rebuild re-hugs the text, which restores the drift, and a separate "now center the labels" step is forgotten or applied to a chart instance that is later replaced. Put the centering loop at the end of the same function that imports, scales and re-hugs, so it cannot be skipped.

**Re-run this whole pass after the last change, not after each one.** Fixes get lost silently: a label-centering pass applied to a chart instance that is later swapped for a re-export leaves the drift back exactly as it was, and every screenshot in between looks correct. And a structural change spends budget elsewhere — lifting an aggregate row to the top added 8px of height, which came straight out of the 12–16px gap and took it to 8.2 without anything reporting a problem. Treat "I already checked that" as false after any re-export, reorder, rescale or restyle.

**A failing check is a finding to report, not a veto.** Measure it, say plainly what fails and by how much, offer the alternatives with their own numbers — then do what the author decides. If they accept the deviation, record it beside the frame and in the report (GUIDELINES.md → Colors) so it reads as a decision rather than an oversight.

Two habits make the difference. **Assert, don't eyeball** — a 1.2px label drift, a 1.18:1 grayscale pair and a scrambled legend all looked perfectly fine in a screenshot. And **re-run the affected checks after every change**, because they interact: applying a text style resets range colors, rescaling rewraps text and shifts label centers, adding an annotation changes the group's width, and swapping one color moves the safety floor to a different pair.

## Step 9 — Checklist pass, review, deliver

1. Run the **Good Data Viz Checklist** (GUIDELINES.md, final section) against the composed frame; fix what fails.
2. `get_screenshot` the new page and show the user — original and adapted version side by side. Iterate on feedback (no re-approval needed within the approved page).
3. **Rename the final frame to the slug** from Step 2 (`child-mortality-asia-decline`) — Figma uses the frame name as the export filename for the website PNG. **Exactly one frame carries the bare slug**; variants get a suffix (`-palette-a`). Two frames with the same name export two files with the same name.
4. **Clear the rejected variants off the page.** Proposal frames accumulate fast — a palette trial, a labeling trial, a layout trial — and a page with four near-identical charts makes the reader work out which one is live. When the user picks, delete what they didn't pick and keep what they asked to keep; a variant kept deliberately is fine, one left behind by accident is not.
5. Do **not** export a PNG by default — the designer usually keeps editing. On request: `get_screenshot` with `maxDimension` at the target size (DI images ship at 2160×2160, i.e. 4× the 540 frame), or let the user export from Figma.
6. Report what was created (page name, frames, edits made) and what remains manual: the no-data pattern and flag plugins, and any design review — **you cannot read Figma comments via MCP, so never report the design review as clean.**

## Gotchas

- **`get_metadata` page listing lies** — it returned only "Cover" for both the Charts and Guidelines files. Enumerate pages via `use_figma` → `figma.root.children`; access known nodes directly by id.
- **`upload_assets`, never `createNodeFromSvg`** — the plugin sandbox has no `fetch`, and inlining an SVG into `use_figma` blows the 50k-char cap. `upload_assets` handles up to 10 MB and yields an editable vector tree.
- **`rescale()`, never `resize()`** on imported charts — `resize` crops instead of scaling children.
- **Figma plugins can't be run from here.** The no-data hashed pattern (Hero Patterns, `4162:5`) and the Flags plugin (`2654:5`) are manual: pre-color no-data shapes `#C9C9C9`, tell the user which manual steps remain.
- **Fonts**: every text edit needs `loadFontAsync` first; the templates use Playfair Display and Lato — if a font is missing in the user's Figma, text edits throw.
- **A text node's `width` is stale for the rest of the script that set its `characters`.** Read it back and you get the *old* width, so any layout computed from it lands wrong — twice in a row, because re-running the same maths in a second script reads the same stale number when the real cause is elsewhere. Two separate things bite here: SVG-imported text arrives at a **fixed** width (the clone of a `22px` value label stays 22px wide and wraps "Poultry" onto two lines), so set `textAutoResize = "WIDTH_AND_HEIGHT"` first; and even then the new width only settles on the **next** `use_figma` call. Write the text and the sizing mode in one call, measure and position in the next.
- **`imType=square` and `imType=uncaptioned` don't render the same chart.** The square re-layout drops per-segment value labels that the uncaptioned crop keeps (and the uncaptioned crop keeps the legend, which is inside the chart area, not the header). Export both and look before deciding which one to embed.
- **`/admin/charts/<id>.svg` doesn't exist**; narrative charts have no public slug — both go through `by-uuid/<uuid>.svg`.
- **Texts come from `.metadata.json`, not `.config.json`** — the latter has no source attribution, omits inherited subtitles/notes, and 404s on MDim slugs. Carry the view's query params on the request.
- **`x`/`y` are parent-relative** — reparent the embed into the template clone before applying the Step 7 coordinates.
- **`?tab=table` silently renders the default tab**; `imSquareSize` is PNG-only; `imWidth`/`imHeight` can't enlarge an SVG (renormalized to ~510k px²).
- **Line charts with >500 points render no dots** (grapher performance cutoff) — don't hunt for dots that were never exported.
- **Never stretch one axis** of the imported chart — dots, squares, and arrowheads distort. Re-export at the right aspect ratio instead.
- **Raising `imFontSize` makes grapher drop labels it can no longer fit.** Bigger type means narrow segments lose their value entirely — Brazil's 7.3% fish label vanished between two exports, and a chart can come back with fewer labels than the one you measured. After changing the font size, check that the specific values an annotation or a recommendation relies on are still present.
- **The Plugin API's shape is not uniform, and guessing costs a round trip.** `figma.getLocalVariableCollectionsAsync` does not exist — variables live under `figma.variables.*`, and this file has paint and text styles but **no color variables at all**, so a variables sweep comes back empty and means nothing. The range setters are **synchronous** (`setRangeFontName`, `setRangeFillStyleId`) while the node-level ones are async (`setFillStyleIdAsync`, `setTextStyleIdAsync`); `setRangeFontNameAsync` is not a method. Read the typings rather than pattern-matching the `Async` suffix.
- **The SVG import renames nodes: spaces become hyphens.** A category displayed as "Beef and buffalo" is the node `Beef-and-buffalo`, so `query('[name=Beef and buffalo]')` finds nothing while the legend text still reads with spaces. Query by the hyphenated node name and map to the label text explicitly — that mismatch is also why the legend has to be paired by geometry rather than by name.
- **`search_design_system` returns about 14 styles per query.** It cannot enumerate a library group in one call, so query each color by name (or query several times with different wording) and resolve hexes with `importStyleByKeyAsync`. Never conclude a group is small because one search returned few results.
- **`get_screenshot` hands back a URL, not an image.** Download it with `curl` and open it with Read — an inline base64 response costs far more context for the same picture.
- **New year, new file** — ask for the link and re-verify every node id in the map above before the first run of a new year.
