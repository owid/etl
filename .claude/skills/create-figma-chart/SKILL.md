---
name: create-figma-chart
description: Turn an OWID grapher chart — given as a slug, a customized grapher link, an MDim view, an admin link, a narrative chart, or just a description — into a templated chart in the design team's yearly "Charts (YYYY)" Figma file. Exports the chart SVG, creates a new page named "YYYYMMDD Title (Author)", places the original chart and an adapted template side by side, replicates title/subtitle/data source/note in the template's styles, fits the chart into the template, proposes better labelling (direct line/bar labels instead of legends) and annotations with the file's curvy arrows, and names the final frame with the kebab-case slug used for the website PNG. Trigger when the user asks to "create a figma chart", "make a static chart in Figma", "prepare this chart for Instagram / as a data insight image", "put this grapher chart into the Charts file", or pastes a grapher/admin/narrative-chart link asking for a designed static version.
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
| InstagramPost_Template_Spanish | `4900:19` | 540×540 | + `ourworldindata_es` handle |
| InstagramPost_Template_Portrait_English | `6689:8` | 560×700 | footer includes a Note line |
| InstagramPost_Template_Portrait_Spanish | `6689:22` | 560×700 | |
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
- Everything else (formats, language, author, slug, topic link) is collected once in Step 2.

## Step 1 — Resolve the chart and gather its text

Get an SVG URL for the chart, whatever form the reference takes:

| Input | SVG URL |
|---|---|
| Slug or default grapher link | `https://ourworldindata.org/grapher/<slug>.svg` |
| Customized grapher link (query params) | insert `.svg` before the `?`, keep the query verbatim: `https://ourworldindata.org/grapher/<slug>.svg?country=USA~CHN&time=1990..latest` — `country`, `time`, `tab`, `stackMode`, `region`, `focus`, … are all honored, and slug redirects work |
| MDim view | same — the dimension params select the view: `.../energy-mix.svg?metric=per_capita&source=coal` |
| Admin link `/admin/charts/<id>/edit` | **`/admin/charts/<id>.svg` does not exist** (it returns the admin SPA shell). Resolve the chart's `configId` — `SELECT configId FROM charts WHERE id = <id>` on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use `https://ourworldindata.org/grapher/by-uuid/<configId>.svg`. Works for unpublished drafts too. |
| Narrative chart (name or admin link) | name → uuid via the unauthenticated map `https://admin.owid.io/api/narrative-chart-map`, then `https://ourworldindata.org/grapher/by-uuid/<uuid>.svg` |
| Description only | find candidates via site search (`https://ourworldindata.org/search?q=...`) or a Datasette title match; show the candidates and confirm before proceeding |

Then pull the chart's texts — title, subtitle, note, source attribution, `originUrl` (the topic page) — from `https://ourworldindata.org/grapher/<slug>.config.json` (or `by-uuid/<uuid>.config.json`). These seed the template texts in Step 6.

## Step 2 — Ask the run options, all at once

One `AskUserQuestion` batch — don't drip-feed:

1. **Output format(s)** (multi-select — several deliverables from one run are normal). Constraint from the design team: **Instagram and DI images are always square/mobile**; a static chart (for the OWID website) can be desktop and/or mobile:
   - Instagram post (square 540×540, English/Spanish) or portrait (560×700)
   - Data insight image (DI_Template, 540×540)
   - Static chart — mobile/square (540×540 or 540×824) and/or desktop (Horizontal 850×638 / Vertical 850×1095; Vertical when the chart needs height — rankings, long bar lists)
2. **Author** — goes into the page name and (static templates) the "Licensed under CC-BY by the author …" line. Default: the user.
3. **Annotations** — should the chart carry annotations replicating what the accompanying text says? If yes, ask for that text (DI draft, article paragraph).
4. **Topic page** for the `OurWorldinData.org/[Topic]` footer line — default from the config's `originUrl`.
5. **Slug** for the final frame — short, kebab-case (`child-mortality-asia-decline`). It becomes the PNG filename when the frame is exported for the website. Propose one; let the user override.

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

Caveats: `?tab=table` is silently ignored (renders the default tab); `imSquareSize` affects PNG only; add `nocache` when re-exporting after a config change.

## Step 4 — Propose, then get the go-ahead

Before touching the file, show the user in one message: the page name **`YYYYMMDD <Title> (<Author>)`** (today's date, the *final* — possibly rewritten — title), the chosen template(s), every text that will go into the template, the labelling changes you propose (Step 8), and the annotations with their content. **Wait for explicit approval.** This is the single checkpoint; after it, iterate freely on the same page without re-asking.

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

4. **Lay out the page**: the original chart on the left; the adapted template **to its right** (~100 px gap). If several formats were requested, keep one original and line the templates up to its right. Move imported nodes with `use_figma` (`page.appendChild(node)`, set `node.x/node.y`).

> **Imported SVGs arrive at their natural size** (850×600 / 540×540). Scale with `node.rescale(target / node.width)` — `frame.resize()` does **not** scale children, it crops.

## Step 6 — Fill the template texts

Replace the lorem-ipsum text nodes in the cloned template. Source everything from the chart config (Step 1) and the user's answers (Step 2):

- **Title** — suggest a more colloquial rewrite per GUIDELINES.md ("Death rate in the United States", not "Death rate, US"); keep the user's final say. The page name uses this final title. **Strip the year from the title** — grapher appends it (", 2023") but the templates don't carry it there.
- **Subtitle** — the chart's subtitle, trimmed to what's necessary. When the chart shows a single year (or a narrow period the reader needs), append **`Data for <YYYY>.`** here — this is where the year lives, not in the title.
- **Data source:** `Data source: <producer> (<year>)` — matching what grapher's own footer shows.
- **Note:** only in templates that carry a Note line, and only if the chart has one worth keeping.
- **`OurWorldinData.org/[Topic]`** → the confirmed topic path (e.g. `OurWorldinData.org/child-mortality`).
- **CC BY** stays; static desktop templates also carry `Licensed under CC-BY by the author <Author>`.

Rules: edit `characters` only — never the template's fonts, sizes, colors, or positions. `await figma.loadFontAsync(node.fontName)` before each text edit. If you need a *new* text block the template doesn't have, **clone the nearest template text node and edit it** — that inherits the correct shared style without hunting style ids.

## Step 7 — Fit the chart into the template

The embed spans the full content width, left-aligned with the title/subtitle/logo box, vertically centered in the space between header and footer:

| Template | Content x / width | Vertical space for the chart (≈) |
|---|---|---|
| 540-wide (IG square, DI, static mobile ex. 1) | x=16, w=508 | y 134–488 (IG, 2-line footer) / y 134–508 (DI, static) |
| Static mobile example 2 (540×824) | x=16, w=508 | y 134–792 |
| IG portrait (560×700) | x=26, w=508 | y 145–630 |
| Static Horizontal (850×638) | x=16, w=818 | y 118–550 |
| Static Vertical (850×1095) | x=16, w=818 | y 116–990 |

Verify against the actual clone with `get_metadata` (the templates evolve; the geometry above is a 2026 snapshot). Then `node.rescale(508 / node.width)` (or 818), set `node.x` to the content x, and center vertically. If the rescaled chart overflows the vertical space, re-export the embed at a flatter aspect ratio rather than squashing — **never stretch one axis** (it distorts dots, arrowheads, and text).

## Step 8 — Improve the labelling and annotate

**Read [GUIDELINES.md](GUIDELINES.md) now if you haven't.** Browse 1–2 recent dated pages in the file (`get_screenshot`) to see how finished charts apply these conventions. The imported SVG is a fully editable vector tree — text nodes, line vectors, legend swatches are all addressable via `use_figma`.

The high-value edits to propose (include them in the Step 4 proposal):

- **Direct labels instead of legends and elbows.** Line charts: put the entity label at the end of its line, colored like the line, and delete the elbow/leader connectors; reclaim the freed right margin for the chart. Area/bar charts: label the series inside the chart area (white ≥12px text on dark fills) and delete the separate legend.
- **Annotations replicating the accompanying text** (12–16px; 10–14px on maps): text color = the annotated object's color, `Text/Gray 80` #5B5B5B, or a mix; bold the key phrase; 2–3px **white outside stroke** instead of a background rectangle.
- **Arrows**: copy curvy arrows from node `798:773` — 1px stroke, arrowhead and line the same color as each other and consistent across the chart. Never scale a whole arrow (it distorts the head): Shift-resize the line segment only, then reposition the head. If a curvy arrow gets messy, use a straight thin line. **Maps: never curvy — straight 1px lines or values inside country shapes.**
- **10×10 px dots** marking highlighted years, with the values written out for the first, last, and any mentioned data point (white-outlined dots on stacked areas; no outline elsewhere).
- **Flags** (`2654:5`) beside country labels/bars where they help; **animals** (`5336:5`) for livestock topics; both are copy/paste.
- **Colors**: only the file's Chart colors library, in the cheat-sheet order; check red/green pairs and black-and-white legibility (GUIDELINES.md → Colors).

## Step 9 — Checklist pass, review, deliver

1. Run the **Good Data Viz Checklist** (GUIDELINES.md, final section) against the composed frame; fix what fails.
2. `get_screenshot` the new page and show the user — original and adapted version side by side. Iterate on feedback (no re-approval needed within the approved page).
3. **Rename the final frame to the slug** from Step 2 (`child-mortality-asia-decline`) — Figma uses the frame name as the export filename for the website PNG.
4. Do **not** export a PNG by default — the designer usually keeps editing. On request: `get_screenshot` with `maxDimension` at the target size (DI images ship at 2160×2160, i.e. 4× the 540 frame), or let the user export from Figma.
5. Report what was created (page name, frames, edits made) and what remains manual: the no-data pattern and flag plugins, and any design review — **you cannot read Figma comments via MCP, so never report the design review as clean.**

## Gotchas

- **`get_metadata` page listing lies** — it returned only "Cover" for both the Charts and Guidelines files. Enumerate pages via `use_figma` → `figma.root.children`; access known nodes directly by id.
- **`upload_assets`, never `createNodeFromSvg`** — the plugin sandbox has no `fetch`, and inlining an SVG into `use_figma` blows the 50k-char cap. `upload_assets` handles up to 10 MB and yields an editable vector tree.
- **`rescale()`, never `resize()`** on imported charts — `resize` crops instead of scaling children.
- **Figma plugins can't be run from here.** The no-data hashed pattern (Hero Patterns, `4162:5`) and the Flags plugin (`2654:5`) are manual: pre-color no-data shapes `#C9C9C9`, tell the user which manual steps remain.
- **Fonts**: every text edit needs `loadFontAsync` first; the templates use Playfair Display and Lato — if a font is missing in the user's Figma, text edits throw.
- **`/admin/charts/<id>.svg` doesn't exist**; narrative charts have no public slug — both go through `by-uuid/<uuid>.svg`.
- **`?tab=table` silently renders the default tab**; `imSquareSize` is PNG-only; `imWidth`/`imHeight` can't enlarge an SVG (renormalized to ~510k px²).
- **Line charts with >500 points render no dots** (grapher performance cutoff) — don't hunt for dots that were never exported.
- **Never stretch one axis** of the imported chart — dots, squares, and arrowheads distort. Re-export at the right aspect ratio instead.
- **New year, new file** — ask for the link and re-verify every node id in the map above before the first run of a new year.
