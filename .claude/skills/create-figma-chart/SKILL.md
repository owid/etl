---
name: create-figma-chart
description: Turn an OWID grapher chart — given as a slug, a customized grapher link, an MDim view, an admin link, a narrative chart, or just a description — into a templated chart in the design team's yearly "Charts (YYYY)" Figma file. Exports the chart SVG, creates a new page named "YYYYMMDD Title (Creator)", places the original chart and an adapted template side by side, replicates title/subtitle/data source/note in the template's styles, fits the chart into the template, proposes better labeling (direct line/bar labels instead of legends) and annotations with the file's curvy arrows, and names the final frame with the kebab-case slug used for the website PNG. Trigger when the user asks to "create a figma chart", "make a static chart in Figma", "prepare this chart for Instagram / as a data insight image", "put this grapher chart into the Charts file", or pastes a grapher/admin/narrative-chart link asking for a designed static version.
metadata:
  internal: true
---

# Create a templated Figma chart from a grapher chart

This skill takes any OWID grapher chart and produces a designed static version in the design team's yearly **Charts (YYYY)** Figma file, following the team's DI Charts Guidelines and the Good Data Viz Checklist.

**The defining principle:** the template is law. You adapt the chart's content *into* the template — you never restyle what the template provides (fonts, colors, spacing, logo, footer layout). Anything you add on top (annotations, direct labels, arrows) uses the file's shared text styles and the Chart colors library, nothing else.

**The single checkpoint rule:** the Charts file is a shared design file other people work in. Nothing is written to it before the user has seen the full proposal (page name, template choice, texts, planned label/annotation edits) and explicitly approved. Reading the file to check conventions needs no permission.

Read [GUIDELINES.md](GUIDELINES.md) (sibling file) before editing any chart — it distills the DI Charts Guidelines per chart type and the Good Data Viz Checklist.

Three sibling skills do the text work this one depends on, and Step 8c calls them: **`/adversarial-data-review`** (is the FAUST true of the indicator, and is the data), **`/check-metadata-style`** (the Writing and Style Guide) and **`/check-metadata-typos`** (codespell). Anything they turn up is an upstream fix in the garden step, not a Figma edit.

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
| "No data" hashed-pattern instructions | `4162:5` | — | Hero Patterns plugin (manual route). Scriptable instead — TILE `IMAGE` fill from `assets/no-data-hatch-tile.png`, see GUIDELINES.md → Flags, animals, no-data pattern |
| Flags | `2654:5` | — | Flags **plugin** — manual; US flags provided in the file |
| Animals | `5336:5` | — | chicken, rooster, turkey, fish, cow, egg-laying hen, pig |
| Good Data Viz Checklist | `20729:1027` | — | distilled in GUIDELINES.md |

Shared styles in the file: text styles `Data Insights/Title` (Playfair Display SemiBold 25) and `Data Insights/Subtitle` (Lato 16); color variables `Text/Gray 100` #2D2E2D, `Text/Gray 80` #5B5B5B, `Website/Text/Blue 100` #002147, `Instagram/Beige Background` #FBF9F3; plus the **Chart colors** library (see GUIDELINES.md → Colors).

The DI Charts Guidelines file (`8gxqkVmZ9x3MK3ky5oigrJ`) is the source of truth behind GUIDELINES.md — six pages: line `0:1`, stacked area `130:35045`, bar/stacked bar `130:35046`, slope `130:35047`, scatter `130:35048`, map `130:35049`. Re-read the relevant page if GUIDELINES.md seems stale.

## Inputs

- **A chart reference**, in any of the forms of the Step 1 table. If the user only describes the chart ("the life expectancy chart with just the US and China"), resolve candidates first and confirm.
- Optionally, **the DI/article text** the chart accompanies — the best source for annotation content. Ask for it if annotations are wanted and it exists.
- Optionally, **a link to a finished page in the file to work like** (see below).
- Everything else (formats, credit, slug, topic link) is collected once in Step 2.

### When you're pointed at a finished page as the model

A designer's own page is a far better spec than this file, so read it before building rather than after. It answers the questions Step 2 would otherwise have to ask, and it answers them in measurements:

- **Which template.** Don't guess from the size — the 540×540 candidates differ on two tells: the **frame fill** (`DI_Template` is `#ffffff`, the Static Chart Mobile templates are cream `#fffbf5`) and the **footer row count** (DI and static carry one row of source + CC BY; the Instagram ones carry two, with the `OurWorldinData.org/[Topic]` line). Whichever text styles the page's title and subtitle are *bound* to settles it.
- **Which export route.** The chart group's height, the span between its first and last gridline, and its font size together identify the export — compare them against a couple of candidate exports rather than reasoning about `imFontSize` from scratch. A gridline span that matches the square render and not the uncaptioned one is conclusive.
- **What was done by hand.** Anything in the page that no export could produce is a design decision to carry over: a **bound library style** (an SVG import can never bind one), a color that isn't in the export, a hidden node, a moved label.

**Every number you read off the reference is a target, not a description of your own export.** This is the trap the whole mode sets: having measured their page you feel informed, and you then report your frame's properties from memory of theirs. A reference showing 1px context lines does not mean the export handed you 1px — it means the designer *set* 1px, and mine had come in at 2px and stayed there through a full verification pass, because I checked the colors I had changed and not the weights I hadn't. **Measure your own frame for every property the treatment names**, including the ones you believe you never touched, and state figures only from that reading.

**Read the reference's small details off a rendered crop, not off its vector geometry.** A node read tells you the values a treatment uses; it does not reliably tell you what the treatment *looks like*, because transformed nodes report geometry in a space you have to reconstruct. Vertex coordinates mapped through a node's bounding box come out flipped or rotated for any node its parents transformed — which put a reference leader's terminal dot at the annotation end when it is plainly on the country, and had me build the elbow inside out (long horizontal dragging across the map, where the reference runs a long vertical with a short jog). Download the reference's screenshot once and crop the detail at 4–8× with Pillow; three crops settled the leader shape, where two rounds of vector reads had argued for the wrong one:

```python
im.crop((x0, y0, x1, y1)).resize(((x1-x0)*5, (y1-y0)*5), Image.NEAREST).save(out)
```

Use the vector read for *what* (weights, colors, caps, which nodes exist) and the crop for *where and which way round*. Cropping both frames into one side-by-side image is also the cheapest honest check that a reconstruction matches.

**Then re-render the chart yourself before assuming the page is reproducible.** A finished page is a snapshot of the chart on the day it was made, and both sides move — the chart's config, and grapher's rendering of it. Where your fresh export and their page disagree, work out which of the two changed before you start matching pixels: it is the difference between a design decision to copy and a chart change to report. (On one run the reference showed a single highlighted line against gray context while the fresh export came back with thirteen colored ones — the answer was that the graying had always been manual, not that anything had regressed.)

**Assume the page is stale, because on a five-page run every one of them was.** Six weeks was enough for: revised values (`$1,394 → $1,331`, `$357 → $330`), a **rounded label that flips** (`0.849%` now prints `0.8%` where the page says `0.9%`), and tick labels grapher has since started dropping at that width. Most importantly, **a claim can go stale, not just a number** — "spend around 60 times as much" was 59× when the page was made and is **62.9×** now, so a faithful recreation reproduces a sentence that is no longer true. Diff the fresh export's texts and values against the page *before* the Step 4 proposal, list what moved, and put any claim that no longer holds in front of the user as a decision rather than quietly copying it.

**When a designer reworks a page you shipped, that rework is the next version of this file.** It is the only feedback on this skill that comes with the answer attached, and it is cheap to read: the page keeps just the surviving frame, so diff it against what you left. Do it with a `use_figma` property dump, not a screenshot — at 540px the changes that matter are invisible. Six classes are worth checking every time, and on one page five of them had moved: **nodes that disappeared** (a whole legend group), **nodes that appeared** (leader arrows), **text content** (a restored category name, a shortened entity, a rewritten subtitle), **fills and their bound styles** (a color moved from the `Default Palette` group to the darker `Line and Slope Charts` one), **sizes and text styles**, and **the frame name**. For each delta ask what constraint the designer was solving, not what they preferred — that is the part that generalizes to the next chart. Then write it here.

**Hand-rounded value labels are house practice on a DI bar chart — expect them and reproduce them.** Both bar-chart references replace grapher's precision with story precision (`1.03% → 1%`, `$7,298 → $7,300`, `$116 → $120`), which means the image deliberately disagrees with the interactive chart. Keep the rounding consistent within a chart, keep a second digit only where 1 dp would collapse two visibly different bars into the same label (`0.35%` beside `0.3%`), and record it as an accepted deviation — it is the kind of thing a later audit reads as an error.

## Step 1 — Resolve the chart and gather its text

Get an SVG URL for the chart, whatever form the reference takes:

| Input | SVG URL |
|---|---|
| Slug or default grapher link | `https://ourworldindata.org/grapher/<slug>.svg` |
| Customized grapher link (query params) | insert `.svg` before the `?`, keep the query verbatim: `https://ourworldindata.org/grapher/<slug>.svg?country=USA~CHN&time=1990..latest` — `country`, `time`, `tab`, `stackMode`, `region`, `focus`, … are all honored, and slug redirects work |
| MDim view | same — the dimension params select the view: `.../energy-mix.svg?metric=per_capita&source=coal` |
| Admin link `/admin/charts/<id>/edit` | **`/admin/charts/<id>.svg` does not exist** (it returns the admin SPA shell). Resolve the chart's `configId` — `SELECT configId FROM charts WHERE id = <id>` on the public Datasette (see the `query-grapher-db` skill), or `GET /admin/api/charts/<id>.config.json` — then use `https://ourworldindata.org/grapher/by-uuid/<configId>.svg`. Works for unpublished drafts too. |
| Narrative chart (**name**) | name → uuid via the unauthenticated map `https://admin.owid.io/api/narrative-chart-map`, then `https://ourworldindata.org/grapher/by-uuid/<uuid>.svg` |
| Narrative chart (**admin link with a numeric id**, `/admin/narrative-charts/<id>/edit`) | **Try the direct lookup first** — `select id, name, chartConfigId from narrative_charts where id = <id>` on the public Datasette hands you the uuid outright (note the column is `chartConfigId`, not `configId`). Only when the id isn't mirrored yet do you need the guessing route below. |
| … the same, when the id is **newer than the Datasette mirror** | there is no id→uuid endpoint, and the mirror lags production by days (it once stopped at 338 while 341 existed). Diff the live name-keyed map against `select name from narrative_charts` to get the unmirrored names, then order them by uuid — they are **uuidv7, so lexical order is creation order** — and count up from the mirror's highest id. That gives a *candidate*, not an answer: ids have gaps where charts were deleted. **Always render the candidate and have the user confirm it before building.** In practice the **name is a far stronger signal than the id arithmetic** — these are named after the piece they serve (`share-of-women-in-parliament-di`), so an unmirrored name matching the DI's topic, *and* carrying the highest uuid, is near-certain. Note the DI page itself is not a reliable route: an older published DI can have `linkedNarrativeCharts: {}` because it ships a hand-made PNG, so the narrative chart you were handed may be newer than the post. Its embedded JSON is still worth reading for `grapher-url`, `authors` and the body text you need in Step 2. |
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
>
> **In that case read *every* text off the SVG, not just the source.** `.config.json` hands back raw detail-on-demand markup, so a note stored as `expressed in [international-$](#dod:int_dollar_abbreviation)` arrives with the brackets attached and you are one hand-edit away from pasting them into the template. The rendered SVG carries the same strings already unwrapped, in `<g id="header">` (title, subtitle) and `<g id="footer">` (sources, note) — one fetch, render-ready, and it is the file you are importing anyway:
>
> ```bash
> .venv/bin/python -c "import html,re,sys; s=open(sys.argv[1]).read(); cut=lambda a,b: html.unescape(re.sub(r'\s+',' ', re.sub(r'<[^>]*>?',' ', s[s.find('>',s.find(a))+1:b]))).strip(); print(cut('id=\"header\"', s.find('id=\"chart-area\"')), '\n\n', cut('id=\"footer\"', None))" chart.svg
> ```
>
> Two details the command has to get right: the footer group sits **after** `chart-area` in the document, so a slice that stops at the plot returns the title and subtitle only and quietly loses the source and note; and the SVG carries XML entities (`&#x27;` for an apostrophe), so unescape before pasting.

**Never shorten the producer's name to make it fit.** The footer string is the producer's official name — verify it against `rg "producer: .*<name>" snapshots/` if you're unsure — and "Food and Agriculture Organization of the United Nations" does not become "UN Food and Agriculture Organization" because the line is too long. When it overruns the CC BY text, wrap it (Step 7) rather than editing it.

**Check that every selected entity actually renders.** The failure is not always a missing *latest* year — an entity can be selected and have **no data at all**: `cereal-yield` carries both `North America` and `Northern America` in its selection and the latter has zero rows, so seven selected entities render six lines, and have done for as long as the chart has existed. Two near-identical region names in one selection is the signature; check the count of drawn series against the selection every time, and report a permanent absence as a chart-config bug (not a regression) so someone removes it. Grapher also silently drops an entity whose data doesn't reach the displayed year, with no warning anywhere — a chart pinned to 2023 quietly showed ten of its eleven countries because one stopped at 2022, and the DI text still discussed the missing one. Compare the **effective** selection against the entity labels in the exported SVG, and if they differ, say so before building: the fix is the narrative chart's tolerance setting (or pinning the year), and it belongs to whoever owns the chart.

  "Effective" is the catch. `selectedEntityNames` in `.config.json` is the *saved* chart's selection, so it is the wrong baseline for exactly the inputs this skill takes most often:

  | Input | Baseline to compare the SVG against |
  |---|---|
  | Bare slug, or `by-uuid` | `selectedEntityNames` from `.config.json` |
  | Link carrying `country=` (or `focus=`) | the **URL's** list — it overrides the saved selection entirely |
  | MDim view | the URL's `country=` if present, otherwise the view's resolved `selectedEntityNames` read from the grapher DB — `multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId` (`/query-grapher-db`) |

  `life-expectancy.config.json` lists `World, Americas, Europe, Africa, Asia, Oceania`, but `life-expectancy.svg?country=USA~CHN` contains only `China` and `United States` — take the config as the baseline there and the check reports six entities missing and two unexpected, on a chart where nothing is wrong. And note the two sides speak different languages: `country=` takes **ISO codes** while the SVG prints **names**, so resolve the codes before comparing rather than diffing the strings.

  **Never use the rendered labels as their own baseline.** For an MDim view with no `country=` it is the tempting shortcut — `.config.json` 404s on MDim slugs (see Gotchas), so the SVG is the only thing in reach. But Step 8c then compares the SVG against the SVG: an entity grapher silently dropped for having no data in this view is missing from **both** sides, so the must-pass completeness check reports success precisely when it should fail. That is the ten-of-eleven defect further down this page, dressed as a green check. Read the resolved selection from the DB, and if you cannot, **report the completeness check as unavailable** for that frame. An honest gap in coverage is worth more than a check that cannot fail.

## Step 2 — Ask the run options, all at once

One `AskUserQuestion` batch — don't drip-feed:

1. **Output format(s)** (multi-select — several deliverables from one run are normal). Constraint from the design team: **Instagram and DI images are always square/mobile**; a static chart (for the OWID website) can be desktop and/or mobile:
   - Instagram post (square 540×540) or portrait (560×700)
   - Data insight image (DI_Template, 540×540)
   - Static chart — mobile/square (540×540 or 540×824) and/or desktop (Horizontal 850×638 / Vertical 850×1095; Vertical when the chart needs height — rankings, long bar lists)
2. **Who is building the chart** — this is the page-name credit, and it is **not** the author of the DI or article the chart accompanies. Default to the user; don't infer it from the gdoc, which names the writer rather than whoever does the design work.
   - **First names only**, matching the file's existing pages: `(Charlie)`, `(Hannah)`, `(Bertha)`.
   - **Disambiguate a shared first name with the last initial** — `(Pablo A)` for Pablo Arriagada, `(Pablo R)` for Pablo Rosado. Both are in use, so a bare `(Pablo)` is ambiguous.
   - **Several people, comma-separated**: `(Bastian, Charlie)`.
   - An organization instead of a person when there is no individual: `(Our World in Data - Global Change Data Lab)`.
3. **The author of the piece**, separately, and only when a **static desktop template** is among the formats — it is the one that carries a `Licensed under CC-BY by the author <Name>` line, and that name is the writer being credited for the work, which is often *not* the person building the chart. Skip the question entirely for DI and Instagram formats, whose footers have no author line.
4. **The DI's own title — or the claim the image is meant to make.** Ask for this whenever a DI or Instagram image is among the formats, and ask *independently of annotations*: grapher's descriptive title must not survive into those images (GUIDELINES.md → Titles), and the story is not yours to invent. If there's no title written yet, ask for the sentence the image supports and derive a candidate from it for approval in Step 4.
5. **Annotations** — should the chart carry annotations replicating what the accompanying text says? If yes, ask for that text (DI draft, article paragraph).
6. **Topic page** for the `OurWorldinData.org/[Topic]` footer line — default from the config's `originUrl`.
7. **Slug** for the final frame — short, kebab-case (`child-mortality-asia-decline`). It becomes the PNG filename when the frame is exported for the website. Propose one; let the user override.

## Step 3 — Export the SVGs

Two exports per format family: the **original** (placed on the page as the reference copy) and the **embed** (chart area only, placed inside the template).

**Export the original now; export the embed only after the template texts are in (Step 6).** The embed's aspect ratio has to match the band between the header and the footer, and the header's height is not known until the real title and subtitle have reflowed it — a three-line title moves the band by ~30px, which is twice the whole gap budget. The order that avoids re-exports is: original → clone the template → fill the texts → **measure the band** → export the embed → import and fit. Every page in this run that skipped ahead needed a second export.

```bash
DIR=/tmp/figma-chart && mkdir -p $DIR   # or the session scratchpad

# Original — desktop templates (850×600 default render)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&nocache" -o $DIR/original.svg
# Original — square/mobile templates (grapher's own square re-layout, 540×540)
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=square&nocache" -o $DIR/original_square.svg

# Embed — chart area only, no grapher header/footer, at the template chart-area aspect ratio
curl -sL "https://ourworldindata.org/grapher/<slug>.svg?<params>&imType=uncaptioned&imWidth=<W>&imHeight=<H>&nocache" -o $DIR/embed.svg
```

`imWidth`/`imHeight` set the **aspect ratio only** — the server renormalizes the SVG to ~510k px², so you cannot request a bigger SVG (irrelevant: it's a vector; you scale it in Figma). Sanity-check what came back:

```bash
head -c 300 $DIR/embed.svg   # expect <svg ... width="..." height="...">, no <html
```

**The aspect you request is the *canvas*, not the chart — solve for the padding or you will re-export every page.** Grapher insets the drawing inside the SVG it hands back, so the group Figma imports is smaller than the declared size, and it is the *group* that has to fill the template band. Measured on this file's charts, the inset is close to **1.4 × `imFontSize` on each axis** (at `imFontSize=32`: declared 901×566 → content 857×520; at 30: 862×591 → 818.9×550). So don't request the aspect you want — request the aspect that *yields* it, by solving

```
(W − 1.4F)/(H − 1.4F) = contentWidth / contentHeight      with   W·H ≈ 510000
```

for `H`, then `W = 510000/H`, and pass `imWidth=round(W/H × 1000)&imHeight=1000`. Taking the declared aspect at face value produced a 336.9px chart where 343 was needed — a 17px gap against a 14px target, on the first page of this run. The model is approximate (±3px), so **measure the imported group and expect at most one correction**; a naive request costs one re-export *per page*.

**Then be ready for the target itself to move: hiding furniture changes the group's aspect.** `connectors` extend to the right of the plot, so hiding them (Step 8) narrows the group and makes it relatively *taller* — the same export that was solved for a 1.6026 content aspect measured 1.5558 once the elbows were hidden, turning a 14px gap into 9.5px. Hide the connectors and the year markers **before** you measure and scale, not after, and re-read the aspect from the group you are actually going to fit.

> **Square charts, second route:** grapher's `imType=square` render re-lays out the chart for a square canvas (legend placement, font sizing tuned by the web team). When that layout is better than the uncaptioned crop — commonly for maps and charts with big legends — import the full square SVG instead and delete its `header` and `footer` groups in Figma after import. Offer both routes; pick per chart.
>
> **For a 540-wide template this is often the route to prefer, not the fallback — export both and measure before choosing.** The square render is already sized for the frame you are filling, and that can remove two whole steps: on one chart its chart area came out ≈505×328 with every label at exactly **15px** — a value on the annotation ladder — so there was no `imFontSize` to tune and no rescale at all (see Step 7 on why not rescaling is worth engineering for). Reaching the same 15px through `imType=uncaptioned` took `imFontSize≈36`, and that export also spent more of the frame on furniture: the same chart came back with a plot **279px** tall against the square route's **294.6px**, and a wider reserved right margin. Compare the two on three numbers — final font size at the template width, plot height, and plot width — rather than on which one is nominally "the embed".
>
> **The check that decides it is the plot's height against the band, and the square route often loses it.** The square export lays the chart out under grapher's *own* header and footer, which are not the template's — so its chart area is sized for a band you are not filling. Across five DI pages the square route came back **314.9px** tall for a **371px** band: a 28px gap at each end, twice the 12–16px target, with no way to close it except a rescale that then breaks the width. The square route wins when its chart area happens to fill your band (short template header, or a map/big-legend chart whose square re-layout is genuinely better); the solved uncaptioned aspect wins whenever it does not. **Measure the band first (Step 7), then pick** — and note the band is only knowable *after* the template texts are in, which is why Step 6 comes before the embed export.

**Size the text at export time with `imFontSize` — scaling in Figma cannot fix it.** Grapher picks a base font for the canvas it renders (`max(10, height/25)`, so ~24 for the default uncaptioned export), and every label is derived from it — the segment values and country names land at about **0.75 × the base**. Placing that export at 508px wide shrinks all of it by the same factor, so a default export ends up with ~12px labels: legal, but on the floor of the 12px minimum. Ask for a bigger base instead — `imFontSize=28` gives ~13.5px labels and ~14px legend text in a 540 frame, which matches the template's own 14px source line. Check the export before importing:

```bash
grep -oE 'font-size="[0-9.]+"' chart.svg | sort | uniq -c | sort -rn | head -3
# multiply the most common value by (508 / the export's content width) to get the final size
```

Bigger text needs more room, so this trades against how much fits — see the axis rule in Step 8 and, failing that, the entity count.

Caveats: `?tab=table` is silently ignored (renders the default tab); `imSquareSize` affects PNG only; add `nocache` when re-exporting after a config change.

## Step 4 — Propose, then get the go-ahead

Before touching the file, show the user in one message: the page name **`YYYYMMDD <Title> (<Creator>)`** (today's date, the *final* — possibly rewritten — title), the chosen template(s), every text that will go into the template, the labeling changes you propose (Step 8), and the annotations with their content. **Wait for explicit approval.** This is the single checkpoint; after it, iterate freely on the same page without re-asking.

## Step 5 — Create the page and place the pieces

> **Load the `figma-use` skill before any `use_figma` call** — hard prerequisite. It covers `loadFontAsync` before text edits and the other plugin-API rules.

1. **Enumerate pages with `use_figma`**, not `get_metadata` — the MCP page listing is unreliable (it returns only "Cover" for this file). Dated chart pages sort newest-first after the instructions/templates pages; insert the new page at the top of the dated block, matching the existing order. **The dated block starts *below* a divider page named `-----------------------------------------`** — find it (`/^-{10,}$/` on the trimmed name) and insert after it rather than at a counted index, or the new pages land among the instruction pages. Watch the off-by-one when you move several: `insertChild(i, p)` removes before it inserts, so re-inserting pages that are already at those indices is a no-op — the reliable fix is to place the pages and then move the *divider* to its own index once.

```js
const pages = figma.root.children.map((p, i) => `${i}: ${p.name}`)
const page = figma.createPage()
page.name = "20260810 Child mortality in Asia (Pablo A)"   // creator, first name + last initial if ambiguous
// move it to the right index with figma.root.insertChild(index, page)
await figma.setCurrentPageAsync(page)
```

2. **Clone the template frame(s)** onto the new page: `(await figma.getNodeByIdAsync("<template-id>")).clone()`, then `page.appendChild(clone)` and position it.

3. **Import the original SVG with `upload_assets`** — never `createNodeFromSvg` (the `use_figma` code param caps at 50k chars; a grapher SVG is ~165 KB). `upload_assets` returns a single-use `submitUrl`; POST the file to it and keep the returned `placedOnNodeId`. **Only the original at this stage** — the embed has not been exported yet (Step 3), and it arrives in Step 7 once the band is measurable:

```bash
curl -s -X POST "<submitUrl>" -F "file=@$DIR/original.svg;type=image/svg+xml"
# → {"success":true, ..., "placedOnNodeId":"<id>"}
```

4. **Lay out the page**: the original chart on the left; the adapted template **to its right** (~100 px gap). If several formats were requested, keep one original and line the templates up to its right. Move imported nodes with `use_figma` (`page.appendChild(node)`, set `node.x/node.y`). This page-level parenting is for the **original** reference chart only — the embed gets reparented into the template clone in Step 7.

> **Imported SVGs arrive at their natural size** (850×600 / 540×540). Scale with `node.rescale(factor)` — never `resize()`, see Step 7.

**Bin the import frame — on every import.** `upload_assets` wraps the SVG in a FRAME that OWID's charts don't have and that causes two failures of its own: it carries a **white fill** that paints over the footer as soon as the frame overhangs, and `resize()` on it *stretches its children through their constraints* — which silently rewraps every text box in the chart, because grapher's exported labels are sized to their glyphs with no slack ("Brazil" becomes "Bra zil"). Move the frame's children out to their real parent and delete the frame; a plain group is what the finished pages use. Only the destination differs: the **original** unwraps onto the page, here in Step 5; the **embed** unwraps into the template clone in Step 7, after its own export.

```js
const unwrap = (imported, parent, name) => {        // parent = the page (original) or templateClone (embed)
  const kids = [...imported.children];
  for (const k of kids) parent.appendChild(k);
  imported.remove();
  const node = kids.length === 1 ? kids[0] : figma.group(kids, parent);
  node.name = name;
  return node;
};
```

## Step 6 — Fill the template texts

Replace the lorem-ipsum text nodes in the cloned template. Source everything from the chart config (Step 1) and the user's answers (Step 2):

- **Title** — for a DI or Instagram image, start from the DI title collected in Step 2, not grapher's; otherwise suggest a more colloquial rewrite per GUIDELINES.md ("Death rate in the United States", not "Death rate, US"). Keep the user's final say. The page name uses this final title. Two or three lines is normal; check the line breaks and the year and highlight-color rules in GUIDELINES.md → Titles.
- **Subtitle** — the chart's subtitle, trimmed to what's necessary. When the chart shows a single year (or a narrow period the reader needs), the image has to state that year somewhere. Append **`Data for <YYYY>.`** here — *unless the title already carries the year*, which is the rule for a year-specific claim (GUIDELINES.md → Titles). One or the other, never both.

  **When the entities aren't all on the same year, state the span, not the exception.** A subtitle that names the odd one out — `Data for 2023, except Japan (2022).` — spends a clause on a caveat no reader acts on, and invites the same treatment for the next straggler. Append the range to the sentence with a comma instead: `Breakdown of meat supply in a given country by type, 2022–2023.` It is shorter, it is true of every entity, and it keeps the year where the single-year form puts it. Use an en dash.
- **Data source:** `Data source: ` + `chart.citation` from Step 1 — that field *is* grapher's own footer line, so don't re-derive a `<producer> (<year>)` string by hand, and don't abbreviate it to save space. A long producer name overruns the CC BY text at x=468. **Give the source its own full line and move CC BY to the row beneath it** — the source stays one unbroken line, which reads better than a wrap, and the template's own two-row footers (the Instagram ones) already use exactly this geometry:

  ```js
  const W = footer.width, BOTTOM = footer.y + footer.height;   // read off the clone, before resizing
  source.textAutoResize = "WIDTH_AND_HEIGHT";   // one line, its natural width
  source.x = 0; source.y = 0;
  ccby.x = 0;  ccby.y = 20;                     // left-aligned under the source, template row pitch
  footer.resize(W, 36);
  footer.y = BOTTOM - footer.height;            // grow upward; the template's bottom margin is untouched
  ```

  **Take `W` and `BOTTOM` from the clone rather than typing them.** They are 508 and 524 in a 540-wide template, but the content is 818 wide in the 850-wide ones and the footer edge differs in every template (Step 7's table) — hardcoding the square template's pair narrows the content and lifts the footer off the bottom everywhere else. Then re-fit the chart into what's left (Step 7). Only if the source is too long even for a full line — beyond the template's content width — wrap it with `textAutoResize = "HEIGHT"` at a width that breaks after the organization's name, and top-align CC BY with its first line. Either way CC BY is **left-aligned** once it has its own row — it only sits at x=468 while it shares the source's line.

  **Simpler still: move the source *up* by one row pitch instead of resizing the footer** — `source.y = -20` inside the untouched footer frame, CC BY left at `y = 0`. The footer keeps its own geometry (so nothing else in the template shifts), and the band's real bottom becomes `footer.y + source.y`, which is the number to feed Step 7. That is how the file's own finished pages do it.

  **And know when *not* to spend the second row.** A source that overruns CC BY by only a few pixels is not worth 20px of chart: the full FAO name at the Source style's 14px measured 473px against CC BY starting at x=468 — a 2px overlap — and the finished page's answer was to set that one line to **13px** and keep the footer one row deep. Weigh the two costs explicitly (one off-ladder size on the least important text, versus a fifth of the gap budget and a re-export) and record whichever you pick.
- **Note:** only in templates that carry a Note line, and only if the chart has one worth keeping. **DI images normally carry no note at all** — drop it, or, when it's genuinely load-bearing for understanding the chart, fold it into the subtitle as a bolded second line (only if the subtitle isn't already crowded).
- **`OurWorldinData.org/[Topic]`** → the confirmed topic path (e.g. `OurWorldinData.org/child-mortality`).
- **CC BY** stays; static desktop templates also carry `Licensed under CC-BY by the author <Name>` — the author of the piece from Step 2, not the page-name credit.

Rules: replace `characters`, and leave the node's **base** styling alone — the fonts, sizes, colors, and positions are the template's, not yours. `await figma.loadFontAsync(node.fontName)` before each text edit. If you need a *new* text block the template doesn't have, **clone the nearest template text node and edit it** — that inherits the correct shared style without hunting style ids.

**Watch for template text that is already mixed-weight, and restore it after writing.** Setting `characters` propagates the *first character's* style over the whole new string, so any node whose label is bolder than its content comes out uniformly bold. The source line is the one that bites — the templates ship `Data source:` in Bold and the attribution in Regular — so write the string, then push Regular back over the tail:

```js
const PREFIX = "Data source:";
src.characters = PREFIX + " " + citation;
src.setRangeFontName(0, PREFIX.length, {family:"Lato", style:"Bold"});
src.setRangeFontName(PREFIX.length, src.characters.length, {family:"Lato", style:"Regular"});
```

Read the segments back (`getStyledTextSegments(['fontName'])`) and compare against the untouched template node — a wholly-bold source line looks deliberate enough that nobody catches it in a screenshot.

Two **range-level** exceptions the guidelines actively require, applied after the characters are in place and scoped to just those characters — never to the whole node:

- the title's highlight word → `setRangeFills`, in the exact color of the marks it names (GUIDELINES.md → Titles);
- a load-bearing note folded into the subtitle as a bolded second line → `setRangeFontName` to the family's bold weight, which needs its own `loadFontAsync` (GUIDELINES.md → Subtitles and notes).

Nothing else gets restyled.

## Step 7 — Fit the chart into the template

The chart spans the full content width, left-aligned with the title/subtitle/logo box, and sits in the band between the header and the footer with an even gap top and bottom.

**This is where the embed arrives.** The band's edges — `headerBottom` and `footerTop` — don't depend on the chart, so read them first, solve the export aspect against that band (Step 3), *then* export the embed, import it, and unwrap it into the template clone with the `unwrap` helper from Step 5. Fitting comes after. That ordering is the whole reason the embed waited this long.

**Measure that band; don't hardcode it.** The header's height depends on how many lines the title and subtitle take, so a fixed y is wrong as soon as the subtitle wraps — and centering inside a guessed band leaves a lopsided result (18px above, 6px below on the first run of this skill). Read the real edges instead:

```js
const headerBottom = header.y + header.height       // Frame 14: title + subtitle + logo
const footerTop = footer.y + Math.min(0, source.y)  // the footer's first *visible* ink, not its frame top
const gap = (footerTop - headerBottom - chart.height) / 2
chart.x = header.x
chart.y = headerBottom + gap
```

**`footer.y` is not always the band's bottom — the source row can sit above it.** Step 6's simpler two-row footer leaves the footer frame alone and moves the source *up* inside it (`source.y = -20`), so the first ink the reader sees is 20px above `footer.y`. Solve against the frame top there and a nominally 14px gap puts the chart 6px into the source row. Take the bottom from the footer's topmost visible child — `footer.y + Math.min(0, source.y)` covers both routes, since the resize route leaves `source.y = 0` and grows the frame upward — and carry that same `footerTop` through the fit in Step 8 and the gap audit in Step 8c.

**Fit to the band's *height*, then map x to fill the width — not the other way round.** `rescale(header.width / chart.width)` is the obvious move and it is the wrong one on any chart with a reserved label margin: it locks the width, leaves the height wherever the export's aspect fell, and hands you a gap you cannot fix without re-exporting. Fitting the height instead makes the gap correct by construction *and* lets you choose the scale so the labels land exactly on the ladder (`rescale(15 / currentFontSize)` — see Step 8c), after which a scripted x-map takes the plot out to the content width without touching a single font size.

**Pick the ladder value nearest the export's own size — usually the one *above*, not below.** The ladder is there to remove arbitrary sizes, not to shrink the chart: an export whose labels come out at 15.4 or 15.75 wants **15**, and choosing 14 because it made some other number come out round costs a size step on the most-read text in the plot. Two frames here went to 14 that way and a reviewer asked for them back at 15, which is also what the finished pages use. Compare against the reference or the raw export before committing to a rung. On this run that one reordering removed the last re-export from four of five pages:

```js
chart.rescale(TARGET_H / chart.height)      // TARGET_H = band − 2×14; sets every font size
// ...then the x-map below brings the plot out to the full content width
```

**Anchor the x-map in closed form, from the label widths — never on a re-measured plot edge.** Recomputing the plot's left/right from the tick marks after each pass makes the target drift, because the labels you just resized move the group's bounding box: three successive "stretch to 524" passes landed at 519.3, then 527.3, then 524 only once the arithmetic stopped depending on its own output. Solve both edges up front and map once:

```js
const plotLeft  = contentX + 6 + Math.max(...yTickLabels.map(t => t.width))
const plotRight = contentX + contentW                    // the last x tick label is right-aligned on it
const s = (plotRight - plotLeft) / (R - L)               // L, R = current tick-mark extremes
const mapX = x => plotLeft + (x - L) * s
```

Then place the y tick labels at `plotLeft − 6 − t.width` and re-anchor each x tick label on its own mark (Step 8). Verified this way, every tick delta on five charts came back exactly `0.000`, and the group's width came back exactly `508`.

**How much gap is right: 14px, and 12–16 is the comfortable band.** That's what the finished pages and grapher itself converge on, measured in 540-wide frames — grapher's own square export leaves 13px above the plot and 14px below; recent DI pages in the file sit at 14/19, 15/14 and 7/15. Below ~10px it reads cramped and the legend starts to look like part of the subtitle; above ~20px you are wasting space the plot could use. When the chart comes out a few pixels too tall, spend the slack down to 12px a side **before** shrinking it — that is usually enough, and it keeps the full content width, which matters more than the last pixel of gap.

**The 12–16 band assumes the chart group still contains its axis furniture — once you measure the group tightly, the same picture reports a much bigger gap.** Trimming the dangling reference lines and hugging the label boxes to the ink (Step 8) removes ~10–25px of invisible slack from the group's bounding box without moving a single pixel of ink, and the gap number jumps: **20px** on a 14-row bar chart, **30px** on a 4-row one, both of which look wrong against the band and are in fact correct. The tell is that the equivalent measurement on the reference page agrees (17/19 and ~32/33 there). So on an axis-less chart — a discrete bar chart with every value labelled — measure the gap on the reference too and match *that*, and record the figure with a note that the group is tightly measured. Do not shrink a correct chart to force a number.

**A reference line wants a small overhang past the bars — bounded, and symmetric.** Grapher's plot area is taller than the bars it contains, so the inherited `vertical-zero-line` runs well past the last bar and reads as pointing at the footer; a reviewer described it as "going down and even overlapping the data source". But **trimming it flush to the bars is the other error** — the overhang is the design, and cutting it makes the baseline look clipped. Give it about **4px each way**, and let a guide line you add yourself lead in a little higher (~12px) so it reads as annotation rather than as part of the axis, ending level with the zero line:

```js
const top = bars[0].y, bot = bars.at(-1).y + bars.at(-1).height, OVER = 4, LEAD = 12;
zero.resize(zero.width, (bot + OVER) - (top - OVER)); zero.y = top - OVER;
guide.vectorPaths = [{ windingRule: "NONE", data: `M 0 0 L 0 ${(bot + OVER) - (top - LEAD)}` }];
guide.y = top - LEAD;
```

Then state the clearance from the line's bottom to the source row in the report (16px here) — that is the number the complaint was really about, and it is not visible in a gap measurement taken on the chart group.

**The 12–16 band is for the 540-wide frames. The Instagram portrait runs at 30.** It is 700px tall with the same 508px of content width, so a 14px gap there reads as a chart jammed against its own header; the finished portrait pages in the file sit at exactly 30px top and bottom. Take the band figure from the template you are filling, not from the last chart you made.

**A GROUP's `x`/`y`/`width` are derived from its contents, so they move whenever you edit a label.** Rounding four value labels and dropping them from 15.75px to 14px silently walked a perfectly fitted chart from `x=16, w=508` to `x=18, w=505` — the group shrank around its narrower content and Figma re-origined it. So **assert the box after the last content edit, not after the fit**, and close any residual gap by re-laying out the plot (below), never by another `rescale()`, which would undo the font sizes you just set.

**When the plot and its legend are separate elements, the gap between them is its own decision — and a minimal one is wrong.** A legend strip sitting 16px under a map reads as part of the graphic, a caption bar welded to the bottom edge, rather than as a key you consult; the coastline and the colour band start competing. **26px on a 540-wide frame** is what worked here. But don't take grapher's lead and over-correct: its own square export leaves ~57px, which detaches the legend and lets it drift toward the source line. The rule that settles it is **proximity as grouping — map→legend must stay clearly smaller than legend→footer** (26 against 45 here), so the key reads as belonging to the chart and the footer reads as separate.

**And where the slack goes is a design decision, not a residue.** A chart that cannot fill the band — a wide map in a square frame — leaves a fixed surplus (≈116px here) to distribute across three gaps, and "centre the block and leave the middle minimal" is a choice you made by default rather than on purpose. Take an increase in the internal gap out of the **outer** gaps, never out of the chart: the plot keeps its full size and the frame stays symmetric.

Two mechanics when you re-space: the annotations and leaders are **siblings of the chart group, not children**, so they do not travel with it — translate them by the same delta or every label lands over the wrong geography. Then re-verify that each leader still ends inside the thing it points at; that check is cheap and catches a mistranslation immediately.

Side margins and the footer edge are the template's, not yours: content starts at the header's `x` and the footer's bottom stays where the template put it.

| Template | Content x / width | Header bottom → footer top (unwrapped subtitle) |
|---|---|---|
| 540-wide (IG square, DI, static mobile ex. 1) | x=16, w=508 | 118 → 508 (DI/static) or 488 (IG, 2-line footer) |
| Static mobile example 2 (540×824) | x=16, w=508 | 118 → 792 |
| IG portrait (560×700) | x=26, w=508 | 135 → 640 |
| Static Horizontal (850×638) | x=16, w=818 | 118 → 556 |
| Static Vertical (850×1095) | x=16, w=818 | 116 → 997 |

Verify against the actual clone with `get_metadata` (the templates evolve; the geometry above is a 2026 snapshot). These are **frame-local** coordinates, and `x`/`y` are relative to a node's parent — so append the embed to the template clone **before** positioning it. Left parented to the page (where Step 5 puts imported nodes), the same numbers land it near the page origin, on top of the reference chart. One wrinkle in the same rule: **a GROUP is transparent for coordinates**, so once the imported chart is inside the template, its descendants report `x`/`y` in the *template frame's* space, not the group's — which is what makes the frame-local numbers above directly usable on the plot's internals.

**The header reflows itself — don't reposition it.** `Frame 14` is a vertical auto-layout and `Frame 13` (title + logo) a horizontal one, so a title that grows from two lines to three pushes the subtitle down and grows the header on its own. Set `characters`, then **read the new `header.y + header.height` back** and measure the band from that; any y you computed before the text went in is stale.

**Prefer reaching the content width without `rescale()` at all.** `rescale()` multiplies font sizes along with geometry, so a 1.006× nudge to close a 3px gap silently moves every label from 15px to 15.09 — off the ladder, and the Step 8c "sizes are named styles" check then fails on a difference no one can see. When the width can be closed another way — the label reclaim above is the usual one — take that route and every size stays exactly where the export put it.

**Anything you add to the chart aligns to the same box as the subtitle** — annotations, captions, notes all start at the content left edge and may run its full width. Aligning them to the bars' left edge instead leaves a ragged inner margin that reads as a mistake.

**But size them against the plot's own bounds, not the group's.** An annotation is a child of the chart group, so the moment it is wider than the plot it *becomes* the group's width — and a width-first `rescale(header.width / chart.width)` then scales the plot down to make room for it (a 508-wide group silently became 527). Measure the plot by walking the group and skipping the annotation nodes, size the annotations to that, and only then fit:

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

**Match the header box exactly — same left edge and same width.** A chart even a few pixels narrower than the title reads as a mistake. Read the target box off the header rather than off a constant, and do it *after* the frame is gone, so the group's bounding box is the plot's real extent and no export padding is baked into the width:

```js
const header = clone.children.find(c => c.name === "Frame 14")   // title + subtitle + logo
const contentX = header.x, contentW = header.width               // the box to match
chart.rescale(TARGET_H / chart.height)                           // height-first; never resize()
chart.x = contentX                                               // same left edge
chart.y = top + (bottom - top - chart.height) / 2                // centered between header and footer
// then the closed-form x-map above takes the plot out to contentX + contentW
```

**The width is the x-map's job, not a second `rescale()`.** `chart.rescale(header.width / chart.width)` is the width-first move this step opened by rejecting, and reaching for it here re-multiplies every font size and re-breaks the band gap the height-fit just made correct. `rescale()` on the group is otherwise safe where `resize()` on a frame is not (see Step 5) — so spend it on the one height-fit and close the width by mapping x. If the height-fitted chart still overflows the vertical space, re-export at a flatter aspect ratio rather than squashing — **never stretch one axis** (it distorts dots, arrowheads, and text).

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

**Re-anchor to the marks, not to a remembered box width.** The snippet above is the fallback for when nothing addressable is nearby; wherever the export gives you the mark, drive off it, because then no amount of re-hugging or stretching can accumulate error. On an axis every anchor is already in the tree: the `tick-marks` group carries one zero-width vector per tick named after its value, and `horizontal-grid-lines` one per gridline, so tick labels align on their mark (grapher **left**-aligns the first and **right**-aligns the last to keep them inside the plot, everything between centered) and value labels right-align on the axis edge. Verified that way, all six tick deltas come back exactly 0 rather than approximately 0.

**On an axis, expect a uniform ~1px vertical offset and leave it — but know the bound, because a *large* uniform offset is a real defect.** Grapher positions text by baseline, and digit-only labels have no descenders, so an axis label's visual center sits slightly below its box center: ~1.2px above its gridline, by construction. Uniform and small is fine; uniformity alone is not the test. Bar labels measured **5.46px** above their bars' centers on every row of a 14-row chart — perfectly uniform, and visibly high on the render, which is what a reviewer noticed first.

**The fix, and the rule for anything labeling a mark rather than an axis: trim the box to the ink, then center on the mark.** `leadingTrim = "CAP_HEIGHT"` drops the line box's leading (a 14px label went from 18px tall to 10px — the 8px of leading was the whole error), after which box center *is* ink center and `label.y = markCenter − label.height/2` lands at exactly 0.00 on every row. Do it for the value labels **and** the entity labels, since both read against the same bar:

```js
for (const t of [...valueLabels, ...entityLabels]) t.leadingTrim = "CAP_HEIGHT";   // one call
// ...next call, once the heights have settled:
for (let i = 0; i < bars.length; i++) {
  const mid = bars[i].y + bars[i].height / 2;
  valueLabels[i].y = mid - valueLabels[i].height / 2;
  entityBlocks[i].y = mid - entityBlocks[i].height / 2;    // a wrapped name is a GROUP — center the block
}
```

Two consequences to expect. The trim is **height-only for a left- or right-aligned label**, so `x` needs no repair. And it shrinks the chart group, which changes the gap — see the band caveat in Step 7.

## Step 8 — Improve the labeling and annotate

**Read [GUIDELINES.md](GUIDELINES.md) now if you haven't.** Browse 1–2 recent dated pages in the file (`get_screenshot`) to see how finished charts apply these conventions. The imported SVG is a fully editable vector tree — text nodes, line vectors, legend swatches are all addressable via `use_figma`.

The high-value edits to propose (include them in the Step 4 proposal):

- **Direct labels instead of legends and elbows.** Line charts: put the entity label at the end of its line, colored like the line, and delete the elbow/leader connectors; reclaim the freed right margin for the chart. Area/bar charts: label the series inside the chart area (white ≥12px text on dark fills) and delete the separate legend.

  **This is not a free win on a stacked chart — check that it beats the legend before proposing it.** Direct labeling works when every label can sit *on the mark it names*: over its own segment of the top bar (the pattern in [this DI](https://ourworldindata.org/data-insights/most-collected-waste-in-many-low--and-middle-income-countries-is-stored-in-open-dumps-or-is-burned), where colored category labels sit above the first row and the widest series is labeled in white inside the bar), or inside the widest segment of each category. Judged as single-line labels laid end to end, that caps out at three or four categories — and that is the wrong test. **Six fit**, on a 100% stacked bar whose two smallest segments were 1% slivers, once the labels were allowed to tier, wrap and point (recipe below); a designer reworked this skill's own six-category legend into exactly that, and it is the stronger chart. What is genuinely disqualifying is a different move: spreading the labels evenly across the plot rather than over their own segments, which yields a color-coded legend that is *harder* to read than the real one — the reader has lost the swatch and gained nothing. Try the tiered version first, and when even that doesn't fit, keep grapher's legend and say why; a conventional legend is not a failure to improve the chart.

  When it *does* fit, the reliable recipe is: for each category, find the row where its segment is widest, **clone that segment's existing value label** (the clone inherits the right font, size and — importantly — the black-on-light vs white-on-dark fill grapher already chose), set its characters to the category name, then center the `[name, 4px, value]` pair on the segment. To rebuild a legend you removed too eagerly: recolor the labels to `Text/Gray 80` #5B5B5B, add a 10×10 swatch in each category's own color 4px to their left, and lay them out in grapher's own split — as many as fit on the first row, the longest alone on the second.

  **Labels over the reference row: anchor, tier, wrap, point.** The four moves that took six categories past the "they don't fit side by side" cap, measured off [a designer's rework](https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=24853-5) of a page this skill had produced with a conventional legend:

  - **Anchor each label to its segment's edge, not its center.** Left-align to the left edge of that category's segment in the reference row (the top row — `World`, or whichever row the chart is read from); right-align the *last* category to the bar's right edge. Centering drifts a label off a narrow segment and reads as pointing at its neighbor.
  - **Tier vertically rather than shrink.** Labels that fit above their own segment sit closest to the bar, bottom ~6px above it; two-line labels keep that same bottom band, so they start higher; any label that has to overhang a neighbor's territory goes up to a third tier ~33px above the bar. Three tiers de-collided six labels sharing 440px, with no label smaller than the values.
  - **Wrap rather than shorten.** A label wider than its own segment — or landing within ~10px of the next one — wraps to two lines: fixed width ≈ the segment's, `textAutoResize = 'HEIGHT'`, and an explicit **15px line height at 14px type**, tighter than the style's AUTO. That detaches the node from the text style, which is fine; keep the *fill* bound to its library color.
  - **Point at what you cannot label.** For a segment too narrow to carry anything — a 5px sliver at 1% — lift its label to the top tier and drop a **0.7px `Data Insights/Annotations` gray line ~27px long with an `ARROW_EQUILATERAL` cap at the bar end**, starting ~2px under the label and stopping ~4px above the bar, at the sliver's x (take it from the label's anchored edge — left-aligned label, left edge; right-aligned, right). Two of these carried the two smallest of six categories. Draw it as an explicit vertical path rather than a rotated `createLine()` (Gotchas), and set the arrowhead **per vertex** on the `vectorNetwork` — `node.strokeCap = 'ARROW_EQUILATERAL'` caps *both* ends and gives you a double-headed arrow. This is what replaces the old advice to abandon direct labeling when a category is too small to hold a label.

  **A direct label is colored *exactly* like the segment it names, which makes readability-as-text a palette constraint.** A legend swatch can be any color, because the word beside it is gray; a direct label *is* the color. So the palette moves with the labeling: two of the six **bars** ended up on `Line and Slope Charts` variants (`Camel #996d39`, `Peach #c4523e`) rather than their `Default Palette` counterparts, which is the group that exists for exactly this — one bound style then serves the fill and the label both. It is the rule already stated for annotation words, applied one level up, to the palette itself. Check it in both directions: the color clears 4.5:1 as text on white, *and* the white value label inside the bar clears 4.5:1 on it. If a category's color can only satisfy one, it is the palette that has to move, not the label.

  **Size them with the value labels, not above them** (`Data Insights/Annotation M`, 14px on a 540 frame). Once the label sits next to the mark it names it is at the same rank as the numbers on the bars, and same rank means same size (Step 8c → Text hierarchy). A legend strip is the thing that needed to be a step larger to hold its own away from the plot.

  **What it buys beyond the freed legend row.** The labels stop competing for one row's horizontal budget, so category names no longer have to be shortened to fit — the rework restored `Beef & buffalo` where the one-row legend had forced `Beef`, and that is a factual gain, not a cosmetic one (Step 8c: a shortened label is a claim about the data). The rework also took `and` → **`&`** across every category label, which pays back most of the width a restored name costs — the file's older legend samples still spell out "and", so prefer `&` when width is tight rather than treating either as a rule.

  **On a line chart, grapher has already done most of the work and left you three moves.** The export ships the labels as a `text-labels` group and the elbows as a sibling `connectors` group, so the first move is one line: hide `connectors`. The other two are where the value is:

  1. **Re-place each label against its line's endpoint, which the connectors encode.** Each connector's bounding box spans *line end → label center*, so the end **further** from the label's current center is the line end — that is your target, and it is the only place the endpoint is recoverable from, since a path's bbox won't tell you which corner the line arrives at. Then de-collide with a **minimum pitch of the font size × 1.33** (20px at 15px labels) by relaxing overlaps half-and-half until stable; that converges on minimum total drift, and it reproduced a designer's hand-placement of the same chart to within a pixel (worst label 8.9px off its line against their 9.5px).
  2. **Reclaim the freed right margin — and note that the *longest label* is what caps the reclaim, so shortening the longest labels is the lever, not deleting the elbows.** Grapher sizes the margin to fit its widest label, so on a chart where "United Kingdom" is present the label block cannot move right at all; shortening that one and "United States" to **UK** and **US** made "Switzerland" the constraint and bought 30px of plot. The arithmetic is exact: `LABEL_X = content_right − max(label widths)`, then `plot_right = LABEL_X − 5`, and the chart's own width comes out equal to the header's for free.

  **Placing direct labels is a constrained search, not an offset — and "clear of its own line" is not the test.** Putting each label at a fixed offset from its anchor (say `startX + 5`, centered) reads fine in a node listing and lands labels **on top of other entities' lines**, which is the first thing a reviewer sees. Make it a search instead: per label, generate candidate slots and accept the first that passes every acceptance test, with the polyline test (Step 8c) doing the real work.

  - **Candidates** — beside the anchor and on both sides of the line, at several vertical offsets: `left-of-anchor` (centered), `above`, `below`, each also at ±10 and ±22px. On a convergence chart the anchor is the line's *first* point (GUIDELINES.md → Line charts); otherwise its last, or a fraction along it.
  - **Acceptance** — inside the plot; no overlap with any already-placed label (+2px); and **crosses no line's sampled polyline, its own included**.
  - **Order** — leftmost/earliest anchor first, so the labels with the most empty space around them commit before the crowded ones.
  - **Obstacles first.** An annotation's position is a design decision, so seed the placed-set with its box **before** placing any label. Skip that and a label lands under the knockout and is simply erased — which happened here, and is invisible in every measurement that doesn't test for it.
  - **Report forced placements.** If no candidate passes, fall back to the first and say so; `forced: 0` is the line worth putting in the report.

  Six candidates × five labels resolved two charts here with zero forced placements, including a pair whose lines start at the same year and needed one label pushed 22px down.

  **Apply the stretch as a scripted x-map, never as a group `resize()`.** Map `x → L + (x − L) · s` over the `tick-marks`, `horizontal-grid-lines` and `lines` subtrees, scaling each vector's width by `s`; **skip TEXT entirely** (re-anchor it afterwards) and map the *center* of the year markers while keeping their size, so dots stay round — verified 6×6 after a 1.17× stretch. A `resize()` on the group would rewrap every label through its constraints and oval every dot.

  ```js
  const mapX = x => L + (x - L) * s;
  const stretch = n => {
    if (n.type === 'TEXT') return;
    if (n.children?.length) return n.children.forEach(stretch);
    if (/^\d{4}$/.test(n.name) && n.width < 8) { n.x = mapX(n.x + n.width/2) - n.width/2; return }  // year marker
    n.x = mapX(n.x);
    if (n.width > 0.01) n.resize(n.width * s, n.height);
  };
  ```

  **Hiding a series beats deleting one when the labels won't fit** — it is reversible in a click and a reviewer can see what was taken out — but it still changes what the image shows relative to the interactive chart, so it stays a chart-author decision you surface rather than take. Say what it bought: five labels needing 100px of pitch across ~70px of line endpoints is a real collision, and dropping one is one of the two fixes (the other is accepting the drift).
- **Any chart with an entity column reserves it for the longest name, so shortening that one name is plot width.** **`United Kingdom` → `UK` and `United States` → `US`** are the two standing abbreviations — both are universally read, neither needs explaining (the "no unexplained abbreviations" line in the checklist is about the others), and between them they are the longest name on a large share of OWID charts. On a **stacked** bar chart the entity column sits at the left and grapher sized it for `United States`; taking that to `US` moved the bars ~28px left and cost nothing. Reach for it before you rescale — the reflex when a name is clipped or the plot is a few pixels too wide is to shrink the chart, which shrinks every label with it. It applies wherever the name is set in type: the left column of a bar chart, the end-of-line labels on a line chart (where the same swap bought 30px), and a legend or direct label naming a country. The **title** is the exception and spells the country out — "Death rate in the United States", not "Death rate, US" (GUIDELINES.md → Titles).
- **On a ranked bar chart, the same reclaim is available and it is pure profit — grapher sized the gutter for labels and values you have since replaced.** The label column is wide enough for the longest *un-shortened* entity name and the value column for the *unrounded* numbers, so the moment you shorten `United Kingdom → UK` and round `1.03% → 1%` (Step 6), that reserved space is dead. On a 14-row chart it was **36.8px, 7% of the plot**. The transform is closed-form, distorts nothing (every bar scales by one factor, so the value→length mapping stays linear through zero) and lands the group on the content box exactly. **It assumes every bar is nonnegative and grows rightward from a shared zero** — the usual shape of a ranked bar chart, and the only shape the loop below is correct for. With negative or diverging values it reverses them, in three places at once: it pins every bar's left edge to `newZero`, budgets the available width to the right of zero only, and puts every value label on the right. For those charts, keep each bar's sign — give each side of zero its own budget, and mirror `x`, width and label side per bar.

  ```js
  const newZero = LEFT + Math.max(...entityLabels.map(e => e.width)) + G;      // G = 6
  let k = Infinity;                                                            // longest row caps the stretch
  for (let i = 0; i < bars.length; i++)
    k = Math.min(k, (RIGHT - newZero - G - valueLabels[i].width) / bars[i].width);
  zeroLine.x = newZero;
  for (let i = 0; i < bars.length; i++) {
    bars[i].resize(bars[i].width * k, bars[i].height);
    bars[i].x = newZero;
    valueLabels[i].x  = newZero + bars[i].width + G;
    entityLabels[i].x = newZero - G - entityLabels[i].width;   // right-aligned on the zero line
  }
  ```

  Two details. **Entity labels can be GROUPs, not TEXT** — grapher wraps a long name onto two lines and groups them, so drive the loop off `entity-labels`' *children* (each child is one row's block, whatever its type) and set sizes via `.query("TEXT")`. And **anything derived from the plot's scale has to be recomputed afterwards**: a target/reference guide line must be re-placed from the new bar widths, or it will still be sitting at the old scale's position.

- **A computed guide line comes from the data value, not the printed label.** A "0.7% target" line is `zeroX + 0.7 × (bar.width / trueValue)` where `trueValue` is the entity's *actual* number (Norway's 1.0307%), not the `1%` its rounded label shows — using the label put the line 8px off. Computed from the true value it landed at x=370.3 against a designer's hand-placed 371, which is also the cheapest confirmation that your whole x-scale is right.

- **On a map, trimming sub-pixel territories is worth real canvas — but hide them, never delete them, and never prune before importing.** A world map's bounding box is set by its most remote specks, and a country that straddles the antimeridian is drawn on *both* edges, so one invisible island chain can double the width (Fiji: a 6.9px speck spanning x 6→954 of a 951-wide map). Pruning those buys width the continents get to use. Three rules make it safe:
  - **Hide (`visible = false`), don't delete.** Hidden children do **not** contribute to a Figma group's bbox, so hiding buys exactly the same width as deleting while staying reversible in a click and legible to a reviewer. Park them in the map's own subgroups under an explicit name (`United-States__Hawaii`) so what was excluded is a fact in the file, not a diff nobody can see.
  - **Prune in Figma, not in the SVG before upload.** Editing the SVG deletes the geometry outright, and getting it back later costs a re-import plus replaying every Step 8 edit. When a territory is a *subpath* of a larger country (Hawaii inside `United-States`, Fiji's wrapped islet), split it out: filter `vectorPaths` by subpath bbox into a keep-set and a hidden clone. Identify the split by **fraction of the country's own span**, not absolute coordinates — Hawaii ends at 4% of the US span and Alaska starts at 19%, so a cut at 12% separates them at any scale.
  - **To re-import geometry into an already-scaled chart, include one country that is still present as an alignment reference.** Import the mini-SVG with the same viewBox and map transform, then derive the scale from `existing.width / ref.width` and the translation from `existing.x − ref.x`, apply both to the imported group, and delete the reference. One shared country pins a uniform scale plus translation exactly — it returned a residual of 0 on all four measures here, where reasoning about accumulated transforms would not have.

  **Then re-place every annotation, because the trim moved the water they were sitting in.** This is the trap: removing the map's westernmost territory shifts the whole projection left and *shrinks* the visible ocean on that side (the Pacific west of Mexico went from 71px to 43px when Hawaii went), so labels verified clear before the trim can land off-frame after it. Treat "annotations still fit" as false after any change to which territories are drawn.

- **Placing several annotations is a constrained assignment, not five independent choices — and the constraint set is bigger than it looks.** A label needs: clear of every country bbox, clear of the other labels, inside the plot (a label in the band above the map reads as a third subtitle line), **its leader must not pass through another highlighted country**, and — the one that is easy to miss — **no leader may pass through another label**, or it vanishes behind that label's knockout and looks broken. Encode all of them as acceptance tests over a per-country candidate list, then **search across assignment orderings** rather than trusting one greedy pass: ordering widest-first pushed the smallest label from a 10px leader to a 114px one here, and evaluating all orderings for minimum total leader length recovered it. Report total and worst leader length so the arrangement can be compared against the next attempt.

  **Test "is this spot empty?" against one box per SUBPATH, never one per country.** A country's bounding box is a terrible proxy for a country that comes in pieces: the US spans Alaska *and* the mainland, so its bbox swallows most of the North Pacific and Atlantic, and Russia's wraps the antimeridian and covers the whole northern strip. The tempting shortcut is to exclude those countries from the test — and that shortcut is exactly how a label ends up printed on Florida while your own audit reports it clear. Split every vector's `vectorPaths` on `M`, take each subpath's bbox, and map it to frame coordinates via the node's own local-min offset. On this chart it turned ~200 country boxes into 321 subpath boxes, needed no exclusions at all, and immediately caught a label the exclusion-based test had passed.

  ```js
  const subs = n.vectorPaths.map(p => p.data).join(" ").split(/(?=M)/).filter(s => s.trim());
  const bb = subs.map(s => { const v = (s.match(/-?\d+\.?\d*/g)||[]).map(Number);
    const xs = v.filter((_,i)=>i%2===0), ys = v.filter((_,i)=>i%2===1);
    return {x0:Math.min(...xs), x1:Math.max(...xs), y0:Math.min(...ys), y1:Math.max(...ys)}; });
  const ox = n.x - Math.min(...bb.map(b=>b.x0)), oy = n.y - Math.min(...bb.map(b=>b.y0));  // local -> frame
  ```

  **That offset assumes `n.x`/`n.y` are already frame coordinates — check that before trusting it.** `x`/`y` are *parent*-relative (see Gotchas), so the two-line version above is only right when nothing between the vector and the frame contributes an offset or a scale of its own. That is the usual case straight out of an SVG import, and it is what produced the numbers here. It stops being true the moment a country sits under a nested frame, or under an ancestor that was scaled rather than rescaled — and the failure is the silent kind, a box in the wrong place certifying a label as clear. When the ancestry is anything other than the flat imported tree, derive the offset from the absolute transforms instead of the local ones:

  ```js
  const nb = n.absoluteBoundingBox, fb = frame.absoluteBoundingBox;     // both page-space
  const ox = (nb.x - fb.x) - Math.min(...bb.map(b=>b.x0)) * sx;         // sx, sy from the node's
  const oy = (nb.y - fb.y) - Math.min(...bb.map(b=>b.y0)) * sy;         // absoluteTransform
  ```

  Cheapest way to know which form you need: take one country whose position you can see, run the boxes, and check that its subpath bboxes land on it in the rendered frame. If the whole set is off by a constant, an ancestor offset is missing; if it is off by a factor, an ancestor scale is.

  **On a map, also measure how much of each leader is visible before it hits a filled shape** — a 1px gray line over a mid-blue country is effectively invisible, and optimising for the *shortest* leader actively causes it, because the shortest position hugs the coast. The case to fix is the long-and-buried one: a 31px leader entirely over a continent, where pushing the label out to sea bought ~17px of visible line for 8px of extra length. A **short** leader reads fine even fully over land, because it starts at the label and lands immediately. Judge it; don't gate on it.

  **But measure it on pixels, not on boxes — this is where the bbox model flips from safe to wrong.** The same subpath-bbox model is *conservative* for placement (it over-states land, so it never puts a label on a country) and therefore *false-alarming* for visibility (it reports a line as buried when it is over open water). A diagonal country is the killer: Mexico's bbox swallows a wedge of open Pacific off its west coast, so a leader crossing that ocean scored **0% visible** when the render shows **45%**. Get ground truth by sampling the rendered PNG — `get_screenshot` the frame, then read pixels **perpendicular** to the line (±2–3px for a 1px stroke, less for a hairline — the offset is derived below, and its job is to clear the leader's own stroke without answering for the next shape over) and count how many are the canvas colour:

  **Scale the coordinates into the raster first — the screenshot is usually not 1:1.** `get_screenshot` honours `maxDimension`, and the size worth exporting is well above the frame's own units (2160 for a 540 frame is 4×), so leader endpoints and the perpendicular offset are in *frame* units while `px` is indexed in *raster* pixels. Feed one to the other unconverted and you sample somewhere else entirely — which is the same false verdict this check exists to remove, arriving by a different route. Derive the factor from the image rather than assuming the one you asked for, and round: Pillow truncates a float index silently, so a half-pixel offset lands a pixel short on one side and not the other.

  ```python
  s = img.width / frame_width                          # raster px per frame unit
  assert s >= 1, "export at 1:1 or larger, or the leader's stroke is sub-pixel"
  nx, ny = -uy, ux                                     # unit normal to the leader
  w   = leader.strokeWeight                            # 1 on a plot arrow, 0.3 on a map leader
  off = max(2.5 * w * s, 2)                            # clear this stroke's rendered ink, and no more

  def is_canvas(x, y):                                 # x, y in raster px
      xi, yi = round(x), round(y)
      if not (0 <= xi < img.width and 0 <= yi < img.height):
          return False
      return all(abs(a - b) <= 8 for a, b in zip(px[xi, yi], CANVAS))   # tolerance, not equality

  ocean = sum(is_canvas(x + nx * off, y + ny * off) or is_canvas(x - nx * off, y - ny * off)
              for x, y in samples_along(sx * s, sy * s, tx * s, ty * s))
  ```

  **Compare against the canvas colour with a tolerance, never `== CANVAS`.** This is the line that decides whether any of the rest works. The leader is antialiased, so the pixels beside it are blends of stroke and canvas, and exact equality reads them as "not canvas" — i.e. as land. Measured on a synthetic 4× render of a leader that is **71.4% over open canvas**: exact equality returns **41.5%**, a 30-point false *buried*. The same code with a tolerance of 8 per channel returns **71.6%**, and stays within a point of the truth at 1×, 2× and 4×. Tolerance also makes the result insensitive to the offset, which is what you want from a measurement.

  **Scale the perpendicular offset with `s` *and* with the stroke; a fixed constant fails silently at high `s`.** The offset exists to clear the leader's own stroke, and the stroke's rendered footprint grows with the raster — at 4× a 1px stroke covers 4px plus its antialiasing fringe, so an offset of 4px is still inside the line and reports it buried. `2.5 × s` clears a **1px** stroke at every scale tested — but that is `2.5 × w × s`, and the house map leader is **0.3px**, where the same number samples 2.5 frame units out into the map and, along a narrow coast or a small island, answers for different geography than the pixel under the line. Take `w` from the node. Floor it at ~2 *raster* pixels, which is roughly where the antialiasing fringe ends: an offset proportional to a sub-pixel stroke alone sits *inside* the fringe, which the tolerance paragraph below measures as a 30-point false *buried*. The 1px figures here are measured; the hairline floor is reasoned from them, so check one leader you can see on the render before trusting a sweep. Stepping matters less but is free: step `samples_along` one *raster* pixel, since one frame unit on a 4× raster reads every fourth pixel.

  **Guard the direction of `s`, because inverting it is the plausible silent error.** Writing `frame_width / img.width` gives `0.25` instead of `4`, which shrinks every sample into a corner of the raster that is usually empty canvas — so the leader reads as fully visible, the exact false clear this whole check exists to remove. `assert s >= 1` catches it outright for any real export. The known-ocean / known-land probe catches it too, and it is worth keeping because it also validates the orientation: under an inverted `s` the land probe comes back canvas and the assertion fails. Just don't rely on it alone — it only fires if the mis-mapped point isn't coincidentally dark, whereas the `s >= 1` guard cannot miss.

  Rule of thumb: **use boxes to decide where things may go, use pixels to judge how it reads.** And when the user says a line looks fine and your metric says it doesn't, believe the render — they are looking at ground truth and you are looking at a proxy.

  **When labels won't fit beside their countries, make them narrower before you move them further away.** Breaking `Country 55.0%` onto two lines (name over value) roughly halves the width and costs one line of height, which is what lets a label sit in a narrow strait instead of an ocean away — it took the worst leader from 134px to 64px on this chart. A long leader is a worse defect than a two-line label.

- **Fill in the gaps in a time axis — but only where they measurably fit, and otherwise leave grapher's axis exactly as it is.** Grapher drops tick labels to avoid collisions at the width it rendered for, so once you reclaim a margin (above) the axis can be left reading `1990 · 2000 · 2005 · 2010 · 2015 · 2025` when the room for a complete 5-year run is now there. Clone an existing **interior** tick vector and label — never build one from scratch, the clone inherits the stroke, size, color and alignment — set the characters, and place both at `x = x(1990) + (year − 1990)/(span) · (x(2025) − x(1990))`.

  **Two fit tests, not one, because the edge labels are anchored differently.** Interior labels are centered on their tick, so they need `pitch ≥ labelWidth + ~8px`. The **first** label is left-aligned *at* its tick and the **last** right-aligned *at* its tick — grapher does this to keep them inside the plot — so each spends a full label width on its inward side, and the two slots next to them need roughly `1.5 × labelWidth + gutter`. Miss that and the arithmetic says yes while the render overlaps: on one chart a 50.3px pitch cleared the 43px interior requirement, added 1995 and 2020, and left both of them 2.2px *inside* the edge labels. Measure the neighbor gaps after adding and revert if any is negative — the honest outcome is often that grapher's axis was already right, and the years it dropped were exactly the edge-adjacent ones.

- **Annotations replicating the accompanying text** (12–16px; 12–14px on maps): text color = the annotated object's color, `Text/Gray 80` #5B5B5B, or a mix; bold the key phrase; and each one wrapped in its **own white (canvas-colored) auto-layout frame, hugging the text on both axes** and appended last so it sits above the chart — see GUIDELINES.md → Annotations for why hugging is the part that matters and when a white outside stroke is the fallback instead.

  ```js
  const box = figma.createAutoLayout("HORIZONTAL");         // real API — see the note below
  box.name = "annotation__<what>";
  clone.appendChild(box);                                   // parent before setting HUG
  box.fills = clone.fills.map(f => ({...f}));               // the template's canvas, not white
  box.clipsContent = false;                                 // else the trim below cuts every descender
  txt.leadingTrim = "CAP_HEIGHT";                           // hug the ink, not the line box
  const lastSize = txt.getRangeFontSize(txt.characters.length - 1, txt.characters.length);
  box.paddingLeft = box.paddingRight = box.paddingTop = 0;  // hug the glyph box on three sides
  box.paddingBottom = Math.round(0.22 * lastSize);          // ≈3px at 14px: fill reaches under descenders
  box.appendChild(txt);
  txt.layoutSizingHorizontal = txt.layoutSizingVertical = "HUG";
  box.layoutSizingHorizontal = box.layoutSizingVertical = "HUG";
  ```

  > **`clipsContent = false` and `paddingBottom` are one fix in two halves; neither works alone.**
  > Unclipping brings the cut glyphs back; the padding puts opaque fill *behind* them, so a gridline
  > crossing below the baseline doesn't show through the recovered letterform. See
  > GUIDELINES.md → Annotations for the reasoning, and for the `y` nudge the trim needs (the box
  > shrinks around the ink, so move it up by half the height lost). Read the size off the **last**
  > character rather than `txt.fontSize`: a two-line label steps the ladder down between its lines,
  > so `fontSize` is `figma.mixed` there, and it is the bottom line whose descenders hang out.

  > **Take the fill from the template, never hardcode white.** The DI and static templates are white,
  > but the Instagram ones sit on `Instagram/Beige Background` `#FBF9F3` — a white frame there is a
  > visible rectangle behind the text, which is exactly the background box the guidelines forbid.
  > Copying `clone.fills` makes the knockout invisible on whichever template was chosen, and keeps
  > working if a future template introduces another canvas color.

  > **`figma.createAutoLayout()` is a real API — do not "fix" this to `createFrame()`.** It is declared
  > in the official plugin typings (`createAutoLayout(direction?: 'HORIZONTAL' | 'VERTICAL'): FrameNode`)
  > and the `figma-use` skill's rule 12a says to prefer it *over* `figma.createFrame()` with absolute
  > coordinates. A review pass on this branch asserted it did not exist and the snippet was rewritten to
  > `createFrame()` + `layoutMode`; that swap is the anti-pattern rule 12a names, and it was reverted.
  > If a reviewer flags it again, check `references/plugin-api-standalone.d.ts` before changing anything.
- **Arrows**: copy curvy arrows from node `798:773` — 1px stroke, arrowhead and line the same color as each other and consistent across the chart. Never scale a whole arrow (it distorts the head): Shift-resize the line segment only, then reposition the head. If a curvy arrow gets messy, use a straight thin line. **Maps: never curvy and never an arrowhead — the hairline leaders of GUIDELINES.md → Maps (`#2d2e2d` at 0.3px, filled dot at the country end), or values inside country shapes.** The 1px stroke above is for arrows on a plot; at 1px a leader on a map reads as a border.
- **Drop the axis and gridlines when every data point is already labeled.** The checklist says so outright, and it is the cheapest space you will ever find: deleting `horizontal-axis`, `vertical-grid-lines` and `vertical-zero-line` from the imported group frees ~25px — usually the difference between text at the 12px floor and text at a comfortable 13–14px. It applies most obviously to a **100% stacked bar**, where every bar spans 0–100% and the axis tells the reader nothing they can't read off the segment values. Don't do it where the reader still has to estimate: a line chart's y-axis, or any chart whose points are mostly unlabeled.
- **Dropping entities does not buy vertical space — it buys thicker bars.** Easy to get wrong: the export canvas is a fixed size, so grapher redistributes the freed rows into the remaining ones and the chart comes back exactly as tall. Measured: eleven countries and ten countries both returned a 346px chart, with the row pitch going from ~28 to ~31px. So cut entities to reduce clutter or to make bars more readable, never to make something fit. **The lever for fit is the export's aspect ratio** (`imWidth`/`imHeight`, which set the shape the layout is computed for) or removing furniture like the axis — not the entity list. Either way the selection belongs to the chart's author: surface it, don't decide it.
- **10×10 px dots** marking highlighted years, with the values written out for the first, last, and any mentioned data point (white-outlined dots on stacked areas; no outline elsewhere).
- **Flags** (`2654:5`) beside country labels/bars where they help; **animals** (`5336:5`) for livestock topics; both are copy/paste.
- **Colors**: only the file's Chart colors library, in the cheat-sheet order. **Audit them — never eyeball this.** A palette that looks fine can collapse for the ~8% of men with red-green deficiency, and the failure is invisible to you:

  ```bash
  .venv/bin/python .claude/skills/create-figma-chart/scripts/color_audit.py \
    '#bc8e5a,#883039,#6d3e91,#d73c50,#4c6a9c,#6e7581' \
    --names 'Poultry,Beef and buffalo,Sheep and goat,Pork,Fish and seafood,Other meats'
  ```

  It simulates deuteranopia, protanopia and tritanopia, reports the closest pairs as CIELAB ΔE (**under 20 fails, 20–30 is tight**), flags which pairs actually touch in the stack, checks white-vs-black label contrast on every fill, and measures the **grayscale seam** between each pair of touching fills (under **1.6:1** they merge when printed — two different hues at the same lightness pass every color check and still fail this one). Add `--suggest` (with `--keep` for the colors that carry meaning) to search the OWID palette for a safer set; it ranks by **hue variety first, then safety, then drift** from the colors already in use, because ranking on safety alone returns sets that are entirely blues and greens — technically separable, but the reader can no longer tell six categories apart at a glance — and among equally varied, equally safe palettes the one that moves the colors least is the one a designer reads as a fix rather than a different chart. Every suggestion it prints has also cleared the grayscale seam check, and it reports the seam alongside the ΔE so you can see it did: a palette can clear ΔE 20 comfortably and still have touching fills that merge in print, so the search picks the *order* as well as the colors. Where it can't help you is a failing seam between two colors you told it to keep — it says so rather than silently returning nothing. The seam is a **stacked-fill** rule, and which charts have seams is something you tell it: only a stacked or segmented chart lays its fills edge to edge in the order given. A plain or grouped bar chart draws each fill against the background, so legend order says nothing about adjacency and gating on it would reject good palettes for an arbitrary reason — pass **`--separated`** there, and for lines and maps (`--line`/`--maps` imply it). In that mode it reports the closest pairs for you to judge and never gates. It prints the assumption it used on the first line, so a mode you forgot to pass is visible rather than silent. Constrain the roles as well when you search by hand (fish should stay blue, beef reddish): the unconstrained optimum is rarely the one to propose. Read the results with two cautions: **tritanopia is vanishingly rare**, so never repaint for it alone; and **swapping a single color usually doesn't help**, because the failures are independent — this chart's floor stayed at 9.2 whether you changed Pork or Sheep-and-goat, since a different pair took over each time. Colors live in the chart, so a repaint is a recommendation to its author, not an edit you make.

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
const band = (footer.y + Math.min(0, source.y)) - (header.y + header.height)   // footerTop, per Step 7
chart.rescale((band - 2 * GAP) / chart.height)        // height-first, as in Step 7; GAP = the template's band figure
// ... re-hug every TEXT, preserving its alignment anchor ...
// re-hugging moves the bbox, so re-run the closed-form x-map — not a second rescale, which would
// re-multiply the font sizes this fit just put on the ladder
chart.x = header.x
chart.y = header.y + header.height + (band - chart.height) / 2
```

**Everything that lived inside the old chart goes out with it — replay it, from a list.** That pass restores only the furniture removal, the scale and the text re-hug. Every other Step 8 edit was parented under the group you just removed: the hidden `connectors`, the cloned direct labels and their placement, the added ticks, the bound stroke and fill styles, and the whole highlight treatment (gray context lines at 1px, the palette color on the protagonist, the widened halo, the hidden markers). Only the annotations survive, because they are parented to the template clone rather than to the chart. Keep the chart-local edits as **one scripted function you re-run after the import**, or as an explicit list you work down — memory is not enough, because a frame that has quietly reverted to grapher's raw rendering looks finished. Then re-run Step 8c on the new chart; the earlier pass certified an object that no longer exists.

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
| Color-vision safety | `color_audit.py` | no pair under **ΔE 20** for deuteranopia or protanopia; tritanopia noted, never acted on alone. **Categorical fills only** — a sequential map ramp is exempt, see below |
| Spelling and prose | `.venv/bin/codespell` over the texts, plus a read against the style guide | American spelling (CLAUDE.md), no typos, no style-guide breaches — see below |
| The text is *true* of the indicator | `/adversarial-data-review` on the dataset behind the chart | **every** string that says something about the data survives checking against the producer's documentation — title, subtitle, note, year, annotations, direct and value labels, legend and category labels, units, entity names, source line. Labels you shortened are in scope |
| Entities all render | the **effective** selection (Step 1's table, not the saved `selectedEntityNames`) vs the labels in the SVG | every selected entity appears — a member missing its latest year is dropped silently (`/check-empty-entities` is the pipeline sweep) |
| Year or period stated, and not stale | the period the export actually shows — the link's `time=` where there is one, otherwise the rendered SVG — plus the source chart's `maxTime` | a **single-time** image says which year it shows, in the title or subtitle; a **time series** states its period on its own time axis and takes no caption (adding one makes a series read as a snapshot — see "A pinned year, and a frozen image" below). Either way the source chart isn't pinned to an old year (`/check-hardcoded-years`) |
| Grayscale survival | `color_audit.py` (grayscale seam section) | **adjacent** pairs above ~**1.6:1**; below that they merge in print. **Stacked or segmented fills only** — for a plain or grouped bar chart, a line chart or a map pass `--separated` (`--line`/`--maps` imply it) and read the closest pairs as information, since legend order says nothing about which marks meet |
| Off-palette fills | compare every fill against the library groups | every fill is a library color, **bound as a style** — grapher emits `#585c64` for residual categories, which is in no group. Two standing exceptions, listed rather than flagged: the muting grays of a highlight treatment, and a grapher-managed sequential map ramp (see below) |
| Legend agreement | pair swatch→label by geometry, compare against the bars | zero mismatches |
| Direct labels name what they sit on | for each category label, compare its **fill** against the fill of the segment it names, and its **x** against that segment's edges in the reference row | the color is identical (same bound style, not merely a close hex) and the label is anchored on its own segment. A direct label carries the swatch's job with none of the swatch's proximity, so a mispaired one is unfalsifiable by eye |
| Direct labels readable as text | `contrast(labelHex, "#ffffff")` for every category label drawn on the background | **4.5:1**. The same color must also clear 4.5:1 against the white value label inside its bar — a palette that only clears one of the two has to move (Step 8) |
| Text size | read `fontSize` off every text node | nothing below **12px**; annotations on the named ladder |
| Mark weight | read `strokeWeight` off **every** line and halo, after the last scale | on a highlight treatment: context **1px** (the settled value — GUIDELINES.md → Highlighting; 1.5px is the reference-page treatment this skill tells you not to copy), protagonist **3px**, halo 2× (or line+1 where nothing crosses). Read it even when you never set it — and especially *because* you never set it: `rescale()` multiplies stroke weight, so fitting a chart to the band took grapher's 2.5px lines down to **0.88px** hairlines on a frame that otherwise measured perfect. Set the weights explicitly *after* the final scale, never before |
| Label-on-fill contrast | `contrast(labelHex, barHex)` for every in-bar label | **4.5:1** at 13.5px regular — the 3:1 large-text allowance does not apply |
| Text hierarchy | list every distinct `fontSize` with what it belongs to, **and its rank** | title > subtitle ≥ annotations > supporting text ≥ labels. Sizes may vary inside the plot by rank; a lead annotation may *equal* the subtitle (Annotation XL 16) but nothing may exceed it, and same-rank items must share a size |
| Sizes are named styles | every size matches a style in the file | no arbitrary sizes left over from scaling the export (13.7, 16.8). Choose from the ladder by rank rather than by element type — see GUIDELINES.md → Subtitles and notes |
| Annotation knockouts cover only furniture | for each `annotation__*` frame, test its rect against every line's **sampled polyline** (not bboxes — see below) | gridlines, empty space or a muted context line — never a highlighted line, a dot, a value label or a bar segment carrying a number |
| Label alignment | compare each label's center against its mark | bar values centered on bars, legend labels on swatches |
| Box alignment | compare the chart's left/right against the header frame | identical to the subtitle box, to the pixel |
| Gap | `(footerTop - headerBottom - chart.height) / 2`, with `footerTop = footer.y + Math.min(0, source.y)` — a source row raised inside the footer lifts the band's bottom (Step 7) | equal top and bottom, at the band figure of **the template you filled**: **12–16px** on the 540-wide frames, **30px** on the IG portrait (see Step 7). **Exception — a tightly measured group:** on an axis-less chart whose furniture was trimmed and label boxes hugged (Step 8), the band no longer applies as written; the figure to match is the one the **reference page** measures the same way, typically **20–30px**. Measure it there, record yours with a note that the group is tightly measured, and do not shrink a correct chart to force the band |
| Annotation block gap | the **block's** outer edges (topmost annotation, bottommost annotation, plot — whichever is extreme) vs the header and footer frames | the same clearance the plot owes: **27px** each side on the 540×540 pages. An annotation outside the plot is part of the block, so spacing the plot alone is not enough (GUIDELINES.md → Annotations) |
| Every pointer lands on its target | for each leader, the **terminal vertex** (transformed, not the bbox) vs the thing it names — the country's own **ink** on a map, the band border at the stated year on a chart | the dot or tip is inside/on its target, and where the text names a year, at that year's x — with the first and last year taken from the plot's edge, not the tick label's centre. **A country's bounding box is not the target.** Countries are concave and multi-part, so a point can sit well inside the box and still be in open ocean — the US box reaches past Hawaii, an antimeridian straddler's spans the whole Pacific (see the map fit in GUIDELINES.md → Maps). Test the terminal point against the country's **pixels**, using the same hide-and-diff mask as the arrow probe below: hide the country vector, diff the renders, and require the dot to fall on (or within ~1px of) that pixel set |
| Nothing in the margins | every visible mark's `absoluteBoundingBox` vs the content band | no ink outside **16…524** on a 540-wide frame. A speck left in the margin after a map fit renders as a cut sliver at the frame edge |

**For arrows, drop vectors entirely and probe the rendered pixels.** Arrow groups are rotated, so every vector-space measurement of theirs is wrong (see Gotchas), and "very close but never on top" is a pixel property anyway. Screenshot the frame at 1:1, take the arrow's **`absoluteBoundingBox`** in frame coordinates, and inside it measure how close the arrow's pixels come to the target line's.

**Identify each shape's pixels by node identity, never by color.** A pixel belongs to the shape whose hiding changed it, which is true whatever either shape is colored. Screenshot the frame at 1:1 **four times** — whole, with the arrow's `visible = false`, with the target line's, and with **both** hidden — and diff each shape against the both-hidden render, from the pass where the *other* shape was already gone:

```python
from math import hypot

crop   = [(x, y) for y in range(y0, y1) for x in range(x0, x1)]   # arrow's absoluteBoundingBox, padded
arrow  = [p for p in crop if no_target[p] != no_both[p]]          # arrow alone vs neither
target = [p for p in crop if no_arrow[p]  != no_both[p]]          # line alone vs neither

assert arrow,  "no arrow pixels — wrong bbox, wrong frame, or the hide never applied"
assert target, "no target pixels — pad the bbox, or this is not the node the arrow points at"

d        = lambda a, b: hypot(a[0]-b[0], a[1]-b[1])
minGap   = min(d(a, b) for a in arrow for b in target)
touching = sum(1 for a in arrow for b in target if d(a, b) <= 1.5)
```

**Don't diff either mask against the whole render — that hides the overlap you are testing for.** Whichever node paints on top covers part of the other, and hiding the *covered* one changes nothing in those pixels, so a mask taken from `full` comes back with a hole exactly where the two shapes meet. An arrowhead sitting on the end of its line then measures its `minGap` to the nearest still-*exposed* line pixel and reports a comfortable 3–7px with `touching == 0` while the two are plainly overlapping — the one verdict this check exists to prevent. Diffing from the other-hidden pass costs one extra screenshot and is symmetric, so it holds whichever node is on top.

Restore `visible = True` on both afterwards, and **guard the masks**: each difference must fall inside that shape's own `absoluteBoundingBox`. If it doesn't, hiding the node reflowed something else (a group's derived box, an auto-layout sibling) and the mask is measuring the reflow, not the shape.

**Classifying pixels by color instead is the version to avoid — it produced two different false verdicts before it was replaced.** That first cut called the arrow "gray" (`abs(r−g) < 14 and 60 < r < 165`) and the line by its own hue, and both halves break on ordinary charts. A **gray target** — an arrow aimed at a muted context line — satisfies the arrow predicate, so every target pixel is filed as arrow ink and the check dies with an empty target set on a chart where nothing is wrong. And **gray furniture** in the padded crop — a gridline, a second context series, gray annotation text — is collected as arrow ink too, so the target line merely *crossing a gridline* reports `touching > 0` while the arrow itself is comfortably clear. Neither is fixable by narrowing the crop, since a crop cannot separate two shapes that answer the same predicate. Three extra screenshots cost less than one wrong verdict, and hardcoding the hue is worse again: the palette runs to 24 fills, so a fixed `TARGET` collects nothing on most charts and `min()` then dies on an empty sequence instead of reporting a clearance.

**Pass is `touching == 0` with `minGap` about 3–7px.** This is the only check that caught the real defects: it found the peak arrow overlapping the line by 11 pixel pairs where the vector math had reported a comfortable clearance, and it confirmed the fix at 3.0px with zero contacts. Report both numbers per arrow.

**On a line chart the bbox overlap test is not conservative, it is useless — sample the polyline.** A diagonal line's bounding box is most of the plot, so a bbox test reports every annotation as covering every line: on this run it returned 5 collisions across 4 frames, all but one of them phantom, and it *buried the one real defect in the noise* (a portrait annotation genuinely clipping the projection line). Extract the path's points — the numbers in `vectorPaths[0].data` alternate x,y, so map each pair through the node's own transform — then walk the segments and sample each at ~1px:

```js
const pts = (v, frame) => {                          // path space -> FRAME space
  const n = ((v.vectorPaths||[]).map(p=>p.data).join(" ").match(/-?\d+\.?\d*/g)||[]).map(Number);
  const [[a,b,tx],[c,d,ty]] = v.absoluteTransform;   // rotation + scale + translation, in one matrix
  const fb = frame.absoluteBoundingBox;
  const out = [];
  for (let i = 0; i + 1 < n.length; i += 2)          // path coords are local to the node
    out.push({ x: a*n[i] + b*n[i+1] + tx - fb.x,
               y: c*n[i] + d*n[i+1] + ty - fb.y });
  return out;
};
```

**Drive it off `absoluteTransform`, not `v.x`/`v.y`, even though the naive version happens to work on a fresh import.** Group ancestors are transparent for coordinates, so a line nested under `lines` → `chart-area` does report frame coordinates and the short form measures correctly — that is why it produced sound numbers here. But the assumption is invisible and it fails three ways: under a nested **FRAME** ancestor, under an ancestor that was **scaled** rather than rescaled, and on any node with non-zero **rotation** (which is exactly how the arrow measurements in this skill came out as fiction). The transform costs one property read and cannot be wrong, so prefer it and keep the audit trustworthy when someone later reparents the chart.

**And take the transform, not the bounding box, or rotation silently defeats the fix.** The tempting short version — normalize the local x,y into `absoluteBoundingBox` by their own min/max — reads like it handles rotation, because for a rotated node the bbox *is* the visual one. It does the opposite: normalizing two axes independently into an axis-aligned box cannot rotate anything, so you get an **unrotated polyline stretched across the visual box**, a shape the reader never sees, and the audit then certifies the wrong geometry with more confidence than before. `absoluteTransform` carries the rotation in the matrix, so applying it to each point is both shorter and the only version that is actually rotation-safe. (The regex takes every number in the path data, which is right for the M/L polylines grapher exports; a path with curve commands would need its control points dropped first.)

That took the same four frames to **one** finding, which was real. And the same routine fixes it without guesswork: take the topmost line point under the annotation's x-range and set `box.y = thatY − 12 − box.height` — the ~12px the knockout rule asks for (GUIDELINES.md → Annotations), not the 5px that merely clears the test. **A clear audit is necessary here, not sufficient:** the polyline check only asks whether the box *touches* the line, so it reports 5px as clean, and 5px is the gap a reviewer called visibly too close. If 12px pushes the block somewhere awkward, narrow the block — re-wrap the same sentence into more, shorter lines — rather than moving it further away. Then re-run the test and confirm it still reports clear. (This is the line-chart counterpart of the subpath-bbox rule for maps: boxes decide where things may go, geometry decides how it reads.)

**A sequential map ramp is not a categorical palette, and two of the rows above don't apply to it as written.** GUIDELINES.md → Colors keeps map colors in grapher on a Viridis or ColorBrewer sequential scale and off the OWID categorical palette, because ordered bins separate better once a map shows many classes. That has two consequences here, and both look like defects if you don't know them. **The ΔE 20 bar is an all-pairs *categorical* test, so a ramp fails it by construction** — neighboring stops are supposed to be close, that is what makes the ramp read as ordered — and `color_audit.py` has no sequential mode: `--maps` swaps the search over to the **Categorical Maps** group, so `--maps --suggest` on a ramp cheerfully proposes an unordered set and destroys the encoding. Don't run it there. **And the off-palette sweep can't pass either**, because grapher's ramp belongs to no library group and arrives as raw fills — demanding a bound style would mean repainting the map in Figma, which the guidelines forbid. So for a sequential map, check the scale where it is actually set, in grapher: that the bins are ordered and distinguishable, and that the legend labels and any values written onto the shapes clear their own contrast bar. Then record the ramp as grapher-managed in one line instead of listing every stop as an off-palette fill. `--maps` and the ΔE gate are for a **categorical** choropleth — one color per region or class, no order between them — which is the case those rows were written for.

**Filter the fill sweep to what actually paints, or it invents failures.** Two kinds of phantom show up and both look exactly like a real off-palette color in a listing. **Hidden ancestors:** `visible` is per-node, so the children of a group you hid are still individually `visible: true` — walk up to the frame and skip anything with a hidden ancestor, or a hidden `connectors` group reports a dozen stray colors. **Zero-area vectors:** grapher's exported tick marks are zero-width stroked paths that carry a default black `fill` which can never paint, so an unfiltered sweep reports twelve `#000000` fills on a chart that has none. With both filters the same chart went from 4 apparent off-palette colors to the 2 real ones.

```js
const paints = n => { let m = n; while (m && m !== clone) { if (!m.visible) return false; m = m.parent } return true }
// ...and ignore `fills` on nodes whose width or height rounds to 0
```

**Make label-centering part of the build, not a follow-up.** It regressed three times in one run — each rebuild re-hugs the text, which restores the drift, and a separate "now center the labels" step is forgotten or applied to a chart instance that is later replaced. Put the centering loop at the end of the same function that imports, scales and re-hugs, so it cannot be skipped.

**Re-run this whole pass after the last change, not after each one.** Fixes get lost silently: a label-centering pass applied to a chart instance that is later swapped for a re-export leaves the drift back exactly as it was, and every screenshot in between looks correct. And a structural change spends budget elsewhere — lifting an aggregate row to the top added 8px of height, which came straight out of the 12–16px gap and took it to 8.2 without anything reporting a problem. Treat "I already checked that" as false after any re-export, reorder, rescale or restyle.

### Checking the words, not just the geometry

The chart's text is not yours — you transcribed it from the indicator's metadata — so a defect in it is a defect **upstream**, and fixing it only in the image leaves the interactive chart, the data page and every other surface still wrong. Check it here because this is where someone finally reads it slowly; fix it where it lives.

- **Spelling and prose.** You transcribe these strings verbatim, so you are not the one introducing a typo — you are the last reader before it is frozen into an image, which is a worse place for it than a chart that can be corrected in place. Run `.venv/bin/codespell` over the strings (it is a dev dependency; `/check-metadata-typos` covers the same ground on `.meta.yml` and `.dvc`). American spelling always, per CLAUDE.md, including in text copied out of a chart. For the wording itself, `/check-metadata-style` holds the Writing and Style Guide, whose FAUST rules govern exactly the strings this skill moves.
- **Whether the text is true.** Run **`/adversarial-data-review`** on the dataset behind the chart, over the data *and* every string in the frame that says something about it. That skill fetches the producer's own documentation from the snapshot's links and treats each sentence as a claim to be refuted, which is the right posture for text about to be published as an image. Its scope here is **everything, not just the FAUST**:

  | Text | The claim it makes |
  |---|---|
  | Title | the headline assertion — that the data shows this |
  | Subtitle | what is measured, in what units, over what population |
  | Note | the caveats, and that these are the ones the producer actually states |
  | The year or period, wherever it is stated — in the title, as `Data for <YYYY>.`, or on a time axis — and any year caveat | that it is what the export actually shows, for every entity |
  | Annotations | each number, comparison and superlative — transcribed *or* derived |
  | Direct labels and value labels | that this number belongs to this mark |
  | **Legend and category labels** | that the category contains what the label says it does |
  | Axis labels and units | the scale, and whether it is a share, a rate or a count |
  | Entity names | that the entity is the one the producer means (aggregates especially) |
  | `Data source:` line | the producer, and the year of *their* release |

  **Shortening a label is a factual edit, so put it through this check too — not just the strings you inherited.** Words in a category label are the definition of the category: "Other meats" → "Other" loses nothing on a chart entirely about meat, but "Beef and buffalo" → "Beef" drops a species the category counts, and where buffalo is most of it (India, Pakistan) the shorter label understates what the bar contains. Check it against the producer's own indicator title — FAO's is "beef and buffalo meat" — and note that the interactive chart will still carry the long form, so the image and the chart will disagree.

  That does not make the short form forbidden. A team may prefer the plain word and accept the imprecision; on this chart the owner did. What it makes it is **a decision, taken knowingly and recorded** (see the accepted-deviations rule below) rather than a side effect of needing 20px. Say what the short label costs, say what keeping the long one costs — here, one row at 12px or two rows at 15px — and let the owner choose.
- **Rendered spacing.** Metadata is often Jinja-templated, and a template defect shows up only in the rendered string — a double space, or a missing one where a conditional collapsed. You are pasting the rendered form, so you inherit it silently. `/check-metadata-spacing` is the pipeline check for this; here it is enough to read the placed strings once for spacing, and to distrust any sentence whose shape suggests a template (`in {country}`, a units clause that reads oddly).
- **Entities that render empty — the check this skill learned the hard way.** A pinned selection can silently lose a member: grapher drops an entity whose data doesn't reach the displayed year, with no warning anywhere. This run shipped ten of eleven countries for exactly that reason, and only the accompanying text naming the missing country exposed it. `/check-empty-entities` is the pipeline sweep for this class; the local version is Step 1's rule — compare the **effective** selection against the entity labels in the exported SVG, every time. Effective, not saved: a link carrying `country=` overrides `selectedEntityNames` entirely, and diffing against the config there reports every saved default as missing on a chart where nothing is wrong.
- **A pinned year, and a frozen image.** `/check-hardcoded-years` exists because a chart pinned to `maxTime: 2019` quietly stops showing new data. The static image has the sharper version of the problem: it is pinned to whatever year it was exported at, permanently, and nothing will ever refresh it. So check two things — that the *source chart* isn't pinned to a stale year (you would be freezing someone else's oversight), and, **for a single-time export**, that the year is stated **somewhere the reader will see it**: in the title when the claim is year-specific, otherwise in the subtitle as `Data for <YYYY>.` (GUIDELINES.md → Titles). Check for it in both places before calling it missing, and check it appears in only one of them. The year to state is the one the export shows — a `time=` in the link overrides `maxTime`, so read it off the link or the rendered SVG rather than the saved config. An undated single-time image is the one defect that gets worse with time.

  **A time series needs no such caption — its own axis is the date line.** There is no single year a 1990–2025 chart "shows", and appending `Data for 2025.` to one makes a whole series read as a snapshot of its last year. What a time series needs from this check is the *other* half: that the axis actually runs to the latest year the data has, which is the stale-`maxTime` question above. The caption rule is scoped to single-year charts everywhere else it appears (Step 4's subtitle rule, GUIDELINES.md → Subtitles and notes); keep it scoped here too.
- **Where a finding goes.** A wrong or misspelled string belongs upstream in the chart's own text, not in the Figma frame — same rule as sort order and colors. Route the fix through `/edit-faust-metadata`, always, and don't pick the layer yourself: that skill decides which layer the field actually lives in (garden `.meta.yml`, an MDim's yaml, or the chart config on staging) and reports which *other* charts inherit the same string before anything changes. Editing the garden file directly because it looked like the obvious home is how a one-chart correction silently rewrites text on a dozen others. Report the finding, hand it over, and hold the image until it's fixed if the claim is load-bearing; a static image outlives the chart text it was copied from, so shipping a known-wrong sentence is worse here than on the live chart, where it can be corrected in place.
- **Annotations you wrote are your own claims.** Anything you drafted rather than transcribed — a derived percentage, a "more than half" — carries no upstream provenance, so verify it against the data yourself and say in the report which annotations are transcribed and which are derived.

**A failing check is a finding to report, not a veto.** Measure it, say plainly what fails and by how much, offer the alternatives with their own numbers — then do what the author decides. If they accept the deviation, record it in the report; chart-side work goes in the handover doc and reusable mechanics go in this skill. **Add a note to the Figma page only if the user asks for one** — don't volunteer it (GUIDELINES.md → Colors).

**Check the properties you didn't change, not just the ones you did.** A verification pass naturally retraces the edits — it measures the colors because you set colors, the positions because you moved things — and that is exactly how an inherited value survives it. The context lines on this chart stayed at the export's 2px through a full pass that confirmed their color, because nothing in the pass ever asked what weight they were. Derive the check from **what the finished frame is supposed to look like**, property by property, rather than from your own edit history; anything the treatment specifies gets read back, whether or not you believe you touched it.

Two habits make the difference. **Assert, don't eyeball** — a 1.2px label drift, a 1.18:1 grayscale pair and a scrambled legend all looked perfectly fine in a screenshot. And **re-run the affected checks after every change**, because they interact: applying a text style resets range colors, rescaling rewraps text and shifts label centers, adding an annotation changes the group's width, and swapping one color moves the safety floor to a different pair.

## Step 9 — Checklist pass, review, deliver

1. Run the **Good Data Viz Checklist** (GUIDELINES.md, final section) against the composed frame; fix what fails.
2. `get_screenshot` the new page and show the user — original and adapted version side by side. Iterate on feedback (no re-approval needed within the approved page).
3. **Rename the final frame to the slug** from Step 2 (`child-mortality-asia-decline`) — Figma uses the frame name as the export filename for the website PNG. **Exactly one frame carries the bare slug**; variants get a suffix (`-palette-a`). Two frames with the same name export two files with the same name.

   **When the user picks a variant, move the bare slug onto it in the same breath — never leave the rename as an open item.** It reads like a one-line loose end and it is not: the page ends up with a single finished frame still called `…-palette-a`, and the PNG the website gets is named after a trial. Renaming is free while the choice is being made and invisible afterwards.
4. **Clear the rejected variants off the page.** Proposal frames accumulate fast — a palette trial, a labeling trial, a layout trial — and a page with four near-identical charts makes the reader work out which one is live. When the user picks, delete what they didn't pick and keep what they asked to keep; a variant kept deliberately is fine, one left behind by accident is not.
5. Do **not** export a PNG by default — the designer usually keeps editing. On request: `get_screenshot` with `maxDimension` at the target size (DI images ship at 2160×2160, i.e. 4× the 540 frame), or let the user export from Figma.
6. **Give the user a clickable link to the frame — once, when you first create it.** `https://www.figma.com/design/<fileKey>/<FileName>?node-id=<node-id>`, with the node id's colon written as a hyphen (`24977:6` → `node-id=24977-6`). Deep-link the **frame**, not the page: it opens with the chart on screen rather than wherever the canvas was last parked. A first delivery without the link is not delivered — making someone hunt for a page in a 180-page file is pure friction. But **don't repeat it on every iteration**: they already have the tab open, and a link at the top of every reply is noise. Re-send it only if the frame moves to a new page or they ask.
7. Report what was created (page name, frames, edits made) and what remains manual: the Flags plugin if it was used, and any design review — **you cannot read Figma comments via MCP, so never report the design review as clean.** Deviations and open items go in the report and the handover doc; put them on the Figma page **only if the user asks** — an unrequested note is clutter in someone else's design file.

## Gotchas

- **`get_metadata` page listing lies** — it returned only "Cover" for both the Charts and Guidelines files. Enumerate pages via `use_figma` → `figma.root.children`; access known nodes directly by id.
- **And its node tree is lossy: a childless-looking frame usually isn't.** Every bar segment whose group held only a fill vector and no value label came back as an empty `<frame …/>`, while segments with both were listed in full — so reading the XML alone would say the small segments have no bar drawn at all. The tell is in the ids: consecutive siblings numbered `…494` and `…496` have a `495` that was dropped. Use it for structure and names, and confirm anything you intend to *assert* (a missing label, an unpainted mark) with a `use_figma` read.
- **An empty `fills` array is NOT a reliable marker for "no-data shape".** It is the marker the no-data hatch rule leans on, and it over-matches: grapher's map export also contains an invisible `swatch-hit-areas` group — full-size rectangles over each legend bin, with no fill, there for mouse targeting. A blanket "every empty-fill vector gets the hatch" pass therefore painted diagonal stripes across all three legend bins while correctly hatching one country. Scope the sweep by parent instead — `countries-without-data` for the map and the legend's own `swatches` group for the key — and hide `swatch-hit-areas` outright, since a static image has nothing to target.
- **A path with negative coordinates needs `x`/`y` at its bounding-box minimum, not at its first vertex.** Figma normalizes a vector's bbox, so `M 0 0 L 24 -104` assigned `v.y = startY` puts the box's *top* at `startY` and the line draws downward — the opposite of the intent. One leader aimed up at Chad ran down through the legend into the footer instead. Compute `min(y1,y2)` and offset the path data by it (snippet in GUIDELINES.md → Straight elbowed arrows).
- **`useColumnShortNames` suffixes every CSV column with the chart's slug, and the slug contains the other series' names.** On `elec-fossil-nuclear-renewables` every column ends `..._chart_elec_fossil_nuclear_renewables`, so `next(c for c in cols if "nuclear" in c)` returns the **fossil** column and every share you compute is wrong — in a way that looks plausible (61.7% "nuclear" in 1985). Match on the prefix (`c.startswith("nuclear_")`), and sanity-check one number against the rendered chart before writing it into an annotation.
- **On a rotated node, `x`/`y`/`width`/`height` are NOT the visual bounding box — and the curvy arrows are all rotated.** Their `x` is the untransformed origin, so an arrow group reporting `x: 534.3, width: 29.6` in a 560-wide frame actually paints at `494.9`, 40px wide. Everything downstream inherits the error silently: a pixel probe over that box found *zero* arrow pixels, and — worse — the path→frame mapping used everywhere in this skill (normalize `vectorPaths` numbers by the path bbox, scale onto `node.width/height`) is only valid when rotation is 0, so every arrow-to-line distance measured that way was fiction. Use `absoluteBoundingBox` minus the frame's for anything that might be rotated, and check `node.rotation` before trusting a bbox-normalized path mapping. Imported chart geometry (lines, bars, ticks) is unrotated, which is why the mapping works there.
- **`clone()` copies a node's own transform and drops its parents' — and `rotation` won't tell you.** Assets in the finished pages sit inside groups that are themselves mirrored or rotated, so a cloned child arrives with the group's half of the orientation missing: a solid arrowhead renders as a hollow chevron. The `rotation` getter is no help, because for a mirrored node it reports the un-mirrored angle — the source read `169.9` and its own clone read `10.1`, and only `absoluteTransform` (linear part `[[-1,0],[0,1]]` on the parent group) showed the flip. When cloning out of a group, set `clone.relativeTransform` from the source's `absoluteTransform` linear part, then translate. And seat the result by a **transformed vertex**, not the bbox: for a rotated shape the visual tip is a couple of pixels off the box centre in both axes, which is the difference between an arrow that touches its target and one that looks detached.
- **Per-vertex `strokeCap` needs `setVectorNetworkAsync`; the node-level `strokeCap` caps both ends.** A leader that ends in a dot — the house treatment on maps — is one `CIRCLE_FILLED` vertex and the rest `NONE`, which `vectorPaths` cannot express. Build the whole path as a network (`vertices` + `segments`, `regions: []`), and re-assert `x`/`y` afterwards, since the call can re-origin the node.
- **Rewriting `vectorPaths` to drop subpaths moves the node.** Trimming Hawaii out of the US shape, or an antimeridian half out of Fiji, changes the geometry's bbox and Figma re-origins the vector — so the surviving shape lands somewhere else on the map. Compute the kept subpaths' union *before* the write and correct `x`/`y` after it (snippet in GUIDELINES.md → Maps).
- **The plot's edge is where the gridlines stop, not where the last tick label sits.** Grapher insets the first and last x-axis labels so they don't clip — ~17px on a 540px frame — so a year→x mapping fitted through them is wrong everywhere, and "point at the last year" aimed at the label centre lands well inside the plot. Fit on interior ticks (residuals ±0.1px; the two edge labels appear as equal opposite outliers) and take the plot extent from the `horizontal-grid-lines` boxes. Note the group is **plural**: an equality test against `"horizontal-grid-line"` matches nothing, and `Math.max(...[])` then yields `-Infinity`, which surfaces as `Invalid command at Infinity` from `set_vectorPaths` rather than as an empty-selection error.
- **A comma in the upload filename silently loses the asset.** `upload_assets` names the layer from the multipart filename, and a POST of `…(original, with World).svg` returned `{"success":true}` with a `placedOnNodeId` — but no such node existed and only the *other* upload had landed. Keep upload filenames free of commas (parentheses are fine), then rename the node in Figma. And **verify after every batch**: list the page's children and count them, rather than trusting N success responses.
- **Local file styles cannot be imported by key; library styles cannot be applied by id.** The two kinds look identical in a harvest and need opposite handling. `Data Insights/*` and `Instagram/*` are **local** to the Charts file — `importStyleByKeyAsync` throws `Style with key "…" not found`, and you apply them by passing the id straight through (`"S:e06b99…,"`, note the trailing comma). `Default Palette/*` and `Line and Slope Charts/*` come from the **[Chart Colors] Library** and must be imported by key first. Tell them apart by the id shape: a library style's id carries a node suffix (`S:28466fa…,2401:49`), a local one ends at the comma. Get every local id in one call with `figma.getLocalPaintStylesAsync()` / `getLocalTextStylesAsync()`; get library keys from `search_design_system` — and note that a query for the *group* name (`"Default Palette"`, `"Line and Slope Charts"`) is far cheaper than one query per color, **but it is a partial harvest, not an enumeration**: the call caps at ~14 results (gotcha below) while the Default Palette alone runs to 24 fills plus `Gray` (GUIDELINES.md → Colors). Take what it returns, then query the colors still missing by name.
- **Load the fonts you are about to *write*, not only the ones already on the node.** Scanning `getStyledTextSegments(['fontName'])` over the imported chart loads what the export used — and then `label.fontName = {family:"Lato", style:"Bold"}` throws, because nothing in the chart was bold. Two variants of the same trap: `set_fontSize` also throws on a node that merely *contains* an unloaded weight (a template's `Data source:` line is Bold + Regular), so a size sweep over template text needs both weights loaded. Load `Lato Regular`, `Lato Bold` and `Playfair Display SemiBold` unconditionally at the top of any script that touches text.
- **A hugging annotation frame clips its own descenders.** Frames have `clipsContent = true` by default, and `leadingTrim = "CAP_HEIGHT"` puts the baseline *at* the box bottom — so every descender is cut and "today" renders as "todav", "very" as "verv". It is invisible in a node listing and easy to miss in a thumbnail. Set `box.clipsContent = false` on every annotation frame you create; keep the trim (it is what keeps the knockout tight). Clipping is only half of it — the opaque fill still stops at the baseline, so pair it with `paddingBottom ≈ 0.22 × the last line's font size` or the recovered descenders sit outside their own knockout. Both lines are in the Step 8 construction snippet; take them from there rather than patching a frame after the fact.
- **`entity-labels` children are not always TEXT.** When a bar's entity name wraps, grapher groups the two lines, so `node.fontSize = 14` throws `no such property 'fontSize' on GROUP` — and because `use_figma` is atomic you lose the whole pass. Iterate `group.query("TEXT")` for styling and `group.children` for per-row layout.
- **`upload_assets`, never `createNodeFromSvg`** — the plugin sandbox has no `fetch`, and inlining an SVG into `use_figma` blows the 50k-char cap. `upload_assets` handles up to 10 MB and yields an editable vector tree.
- **`rescale()`, never `resize()`** on imported charts — `resize` crops instead of scaling children.
- **Figma plugins can't be run from here — but the no-data hatch no longer needs one.** Imported no-data shapes arrive with an **empty `fills` array**, and the hatch the design team applies by hand is just an `IMAGE` fill, `scaleMode: "TILE"`, `scalingFactor ≈ 0.5` from a 12×12 tile. Reproduce it by copying `fills` from a shape that already has it, or rebuild it from `assets/no-data-hatch-tile.png` via `figma.createImage(bytes)` — and apply it to **every** no-data shape *and* the legend's "No data" pill, never a flat `#C9C9C9` (GUIDELINES.md → Flags, animals, no-data pattern). The Flags plugin (`2654:5`) is still manual.
- **Fonts**: every text edit needs `loadFontAsync` first; the templates use Playfair Display and Lato — if a font is missing in the user's Figma, text edits throw.
- **A text node's `width` is stale for the rest of the script that set its `characters`.** Read it back and you get the *old* width, so any layout computed from it lands wrong — twice in a row, because re-running the same arithmetic in a second script reads the same stale number when the real cause is elsewhere. Two separate things bite here: SVG-imported text arrives at a **fixed** width (the clone of a `22px` value label stays 22px wide and wraps "Poultry" onto two lines), so set `textAutoResize = "WIDTH_AND_HEIGHT"` first; and even then the new width only settles on the **next** `use_figma` call. Write the text and the sizing mode in one call, measure and position in the next.
- **`imType=square` and `imType=uncaptioned` don't render the same chart.** The square re-layout drops per-segment value labels that the uncaptioned crop keeps (and the uncaptioned crop keeps the legend, which is inside the chart area, not the header). Export both and look before deciding which one to embed.
- **`/admin/charts/<id>.svg` doesn't exist**; narrative charts have no public slug — both go through `by-uuid/<uuid>.svg`.
- **Texts come from `.metadata.json`, not `.config.json`** — the latter has no source attribution, omits inherited subtitles/notes, and 404s on MDim slugs. Carry the view's query params on the request.
- **`x`/`y` are parent-relative** — reparent the embed into the template clone before applying the Step 7 coordinates.
- **`?tab=table` silently renders the default tab**; `imSquareSize` is PNG-only; `imWidth`/`imHeight` can't enlarge an SVG (renormalized to ~510k px²).
- **Line charts with >500 points render no dots** (grapher performance cutoff) — don't hunt for dots that were never exported.
- **Never stretch one axis** of the imported chart — dots, squares, and arrowheads distort. Re-export at the right aspect ratio instead. The one sanctioned exception is the scripted plot-only x-map in Step 8, which skips text and preserves marker sizes by construction; verify the markers are still square afterwards rather than trusting that.
- **A sweep over mixed node types must guard every property read.** `dashPattern`, `strokes` and `fills` don't exist on `GROUP`, so one un-guarded read aborts the whole script — and `use_figma` is atomic, so you lose the entire pass, not just that node. Wrap each read in its own `try`, and remember `fontSize`/`fontName`/`lineHeight` can come back as `figma.mixed` rather than a value.
- **To draw a dashed leader or guide line, create a VECTOR with an explicit path** — `figma.createLine()` gives you a horizontal line you then have to rotate, which is fiddly to place. `v.vectorPaths = [{windingRule:"NONE", data:`M 0 0 L 0 ${len}`}]` then `v.dashPattern = [2,2]` is exact and needs no rotation math.
- **The line-chart export's group names are `text-labels`, `connectors`, `lines`, `datapoints__<Entity>`, `outline__<Entity>`, `tick-marks`, `horizontal-grid-lines`** — worth knowing before you go hunting, and worth re-checking per chart type, since the tree is grapher's and it changes.
- **Raising `imFontSize` makes grapher drop labels it can no longer fit.** Bigger type means narrow segments lose their value entirely — Brazil's 7.3% fish label vanished between two exports, and a chart can come back with fewer labels than the one you measured. After changing the font size, check that the specific values an annotation or a recommendation relies on are still present.
- **The Plugin API's shape is not uniform, and guessing costs a round trip.** `figma.getLocalVariableCollectionsAsync` does not exist — variables live under `figma.variables.*`, and this file has paint and text styles but **no color variables at all**, so a variables sweep comes back empty and means nothing. The range setters are **synchronous** (`setRangeFontName`, `setRangeFillStyleId`) while the node-level ones are async (`setFillStyleIdAsync`, `setTextStyleIdAsync`); `setRangeFontNameAsync` is not a method. Read the typings rather than pattern-matching the `Async` suffix.
- **The SVG import renames nodes: spaces become hyphens.** A category displayed as "Beef and buffalo" is the node `Beef-and-buffalo`, so `query('[name=Beef and buffalo]')` finds nothing while the legend text still reads with spaces. Query by the hyphenated node name and map to the label text explicitly — that mismatch is also why the legend has to be paired by geometry rather than by name.
- **`query('[name=…]')` also breaks on punctuation the selector parses** — `Micronesia-(country)` returns nothing because of the parentheses, and a template's `"This is a title on two lines, lorem ipsum…"` fails on the spaces and comma. Both failures are silent `null`s that surface later as `cannot read property of null`. For anything whose name you don't fully control, **build a name→node map by walking `children` once** and look up in that instead of trusting the selector.
- **Figma deletes a group the moment it becomes empty, so never touch it afterwards.** Moving the last child out and then reading `group.children.length` throws `The node with id … does not exist` — and because `use_figma` is atomic you lose the whole script, not just that line. Drain the group and simply don't refer to it again.
- **A mixed-weight text node cannot hold a text-style binding.** The annotation ladder is all Lato Regular, so the moment you bold the country name Figma drops `textStyleId` — `setTextStyleIdAsync` then reads back as unbound, before *and* after. That is expected, not a failure to fix: take the **size** from the ladder value and bind the **fill** style (which does survive), and don't chase the text-style binding. Report it that way rather than as a defect.
- **`insertCharacters`/`deleteCharacters` need every font on the node loaded, not just the one you're writing.** Editing a mixed-weight note throws `Cannot write to node with unloaded font "Lato Bold"` even when the inserted text is Regular. Loop `getStyledTextSegments(['fontName'])` and load each before any character surgery.
- **To re-centre after a block's height changes, translate everything by the same delta rather than re-solving the layout.** A shorter legend leaves the map+legend block off-centre; shifting the map, every annotation frame and every leader by one shared `dy` preserves all relative geometry exactly — labels stay over the same water, leaders stay valid, and no placement search has to run again. Verify afterwards that the leaders still end inside their countries; that check is cheap and catches a mistranslation immediately.
- **`search_design_system` returns about 14 styles per query.** It cannot enumerate a library group in one call, so query each color by name (or query several times with different wording) and resolve hexes with `importStyleByKeyAsync`. Never conclude a group is small because one search returned few results.
- **`get_screenshot` hands back a URL, not an image.** Download it with `curl` and open it with Read — an inline base64 response costs far more context for the same picture.
- **New year, new file** — ask for the link and re-verify every node id in the map above before the first run of a new year.
