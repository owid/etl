---
name: create-insta-reddit-chart
description: >-
  Turn a finished Data Insight or static chart frame in the design team's yearly "Charts (YYYY)"
  Figma file into a 540×675 (4:5) Instagram and Reddit version on the same page: beige Instagram
  background, Instagram portrait title and subtitle styles, the Instagram footer with an
  OurWorldinData.org/<topic> line, and the chart re-laid out to fill the taller frame. Delivers
  the frame's Figma link; the PNG is exported from Figma by hand. Trigger when the user pastes a Figma frame
  link and asks for an "insta", "instagram", "reddit", "4:5", "portrait" or "540x675" version of
  an existing chart frame.
metadata:
  internal: true
---

# Create an Instagram / Reddit version of a finished chart frame

Takes a **finished 540×540 frame** in the Charts file — a Data Insight (DI) or a static mobile chart
that a person has already designed — and produces the **540×675 (4:5)** version used for Instagram
feed posts and Reddit. One image serves both destinations; nothing is Reddit-specific.

Where a decision was *not* made, this file says so and tells you to ask — don't fill the gap with
a guess.

**Model check, before anything else:** the session context names the running model. On **Fable**,
recommend re-running on **Opus** (or **Sonnet** for a re-export) and continue only on the user's
say-so — the same rule as [`/create-figma-chart`](../create-figma-chart/SKILL.md), which this skill
leans on for everything it does not spell out.

**The Charts file is shared.** Nothing is written to it before the user has seen the proposal
(Step 3) and approved it. Reading the file needs no permission.

**Say it in plain words.** The person asking is usually not a designer or a data scientist. Lead
with what changed and why it matters; keep node ids, style ids and pixel arithmetic for the final
report. The parent skill's GUIDELINES.md → *Talking to the person you're building for* has the
before/after.

## What the skill produces

| Decision | Fixed value |
|---|---|
| Input | A Figma link to a finished frame in the Charts file (`?node-id=` present). Nothing else — no grapher links, no local SVGs, no descriptions. Route those to `/create-figma-chart` first. |
| Output | One frame, **540×675**. No square, no reel. |
| Background | Always the shared paint style **`Instagram/Beige Background`** (#FBF9F3), bound, not a raw fill. |
| Placement | On the **same page** as the source frame, **100px to its right**, same `y`. Never a new page. |
| Frame name | **`<source frame name>-insta-reddit`** — `my-chart-DI` → `my-chart-DI-insta-reddit`. |
| Title / subtitle | Rebound to the Instagram **portrait** text styles: `Instagram/Title (portrait)` (Playfair Display SemiBold **28**, line height 32) and `Instagram/Subtitle (portrait)` (Lato **18**), fill styles `Instagram/Title` and `Instagram/Subtitle` — **unless that makes the header more than 30px taller than in the source**, in which case the source's own sizes stay (Step 5). Text content unchanged either way. |
| Footer | Always the **Instagram footer**, cloned from the square Instagram template's footer in the linked file: `Data source: …` (bold prefix), then `OurWorldinData.org/<topic>` and `CC BY`. A source frame that carries a `Note: …` keeps it as a **first row** above those two. The source frame's own footer is removed. |
| Extra height | **The chart fills it**, by chart type (Step 7): bar rows are re-spaced with taller bars; an axis chart's plot is stretched vertically with text, dots and tick marks moved rather than stretched; a map keeps its size and is centred in the taller band; anything else stops and asks. The chart keeps the source frame's own gaps above and below it. |
| Knockout halos | Text with a **white outside stroke** (the halo that keeps an annotation legible over a line) and any **white backdrop** behind an annotation take the beige — bound to `Instagram/Beige Background` — because a white halo on beige is a visible outline. Flags and the logo keep their white. |
| Checks | (1) every element inside the frame and its 16px margin; (2) a text diff against the source frame — only the footer may differ; (3) the `text-floor` and `ladder-sizes` rows of the parent skill's `verify_page.js` **type** slice. Nothing else. |
| Delivery | The frame's **deep link**. No PNG export — the user exports from Figma (the Instagram family's export scale is **3×**, 1620×2025). |

## Prerequisites

- **Figma MCP tools.** If they arrive deferred, load them in one call, taking the prefix from your
  own tool list (`mcp__Figma__` or `mcp__claude_ai_Figma__` are both in use):
  `select:<prefix>use_figma,<prefix>get_screenshot`.
- **The `figma-use` skill**, before the first `use_figma` call: `/figma-use` if listed, otherwise
  read `skill://figma/figma-use/SKILL.md` via `read_skill_uri`. Pass `skillNames: "resource:figma-use"`
  on every `use_figma` call.

## Round-trip budget

Every Figma call costs a network hop plus a model turn, so **batch independent reads into one
message** and keep writes to **one `use_figma` per message** (two writes to the same page in one
message race each other). A build is about **9 Figma calls**: 3 reads, 4 writes, 2 renders.

## Step 1 — Read the source frame

Parse the link: `figma.com/design/<fileKey>/…?node-id=<a>-<b>` → `fileKey`, node id `<a>:<b>`.
The file key is the year's Charts file (`s6Sv60bakebRRW2TxsMQbF` for 2026; ask for the link if the
year differs).

One `use_figma` read, plus one `get_screenshot` of the frame, in the same message. The read returns:

- the **page** (id, name, top-level children — so you can see the frame is alone or beside siblings);
- the **frame**: name, size, fill (`fillStyleId` — DI is white, static mobile cream), `layoutMode`
  (expect `NONE`);
- every **child** with id, name, type, `x/y/w/h`, fills, `constraints`, and for TEXT its
  `characters`, `fontSize`, `textStyleId`, `fillStyleId`, `textAutoResize`;
- for VECTOR children, `vectorPaths[0].data` (a rectangle is `M 0 0 L w 0 L w h L 0 h L 0 0 Z`).

From that, identify by **position and type, never by name** (names are `Vector`, `Frame 37`,
`Clip path group` …):

| Part | How to find it |
|---|---|
| Title | the TEXT with the largest `fontSize` (25 on a DI) |
| Subtitle | the TEXT directly below the title, `textAutoResize = HEIGHT`, full content width |
| Logo | the FRAME at top-right (`x ≈ 460`, `w = 64`) |
| Footer | the auto-layout FRAME at the bottom (`y ≥ 480`) |
| Bars | VECTORs whose path is a rectangle, all sharing one `x` (the axis) and one height (24px on a DI) |
| Axis | the zero-width VECTOR spanning the bars (`w = 0`, inside a GROUP named `axes`) |
| Row companions | TEXT labels, flag FRAMEs and value TEXTs whose vertical centre sits within a bar's row |

**Classify the chart — the fill-the-height rule in Step 7 depends on it:**

| Chart | Tell | Step 7 rule |
|---|---|---|
| Horizontal bars | rectangle VECTORs of one height, stacked in rows — one per row, or several per row when the bars are stacked or run both ways from the axis | re-space the rows |
| Axis chart — line, stacked area, slope, column, scatter, small multiples | one plot GROUP holding horizontal gridlines (hairline VECTORs, `h < 1`) or a vertical axis, plus stroked polylines, filled areas, columns or dots | stretch the plot vertically |
| Map | one group of hundreds of filled country VECTORs, no gridlines, usually a legend strip | keep its size, centre it |
| Anything else — marimekko, combination, or a frame you cannot place in a row above | — | **stop and ask**, offering "centre it with whitespace" as the option you can execute |

**Stop if** the frame is not 540×540 (every rule below assumes the DI/mobile proportions), or a bar
chart's bars are not all one height.

**Resolve every style and template by name in the linked file — never by a remembered id.** Node
and style ids are file-local, and the yearly Charts file changes every January, so an id copied from
one year returns `null` in the next and `.clone()` on it aborts the build. In the same read as the
frame survey, collect:

```js
const paint = Object.fromEntries((await figma.getLocalPaintStylesAsync()).map(st => [st.name, st.id]));
const text  = Object.fromEntries((await figma.getLocalTextStylesAsync()).map(st => [st.name, st.id]));
const STYLE = {
  beige:            paint["Instagram/Beige Background"],   // frame fill, halos, backdrops
  titleText:        text["Instagram/Title (portrait)"],    // Playfair Display SemiBold 28 / 32
  subtitleText:     text["Instagram/Subtitle (portrait)"], // Lato 18
  titlePaint:       paint["Instagram/Title"],              // #2D2E2D
  subtitlePaint:    paint["Instagram/Subtitle"],           // #5B5B5B
};
// the square Instagram template's footer: bottommost auto-layout child of that frame, on the Templates page
const templates = figma.root.children.find(pg => pg.name.trim() === "📑 Templates");
await figma.setCurrentPageAsync(templates);
const square = templates.findOne(n => n.type === "FRAME" && n.name === "InstagramPost_Template_English");
const FOOTER_TEMPLATE_ID = square.children.filter(c => c.layoutMode && c.layoutMode !== "NONE").sort((a, b) => b.y - a.y)[0].id;
```

Every value must be non-empty before you go on — a missing style name means the file's styles were
renamed, and that is a stop, not something to paper over with a raw color. The ids come back with a
**trailing comma** (`S:<hash>,`); that comma is part of the id (gotcha 2). This read switches to the
Templates page, so keep it in its own `use_figma` call, separate from the source-frame read.

## Step 2 — Ask for what only the user knows

One `AskUserQuestion`, not a drip:

1. **Topic slug** for `OurWorldinData.org/<topic>` — propose one from the chart's subject
   (e.g. `migration`). It has to be a real topic-page slug.
2. **Anything the user noticed in the source** that should be fixed in the copy — and whether to fix
   it in the original too (a wrong flag color, say). Don't go looking for such things yourself; this
   is the user's call.

Everything else is fixed by the table above.

## Step 3 — Propose, then wait

Show in one message: the new frame name, where it will sit, the topic line, the header restyle
(25→28, 16→18, or why the source sizes stay), the footer change, the chart type you classified
and the layout numbers you computed for it (band, and bar height and pitch for bars, or the stretch
factor for an axis chart). **Wait for explicit approval.** After it, iterate freely on the same frame without re-asking.

## Step 4 — Clone, resize, recolor (one write)

```js
const src = await figma.getNodeByIdAsync(SRC_ID);
let page = src; while (page.type !== "PAGE") page = page.parent;
await figma.setCurrentPageAsync(page);
const clone = src.clone();
page.appendChild(clone);
clone.x = src.x + src.width + 100; clone.y = src.y;
clone.name = src.name + "-insta-reddit";
clone.resizeWithoutConstraints(540, 675);          // NOT resize(): see gotcha 1
await clone.setFillStyleIdAsync(STYLE.beige);        // resolved by name in Step 1; id WITH its trailing comma: gotcha 2
// return clone.id and a [source child id → clone child id] map: children keep their order
return { createdNodeIds: [clone.id], map: src.children.map((c, i) => [c.id, clone.children[i].id]) };
```

Read `clone.fillStyleId` back in the **next** call — the same call reports it empty even when it
took (gotcha 2).

## Step 5 — Restyle the header (one write)

Load `Playfair Display SemiBold` and `Lato Regular` first (`loadFontAsync`), then on the clone's
title and subtitle:

1. `await title.setTextStyleIdAsync(STYLE.titleText)`; `await title.setFillStyleIdAsync(STYLE.titlePaint)`.
2. A DI title is `WIDTH_AND_HEIGHT` (auto-width). At 28px it may run into the logo, so give it the
   templates' title box: `title.resize(428, title.height)` **then** `title.textAutoResize = "HEIGHT"`
   — `resize()` resets the sizing mode, so the order matters (parent GOTCHAS).
3. Same two bindings on the subtitle with the portrait subtitle style; it is already `HEIGHT` at
   full content width (508).
4. `subtitle.y = title.y + title.height + 6` — the templates' header gap.
5. Return `title.height`, `subtitle.y + subtitle.height` (the **header bottom**) — Step 7 needs it.

**The header may not eat the height it is meant to free.** Read the header bottom before and after
the restyle. If it grew by **more than 30px**, put the source header back: clone the source frame's
title and subtitle nodes into the clone at their original positions, delete the restyled pair, and
say in the report that the header kept the source sizes and why. A long title and a four-line
subtitle at the portrait sizes can run to three lines and five — that costs the chart most of the
gain, and the chart is what the format is for. A header that grew 30px or less keeps the portrait
styles.

## Step 6 — Replace the footer (one write)

```js
await figma.loadFontAsync({family:"Lato", style:"Bold"});
await figma.loadFontAsync({family:"Lato", style:"Regular"});
const footer = (await figma.getNodeByIdAsync(FOOTER_TEMPLATE_ID)).clone();   // square IG footer, 508×36 (Step 1)
clone.appendChild(footer); footer.x = 16; footer.y = 675 - 16 - 36;   // → 623
const src = footer.children[0];                                        // "Data source: …" row
src.characters = SOURCE_TEXT;                                          // copied from the old footer
src.setRangeFontName(0, "Data source:".length, {family:"Lato", style:"Bold"});
src.setRangeFontName("Data source:".length, SOURCE_TEXT.length, {family:"Lato", style:"Regular"});
footer.children[1].children[0].characters = "OurWorldinData.org/" + TOPIC;  // row 2, left
oldFooter.remove();
```

`SOURCE_TEXT` is the old footer's source node `characters`, verbatim. The `CC BY` on the right is
already in the template row.

**A Note row.** If the old footer carries a `Note: …` text, keep it as the first row: clone the new
footer's source row (`footer.children[0].clone()`), `footer.insertChild(0, noteRow)`, and set its
text with a bold `Note:` prefix the same way. The footer is a vertical auto-layout with a 4px gap, so
it grows on its own (36 → 56px); set `footer.y = 675 - 16 - footer.height` **after** inserting.
Final order: Note, Data source, topic + CC BY.

Read `footer.y` back — it is the bottom of the chart band's clearance in Step 7.

## Step 7 — Fill the height (one write)

The band the chart may occupy keeps the **source frame's own gaps**, whatever they were:

```
bandTop    = old plot top    + (new header bottom − old header bottom)
bandBottom = new footer top  − (old footer top    − old plot bottom)
```

"Plot" is the bar rows' extent for a bar chart and the plot group's box for everything else. Don't
impose gaps of your own — the person who built the frame chose those.

**Bars — [`scripts/relayout_rows.js`](scripts/relayout_rows.js).** Fill its `CONFIG` (clone id,
header bottom, footer top, the source's gaps, the ids to leave alone). It finds the bars (rectangle
VECTORs of the chart's one bar height, grouped into rows by their `y` — a stacked or two-sided bar
chart has several rectangles per row and every one is resized), makes them **30px** tall, inset
**7px** from each end of the
band, at `pitch = (band − 14 − 30) / (rows − 1)`; moves every row companion (label, flag, value
text) by its row's delta **plus 3px** — half the bar-height increase — so it stays centred; redraws
the axis to span the band. Anything it could not assign to a row comes back as `unmatched`, and
**that is a stop**, not a warning.

**Axis charts — [`scripts/restretch_plot.js`](scripts/restretch_plot.js).** Fill its `CONFIG`
(clone id, the plot group's id, `bandTop`, `bandBottom`, the ids to leave alone). It maps the plot
group's box linearly onto the band and treats each descendant by what it is, never by name:

- groups recurse; a small-multiples grid is one plot group, so its panels stretch together;
- **TEXT is moved, never resized.** A label within 30px of the plot's top or bottom edge keeps its
  distance from that edge (x tick labels, a label sitting on the top gridline); every other label
  maps by its centre, which lands y tick labels on their gridlines;
- ellipses and any leaf **≤ 12px both ways** (dots, tick marks, arrowheads) are moved by their centre;
- horizontal hairlines are moved; vertical hairlines are lengthened by rewriting their path;
- every other vector, rectangle or boolean (polylines, areas, columns) gets `height × s` — `resize`
  leaves stroke weights alone, so lines keep their weight;
- siblings of the plot group whose centre lies in the plot's y-range (end dots, value labels beside
  the line, annotations with their leaders) are **translated as rigid wholes** by the map of their
  own centre — nothing inside them is resized, so a curved leader or a backdrop keeps its shape.

It returns what it moved and stretched, and `unmatched` for any leaf it did not know how to treat —
non-empty is a stop.

**Maps.** Nothing is resized. Centre the map-and-legend block in the **new** band: with the block's
current top and height, `dy = (bandTop + (bandBottom − bandTop) / 2) − (blockTop + blockHeight / 2)`,
and move the map group, the legend and any annotations by that `dy`. Don't shortcut it to half the
band's growth — the band's top moves too whenever the header changed height.

**Anything else.** Stop. Say what the frame holds and that no fill rule exists for it, offer the
even-whitespace centring as the option you can execute, and wait.

Text sizes on the chart itself are never changed — only the header may (Step 5).

## Step 7b — Halos and backdrops take the beige (one write)

A DI built on white knocks its annotations out with a **white outside stroke** on the text (typically
3px) or a white-filled frame behind it. On the beige frame that white is a visible outline around
every word. Sweep the clone and rebind those to the background style:

```js
const BEIGE = STYLE.beige;                                            // Instagram/Beige Background, resolved in Step 1
const isWhite = p => p && p.type === "SOLID" && p.visible !== false && p.color.r > 0.99 && p.color.g > 0.99 && p.color.b > 0.99;
const keep = new Set([LOGO_ID, ...FLAG_IDS]);                        // white belongs there
for (const n of clone.findAll(() => true)) {
  if ([...keep].some(id => n.id === id || (n.parent && n.parent.id === id))) continue;
  if (n.type === "TEXT" && n.strokes.some(isWhite)) await n.setStrokeStyleIdAsync(BEIGE);      // halo
  if (n.type === "FRAME" && n.children.some(c => c.type === "TEXT") && n.fills.some(isWhite))
    await n.setFillStyleIdAsync(BEIGE);                                                          // backdrop
}
```

Leave every other white alone — a white stripe in a flag, the logo's lettering, a white value label
sitting inside a dark bar — and **list what you left** in the report, so a white the sweep did not
recognise as a knockout is a known item rather than a surprise. A flag is any FRAME named
`<Country> (<ISO>)` or `Clip path group` beside an entity label; the logo is the top-right FRAME.

## Step 8 — Check, show, deliver

**Checks — one message, three calls in parallel** (all read-only):

1. **Geometry and whites** (`use_figma`): every child of the clone inside `0 ≤ x, x+w ≤ 540, 0 ≤ y,
   y+h ≤ 675` and, for non-GROUP nodes, inside the 16px margin; report breaches with node id and
   name — and say which the source frame already had, since an inherited overhang is the designer's
   choice, not a defect of this run. In the same pass list every remaining **pure-white stroke or
   fill** outside the logo and the flags (Step 7b).
2. **Text diff** (`use_figma`): collect `characters` of every TEXT under the source and under the
   clone, as multisets. The only allowed differences are the footer: the old source/CC BY nodes gone,
   the new source (same text), topic line and CC BY present. Anything else is a defect.
3. **Type slice** of the parent's checker:

   ```bash
   .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js \
       --rows type --frame-id <clone id>
   ```

   Set `chartName` in its `CONFIG` to the plot group's name, paste the output verbatim into one
   `use_figma`, and read **`text-floor` and `ladder-sizes`**. The other rows in that slice assume the
   parent skill's template structure — a header that is an auto-layout frame. A hand-built DI has
   loose title and subtitle nodes, so the script takes the footer for the header: `text-hierarchy`
   then measures plot text against the footer's 14px and fails wrongly, and `source-line-weight`
   finds no footer. Report those two as *not judged*, not as failures, and name the `SKIPPED` rows —
   they are owned by tools this skill does not run.

Then `get_screenshot` the clone at `maxDimension: 1350` (natural size, 540×675) and download it.
Look at it. Fix, re-check, repeat. Re-run **all three** checks after the last change.

**Deliver:**

1. The deep link, once: `https://www.figma.com/design/<fileKey>/<FileName>?node-id=<a>-<b>` (colon
   → hyphen). Deep-link the frame, not the page.
2. **Report** what was created (page, frame name, node id, deep link), the layout numbers, the check
   results including the `SKIPPED` rows, and what stays open — the
   design review above all: **you cannot read Figma comments**, so never report it as clean.

## Gotchas

1. **`resize()` on the frame stretches the chart.** Most of a DI frame's children carry
   `SCALE` constraints, so `frame.resize(540, 675)` scales bars, axis
   and text boxes with it. `resizeWithoutConstraints()` leaves every child where it was.
2. **Style ids end in a comma, and `setFillStyleIdAsync` without it silently does nothing.** The
   id you read off a node is `S:<hash>,`. Passing `S:<hash>` throws no error and leaves the fill
   white; the same call also reports `fillStyleId` as `""` even when a bind *did* take, so verify in
   the next call, not the same one.
3. **A cloned frame's children keep their order**, so `src.children[i]` ↔ `clone.children[i]` is
   the id map — no name matching needed. But every id you captured from the clone is invalid the
   moment you re-clone, so don't cache them across a rebuild.
4. **The DI footer is not the Instagram footer.** A DI carries one row at 13px in
   `Data Insights/Annotation S`; the Instagram templates carry two rows at 14px in `Instagram/Source`.
   Cloning the template's footer gets the bound styles for free; restyling the DI's would not.
5. **`get_screenshot` cannot export at 3×.** `maxDimension` only downscales and clamps at the
   node's natural size, so the deliverable PNG comes from Figma's own export, not from this skill.
   If someone insists on a PNG from here, the only route is a temporary `rescale(3)` copy on the
   page, screenshotted and deleted in the same turn — it leaves a 1620×2025 frame in the shared file
   for the duration.
6. **The plugin's row groups are not chart types.** Flags arrive as FRAMEs named
   `<Country> (<ISO>)` or `Clip path group`, with a hidden white fill; a flag's inner group can be
   wider than its clipping frame (44px inside 29px). Move the frame, never its children.
