# Static-chart template geometry

Measured from the design team's yearly Charts file so an ETL step can be laid out without
re-deriving it through Figma MCP calls every time.

- **File:** `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`
- **Page:** `📑 Templates`, node `798:54`
- **Re-verify with:** `/create-figma-chart`'s [`scripts/verify_templates.js`](../create-figma-chart/scripts/verify_templates.js)
  — it checks the shared geometry (sizes, content box, header band, footer position and growth) for all
  ten templates and returns an `ok`/`DRIFT` verdict. Use `get_metadata` on `798:54` for the per-slot
  positions it does not cover, and `get_screenshot` on a frame for colors. **Run it every refresh —
  the date below never licenses skipping it.** A `DRIFT` verdict stops the refresh and gets reported.
- **Last verified:** 2026-08-20 — the rhythm parameters and the Horizontal, Vertical and Mobile
  example 1 header/footer structure re-measured live; the script returned `ok` on all ten templates.
  The date is provenance, for judging a drift report. If the script *cannot* run, verify by hand with
  `get_metadata` anyway, and let the date say how far to distrust this file meanwhile: **two weeks or
  older, treat every number here as suspect.**

The design team edits these frames in place, and edits that move a chart area's edge have landed days
apart. **Re-verify the geometry at the start of every refresh** rather than trusting this file: a step
laid out against stale numbers still renders and still passes every contract check — it just no longer
matches the frame it gets pasted into.

The page's own instructions frame (`798:151`) states the workflow: *"Copy/paste the template you
want to use and edit it in a new page"*, *"Page name: Date + Chart title"*. `/create-figma-chart`
implements that naming as `YYYYMMDD <Title> (<Creator>)`.

**Division of labor with `/create-figma-chart`, since both skills read this same Figma page.** This
file owns the **measurements** — every slot's position and size, the derived positions, unit
conversions, colors, exact strings — because a matplotlib step has to reproduce them with no Figma
call. That skill owns the **operations**: which node to clone, the single band a chart is fitted
into, and how a clone behaves when you edit it (footer reflow, mixed-weight runs, rescaling). Node
ids and frame sizes are in both by necessity, and that is the whole of the intended overlap — when
you learn something new, add it to the file that owns that side rather than to both, and link.

## The four static-chart templates

| Template | Node | Frame size | Panels it suits |
|---|---|---|---|
| `Static Chart Template_Horizontal` | `5332:75` | 850 × 638 | wide charts, panels side by side |
| `Static Chart Template_Vertical` | `5332:93` | 850 × 1095 | tall charts, many stacked rows |
| `Static Chart Template_Mobile (example 1)` | `24590:20` | 540 × 540 | one square panel |
| `Static Chart Template_Mobile (example 2)` | `24590:32` | 540 × 824 | portrait; two panels side by side |

Do not confuse the 540×540 mobile frame with `DI_Template` (`6799:1859`) or
`InstagramPost_Template_English` (`798:161`), which are also 540×540. The tells, per
`/create-figma-chart`: frame fill (`DI_Template` is `#ffffff`, static mobile is cream) and the
license wording (`CC BY` on DI and Instagram, `Licensed under CC-BY by the author […]` on static).
Footer row count does not separate them: DI carries one row, static mobile and IG square two.

### Not a static-viz target: the "SMALL" Charts section

The same Templates page also holds a `"SMALL" Charts` section (heading `25344:1235`) with
`small-chart-template-guided` (`25344:1357`) and `small-chart-template-pull` (`25344:1391`), both
302 px wide with a **free height**. Those are article thumbnails for the `chart-rows` and
`pull-chart` gdoc blocks, and they are **not** built by an `export://static_viz` step — their
geometry comes from a grapher `imType=thumbnail` export, handled entirely by
[`/create-figma-chart`](../create-figma-chart/SMALL-CHARTS.md).

So do not add a `"small"` entry to `scripts/verify_static_viz.py`'s `TEMPLATE_RATIOS`. It would be
wrong twice: wrong pipeline, and a *ratio* check on a frame whose height is chosen per chart.

## Slot positions

All values in template pixels, y measured **from the top edge** as Figma reports it. Content
margin is **16 px** on all four frames, so content width is `frame width − 32`.

Font sizes are in template px, measured off the live templates on **2026-08-17**. They matter to a
step twice over: the emitted SVG should read like the template it is sized to, and `/create-figma-chart`
fills these same slots when the SVG is imported.

### Horizontal — 850 × 638

| Slot | y | Width | Height | Size |
|---|---|---|---|---|
| Title | 16.22 | 737.84 | 58 (**two lines**) | 25 |
| Logo | 16 | 64 | 35 (top-right, x=770) | — |
| Subtitle | 80 | 818 | 38 (two lines) | 16 |
| *chart area* | *118 → 558.6* | 818 | ~440 | — |
| `Note:` | 558.62 | 818 | 28 (two lines) | **12** |
| `Data source:` | 590.62 | 818 | 14 | **12** |
| Tagline (left) | 608.62 | 467 | 13 | **11** |
| License (right, x=571) | 608.62 | 263 | 13 | **11** |

The slots sit in two auto-layout frames: a header block (`Frame 20` at last check) spanning **0→118**,
and footer block **`Frame 22` (`25808:13`)** starting at **559**. **Neither wrapper carries inner
padding** — `header.y + header.height` *is* the chart area's top edge at 118, and the footer's own `y`
is the `Note:` row. Resolve both structurally (topmost/bottommost auto-layout child) rather than by
name or id: the design team renames and re-ids these frames in place.

### Vertical — 850 × 1095

Same slots, widths, sizes and auto-layout wrappers as Horizontal. The header block (`Frame 23`) is
**0→118**; footer block **`Frame 25` (`25808:16`)** starts at **1015.81**. Absolute y: title 16.22,
subtitle 80.22, chart area 118 → 1015.81, `Note:` 1015.81, `Data source:` 1047.81, tagline/license
1065.81.

**The two header blocks are identical at 118**, so don't reintroduce a Vertical-specific header
offset. They used to differ (136 against 134, from a 30 px title line height against 29), and before
that both read 134.22 — an edge that only existed because the wrappers were padded 16 px on the chart
side. Both the padding and that difference are gone.

> **Wrapper figures re-verified 2026-08-19; the slot table above was not.** The wrapper ids, the 118
> header bottom and the removal of the padding come from the same measurement pass as
> `/create-figma-chart`'s node map. The per-slot `y` values still date from 2026-08-17 and sit within
> ~0.4 px of it (the footer rows derive as 559 / 591 / 609 against the tabled 558.62 / 590.62 /
> 608.62) — immaterial for emitting an SVG, but re-measure before trusting them for anything tighter.

### The license slot holds about fifty characters

263 px at 11 px, and it **shares its row with the 467 px tagline** inside 818 px of content. The
template's own `Licensed under CC-BY by the author [Name of author]` fits; two names do not —
`Licensed under CC-BY by the authors Esteban Ortiz-Ospina and Pablo Arriagada` measures **387 px**,
overruns the tagline by 36 px, and prints on top of it. Drop the two words `the authors`
(`Licensed under CC-BY by <names>`, 329 px) and it clears by 22 px.

Never shorten a name to make it fit; the phrasing is what gives. Mobile is immune — its footer rows
are stacked and full width, so the same string has 508 px to itself.

### Mobile — 540 × 540 (example 1) and 540 × 824 (example 2)

| Slot | y (540×540) | y (540×824) | Width | Size |
|---|---|---|---|---|
| Title | 16 | 16 | 428 (**two lines**; logo sits beside it at x=460) | 25 |
| Subtitle | 80 | 80 | 508 | 16 |
| *chart area* | *118 → 486* | *118 → 770* | 508 | — |
| `Data source:` | 486 | 770 | 508 | 14 |
| License | 507 | 791 | 508 | 14 |

Note the mobile footer rows are **14 px** where the 850-wide pair's are 12 and 11 — mobile has one
row per string and can afford the larger type, so don't copy sizes across the two families.

The footer is one auto-layout block (`Frame 15`, `25343:276` and `25343:275`), 38 px tall, 16 px
above the frame's bottom edge: two full-width rows, 21 px apart.

## What the mobile templates drop

This is the structural difference that matters, not a styling one:

| Row | Horizontal / Vertical | Mobile |
|---|---|---|
| `Note:` | present | **absent** |
| `Data source:` | present | present |
| `OurWorldinData.org` tagline | present | **absent** |
| License | `Licensed under CC-BY by the author [Name of author]` | same string |

Desktop's tagline and license **share one row**, left- and right-aligned. Mobile stacks its two
footer rows instead, both left-aligned at the full content width, so a long source citation and a
long author name no longer compete for one row. A step that lays out both sizes therefore emits the
same license string for each and only varies the alignment.

A caveat that only exists in the `Note:` slot therefore has nowhere to go on mobile. Decide per
caveat whether it is about a *visual artifact* (safe to drop when the artifact is sub-pixel at
mobile size) or about *what the chart claims* (must move into the subtitle instead). See the
`Note:` guidance in `SKILL.md`.

## Every position in the table above is one text length away from being wrong

**The table records the templates as shipped — two-line title, two-line subtitle, two-line Note — and
almost no real chart has all three.** Header and footer are auto-layout blocks: the header grows
*down* from the title, the footer grows *up* from the frame's bottom margin, so a slot that comes in
shorter than the placeholder moves every edge below (or above) it, including the chart band's. A step
that hard-codes any of these numbers is right for one string length and silently wrong for the rest —
this is what leaves a line of dead space above a plot, and it survives every contract check.

So derive them, from this rhythm:

```
title_row   = title_lines × 29 + row_pad_px                   # no max(): the logo is a SIBLING, see below
subtitle_y  = origin_y + title_row + 6                        # 6 px auto-layout gap
band_top    = subtitle_y + subtitle_lines × 19                # the subtitle's ink bottom
band_bottom = note_ink_bottom − note_lines × 14               # the Note's ink top, growing upward
```

| | 850-wide pair | Mobile (both) |
|---|---|---|
| `origin_y` (the header block's own top edge) | 16 | 16 |
| `logo_px` (the logo's **row**, not the logo) | 0 — the logo is a sibling | 0 — the logo is a sibling |
| `row_pad_px` (the title row's own top padding) | 0 | 0 |
| `note_ink_bottom` | 587 (Horizontal) / 1043.81 (Vertical) | — (no Note row) |
| footer row spacing / block top padding | 4 / 0 | 4 / 0 |

**Re-measured live on 2026-08-20** off `5332:93`, `5332:75` and `24590:20`. The headline is that **the
rebuild unified the two families' header rhythm** — both now sit at `origin_y = 16` with no row
padding, where the old padded generation had the 850-wide pair spanning the frame at `origin_y = 0`
and carrying the 16.22 px inside `row_pad_px`. The line steps survived the rebuild unchanged: title
**29**/line, subtitle **19**/line, a **6** px gap between them, and **4** px between footer rows.

The rhythm now reproduces both templates exactly, which is the check that matters:

| Case | Derivation | Live |
|---|---|---|
| Placeholder (2-line title, 2-line subtitle) | `16 + 58 + 6 + 38` | **118** ✓ |
| One-line title, one-line subtitle | `16 + 29 + 6 + 19` | **70** ✓ |

That second row is the one that used to disagree: the stale `origin_y = 0` / `row_pad_px = 16.22` pair
derived **82.48** against a measured 70, and the 12.48 px gap between them was the whole error. Both
figures now fall out of the same formula.

> **The header is a flat auto-layout of `[title, subtitle]` with the logo as a SIBLING**, not a child
> of a title row (`/create-figma-chart`'s SKILL.md → node map). A sibling contributes nothing to the
> header's height, so `logo_px` is 0, the `max(…, logo_px)` cap does not apply, a one-line title *does*
> shrink the header by a line, and there is no logo surplus to land between the title and the subtitle.
> The logo constrains **width** instead: the title node is sized narrower than the content box to clear
> it — 737.84 against 818 on the 850-wide pair, 428 against 508 on mobile.
>
> Everything below this note describes the **superseded nested-logo generation** and is kept only so a
> regression stays recognizable. Its arithmetic no longer applies; the table above is the live one.
> It has moved to [reference/SUPERSEDED-LOGO-GENERATION.md](reference/SUPERSEDED-LOGO-GENERATION.md) so a run does not load it.

## Lay the plot inside the template's band, and draw the slots at the template's own sizes

The band is the room between the subtitle's ink and the footer's, inset at each end (the design asks
for 12–16 px; 14 is the middle):

```python
band_top, band_bottom = ...            # derived, per the rhythm above
chart_top_px = band_top + BAND_INSET + header_px
chart_bottom_px = band_bottom - BAND_INSET - below_px
```

**Both edges are ink, not frame — and on the current templates the footer frame's own `y` *is* its
first row's ink** (Horizontal's footer starts at 559, which is the `Note:` row). So inset once, from
that edge. This used to need a correction: the footer frame started 16 px above its `Note:` ink, and
insetting from the frame's `y` then inset twice and left a visibly loose bottom. If you measure that
gap again, the wrappers have been re-padded — re-verify before compensating for it.

**Draw the step's own copies of these slots at the sizes in the table, not at sizes that merely look
right.** It is tempting to set the step's title and subtitle a size or two smaller — nothing in the
frame uses them, since the import drops them. But then the render's spacing is *not* the frame's: the
band is correct for the frame while the step's smaller subtitle ends 20 px higher, so the PNG shows a
hole that the frame does not have. Whoever reviews the PNG reports it as a bug, correctly. Matching
the sizes, line heights and slot positions costs nothing and makes the render a preview rather than a
proportion sketch — which is most of what the render is for.

## Align to the content box on both sides

Everything in the frame lines up on two verticals: the left edge where the subtitle and note start,
and the right edge where the **logo** ends. Both are the content box, `16 … frame − 16`.

- The left comes free if the country labels are right-aligned against the bars: the widest one starts
  exactly at the margin.
- The right does **not** come free. A total column drawn left-aligned at a fixed gap past the bars
  stops wherever its longest number happens to end, short of the logo. Right-align it on the content
  edge instead — `MINUTES_PER_DAY + total_column_px / px_per_min`, `ha="right"` — and the column, the
  logo and the note all share one edge.

## Two rules for laying out coloured text runs

Both of these produced defects that survived a full visual check and were caught by a reader:

- **A run may not begin with a space.** `TextPath` measures ink, so a *leading* space contributes
  nothing to a run's advance — while matplotlib still draws it. Lay out `["Sleep", " · ", "Eating"]`
  by summed advances and the separator is drawn one space further right than the layout accounted
  for: the gap lands before the dot and vanishes after it (`Sleep ·Eating`). A *trailing* space is
  fine, because `text_advance_px` recovers it with a sentinel glyph. So the space rides with the name:
  `["Sleep ", "· ", "Eating"]`.
- **Punctuation between coloured names needs its own run, in one neutral colour.** Appended to the
  name before it, a separator inherits that name's fill — so it changes colour down the list and all
  but disappears after a pale tint. It is punctuation, not data.

## Predicting the template's line breaks from a step that has neither font

A step decides its layout from strings it measures in its own font, and the template sets those same
strings in Playfair Display and Lato. So every line count it predicts is an estimate, and the error
does not point one way:

| At the same pixel size | vs. a step measuring in Arial/DejaVu |
|---|---|
| Lato, 11 px | **2.4 % narrower** |
| Lato, 16 px | 0.8 % narrower |
| Playfair Display SemiBold, 25 px | **3.2 % wider** |

**Do not "wrap a bit early to be safe".** It reads as prudent and it is not: the footer rows are sized
so the template just fits them, so wrapping 6 % early broke both onto second lines the frame does not
have — a render that looks broken while the frame is fine. Give a Lato slot the few percent it actually
has, take the same few percent off a serif slot, and keep the two directions as separate named
constants so neither gets applied backwards.

**Measure, don't assume — the templates are in Figma and so are the fonts.** One `use_figma` call
settles both the width and the line count for a string:

```js
await figma.loadFontAsync({ family: "Lato", style: "Regular" });   // or the assignment below throws
const node = figma.createText();                    // inside a temp frame you remove afterwards
node.fontName = { family: "Lato", style: "Regular" };
node.fontSize = 16;
node.characters = subtitle;
node.textAutoResize = "WIDTH_AND_HEIGHT";
const naturalWidth = node.width;                    // what the string wants
node.textAutoResize = "HEIGHT";
node.resize(slotWidth, node.height);
const lines = node.height / lineHeight;             // what the slot gives it
```

That is how the three ratios above were measured, and how the Note was confirmed to take **three**
lines at 12 px where a step's smaller footer took two.

## Unit conversions

- A template pixel is **0.72 pt** (100 template px per inch ÷ 72 pt per inch).
- Size a matplotlib figure as `figsize = (width_px / 100, height_px / 100)` and the saved image
  carries the template's exact proportions — **its proportions, not its size**. The `/ 100` puts 100
  template px in an inch, which is what makes every slot figure in this file convert by a plain
  `px / 100`; matplotlib then writes the SVG root in points, so the 850 × 638 frame saves as
  `612pt × 459.36pt`, which Figma reads at the CSS 96 px per inch and imports at 0.96× the template
  (816 × 612.48). Correct that with one uniform rescale on import (`/create-figma-chart` Step 7) — never
  by inflating `figsize`, which would instead put the slot conversion and every point-denominated font
  size in this file out by 1.39×.
- One line of text occupies roughly `1.3 × fontsize` in points, i.e. `1.8 × fontsize` in template
  px. Useful for stacking blocks; **not** accurate enough for line wrapping — measure that
  (see `SKILL.md`).

## Sampled colors — for reference only

The ETL step does **not** set these; Figma does. Recorded so a render can be sanity-checked
against the template it targets.

| Element | Color |
|---|---|
| Frame background | `#fffbf5` (warm off-white, **not** pure white) |
| Title ink | `#2d2e2d` (serif) |
| Subtitle | `#5b5b5b` |
| All footer rows | `#858585` |

## Weights, which are not "regular and bold"

| Slot | Face |
|---|---|
| Title | Playfair Display SemiBold |
| Subtitle | Lato Regular |
| Footer rows | **Lato Medium**, with Lato Bold on the labels |

The footer's body is Medium, not Regular — swapping it for Regular is a visible flattening of the
whole block. Bold marks `Note:`, `Data source:`, `OurWorldinData.org`, `CC-BY` **and the author's
name**: the license row's placeholder reads `Licensed under `(Medium)` CC-BY `(Bold)` by the author `
(Medium)` [Name of author]`(Bold), so bolding a real name is the template's own convention rather than
a request to argue with.

A step cannot reproduce this — matplotlib has no rich text, so a mixed-weight row is several text
objects, and its font may have no Medium at all. Emit the row as runs (`SKILL.md`) and set the faces
in Figma.

## Exact strings the templates use

Copy these verbatim rather than paraphrasing:

```
OurWorldinData.org — Research and data to make progress against the world's largest problems.
Licensed under CC-BY by the author [Name of author]
```

Note `Data source:` is **singular** in the template, and the label is part of the slot — an ETL
step should emit the label plus its content, not a differently-worded prefix.
