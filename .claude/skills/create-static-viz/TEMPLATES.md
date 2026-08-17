# Static-chart template geometry

Measured from the design team's yearly Charts file so an ETL step can be laid out without
re-deriving it through Figma MCP calls every time.

- **File:** `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`
- **Page:** `📑 Templates`, node `798:54`
- **Re-verify with:** `get_metadata` on `798:54` for positions, `get_screenshot` on a frame for colors
- **Last verified:** 2026-08-17 (Vertical and Mobile example 2 measured in full, including the
  header/footer auto-layout structure, every slot's line height and face, and the frame fill)

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
| Subtitle | 80.22 | 817.57 | 38 (two lines) | 16 |
| *chart area* | *118 → 558.6* | 818 | ~440 | — |
| `Note:` | 558.62 | 818 | 28 (two lines) | **12** |
| `Data source:` | 590.62 | 818 | 14 | **12** |
| Tagline (left) | 608.62 | 467 | 13 | **11** |
| License (right, x=571) | 608.62 | 263 | 13 | **11** |

The slots sit in two auto-layout frames: header block `25398:753` spans 0→134.22, footer block
`25398:769` starts at 542.62. Each carries 16 px of inner padding on the chart side, so the visual
chart area starts at 118 while `header.y + header.height` reads 134.22 — that edge plus the padding.

### Vertical — 850 × 1095

Same slots, widths, sizes and auto-layout wrappers as Horizontal. Header block `5332:94` is
**0→134.22**; footer block `5332:101` starts at **999.81**. Absolute y: title 16.22, subtitle 80.22,
chart area 118 → 1001.8, `Note:` 1015.81, `Data source:` 1047.81, tagline/license 1065.81.

**The two header blocks are now identical at 134.22.** They used to differ — 136 here against 134,
from a 30 px title line height against 29 — and that difference is gone, so don't reintroduce a
Vertical-specific header offset.

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
title_row   = max(title_lines × 29, logo_px) + row_pad_px     # the row hugs the taller of the two
subtitle_y  = origin_y + title_row + 6                        # 6 px auto-layout gap
band_top    = subtitle_y + subtitle_lines × 19                # the subtitle's ink bottom
band_bottom = note_ink_bottom − note_lines × 14               # the Note's ink top, growing upward
```

| | 850-wide pair | Mobile (both) |
|---|---|---|
| `origin_y` (block top / its own top padding) | 16.22 | 16 |
| `logo_px` (the logo's row, not the logo) | 41.26 | 35.23 |
| `row_pad_px` (title row's top padding) | 16.22 | 0 |
| `note_ink_bottom` | 1043.81 (Vertical) | — (no Note row) |
| footer row spacing / block top padding | 4 / 16 | 4 / 0 |

**Two things here are counter-intuitive and both cost a round to find.** A one-line title does *not*
shrink the header by a line: below `logo_px` the **logo** sets the row's height, so the 850-wide
header bottoms out at 82.47 however short the title gets. And the footer's rows are pinned to the
frame's bottom margin, so a Note gaining a line does not push the source row down — it eats the
chart's height instead.

### A one-line title leaves a gap the templates were never exercised for

That first point has a visible consequence, not just an arithmetic one. Because the title row hugs the
taller of the title and the logo, and both are top-aligned, the logo's surplus height lands *between
the title and the subtitle*: 12.25 px on the 850-wide pair, on top of the 6 px auto-layout gap. Every
finished page in the Charts file shows **6 px** there — they all have two-line titles, taller than the
logo — so a one-line title is the only case that looks wrong, and it looks wrong by a factor of three.

The fix is in Figma, not in the step: set the logo to `layoutPositioning = "ABSOLUTE"` in the title
row, which keeps it exactly where it is and stops it padding the block. Then set the step's `logo_px`
to 0 for that frame so the derived band follows the header up.

**Check one thing before doing it, per frame.** With the header raised, the subtitle's first line now
runs at the logo's height, and the subtitle slot is full-width in every template. So it is safe only
where that line's ink stops short of the logo's left edge — on the 850-wide frame it ended at x=588
against a logo at 770, comfortably clear; on the 540-wide one it reached x=493 against a logo at 476,
so the logo has to stay in the flow and that frame keeps the wider gap. Measure the first line rather
than eyeballing it (the recipe below), and expect the answer to differ between the two frames of the
same chart.

Calibrate any implementation against both ends: the templates' own two-line/two-line case must
reproduce **118.22**, and a one-line/one-line clone **82.47**.

## Lay the plot inside the template's band, and draw the slots at the template's own sizes

The band is the room between the subtitle's ink and the footer's, inset at each end (the design asks
for 12–16 px; 14 is the middle):

```python
band_top, band_bottom = ...            # derived, per the rhythm above
chart_top_px = band_top + BAND_INSET + header_px
chart_bottom_px = band_bottom - BAND_INSET - below_px
```

**Both edges are ink, not frame.** The footer *frame* starts 16 px above its `Note:` ink, so insetting
from the frame's `y` insets twice and leaves a visibly loose bottom.

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
