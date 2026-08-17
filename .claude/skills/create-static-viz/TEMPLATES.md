# Static-chart template geometry

Measured from the design team's yearly Charts file so an ETL step can be laid out without
re-deriving it through Figma MCP calls every time.

- **File:** `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`
- **Page:** `📑 Templates`, node `798:54`
- **Re-verify with:** `get_metadata` on `798:54` for positions, `get_screenshot` on a frame for colors
- **Last verified:** 2026-08-14

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

## Two positions that are derived, not arbitrary

- **`subtitle_y = 80` assumes a two-line title.** It is `16 + 2 × 29`, where 29 px is one line at
  the template's title size. Pin a one-line title to y=16 and leave the subtitle at 80 and you
  get a dead line of whitespace. Derive `subtitle_y` from the title's actual line count, and
  calibrate so the two-line case reproduces 80 exactly.
- **The title slot is two lines tall** in every template. A title that wraps to one line
  under-fills it; one that wraps to three overflows into the subtitle.
- **The `Note:` slot is two lines tall too, and the chart band's bottom edge is the footer block's `y`** —
  so a one-line note moves the band. Derive both from the actual slot positions rather than from the
  table above. (What that implies for *editing* a clone in Figma is `/create-figma-chart`'s Step 6, not
  this file's business.)
- **A header block's height is derived from its two text slots, so it moves whenever either reflows** —
  a slot gaining a line grows the block without anything being repositioned. Read
  `header.y + header.height` back after setting text rather than trusting a recorded band.

## Lay the plot inside the template's band, not inside your own text's leftovers

A step draws its own copies of the title, subtitle and footer so its PNG stands alone, and it draws them
smaller than the template does. So a band derived from the step's own text metrics is too generous: the
plot's ink sits flush under the step's subtitle, and once the frame carries the template's larger text
the clearance falls below the 12–16 px the design asks for. Take the band from this file and inset it:

```python
band_top, band_bottom = layout["band"]                                  # the template's own text edges
chart_top_px = max(subtitle_bottom_px, band_top + BAND_INSET) + header_px
chart_bottom_px = min(layout["chart_bottom_y"], band_bottom - BAND_INSET) - below_px
```

**Both band values must be ink edges, not frame edges.** The footer *frame* starts 14 px above its
`Note:` ink, so insetting from the frame's `y` insets twice and leaves a visibly loose bottom (28 px
against a 14 px target). For the Vertical template that means `(118, 1015.81)` — the subtitle's ink
bottom and the note's ink top — not `(118, 1001.8)`.

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
| Frame background | `#fbf9f3` (warm off-white, **not** pure white) |
| Title ink | `#2d2e2d` (serif) |
| Subtitle | `#5b5b5b` |
| All footer rows | `#858585`, with bold labels on `Note:`, `Data source:`, `OurWorldinData.org`, `CC-BY` |

## Exact strings the templates use

Copy these verbatim rather than paraphrasing:

```
OurWorldinData.org — Research and data to make progress against the world's largest problems.
Licensed under CC-BY by the author [Name of author]
```

Note `Data source:` is **singular** in the template, and the label is part of the slot — an ETL
step should emit the label plus its content, not a differently-worded prefix.
