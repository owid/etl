# Static-chart template geometry

Measured from the design team's yearly Charts file so an ETL step can be laid out without
re-deriving it through Figma MCP calls every time.

- **File:** `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`
- **Page:** `📑 Templates`, node `798:54`
- **Re-verify with:** `get_metadata` on `798:54` for positions, `get_screenshot` on a frame for colors
- **Last verified:** 2026-08-14

The design team edits these frames in place, and they have moved twice in two days. **Re-verify the
geometry at the start of every refresh** rather than trusting this file: on 2026-08-13 both mobile
templates replaced a single shared footer row with a two-row block, and on 2026-08-14 the Vertical's
subtitle grew to two lines — each time moving a chart area's edge, the second time by 20px. A step laid
out against the stale numbers still renders and still passes every contract check — it just no longer
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
Footer row count no longer separates them — since 2026-08-13 static mobile carries two rows too.

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

### Horizontal — 850 × 638

| Slot | y | Width | Height |
|---|---|---|---|
| Title | 16 | 738 | 58 (**two lines**) |
| Logo | 16 | 64 | 35 (top-right, x=770) |
| Subtitle | 80 | 818 | 38 (two lines) |
| *chart area* | *118 → 556* | 818 | 438 |
| `Note:` | 556 | 818 | 28 (two lines) |
| `Data source:` | 589 | 818 | 14 |
| Tagline (left) | 609 | 467 | 13 |
| License (right, x=571) | 609 | 263 | 13 |

The slots are wrapped in two auto-layout frames mirroring Vertical: header block `25398:753`
spans 0→134, footer block `25398:769` starts at 540. Each carries 16 px of inner padding on the
chart side, so every slot y above is unchanged and the visual chart area is still 118 → 556 —
`header.y + header.height` (134) is that edge plus the padding.

### Vertical — 850 × 1095

Same slots, widths, and auto-layout wrappers as Horizontal. Header block `5332:94` is
0→136; footer block `5332:101` starts at 997. Absolute y: title 16, subtitle 82, chart area
136 → 997, `Note:` 1013, `Data source:` 1046, tagline/license 1066.

Its subtitle became **two lines at 16 px** on 2026-08-14, where it had been one line at 15 px —
which is what moved the header block from 0→116 to 0→136, so a stale 116 now lands inside the
header rather than below it. Both desktop templates now carry the same subtitle style. Their
titles still differ: 29 px line height on Horizontal against 30 px here, so 58 px against 60 px,
which is the whole of the 134 vs 136 difference between the two header blocks.

### Mobile — 540 × 540 (example 1) and 540 × 824 (example 2)

| Slot | y (540×540) | y (540×824) | Width |
|---|---|---|---|
| Title | 16 | 16 | 428 (**two lines**; logo sits beside it at x=460) |
| Subtitle | 80 | 80 | 508 |
| *chart area* | *118 → 486* | *118 → 770* | 508 |
| `Data source:` | 486 | 770 | 508 |
| License | 507 | 791 | 508 |

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
- **A header block's height is derived from its two text slots, so it moves when either reflows.** That
  is what the 2026-08-14 subtitle change did to the Vertical (recorded in its section above): the block
  grew 20px without a single slot being repositioned. Read `header.y + header.height` back after setting
  text rather than trusting a recorded band.

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
