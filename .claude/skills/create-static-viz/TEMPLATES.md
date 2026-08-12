# Static-chart template geometry

Measured from the design team's yearly Charts file so an ETL step can be laid out without
re-deriving it through Figma MCP calls every time.

- **File:** `Charts (2026)`, file key `s6Sv60bakebRRW2TxsMQbF`
- **Page:** `📑 Templates`, node `798:54`
- **Re-verify with:** `get_metadata` on `798:54` for positions, `get_screenshot` on a frame for colors

The page's own instructions frame (`798:151`) states the workflow: *"Copy/paste the template you
want to use and edit it in a new page"*, *"Page name: Date + Chart title"*. `/create-figma-chart`
implements that naming as `YYYYMMDD <Title> (<Creator>)`.

## The four static-chart templates

| Template | Node | Frame size | Panels it suits |
|---|---|---|---|
| `Static Chart Template_Horizontal` | `5332:75` | 850 × 638 | wide charts, panels side by side |
| `Static Chart Template_Vertical` | `5332:93` | 850 × 1095 | tall charts, many stacked rows |
| `Static Chart Template_Mobile (example 1)` | `24590:20` | 540 × 540 | one square panel |
| `Static Chart Template_Mobile (example 2)` | `24590:32` | 540 × 824 | portrait; two panels side by side |

Do not confuse the 540×540 mobile frame with `DI_Template` (`6799:1859`) or
`InstagramPost_Template_English` (`798:161`), which are also 540×540. The tells, per
`/create-figma-chart`: frame fill (`DI_Template` is `#ffffff`, static mobile is cream) and footer
row count (Instagram carries two rows including `OurWorldinData.org/[Topic]`).

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

### Vertical — 850 × 1095

Same slots and widths as Horizontal, wrapped in auto-layout frames. Header block `5332:94` is
0→116; footer block `5332:101` starts at 997. Absolute y: title 16, subtitle 82, chart area
116 → 997, `Note:` 1013, `Data source:` 1046, tagline/license 1066.

### Mobile — 540 × 540 (example 1) and 540 × 824 (example 2)

| Slot | y (540×540) | y (540×824) | Width |
|---|---|---|---|
| Title | 16 | 16 | 428 (**two lines**; logo sits beside it at x=444) |
| Subtitle | 80 | 80 | 508 |
| *chart area* | *118 → 508* | *118 → 792* | 508 |
| `Data source:` | 508 | 792 | 248 |
| `CC BY` (right, x=468) | 508 | 792 | 40 |

## What the mobile templates drop

This is the structural difference that matters, not a styling one:

| Row | Horizontal / Vertical | Mobile |
|---|---|---|
| `Note:` | present | **absent** |
| `Data source:` | present | present |
| `OurWorldinData.org` tagline | present | **absent** |
| License | `Licensed under CC-BY by the author [Name of author]` | `CC BY` |

Desktop's tagline and license **share one row**, left- and right-aligned. Mobile's `Data source:`
and `CC BY` **share one row** the same way.

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

## Unit conversions

- A template pixel is **0.72 pt** (100 template px per inch ÷ 72 pt per inch).
- Size a matplotlib figure as `figsize = (width_px / 100, height_px / 100)` and the saved image
  carries the template's exact proportions.
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
