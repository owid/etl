# Step 7 — Fit the chart into the template

> Read at Step 7.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


The chart spans the full content width, left-aligned with the title/subtitle/logo box, and sits in the band between the header and the footer with an even gap top and bottom.

**This is where the embed arrives.** The band's edges — `headerBottom` and `footerTop` — don't depend on the chart, so read them first, solve the export aspect against that band (Step 3), *then* export the embed, import it, and unwrap it into the template clone with the `unwrap` helper from Step 5. Fitting comes after. That ordering is the whole reason the embed waited this long.

> **Local-SVG route: nothing is *fitted*, but it still has to be scaled.** The SVG came in at Step 5
> and it is a full frame, not a chart area — importing it into the band would nest the whole
> visualization, title and footer included, inside the template's chart slot. Align it to the clone's
> own origin instead — `svg.x = 0; svg.y = 0`, since `unwrap` made the clone its parent and a child's
> `x`/`y` are frame-relative (which is why the fit below reads `chart.x = header.x` and not a page
> coordinate).
>
> **Its *proportions* come from `TEMPLATES.md`; its *size* does not — so scale it to the clone's width
> before trusting any of it.** The `figsize = (width_px / 100, height_px / 100)` recipe fixes the
> aspect ratio, not the canvas: matplotlib writes the root in **points**, and a template pixel is
> 0.72 pt, so the 850 × 638 horizontal frame arrives as `width="612pt" height="459.36pt"`. Figma then
> reads those points at the CSS 96 px per inch, so the import lands at 816 × 612.48 — the figure was
> drawn at 100 template px to the inch and Figma renders it at 96, leaving every slot position and font
> size 96% of target. One uniform rescale to the clone's width puts all of it right at once, and the
> height lands on the clone's by construction because the ratio already matches:
>
> ```js
> svg.rescale(clone.width / svg.width)          // 850 / 816 = 1.0417 = 100 / 96
> // then assert the height, which is what proves the step used the template's ratio
> console.assert(Math.abs(svg.height - clone.height) < 1, svg.height, clone.height);
> ```
>
> A height that misses tells you the step cropped the canvas rather than that the scale went wrong —
> send it back to `verify_static_viz.py --template`, which now measures the SVG root and not only the
> PNG. What you *do* skip is the band measurement, the ladder pick and the x-map below: those exist to
> reconcile an export whose proportions were chosen by grapher, and this one's were chosen from
> `TEMPLATES.md`. The scale is the one piece of the fit the route still needs.
>
> What *does* happen here is stripping whatever the template already provides. **The background comes
> first, and it is the one that ruins the page.** matplotlib fills the figure and axes patches white
> unless the step turned them off, so the import can carry a frame-sized opaque rectangle (`patch_1`,
> plus a plot-sized `patch_2`) that lands *above* the clone's cream background, logo and text and
> hides all three. They are their own groups, so removing the duplicate text does not uncover them —
> delete them explicitly, before anything else. A step written to the current contract emits
> `fill: none`; an older one will not, so check rather than assume.
>
> **Then the duplicate text:** Step 6 filled the template's slots in the file's bound styles, so
> delete the text groups the step named — `title`, `subtitle`, `note`, `data-source`, `tagline`,
> `license`, whichever of them it emitted — leaving only the plotted marks and the in-chart labels.
> **The footer rows are the easy ones to miss**, because the template's own license line sits under the
> same ink: leave the step's behind and the page carries two, in matplotlib's font. Mobile emits fewer
> of these groups (no `note`, no `tagline`) but still emits `license`. Verify both
> deletions by name rather than by position; the template's own slots sit at the same coordinates, and
> deleting the wrong one of an overlapping pair is invisible in a screenshot.
>
> **Whether this has to happen before the rescale depends on what you are rescaling.** An
> `upload_assets` import arrives as a **FRAME whose size is the SVG canvas**, so its scale factor is
> `frame width / canvas width` — exact, and independent of what the children's ink does. Rescale first
> and delete after; that is the order `scripts/restyle_static_import.js` uses.
>
> A **GROUP** is the other case, and the one this warning was written for: a group's bbox is its
> children's ink, and matplotlib's glyph boxes overhang the figure canvas, so while the text is present
> `clone.width / chart.width` solves against the overhang instead of the frame (848.36 against an 816
> canvas on the run that found it). If you are rescaling a group — a hand-made selection, or a frame
> you have already unwrapped — delete the text first, after which the bbox *is* the canvas and one
> rescale lands the height on the template's to `delta 0`.

### The two-pass export, measured end to end

Validated on a live run rather than argued from the model, so the numbers are the claim:

| | pass 1 (model inset) | pass 2 (measured inset) |
|---|---|---|
| `imFontSize` | 29 (solved for 13.5px labels) | 29 (carried — the inset is only valid at its own font) |
| declared, predicted → returned | 849×601 → **849×601** | 869×587 → **868×587** |
| inset used | 40.6 / 40.6 (symmetric model) | **69.75 / 33.72** (measured) |
| `xMapShortfall` | **24.47px** | **0.15px** |

Two things worth taking from it. The probe's canvas prediction was *exact* (849×601 to the pixel), so
a pass-1 miss is never the canvas arithmetic — it is the inset, and only the inset. And the inset read
**identically** on both imports (69.75/33.72 against declared sizes 20px apart), which is the
stability the second pass depends on: measured drift 0.00px, against the ~2px the docs claim. One
`nextPass` command, run as printed, landed the fit with **no correction** — 14px gap top, 13.91
bottom, right edge 523.96 against a 524 content box.

### An upload lands on the file's *current* page — which is not your page

`upload_assets` places the import on whatever page the file currently has open, and page context is
not what your last `use_figma` call set. It landed on the file's **Cover** page on three consecutive
uploads in one run — including immediately after a `use_figma` call that had switched to the working
page — so treat this as the invariant, not a hazard you might dodge: **every** upload needs the
reparent below, and the `placedOnNodeId`'s PAGE ancestor is worth logging every time so a wrong
landing is visible in the tool result rather than discovered later.

So treat the returned `placedOnNodeId` as the only reliable handle: fetch it with
`getNodeByIdAsync`, `appendChild` it onto the page you meant, and **check the page it came from is
left clean**. Two habits that make this cheap:

- Walk up from the imported node to its `PAGE` ancestor and log it, so a wrong landing is visible in
  the tool result rather than discovered later.
- Before finishing, list the landing page's children and remove anything of yours still sitting there.
  An import you uploaded but did not consume (a spare, a superseded round) is litter in someone else's
  file.

### Keep the untouched import beside the edited frame

Standing practice, not clutter: after the styled chart is swapped into its frame, place a **second,
unedited copy of the same SVG** next to it — rescaled to the template's size, named
`<frame name> — original SVG (unstyled)`. Every later question ("did the restyle move that label?",
"was that gap in the export or did we add it?") is then answered by looking, not by re-importing. It
also makes a font or palette pass reviewable by someone who wasn't watching it happen. Two uploads of
the same file per frame costs nothing; deleting the reference costs the next reviewer an import.

**Put it to the LEFT of the frame**, so the page reads original → edited in reading order. To the
right it reads as an afterthought and the eye reaches the raw export last, when it is the thing being
compared against.

### Bind what has a style; name what doesn't

The palette lives in the **Chart colors** library, and the design team's own pages bind chart *marks*
to it (`OWID Distinct/*`, `Chart/*`) while leaving in-chart text on raw fills. Import a style by key
and bind it — `await node.setFillStyleIdAsync((await figma.importStyleByKeyAsync(key)).id)` — after
which the frame follows the library instead of a hex someone typed. To learn the keys, read them off a page that
already uses them (`Plugin / Bar charts` and `Plugin / Line charts` enumerate most of the palette).

Two things this exercise is good for beyond tidiness:

- **The house styles for text are `Text/Gray 100` (#2d2e2d) and `Text/Gray 80` (#5b5b5b)** — the same
  two the templates bind their title and subtitle to. Any other grey in a chart is ad-hoc: an
  imported step's `#333333` header and `#444444` in-bar labels are just matplotlib defaults nobody
  chose.
- **A colour with no style is a finding, not a leftover.** Binding by exact match tells you which of
  a chart's colours are actually house colours. On one chart three of four categories landed exactly
  on palette entries and the fourth did not — it had come from seaborn's grey rather than the palette,
  which is a question for design, not something to bind to the nearest neighbour. Report the
  unmatched colours with counts, and say which are *derived* (tints of a bound base, legibility
  variants) versus genuinely off-palette.

### Restyling a local-SVG import to OWID's fonts and colors

An `export://static_viz` step deliberately does **not** set them: the machine building it may have
neither Lato nor Playfair Display (this one has 436 font families and neither), so a step that asked
for them would fall back silently and emit different type depending on where it ran. Colors are the
same story — the Chart colors library lives in Figma, not in matplotlib. So the step owns the data,
the geometry and the proportions, and **this page owns the type and the palette**. The template's own
slots cover the title, subtitle, note, source and license; everything *inside* the plot arrives in
matplotlib's fallback stack and seaborn's palette, and needs an explicit pass:

- **Read each label's role off its PARENT group, never off the text node's name.** The import names a
  TEXT node after its own content, while the step's `gid` sits on the wrapping group. A role test
  written against `t.name` matches *nothing*, so every label falls to the default anchor and the
  columns come out ragged — it looks like a font problem and it is a selector problem.
- **Take column edges from geometry a text pass cannot inflate** — a column's own marks, or the frame's
  content box — never from the text boxes. Pad those once and a later pass reads the padded width back
  and walks the column off the frame.
- **Hold each label with `textAlignHorizontal` and a box ending on its anchor**, rather than hugging and
  re-reading the width. Not because the width is slow to settle — it is not, see Gotchas — but because a
  hugged box becomes the geometry the *next* pass reads back and pads again, which is the feedback the
  bullet above is about. Right-aligned columns get `x = 0`
  and a box ending on the shared edge; centred labels get a small pad each side. Keep pads small: a
  GROUP's bbox derives from its children, so a generous pad grows the group past the frame.
- **Re-derive tints rather than mapping them.** Where a step computes member fills as `tint(base, w)`,
  invert that per channel to recover `w` and re-apply it to the new color, and each family keeps its own
  internal steps. A flat hex→hex table catches only the base colors and leaves every tint behind.
- **Everything above is lost on re-import**, along with the rest of Step 8 — see the re-export
  section. Keep the pass as one script you re-run, because a frame that has quietly reverted to
  matplotlib's type looks finished.

#### Changing a font moves every label. Put them back.

This is the single most consequential thing on this page, because it is invisible in the script's
output and unmistakable to the person looking at the frame. A text node's box does not re-centre
itself when you change its face: the glyphs re-measure, the box grows or shrinks from its left edge,
and **the label drifts by half of whatever its width changed** — 10 to 24 px per label on a real
restyle, which is enough to walk a name clean out of the bracket it names.

So bracket the font pass with an anchor pass, on the node's *own* alignment:

```js
const before = texts.map((t) => {                  // BEFORE touching any face
  const b = t.absoluteBoundingBox;
  return { t, align: t.textAlignHorizontal, left: b.x, right: b.x + b.width, center: b.x + b.width / 2 };
});
// ... set faces ...
for (const r of before) {                          // AFTER
  const b = r.t.absoluteBoundingBox;
  const target = r.align === "CENTER" ? r.center - b.width / 2
               : r.align === "RIGHT"  ? r.right - b.width
               :                        r.left;
  r.t.x += target - b.x;
}
```

Two notes that make this work in practice. The import **does** preserve `text-anchor` as
`textAlignHorizontal`, so the node itself tells you which edge to hold — no name-based guessing. And
report the count of nodes you moved: on the pair of frames here it was 174 and 145, which is the
difference between "the restyle worked" and "the restyle worked and nothing moved".

**A label the step drew as one multi-line call has no anchor to hold**, so this pass silently treats it
as left-aligned and it drifts anyway. The fix belongs upstream — one call per line, see
[`/create-static-viz`](../../create-static-viz/SKILL.md) — but you are the one who will see it, so check
the emitted SVG for `<text>` elements carrying a `transform="translate(...)"` and no `text-anchor`
before assuming an import is anchor-safe.

#### A line built from several runs needs re-flowing, not just re-anchoring

Where a line is several independent text nodes — a legend of coloured names, a mixed-weight footer row —
holding each run's own anchor still leaves the *gaps* between them wrong, because a narrower face has
to leave its slack somewhere. Lay the line out again instead: group the runs by y, sort by x, and place
each one a measured space after the last.

```js
probe.characters = "nn"; const tight = probe.width;      // measure the space in the new face,
probe.characters = "n n"; const space = probe.width - tight;   // rather than assuming an em fraction
```

On the run here that took the separator gaps from 4.6-before/1.2-after to exactly 3 and 3.

Both reads sit in the **same** call, which is safe because the probe is created auto-resizing rather
than imported at a fixed width (see Gotchas). Measured 2026-08-18 at 12 px Lato: `"nn"` comes back
**14**, `"n n"` **17**, so `space` is **3** — the same 3 the finished gaps show.

#### Don't hand-write this pass — it is a script

[`scripts/restyle_static_import.js`](../scripts/restyle_static_import.js) is the whole thing: place the
import, strip an opaque background patch, drop the step's slot copies by prefix, derive each family's
tints from its base, set Lato,
restore every anchor, bind the library styles, swap it into the frame, re-flow flowed lines, and park
an unstyled copy beside the frame. Fill in its `CONFIG` block and paste it as one `use_figma` call.

It exists because this pass was hand-written six times in one session and each rewrite lost one of the
details above. Edit `CONFIG`, not the body; when you learn something new, change the script so the next
run inherits it.

#### Re-importing a corrected SVG: dump the styling first

When the step's geometry changes, the chart group has to be replaced and every colour and face on it
is lost. Don't reconstruct them from the step or from memory of the palette — **read them off the frame
you are about to replace**, keyed by node name (`japan__sleep` → fill, one row is enough since a column
shares its colour), then replay that map onto the fresh import. The rest of the recipe:

1. Rescale the import — it arrives as a FRAME sized to the SVG's canvas (0.96× the template), so the
   factor is exact and does not depend on the ink's bbox. Do this *before* deleting anything.
2. Delete the step's own copies of the template's text slots — **by prefix, not by exact name**. A slot
   the step had to emit as runs is `license-0 … license-5`, and an exact-name match silently leaves all
   six behind to print over the template's own row.
3. Paint, set faces, restore anchors, re-flow multi-run lines.
4. Append into the target frame at (0,0) with `clipsContent = false`, then remove the old group.

Keep it as one script per frame: the pass is run more than once, always after a step change, and the
only thing that varies is which frame.

**Measure that band; don't hardcode it.** The header's height depends on how many lines the title and subtitle take, so a fixed y is wrong as soon as the subtitle wraps — and centering inside a guessed band leaves a lopsided result (18px above, 6px below on the first run of this skill). Read the real edges instead:

```js
const headerBottom = header.y + header.height       // header block: title + subtitle + logo
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

Then place the y tick labels at `plotLeft − 6 − t.width` and re-anchor each x tick label on its own mark (Step 8). Verified this way, every *horizontal* tick delta on five charts came back exactly `0.000`, and the group's width came back exactly `508`.

**How much gap is right: 14px, and 12–16 is the comfortable band.** That's what the finished pages and grapher itself converge on, measured in 540-wide frames — grapher's own square export leaves 13px above the plot and 14px below; recent DI pages in the file sit at 14/19, 15/14 and 7/15. Below ~10px it reads cramped and the legend starts to look like part of the subtitle; above ~20px you are wasting space the plot could use. When the chart comes out a few pixels too tall, spend the slack down to 12px a side **before** shrinking it — that is usually enough, and it keeps the full content width, which matters more than the last pixel of gap.

**Whatever figure you pick, the chart group's *box* and its *ink* must be the same thing, or you will be asked about it — and both of your answers will be right.** These two are not the same measurement, and each is what somebody trusts: **box gaps** are what Figma's inspector shows a designer who clicks the chart, and **ink gaps** are what a reader sees. Two things routinely pull them apart on a 540 frame — a **peak dot parented as a sibling** of the chart group (its 5px overhang sits outside the box) and **untrimmed tick-label boxes** (~3.5px of leading below digits that have no descenders). Centre the boxes and the ink comes out lopsided; centre the ink and Figma shows something like **18.77 above / 8.77 below**, which reads as sloppy work in the file even though the render is right.

Don't choose between them — make the box equal the ink, then one centring satisfies both. **This takes three `use_figma` calls, and collapsing them is the mistake to avoid:** setting `leadingTrim` does not update `height` within the call that sets it, so a label repositioned in the same call is placed off its pre-trim height, and a `chart.height` read in the same call is the untrimmed group's. Same rule as the value-label recipe in Step 8 — trim in one call, position in the next.

```js
// Call 1 — trim, and bring the dots inside the group. Heights do not settle within this call.
for (const t of tickLabels) t.leadingTrim = "CAP_HEIGHT";   // boxes stop overhanging their own digits
for (const d of dots) chart.appendChild(d);                 // dots inside the group: box now includes them

// Call 2 — heights have settled. Anchor each label on its own mark rather than on a
// remembered pre-trim centre (see "Re-anchor to the marks, not to a remembered box width"),
// keeping grapher's by-construction axis offset — Step 8 audits for it, so don't trim it to 0:
const AXIS_DY = 1.2;                              // ink sits ~1.2px above its gridline on an axis
for (const t of tickLabels) {
  const mark = gridlineFor(t);                    // horizontal-grid-lines carries one vector per tick
  t.y = mark.y - t.height / 2 - AXIS_DY;
}

// Call 3 — only now does chart.height report the trimmed group, so only now can it be centred.
chart.y = BAND_TOP + (BAND_BOTTOM - BAND_TOP - chart.height) / 2;
```

**The trim here is for the group's box, not for the axis's alignment** — that is the whole reason it keeps the offset. What it removes is the ~3.5px of empty leading below each digit, which was inflating the *group's* bounding box past its ink; where the label sits relative to its gridline is Step 8's call, and Step 8 keeps grapher's ~1.2px. Exact centring on the mark is the rule for a label naming a **mark** (a bar, a dot, a segment), not for an axis tick — so don't let this pass drive the axis offset to 0.00 and don't record 0.00 as the target for it.

Then verify on **both** scales — node boxes *and* a pixel scan of the render for the topmost/bottommost ink row — and report both numbers. On this run that landed box 13.02/13.02 with ink 13/14.

**A residual asymmetry of a few pixels is the template's, not yours, and it is predictable per family.** Once the chart's box equals its ink, what is left is the *template's* own leading: the header's box bottom sits close to the subtitle's real ink (most subtitles end in a descender, which fills the lower leading), while the footer's box top sits a few pixels *above* the source line's cap. So **ink-below reads larger than ink-above by roughly the source row's leading**. Measured 2026-08-19, per side, as `(line box − CAP_HEIGHT box) / 2`:

| Family | Subtitle | Source | Note | Expect ink-below − ink-above |
|---|---|---|---|---|
| DI / IG square (540) | 16px → 4 | 14px → 3 | — | ~2–3px |
| Static mobile 1 & 2 (540) | 16px → 4 | 14px → 3.5 | — | ~3px |
| IG portrait (560) | 18px → 4.5 | 14px → 3 | 14px → 3.5 | ~2–3px |
| Static Horizontal / Vertical (850) | 16px → 4 | **12px → 2.5** | 12px → 2.5 | ~2px |
| Small guided / pull (302) | 11px → 2.5 | 11px → 2.5 | — | ~2px |

Read that table as a prediction, not a correction to apply: **optimise the boxes and let the ink land 2–4px asymmetric**, because a 1–4px ink difference is imperceptible while unequal numbers in Figma's inspector read as sloppy work in a shared file. If someone asks for ink-equal instead, shift the block down by half the difference and say plainly that the box numbers are now unequal by that much — one or the other, and state which you chose. What you must not do is "fix" it by moving the template's own text boxes.

Two by-products worth having: the trim makes each label's box match its own ink — which is also what brings the *group's* box down to its ink — so a label naming a **data mark**, a bar value or a segment value, can be centred on that mark at delta `0.00`, while **axis** tick labels keep grapher's ~1.2px vertical offset, which the pass above preserves on purpose and Step 8 audits for. And dots inside the group can no longer be left behind by a later re-centring.

**The 12–16 band assumes the chart group still contains its axis furniture — once you measure the group tightly, the same picture reports a much bigger gap.** Trimming the dangling reference lines and hugging the label boxes to the ink (Step 8) removes ~10–25px of invisible slack from the group's bounding box without moving a single pixel of ink, and the gap number jumps: **20px** on a 14-row bar chart, **30px** on a 4-row one, both of which look wrong against the band and are in fact correct. The tell is that the equivalent measurement on the reference page agrees (17/19 and ~32/33 there). So on an axis-less chart — a discrete bar chart with every value labeled — measure the gap on the reference too and match *that*, and record the figure with a note that the group is tightly measured. Do not shrink a correct chart to force a number.

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

**When the plot and its legend are separate elements, the gap between them is its own decision — and a minimal one is wrong.** A legend strip sitting 16px under a map reads as part of the graphic, a caption bar welded to the bottom edge, rather than as a key you consult; the coastline and the color band start competing. **26px on a 540-wide frame** is what worked here. But don't take grapher's lead and over-correct: its own square export leaves ~57px, which detaches the legend and lets it drift toward the source line. The rule that settles it is **proximity as grouping — map→legend must stay clearly smaller than legend→footer** (26 against 45 here), so the key reads as belonging to the chart and the footer reads as separate.

**And where the slack goes is a design decision, not a residue.** A chart that cannot fill the band — a wide map in a square frame — leaves a fixed surplus (≈116px here) to distribute across three gaps, and "centre the block and leave the middle minimal" is a choice you made by default rather than on purpose. Take an increase in the internal gap out of the **outer** gaps, never out of the chart: the plot keeps its full size and the frame stays symmetric.

Two mechanics when you re-space: the annotations and leaders are **siblings of the chart group, not children**, so they do not travel with it — translate them by the same delta or every label lands over the wrong geography. Then re-verify that each leader still ends inside the thing it points at; that check is cheap and catches a mistranslation immediately.

Side margins and the footer edge are the template's, not yours: content starts at the header's `x` and the footer's bottom stays where the template put it.

| Template | Content x / width | Header bottom → footer top (placeholder subtitle — see the reflow note below) |
|---|---|---|
| DI_Template (540×540) | x=16, w=508 | 118 → 508 |
| IG square (540×540) | x=16, w=508 | 118 → 488 (2-row footer) |
| Static mobile example 1 (540×540) | x=16, w=508 | 118 → 486 (2-row `Frame 15`) |
| Static mobile example 2 (540×824) | x=16, w=508 | 118 → 770 (2-row `Frame 15`) |
| IG portrait (560×700) | x=26, w=508 | 135 → 640 |
| Static Horizontal (850×638) | x=16, w=818 | **118 → 559** |
| Static Vertical (850×1095) | x=16, w=818 | **118 → 1015.81** |
| Small / pull chart (302 × free) | **x=12, w=278** | 44 → `H − 10` (guided) or `H − 23` (pull) — see below |

DI and static mobile get their own rows above because their bands differ, even though both frames are 540×540.

**Every number in that column is the band of a template still carrying its *placeholder* subtitle — and every placeholder wraps to two lines, so on any frame whose real subtitle is one line the header reflows and the true band is bigger than the table says.** The gain is not a DI quirk; it is **every** template except the 302-wide pair, measured 2026-08-19 on throwaway clones by swapping in a one-line subtitle:

| Template | Placeholder subtitle | Band as tabled | One-line subtitle | **True band bottom** | Gain |
|---|---|---|---|---|---|
| DI (540) | 38 (2 lines) | 118 | 19 | **99** | 19 |
| IG square (540) | 38 | 118 | 19 | **99** | 19 |
| Static mobile 1 & 2 (540) | 38 | 118 | 19 | **99** | 19 |
| IG portrait (560) | 44 | 135 | 22 | **113** | 22 |
| Static Horizontal (850) | 38 | 118 | 19 | **99** | 19 |
| Static Vertical (850) | 38 | 118 | 19 | **99** | 19 |
| Small guided / pull (302) | 13 (1 line) | 44 | 13 | 44 | **0** |

So ~19px of free chart on almost every frame (22 on the portrait), and it is invisible if you fit against the table — `118` reads like a template constant. The 302-wide pair is the only exception, because its placeholder is already one line.

**The two 850-wide rows need no preparation any more, and their 99 is confirmed.** Re-measured on the live templates 2026-08-20: both headers are `AUTO` + `HUG`, so they reflow like the other seven, and the rhythm is `origin_y 16` + a 29px title line + a 6px `itemSpacing` gap + a 19px subtitle line. A two-line title over a one-line subtitle therefore derives `16 + 58 + 6 + 19 = 99`, and a one-line title over a one-line subtitle derives 70. Earlier versions of this file required you to make the clone hug first; that was true of the older `FIXED` templates and is no longer — see the header-sizing rule above.

**Which figure is the baseline, so the two tables never look like they disagree:** the first table's numbers are the **as-shipped placeholder geometry** — what an untouched template measures — and `scripts/verify_templates.js` measures templates untouched, so its output will keep reporting `118` / `135`. That agreement is the script working, not a discrepancy to chase. The `99` / `113` figures are the **true bands once a one-line subtitle is in place**, which is the state you actually fit into. So: expect the tabled figures from the script and before Step 6, expect the reflowed figures after it — and in either case re-read the band off the filled clone rather than trusting either table. This is the same reflow the header rule above describes, but it bites *before* you would think to look. **Treat the table as a way to choose a template, never as an input to the fit:** fill the texts, then read the band back off the frames. And note the two effects compound — a one-line *title* lowers the band further still, so a frame with a one-line title and a one-line subtitle is a long way from the tabled figure: on Static Vertical, `118` becomes **70** once the header is made to hug.

The table gives one number per template — the band you fit a chart into — and that is deliberately all it gives. **Per-slot geometry for the four static templates** (each text slot's own `y`/width/height, the derived positions, unit conversions, the exact footer strings) belongs to [`/create-static-viz`'s TEMPLATES.md](../../create-static-viz/TEMPLATES.md), which needs it to place text without opening Figma. Read it there rather than re-measuring into this file: two copies of a measurement drift, and the copy a session happens to read then decides which one was right.

**The 302-wide row is parametric, and its `H` is an output of this step rather than an input** — width is the only fixed dimension, the frame height is chosen from the content, and there is no fit to perform because the export already arrives at 278px wide. Its header block also hugs its own text width instead of spanning the content, so the plot may legitimately rise beside it. All of that is [SMALL-CHARTS.md](../SMALL-CHARTS.md)'s; don't apply the fit below to it.

**No template's wrappers carry inner padding any more, so the frame band and the text band are the same number everywhere.** `header.y + header.height` is the subtitle's own bottom edge, and `contentX`/`contentW` read 16/818 directly on the 850-wide pair rather than 0/850. There is no per-family padding correction to apply — if you find yourself adding 16px to a band you read off a frame, you are working from a stale note. The per-slot text positions behind these bands are `TEMPLATES.md`'s.

**Every template exposes header/footer frames, and on all of them `header.y + header.height` *is* the text band** — 118 on all four 540-wide frames, 135 on the portrait, matching the first table exactly, because that table and this claim both describe the template still carrying its two-line placeholder subtitle. Fill in a one-line subtitle and the same frames report the reflowed bands instead (99 and 113 — see the reflow table above); the point here is only that no padding correction sits between the frame and the text, at either subtitle length and on any family. Read the band off the frames everywhere.

**Every footer is auto-layout and reflows** — static mobile's `Frame 15`, the 850-wide `Frame 22`/`Frame 25`, both Instagram footers (`25518:14` square, `25518:16` portrait) and DI's `Frame 37`, the last one to be converted. What still differs between them is the *direction* they grow — see the `constraints.vertical` rule in Step 6, since almost all of them grow out of the frame rather than into the band. Run Step 6's structural check rather than reading either fact off this paragraph; the split it used to draw between auto-layout and absolute footers is exactly the sentence that went stale last time.

Verify against the actual clone with `get_metadata` (the templates evolve; the geometry above is a 2026 snapshot). These are **frame-local** coordinates, and `x`/`y` are relative to a node's parent — so append the embed to the template clone **before** positioning it. Left parented to the page (where Step 5 puts imported nodes), the same numbers land it near the page origin, on top of the reference chart. One wrinkle in the same rule: **a GROUP is transparent for coordinates**, so once the imported chart is inside the template, its descendants report `x`/`y` in the *template frame's* space, not the group's — which is what makes the frame-local numbers above directly usable on the plot's internals.

> **[`scripts/measure_fit.js`](../scripts/measure_fit.js) returns every number in this step in one
> read-only `use_figma` call** — the band off the *filled* clone, the content box, the header's
> sizing (including whether it actually `reflows`), and, given the imported group, its measured
> bbox, content aspect, the **height-first** scale this step fits with (`TARGET_H / chart.height`,
> as `fitScaleToBandH`) and the leftover width the x-map has to close (`xMapShortfall`). It reports
> the height-first factor deliberately: the gap comes out right by construction once you fit the
> height, so the leftover width — not the gap — is what tells you the aspect still needs a pass. It
> resolves header and footer with the same structural rule as `verify_templates.js`, so a renamed
> frame cannot silently return `null`.
>
> Three things it does that hand-probing tends to get wrong. It reports **rendered** line counts
> (height ÷ lineHeight) rather than counting `\n`, because a *wrapped* title has no newline in it
> and an explicit-break count reports a two-line title as one. `hideIds` computes the group's
> bbox **as if** `connectors` and the year markers were hidden, without hiding them — so the aspect
> you get is the one you will actually fit, with the file untouched. And it takes the content box
> from the **header** (`header.x`/`header.width`, as `verify_templates.js` does) rather than from a
> union over `frame.children`: by this step the chart is already appended to the clone, so a union
> would include the not-yet-fitted group, inflate the box to the group's own width, and report a
> scale of ≈1 — "nothing to do" — which is the one answer the script exists to produce. The union is
> still reported as `contentBoxFromRows` for cross-checking, with the group excluded.
>
> **Run the `nextPass` command it prints; do not rebuild the second pass yourself.** With
> `CONFIG.declared` and `CONFIG.imFontSize` set to what the probe export used, `nextPass` is the
> **measured-inset** pass 2 (`--declared`/`--ink`/`--im-font-size`): the script subtracts the
> measured ink from the declared size to get the true per-axis inset, and the re-solve with that
> inset is exact rather than another guess — the `1.4 × imFontSize` model is symmetric and the real
> inset is not (64.1/29.0 measured at imFontSize 30 against the model's 42/42). The inset is only
> meaningful on a group still at its natural size, so it bounds the value (≤25% of the declared
> canvas per axis) and refuses with `inset.unusable` when the group has already been rescaled or
> `declared` belongs to a different export — falling back to a fresh probe solve with
> `--target-label` carried, so a portrait solved for 15px labels does not come back at 13.5. Set
> `targetGap`, `slug` and `params` in its `CONFIG` to whatever the probe used: the command carries
> them so the re-export keeps the country selection or MDim view instead of being rebuilt by hand.
>
> It also reports `excluded` as `{requested, matched, unmatched}` rather than a count, and warns on
> any `hideIds` entry that names nothing under the group: an id copied from another page excludes
> nothing, and a bare count would report that as a success while the aspect still carried the
> connectors.
>
> Cross-checked read-only against three live templates; every field matched `verify_templates.js`'s
> expected geometry, Static Vertical's `1015.81` footer included.

> **Portrait bands are fine, and `contentX` is not always 16.** One chart taken into all nine
> in-scope templates spanned target ink aspects from **0.79 to 1.42**, and grapher returned every
> canvas at exactly the requested size — the `MIN/MAX_ASPECT_RATIO` clamp never fired, so a tall band
> (mobile 2 at 0.76, Static Vertical at 0.86, IG portrait at 0.97) needs no special handling. What
> does need handling: **IG portrait insets its content at `contentX` 26, not 16**, and its band top is
> 135 with a 28px title against everyone else's 25. Read the content box per template; don't carry 16
> across.
>
> **A title rewrite after the fit invalidates it — in BOTH directions.** Rewording moved band tops by
> **+29px** where the title grew to three lines and **−29 and −32px** where it shrank to one and two,
> so a *shorter* title breaks the fit just as surely by growing the band. Three of five frames
> survived a rewrite; two needed a fresh export. Re-measure the band after any text change, and if it
> moved, re-solve — but re-solve from the **inset you already measured**: it holds across a band
> change at the same `imFontSize`, so the new export lands first time with no second probe.
>
> **The aspect solve is TWO passes, and the second one is exact. Don't try to land it in one.**
>
> The `1.4 × imFontSize` inset in Step 3 is **symmetric, and the real inset is not.** Measured on an
> `imType=uncaptioned` line chart that reserves a right margin for a direct entity label:
>
> | `imFontSize` | inset X | inset Y | what `1.4 × F` predicts |
> |---|---|---|---|
> | 30 | 64.1 | 29.0 | 42.0 / 42.0 |
> | 21 | 70.6 | 40.8 | 29.4 / 29.4 |
>
> The model is right for the charts it was measured on — the recorded examples come out ~44/46 at
> `imFontSize` 32 — and wrong by 2× on the horizontal axis for this class. Note also that inset Y got
> *larger* as the font got *smaller*, so it is not a simple function of `imFontSize`: don't fit one.
>
> So treat the first export as a **probe**, not an attempt:
>
> 1. `solve_export.py --band WxH --target-label 15` → export, import, hide the furniture, measure.
> 2. `inset = declared − ink`, per axis. `scripts/measure_fit.js` returns it, and the ready pass-2
>    command with it, if you give it `CONFIG.declared` and `CONFIG.imFontSize`.
> 3. `solve_export.py --band WxH --declared … --ink … --im-font-size …` → export, import, fit.
>
> The inset is stable to ~2px across an aspect change at the same font, and `insetX − insetY` was
> *exactly* constant per chart across both passes (35.04 and 29.80), which is what makes pass 2 land
> rather than being another guess. Measured errors: pass 1 predicted the canvas to 1.2–1.4px, pass 2
> to **0.4–0.5px**.
>
> **And solve for the gap, not for the band.** `--gap` defaults to 14 because the target is 12–16px at
> *each end*; solving for the band's own aspect asks the chart to fill it edge to edge and leaves
> nothing. On a 508×409 band that is the difference between a target ink aspect of 1.2421 and 1.3333.
>
> **Don't try to measure the ink from the SVG to skip the first import.** Text ink depends on font
> metrics that are not in the file; parsing every coordinate in one came out 13–33px wide of what
> Figma measured.

**The header reflows itself — don't reposition it.** Every template's header block is a flat vertical auto-layout of `[title, subtitle]` (the logo is a sibling, not a child — see the node map), so a title that grows from two lines to three pushes the subtitle down and grows the header on its own. Set `characters`, then **read the new `header.y + header.height` back** and measure the band from that; any y you computed before the text went in is stale. Measured on the portrait: a two-line title gives a 135 band bottom, three lines 199.

**The logo no longer sets a floor under that** — it is a sibling of the header, not a child, so it contributes nothing to the header's height and a one-line title buys the full reduction. What the logo still constrains is *width*: the title node is sized narrower than the content box to clear it (737.84 against 818 on Static Vertical, 428 against 508 on the 540-wide set), which is the number a title has to be measured against. Read the band back rather than deriving it from line counts either way.

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
// Resolve the header structurally — never by name. Names differ per template and are
// renamed wholesale by design edits; the shape does not. The header is the topmost
// auto-layout child, the footer the bottommost. Match ANY direction: DI's footer is
// HORIZONTAL, so a VERTICAL-only filter drops it. Exclude the logo — it is a sibling
// of the header, and an INSTANCE carries its own auto-layout.
const isLogo = c => /^logo/i.test(c.name) || /^Logos\//.test(c.name)
const autos = clone.children
    .filter(c => "layoutMode" in c && c.layoutMode !== "NONE" && !isLogo(c))
    .sort((a, b) => a.y - b.y)
const header = autos[0]
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

**Centering a value label on its mark is a standing rule of this skill, in every format.** It is not a 540-only nicety or a bar-chart special case: an end-of-line value centers on its dot, a bar value on its bar, a segment value on its segment, a legend label on its swatch. Grapher positions text by baseline, so *every* imported label starts high — uniformly, which is what makes it read as intentional instead of broken. Apply the `leadingTrim` + center-on-mark recipe above wherever a number names a mark, and let Step 8c's *Label alignment* row be the gate.

**Re-anchor to the marks, not to a remembered box width.** The snippet above is the fallback for when nothing addressable is nearby; wherever the export gives you the mark, drive off it, because then no amount of re-hugging or stretching can accumulate error. On an axis every anchor is already in the tree: the `tick-marks` group carries one zero-width vector per tick named after its value, and `horizontal-grid-lines` one per gridline, so tick labels align on their mark (grapher **left**-aligns the first and **right**-aligns the last to keep them inside the plot, everything between centered) and value labels right-align on the axis edge. Verified that way, all six *horizontal* tick deltas come back exactly 0 rather than approximately 0 — the vertical offset is a separate matter, and the paragraph below keeps it.

**On an axis, expect a uniform ~1px vertical offset and leave it — but know the bound, because a *large* uniform offset is a real defect.** Grapher positions text by baseline, and digit-only labels have no descenders, so an axis label's visual center sits slightly below its box center: ~1.2px above its gridline, by construction. **Step 7's box-equals-ink pass trims these labels but deliberately preserves this offset**, so it survives into the audit and ~1.2px stays the expectation here, not 0.00. Uniform and small is fine; uniformity alone is not the test. Bar labels measured **5.46px** above their bars' centers on every row of a 14-row chart — perfectly uniform, and visibly high on the render, which is what a reviewer noticed first.

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
