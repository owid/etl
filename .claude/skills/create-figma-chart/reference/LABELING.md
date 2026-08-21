# Step 8 — Improve the labeling and annotate

## Before placing an arrow: does the note point at a POINT?

An arrow aimed at one dot asserts "this value, here". If the sentence is about a **span of time or
several events**, that is the wrong device, and the recipe below will happily execute it anyway —
which is the trap. "Four military coups between 1955 and 1976 cut short democratic rule" was given a
single arrow to the deepest trough: the note describes 21 years and four events, the arrow claimed one
year and one value.

`reference/per-chart-type/line.md` already carries the devices for this. Pick by what the note is
about:

| The note is about | Device | Archive |
|---|---|---|
| A period | a **pale tinted vertical band** across it, annotation set inside the band | `349:167` ("Global financial crisis", "COVID pandemic"), `541:217` ("Great Leap Forward"), `222:504` (annotation inside, in the band's colour) |
| A period, told through the line itself | **recolour the stretch** the story is about | `202:831` (red where democracy eroded, navy elsewhere), `1:4716`, `1:1465`; `678:490` labels the phases in-plot |
| Several discrete events | a **milestone ladder** — a dot per event, each with its value and short sentence | `222:133`, `366:245`, `228:63`, `359:111`, `241:131` |
| A moment | a **thin vertical event rule** labelled at the top of the plot | `347:69` (three of them), `486:51`, `589:211` |
| One value | an arrow, per the recipe below | — |
| A gap between two lines | a **double-headed arrow labelled with the ratio** | `250:120` ("six times") |

Read that table before the recipe, not after. The arrow mechanics are long and specific, and the
detail is exactly what makes "point at the nearest dot" feel like the answer.

### Building the period band

There is **no library style for it** — `search_design_system` for a pale tint returns nothing — so take
it from the archive. Read off `349:167` and `541:217` (the Great Leap Forward band), which agree:

- a **RECTANGLE**, fill **`#dddddd` at 50% fill opacity** (`fills[0].opacity`, not the node's)
- spanning the **plot**, not the frame: use the `horizontal-grid-lines` group's bbox, which is the
  drawing area without the axis-label rows
- inserted at **`clone.insertChild(0, band)`** so the line, dots and labels all draw over it
- and **no arrow**. The band is the pointer; adding one re-asserts the single point the band exists to
  avoid.

Set the band's own x from the line's vertices at the first and last year of the period, the same way
dots are placed — not from the axis ticks, which are sampled and may not include your years.

**The annotation goes inside the band** (`222:504`), padded ~10px from its top-left, with its width set
to the band's width less that padding on both sides. If the text does not fit the band's width at a
readable size, the band is too narrow for an in-band label — put the note beside it instead of
shrinking the type.

## Placing an arrow

An arrow has two ends and both are load-bearing: the tip has to point at the thing it names, and the
tail has to read as leaving the annotation. Bounding boxes get neither right — the group's box is
mostly tail, and the head's box has the tip in a corner.

**Measure the library once.** Every arrow on the `↪️ Curvy Arrows` page has a natural tail→tip
**span** — a length and an angle. Measured 2026-08-20:

| Arrow | box | span | angle |
|---|---|---|---|
| `Group 18` `6086:565` | 14.6×43 | 41.5 | −87.9° |
| `Group 3` `4937:94` | 56.3×43.9 | 63.4 | 135.1° |
| `Group 5` `4937:96` | 56.3×43.9 | 63.4 | −142.1° |
| `Group 19` `6086:568` | 70.4×25.7 | 72.6 | −8.1° |
| `Group 4` `4937:95` | 68.8×51.8 | 74.7 | 148.5° |
| `Group 1` `4937:92` | 68.8×51.8 | 74.7 | −148.5° |
| `Group 8` `6373:161` | 29.8×76.1 | 81.4 | 110.6° |
| `Group 7` `4941:61` | 95.4×69.4 | 117.6 | 144.1° |
| `Group 6` `4941:42` | 162.9×42.4 | 165.2 | −170.5° |

To get a span: the **head** is the smallest-area vector child and the **tail** the largest; read each
one's real vertices by parsing `vectorPaths` and pushing every pair through its `absoluteTransform`
(`x' = a·x + c·y + e`, `y' = b·x + d·y + f` for `[[a,c,e],[b,d,f]]`). The head's **apex** is its vertex
farthest from its own centroid; the tail point is the tail vector's vertex farthest from that apex.

**Then, per arrow:**

1. **Place the annotation first**, at the position the design wants.
2. **Anchor** = where the arrow should leave it — the bottom edge for a downward arrow, the left edge
   at first-line height (`+ lineHeight/2`) for a sideways one.
3. **Target** = `dotCentre − unit(dotCentre − anchor) × (radius + gap)`. This is what makes the arrow
   point *through* the dot's centre along its own angle, so a diagonal arrow arrives diagonally —
   which aligning the tip on one axis does not achieve. 5px of gap off a 10px dot reads well.
4. **Pick the arrow whose span length is closest to `|target − anchor|`.** Length is what decides
   whether the tail reaches the block; the library spans 41–165px, so something usually fits. Do not
   scale one to fit — that distorts the head.
5. **Rotate**: `rotation = natural − required`, because Figma's rotation is counter-clockwise against
   y-down angles. Then re-measure and correct by `(achieved − required)`; one iteration is exact.
6. **Translate** so the apex lands on the target.
7. **Nudge the annotation** so its anchor edge sits 3–5px off the tail. The arrow is fixed-length, so
   the block moves, never the arrow — moving the arrow loses the tip.

**Check all four, they are one subtraction each:** the tip is `radius + gap` from the dot centre; the
cross product of the heading with `(dotCentre − tip)` is **0**; the arrow's box does not overlap the
annotation's; and the annotation is still inside the content box — pushing a block sideways to meet a
tail is how it ends up hanging off the frame.

**A tail that stops short of the block reads as two unrelated objects.** Sitting 4px *above* an
annotation's top edge is not connected; the tail has to be within the block's vertical span, level
with a line of text.

**The value label and the arrow want the same spot, so place the label opposite the approach.** An
arrow aimed at a dot arrives along its own heading, and a value centred *above* that dot lands right
under the tip — measured on all five templates of one chart. Put the value diagonally opposite the
arrow's approach instead: the restoration arrow came in from the upper right, so its value went
up-**left** of the dot, bottom-right corner near the dot's top-left.

**Check the label against the line on BOTH axes, not just the one you were thinking about.** The
trough value was placed above its dot, clear of the flat trough line it sat over — and straight
across the 1975→1976 **vertical** drop, which the "is it above the line?" check never looked at. It
moved left of the dot, where the descending line is far above and the plot is empty. A steep segment
is as much of an obstacle as a flat one and is easier to forget.

**A bbox overlap between a long arrow and a label is usually empty — confirm on the render.** Three
of five frames reported the arrow's box overlapping the value label after the fix, and all three were
clear in the render: the long library arrows (117–165px) have large boxes around a thin curve, so most
of the box is air. Treat the box test as a screen that tells you where to look, never as the verdict.

**Selection across templates is real work, not a formality.** Ten placements of the same two
annotations across five templates drew **five different library arrows** — `Group 3`, `Group 8`,
`Group 6`, `Group 19` and `Group 7` — because the required spans ran from 62px to 202px as the band
changed shape. Picking one arrow and reusing it would have missed by up to 100px.

> **The knockout stroke matches whatever is DIRECTLY BEHIND the text — usually the frame, but not
> always.** Text sitting inside a period band has the band behind it, so the stroke is the band
> composited onto the frame: `bandFill × opacity + frameFill × (1 − opacity)`. Measured here that is
> `#ecebe8` on the beige templates and `#eeece9` on the cream ones, where the frame alone would have
> given `#fbf9f3` and `#fffbf5` — close enough to look deliberate and wrong enough to see. The rule
> below is the common case, not the whole rule.
>
> **The knockout stroke is the FRAME'S OWN BACKGROUND — read it off the frame, never hardcode white.**
> Only two of the nine templates are white. Measured: the IG pair is **`#fbf9f3`** (beige), the static
> trio **`#fffbf5`** (cream), and only DI and the 302-wide pair are `#ffffff`. A white halo on a cream
> frame draws a visible outline round every line of annotation text — it reads as a deliberate stroke,
> not as a knockout, and it is the first thing a designer sees. `scripts/verify_templates.js` already
> reports each frame's `fill` for exactly this reason ("DI white, static cream, IG beige"): take the
> colour from `frame.fills[0]` and copy its channels into the stroke. It is also assertable —
> every annotation's stroke should equal its frame's fill, which is one comparison per node.
>
> **A value label that lands on the line gets MOVED, not masked.** The white knockout stroke is for
> an annotation that has to cross chart ink to sit where the reader needs it — not a licence to park
> a number on a line and mask it. The house preference is that labels stay off the ink. On the Chile
> chart the trough value sat on the flat `0.03` segment; the fix is 6px above its dot, in the empty
> plot the drop and the rise leave behind, and no stroke at all. Reach for the knockout only when
> there is genuinely nowhere clear to go.
>
> **A direct end label centred on the last point collides with a dot placed there.** grapher's own
> layout starts the entity label about 2.6px after the final data point, so the moment you add an
> endpoint dot the two overlap — measured 3.4px on the square and 2.4px on the desktop, and you
> cannot fix it by sliding the label right because it is already on its 16px margin. Put the label
> **below** the dot (6px clear, right edge still on the margin), which is also the single-series move
> this file recommends further down for reclaiming the right margin.


> Read at Step 8. Also covers Step 8b and re-exporting after a change to the chart itself.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


**Read [GUIDELINES.md](../GUIDELINES.md) now if you haven't.** Browse 1–2 recent dated pages in the file (`get_screenshot`) to see how finished charts apply these conventions. The imported SVG is a fully editable vector tree — text nodes, line vectors, legend swatches are all addressable via `use_figma`.

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
     - **With one series there is a better move: take the label out of the margin entirely.** Put it *below* the last point instead of beside it, right-aligned on the content edge, and the margin stops existing — `plot_right = content_right − dotRadius`, so the end dot's own edge lands on the content edge and the label sits under it. Measured on a 540 frame, that took the plot from **485** to **518.6**: +31.9px, **7% more plot**, against the 39px the beside-the-line placement had reserved for a 34px label. The dot carries the attachment, so nothing is lost. Only for a single series — with several lines the labels must stay beside their own ends to stay distinguishable, and then the arithmetic above is the right one. See `reference/per-chart-type/line.md`.

  **Placing direct labels is a constrained search, not an offset — and "clear of its own line" is not the test.** Putting each label at a fixed offset from its anchor (say `startX + 5`, centered) reads fine in a node listing and lands labels **on top of other entities' lines**, which is the first thing a reviewer sees. Make it a search instead: per label, generate candidate slots and accept the first that passes every acceptance test, with the polyline test (Step 8c) doing the real work.

  - **Candidates** — beside the anchor and on both sides of the line, at several vertical offsets: `left-of-anchor` (centered), `above`, `below`, each also at ±10 and ±22px. On a convergence chart the anchor is the line's *first* point (`reference/per-chart-type/line.md`); otherwise its last, or a fraction along it.
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

  **But measure it on pixels, not on boxes — this is where the bbox model flips from safe to wrong.** The same subpath-bbox model is *conservative* for placement (it over-states land, so it never puts a label on a country) and therefore *false-alarming* for visibility (it reports a line as buried when it is over open water). A diagonal country is the killer: Mexico's bbox swallows a wedge of open Pacific off its west coast, so a leader crossing that ocean scored **0% visible** when the render shows **45%**. Get ground truth by sampling the rendered PNG — `get_screenshot` the frame, then read pixels **perpendicular** to the line (±2–3px for a 1px stroke, less for a hairline — the offset is derived below, and its job is to clear the leader's own stroke without answering for the next shape over) and count how many are the canvas color:

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

  **Compare against the canvas color with a tolerance, never `== CANVAS`.** This is the line that decides whether any of the rest works. The leader is antialiased, so the pixels beside it are blends of stroke and canvas, and exact equality reads them as "not canvas" — i.e. as land. Measured on a synthetic 4× render of a leader that is **71.4% over open canvas**: exact equality returns **41.5%**, a 30-point false *buried*. The same code with a tolerance of 8 per channel returns **71.6%**, and stays within a point of the truth at 1×, 2× and 4×. Tolerance also makes the result insensitive to the offset, which is what you want from a measurement.

  **Scale the perpendicular offset with `s` *and* with the stroke; a fixed constant fails silently at high `s`.** The offset exists to clear the leader's own stroke, and the stroke's rendered footprint grows with the raster — at 4× a 1px stroke covers 4px plus its antialiasing fringe, so an offset of 4px is still inside the line and reports it buried. `2.5 × s` clears a **1px** stroke at every scale tested — but that is `2.5 × w × s`, and the house map leader is **0.3px**, where the same number samples 2.5 frame units out into the map and, along a narrow coast or a small island, answers for different geography than the pixel under the line. Take `w` from the node. Floor it at ~2 *raster* pixels, which is roughly where the antialiasing fringe ends: an offset proportional to a sub-pixel stroke alone sits *inside* the fringe, which the tolerance paragraph below measures as a 30-point false *buried*. The 1px figures here are measured; the hairline floor is reasoned from them, so check one leader you can see on the render before trusting a sweep. Stepping matters less but is free: step `samples_along` one *raster* pixel, since one frame unit on a 4× raster reads every fourth pixel.

  **Guard the direction of `s`, because inverting it is the plausible silent error.** Writing `frame_width / img.width` gives `0.25` instead of `4`, which shrinks every sample into a corner of the raster that is usually empty canvas — so the leader reads as fully visible, the exact false clear this whole check exists to remove. `assert s >= 1` catches it outright for any real export. The known-ocean / known-land probe catches it too, and it is worth keeping because it also validates the orientation: under an inverted `s` the land probe comes back canvas and the assertion fails. Just don't rely on it alone — it only fires if the mis-mapped point isn't coincidentally dark, whereas the `s >= 1` guard cannot miss.

  Rule of thumb: **use boxes to decide where things may go, use pixels to judge how it reads.** And when the user says a line looks fine and your metric says it doesn't, believe the render — they are looking at ground truth and you are looking at a proxy.

  **When labels won't fit beside their countries, make them narrower before you move them further away.** Breaking `Country 55.0%` onto two lines (name over value) roughly halves the width and costs one line of height, which is what lets a label sit in a narrow strait instead of an ocean away — it took the worst leader from 134px to 64px on this chart. A long leader is a worse defect than a two-line label.

- **Fill in the gaps in a time axis — but only where they measurably fit, and otherwise leave grapher's axis exactly as it is.** Grapher drops tick labels to avoid collisions at the width it rendered for, so once you reclaim a margin (above) the axis can be left reading `1990 · 2000 · 2005 · 2010 · 2015 · 2025` when the room for a complete 5-year run is now there. Clone an existing **interior** tick vector and label — never build one from scratch, the clone inherits the stroke, size, color and alignment — set the characters, and place both at `x = x(1990) + (year − 1990)/(span) · (x(2025) − x(1990))`.

  **Two fit tests, not one, because the edge labels are anchored differently.** Interior labels are centered on their tick, so they need `pitch ≥ labelWidth + ~8px`. The **first** label is left-aligned *at* its tick and the **last** right-aligned *at* its tick — grapher does this to keep them inside the plot — so each spends a full label width on its inward side, and the two slots next to them need roughly `1.5 × labelWidth + gutter`. Miss that and the arithmetic says yes while the render overlaps: on one chart a 50.3px pitch cleared the 43px interior requirement, added 1995 and 2020, and left both of them 2.2px *inside* the edge labels. Measure the neighbor gaps after adding and revert if any is negative — the honest outcome is often that grapher's axis was already right, and the years it dropped were exactly the edge-adjacent ones.

- **Annotations replicating the accompanying text** (12–16px; 12–14px on maps): text color = the annotated object's color, `Text/Gray 80` #5B5B5B, or a mix; bold the key phrase; append last so it sits above the chart. **The annotation is a bare TEXT node — no wrapping frame** — and it gets a knockout only if it actually crosses chart ink, in which case that knockout is a **3px outside stroke in the template's canvas color**. GUIDELINES.md → Annotations has the three tiers and why the stroke beats a frame wherever it suffices.

  ```js
  const txt = figma.createText();
  clone.appendChild(txt);                                   // last child => above the chart
  txt.name = "annotation__<what>";
  txt.fontName = { family: "Lato", style: "Regular" };
  txt.fontSize = 15;                                        // a ladder value: XL 16 / L 15 / M 14 / S 13 / XS 12
  txt.textAutoResize = "WIDTH_AND_HEIGHT";                  // hug the longest line; cannot re-wrap later
  txt.characters = lines.join("\n");                        // explicit breaks: never split a quantity
  await txt.setFillStyleIdAsync(ANNOT_FILL);                // Data Insights/Annotations — LOCAL id, not a key
  txt.setRangeFontName(at, at + phrase.length, { family: "Lato", style: "Bold" });
  txt.leadingTrim = "CAP_HEIGHT";                           // box = ink, in every tier

  // Tier 2 only — add the halo when, and only when, the block crosses furniture.
  if (crossesChartInk) {
    txt.strokes = clone.fills.map(f => ({ ...f }));          // the TEMPLATE's canvas, not hardcoded white
    txt.strokeWeight = 3;                                    // set explicitly — see the note below
    txt.strokeAlign = "OUTSIDE";                             // halo the letterforms, don't deform them
  }
  ```

  > **Set `strokeWeight` explicitly; never inherit it.** A text node that has been through `rescale()`
  > carries a *scaled* stroke weight even though it had no strokes — 0.65 after a 0.652 height-fit — so
  > assigning `strokes` without the weight gives a sub-pixel halo indistinguishable from none. Read it
  > back and assert 3.

  > **Take the color from the template, never hardcode white.** The DI and static templates are white,
  > but the Instagram ones sit on `Instagram/Beige Background` `#FBF9F3` — a white halo there is a
  > visible outline around every letter. Copying `clone.fills` works on whichever template was chosen,
  > and keeps working if a future template introduces another canvas color. Same rule for a tier-3
  > frame's fill.

  > **Decide `crossesChartInk` by measurement, per annotation — not per chart.** Run the sampled-polyline
  > and gridline test from Step 8c against the annotation's rect: cross nothing and it needs no
  > treatment at all. A rework of a page built by this skill did exactly this — the annotation over two
  > gridlines got the 3px stroke, the one sitting in open canvas got no stroke and no frame — and the
  > note that came with it was "add a white outside stroke of 3px **when it overlaps** on chart
  > elements". Blanket-treating every annotation is the thing to avoid.

  > **If you do need tier 3** (ink too dense on the canvas for a halo — never a filled area, see GUIDELINES.md), the frame recipe
  > and its two traps — `clipsContent = false` and `paddingBottom ≈ 0.22 × the last line's font size` —
  > are in GUIDELINES.md → Annotations. Note `figma.createAutoLayout()` **is** a real API (declared in
  > `references/plugin-api-standalone.d.ts`; the `figma-use` skill's rule 12a prefers it over
  > `createFrame()` + absolute coordinates) — a review pass once asserted otherwise and the rewrite to
  > `createFrame()` had to be reverted. Check the typings before changing it.
- **Arrows**: copy curvy arrows from node `798:773` — 1px stroke, arrowhead and line the same color as each other and consistent across the chart. Never scale a whole arrow (it distorts the head): Shift-resize the line segment only, then reposition the head. If a curvy arrow gets messy, use a straight thin line. **Maps: never curvy and never an arrowhead — the hairline leaders of `reference/per-chart-type/maps.md` (`#2d2e2d` at 0.3px, filled dot at the country end), or values inside country shapes.** The 1px stroke above is for arrows on a plot; at 1px a leader on a map reads as a border.
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

**Everything that lived inside the old chart goes out with it — replay it, from a list.** That pass restores only the furniture removal, the scale and the text re-hug. Every other Step 8 edit was parented under the group you just removed: the hidden `connectors`, the cloned direct labels and their placement, the added ticks, the bound stroke and fill styles, and the whole highlight treatment (gray context lines at 1px, the palette color on the protagonist, the widened halo, the hidden markers). Only the annotations survive, because they are parented to the template clone rather than to the chart. Keep the chart-local edits as **one scripted function you re-run after the import**, or as an explicit list you work down — memory is not enough, because a frame that has quietly reverted to grapher's raw rendering looks finished.

  **[`scripts/replay_chart_edits.js`](../scripts/replay_chart_edits.js) is that function.** Fill in a
  `CONFIG` saying *what* the frame needs — nodes to hide, subpaths to trim, furniture to shorten to the
  data's extent, the furniture weight, a map legend to re-centre — and it owns the **order**, which is
  the part that is easy to get wrong and expensive to debug: hide and trim *before* measuring (they
  change the group's proportions), fit with one `rescale`, close the width residual, and set strokes
  **last**, because `rescale` multiplies `strokeWeight` and setting them first is the most repeated
  mistake in this skill's history. It runs `dryRun: true` by default and returns the plan; read that
  before letting it write. It also refuses a width squeeze beyond 0.5% rather than silently rewrapping
  labels, and reports a rewrap by **line count** (`height / fontSize`) rather than raw height, since a
  uniform rescale changes every height in proportion and a raw-height check cries wolf on all of them.

  **Set `bindAxis: "width"` for a map.** The default fit is height-first, which is only correct because
  the export solve makes the aspect the band's — and a map's aspect is the projection's, not the
  canvas's. Height-fitting one overflows the content width by a measured **141px** (FITTING.md), so a
  map fits width-first and is centred in the band with the larger gaps that leaves. A value that is
  neither `"height"` nor `"width"` throws rather than quietly fitting the wrong axis.

  **And read the `verdict`, which now carries every way the run can be incomplete** — a refused width
  fit, a map taller than the band, a box that missed the content edges, asymmetric gaps — not just a
  rewrap. It used to say "wrote every edit" whenever no text happened to rewrap, which put a success
  line over a chart hanging off the content box; `result.problems` lists the same facts.
  Harness: [`scripts/test_replay_chart_edits.js`](../scripts/test_replay_chart_edits.js) (36
  assertions, mostly asserting the ordering rather than the arithmetic). Then re-run Step 8c on the new chart; the earlier pass certified an object that no longer exists.

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
- **Wording the guidelines already cover** — "World" → "Global average", units abbreviated to their symbol inside the plot and spelled out in prose (GUIDELINES.md → General), a title that describes rather than tells (GUIDELINES.md → Titles).
- **Anything the checklist flags** that you can't fix yourself.

Split what you find in two, and be explicit about which is which:

- **Yours to do** — labeling, emphasis, spacing, annotation, anything living in the Figma page. Do it, and show it.
- **The chart author's** — sort order, entity selection, colors, tolerance, the year. Give them a short numbered list with the trade-off spelled out (what it costs, what it buys) and let them decide. Never apply these by editing vectors: the image would stop matching the interactive chart.

If you genuinely have nothing to suggest, say that instead of inventing something. A thin recommendation wastes more of the author's attention than none.
