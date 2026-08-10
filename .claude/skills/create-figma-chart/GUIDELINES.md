# OWID static-chart design guidelines (distilled)

Companion reference for the `create-figma-chart` skill. Distilled from the design team's **DI Charts Guidelines and Cheat Sheets** Figma file (`8gxqkVmZ9x3MK3ky5oigrJ`; pages: line `0:1`, stacked area `130:35045`, bar/stacked bar `130:35046`, slope `130:35047`, scatter `130:35048`, map `130:35049`) and the **Good Data Viz Checklist** (Charts 2026 file, node `20729:1027`). The Figma files are the source of truth — re-read the relevant page when in doubt or when this file looks stale.

## Hands off — never change in the grapher export

- Text colors of the title, subtitle, axis labels, source, note, CC BY.
- Grid line colors and line weights.
- Font sizes — unless a particular space problem forces it.
- Margins and spacing between elements in the header and footer.
- Colors that come from the OWID palette. Never introduce colors outside the palette unless strictly necessary.

## Text

### Titles

- Make titles **more colloquial**: "Death rate in the United States", not "Death rate, US".
- Title states the "so what" (or clearly describes what the chart shows) in **5–12 words**; rewrite it several times; make sure the chart actually shows what the title says.
- **Two or three lines is normal.** What matters is how the lines break: fix a break that cuts a phrase abruptly, and rebalance when one line is left mostly empty — reword or move the break rather than accepting grapher's wrapping. Judge it on the rendered frame, not in the text field.
- **Drop grapher's trailing year.** Grapher appends ", 2023" to single-year titles; that appendix does not belong in the template — move it to the subtitle as `Data for <YYYY>.` A year that reads naturally *inside* the sentence can stay ("Four developed countries met the UN's target for foreign aid in 2025").
- **Highlight the entity or category the chart is about by coloring that word in the title**, using the exact color of the marks it refers to — e.g. "**Four** developed countries met the UN's target…" in the same coral as the four highlighted bars. One highlight per title; it must match a color actually used in the chart.

### Subtitles and notes

- Subtitle short, clear, and necessary — cut whatever the visualization already makes obvious.
- The subtitle is where a single-year chart says `Data for <YYYY>.`
- **DI images normally carry no note.** The DI template has no note line, and most notes can go: they explain caveats the DI text already covers. When a note is genuinely load-bearing for reading the chart, fold it into the **subtitle as a bolded second line** — and only if the subtitle isn't already crowded. Anything longer than that belongs in the DI text, not the image.

### General

- Consider replacing "World" with "Global average".
- No abbreviations the audience may not know; write the full word. Plain language throughout.
- Keep text horizontal — title, subtitle, annotations, data labels. If labels end up vertical, change the chart's orientation instead (e.g. horizontal bars).
- Text hierarchy by font size: title > subtitle > source ≈ annotations ≈ labels. All fonts readable, especially the smallest.

## Annotations

- Use them to answer the questions a reader would ask, replicating what the accompanying DI/article text highlights.
- Font size **12–16px** (**10–14px on maps**). Never below 12px for data labels.
- Text color: the color of the object being annotated, dark gray `#5B5B5B`, or a mix of both. Bold the part that matters.
- **No background rectangles** — they hide chart detail. Instead give the text a **white outside stroke, 2–3px** (match the chart background if it isn't white); placed outside so it doesn't deform letter shapes.
- If there's no room next to the target, annotate further away and point with an arrow.
- Annotate important values directly: write out the values of the **first and last data points** and any point the text mentions.

## Arrows

- Copy the signature curvy arrows from the Charts file (node `798:773`); scale, rotate, or tweak as needed — within the rules below.
- **1px stroke.** Arrowhead and line: one color, never two; the same style and size across the whole chart.
- **Never scale a whole arrow** — the head distorts. Select just the line (cmd/ctrl-click inside the group), Shift-resize it, then move the head back into place.
- Don't squish an arrow's width or height independently; always hold Shift.
- If a curvy arrow gets messy in tight space, a straight thin line is better.
- **Maps: no curvy arrows at all** — limited space. Straight 1px lines, or call the value out directly inside the country shape.

## Dots

- **10×10 px dots** to highlight specific years on a line/slope.
- No outline on the dot — except on **stacked areas**, where a **white outline** makes it stand out against the colored fill.
- After any resizing of the chart, verify dots are still round (see "never stretch one axis").
- Grapher renders no dots at all on charts with more than ~500 points — they were never in the export.

## Direct labelling (the default improvement to propose)

- **Kill the legend when labels can live in the chart** — legends force the reader's eyes to ping-pong. Line charts: entity label at the end of its line, colored like the line, without the elbow/leader connectors grapher draws; reclaim the freed margin for the chart area.
- Bars/areas: label inside the chart element when contrast allows — white text on dark fills, **≥12px**.
- If a legend must stay: **squares**, not circles or rectangles; consider moving it into empty chart space (stacked bars often have some) rather than under the subtitle.

## Colors

- Only the OWID palette — the **Chart colors** library in the Charts file (select an object → Fill → the four-circles library icon → Chart colors).
- For **lines, slopes, and scatter dots**, use the line-variant palette and try colors in the cheat-sheet order (better mutual distinction): Denim `#4c6a9c`, Rusty Orange `#b13507`, then the darkened variants Camel* `#996d39`, Light Teal* `#2c8465`, … The starred colors are darkened versions of the standard palette for thin marks on white; in code they are `OwidDistinctLinesPalette` / `DarkerOwidDistinctColors` in owid-grapher's `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`.
- Use color to *mean* something: highlight the entity the story is about, mute the rest with gray. Meaningful associations are fine (forests = green).
- **Color-vision check on every chart**: no red/green (or other confusable) pairs carrying meaning; sufficient contrast of all text and marks against the background; still legible in black and white (readers print).
- Map colors should be set in grapher itself (Viridis / ColorBrewer sequential palettes), not repainted in Figma — Brewer palettes distinguish better than the OWID categorical palette when many classes are shown.

## Flags, animals, no-data pattern

- **Flags** next to country labels or bars help when space allows (small multiples, ranked bars). Copy from the cheat sheet (Charts file node `2654:5`); flag height matches bar height; if a flag edge blends into the background (white stripes), add a **1px `#DBE5F0` outside stroke**. The Flags *plugin* the team uses is manual — and it has a known bug with the US flag's stars; the file provides correct US flags to copy.
- **Animals** (node `5336:5`): chicken, rooster, turkey, fish, cow, egg-laying hen, pig — for livestock/food topics.
- **"No data" hashed pattern**: Figma drops grapher's no-data pattern on import (known bug). The fix is the **Hero Patterns plugin** (instructions at node `4162:5`): select the no-data shapes themselves (not their group), pattern color `#C9C9C9`, the diagonal-stripe pattern, tile 50%. Plugins can't be run by Claude — pre-color the shapes `#C9C9C9` and hand the user the plugin step.

## Per chart type

### Line charts
- Entity labels inside the chart area (end of line, line color, no elbows); use the reclaimed space for the chart.
- Dots + written-out values for first/last/highlighted years.

### Stacked area charts
- Labels inside the areas or in a legend row above the chart; white text over dark fills, ≥12px, strong contrast.
- White-outlined dots for highlighted points; a dot in the chart needs an outline to stand out against the fill.

### Bar / stacked bar charts
- **Spell out the unit in the chart area**, not only in the subtitle.
- Values to the right of the bars by default; inside the bars when space is tight (contrast + ≥12px rules).
- Highlight the entity the DI is about with a different bar color; mute the rest.
- Group bars meaningfully (delineate blocks) when it helps reading.
- Don't cram many entities into a limited height — **stretch the frame vertically instead** (static charts don't have to be square; the 540×824 mobile template exists for this).
- Legend: squares only; move it into empty chart space if there is some.

### Slope charts
- Narrower is better — the slope reads more strongly in a narrow frame.
- Stretching distorts the endpoint circles; if you stretch, fix the circles manually afterwards.
- If the x-axis isn't years, add transparency to the connecting lines so the slope itself doesn't overclaim.
- Consider a dotted line to mark the 0 baseline.
- Small multiples sharing x/y units: put the unit labels only on the outermost charts.

### Scatter plots
- Ask whether the continent color-coding adds anything; if not, one color for all dots, then highlight the countries the story needs — different color and/or a circle drawn around them.
- Grapher's auto-chosen labels are cluttered and hard to attach to dots: prune to the entities that matter, give kept labels the white outside stroke.
- If you stretch the chart, dots deform — select all circles in the layers panel and set equal width/height in one edit.
- For binary/divided axes, annotate the two sides ("countries above this line …") so the divider explains itself.

### Maps
- Annotations 10–14px; straight 1px leader lines or values inside countries — never curvy arrows; give annotated countries a distinct outline stroke so their silhouette stands out; thin lines pointing at small countries work best when the labels sit apart from each other.
- Legends: align left; vertical columns matched to label lengths; one–two categories → shrink the legend; horizontal stretched legends only for sequential palettes, not categorical.
- Tidy grapher's default legend: labels 12–14px, swatch square sized to the font, label color dark gray `#2D2E2D` instead of pure black, group items of similar length into columns.
- No-data pattern: see above (manual plugin step).

## Final pass — the Good Data Viz Checklist (condensed)

Purpose and form:
- [ ] Clear purpose and audience — you can say what the reader should learn.
- [ ] Best chart type for that purpose (would another form say it better?).
- [ ] Axes start where they should — bar and area charts **always at 0**; any cut y-axis has a stated reason.
- [ ] No 3D, no decoration that doesn't aid interpretation, no double y-axes (consider a connected scatter instead).
- [ ] Gridlines helpful and muted (faint gray, not black; solid only for zero/reference lines, dashed otherwise; none needed if every point is labeled); sensible intervals; no stray borders or tick marks.
- [ ] Not overcrowded — focus, or split into small multiples.

Understandability:
- [ ] Title expresses the point (or describes the chart) in 5–12 words; the chart shows what the title says.
- [ ] Subtitle short and necessary.
- [ ] Most text horizontal.
- [ ] Annotations answer the reader's questions; every information-bearing part is labeled.
- [ ] Important values annotated directly, close to their marks.
- [ ] Legends replaced by direct labels where possible; no label/annotation clutter.
- [ ] Source clearly stated; a pointer to the wider work (the OWID topic link in the template does this).
- [ ] Font-size hierarchy correct; smallest text readable; no unexplained abbreviations; audience-appropriate language.

Color:
- [ ] Every color chosen for a reason; highlights pop, context muted.
- [ ] Contrast sufficient; colorblind-safe (ColorOracle-style check); legible in black and white.

Final look:
- [ ] Everything placed exactly where it needs to be.
- [ ] Remove anything not serving the message (entities, title words, footnotes).
- [ ] Seen through a reader's fresh eyes, it's understandable without explanation.
