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
- **For a DI, the title tells the story** — grapher's default title is a dry description of the variable, and it should not survive into the image. Use the DI's own title, or an alternative conveying the same message ("Four developed countries met the UN's target for foreign aid", not "Official development assistance as a share of gross national income"). Ask for the DI title if you weren't given it; the descriptive version belongs in the subtitle, where it says what's actually plotted.
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
- Font size **12–16px** (**10–14px on maps**). Never below 12px for data labels. Pick from the named ladder rather than typing a number — **Annotation XL 16 / L 15 / M 14 / S 13 / XS 12** — taking the largest that fits without forcing an extra line, and staying at or below **L 15** so the subtitle's 16 still leads. Annotations of the same rank take the same size; drop to **XS** for a footnote that qualifies a claim rather than making one (a year caveat, a coverage note), and put it *after* the claim it qualifies. **Applying a text style resets the node's range fills and weights**, so set the size first and re-apply the color-and-bold convention after, never the other way round.
- **The house convention: annotation text in the annotation grey, category words bold and in their category's color.** Set the whole line to `Data Insights/Annotations` `#5B5B5B`, then override just the words naming a series — "Chicken", "beef", "fish and seafood" — to that series' color **and** bold. The colored word is what ties the sentence to the mark, so it does the job an arrow would, without the ink. Use the **line variant** for any category whose fill is too light to read as text (Camel `#BC8E5A` becomes Camel\* `#996D39`); bind both the grey and the category colors to their library styles with `setRangeFillStyleId(start, end, style.id)` and set the weight with `setRangeFontName` — note these range setters are **synchronous**, unlike most of the modern API.
- **No background rectangles** — they hide chart detail. Instead give the text a **white outside stroke, 2–3px** (match the chart background if it isn't white); placed outside so it doesn't deform letter shapes.
- If there's no room next to the target, annotate further away and point with an arrow.
- **On a full-width chart, make room by opening a gap rather than overlaying.** A 100%-stacked bar has no free margin, so an annotation has nowhere to go — but dropping a few entities and re-exporting at a flatter aspect ratio frees a band, and a gap of about one row-and-a-half opened *directly beneath the bar being annotated* puts the text where no leader line is needed at all. Shift every row below the gap down by the same amount, then re-centre. Beware the export arithmetic: a flatter aspect ratio means a bigger downscale to reach the same width, so the base font has to rise with it — at 2:1 the labels came back 8px until `imFontSize` went to 35. And size the export **backwards from the gap rule**: the plot plus the annotation gaps should come to the band minus 28px, so that a 14px gap falls out at each end. Padding a short plot with 33px gaps instead is the visible symptom of having exported the wrong height — retune `imHeight` and re-export rather than living with it.
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
- **"When labels can live in the chart" is a real test, not a formality.** Each label has to sit on the mark it names — over its segment of the top bar, or inside the widest segment of its series. That holds for about three or four categories; past that they collide, and a color-coded row spread evenly across the plot is a legend with the swatches removed, which is worse than the legend. Keep grapher's legend in that case and tidy it instead.
- Bars/areas: label inside the chart element when contrast allows — white text on dark fills, **≥12px**.
- If a legend must stay: **squares**, not circles or rectangles; consider moving it into empty chart space (stacked bars often have some) rather than under the subtitle.
- **A residual category goes last.** "Other meats", "Other", "Rest of the world" and the like belong at the end of a stacked order and at the end of the legend — they are the leftover, so putting them mid-stack breaks the reading order and pushes the named categories apart. Fix this **in the chart** (the dimension's column order), not by moving vectors in Figma, so the image and the interactive chart agree.

## Colors

- Only the OWID palette — the **[Chart Colors] Library**, a shared Figma library, not a local style set (select an object → Fill → the four-circles library icon → Chart colors). It is the source of truth; `scripts/color_audit.py` carries a copy read off the cheat-sheet swatches, verified against `OwidDistinctColors` in owid-grapher. Re-read the library rather than trusting either copy if they ever disagree.
- The 24 fills: Denim `#4c6a9c`, Rusty Orange `#b13507`, Camel `#bc8e5a`, Light Teal `#58ac8c`, Purple `#6d3e91`, Maroon `#883039`, Midnight Blue `#00295b`, Mauve `#a2559c`, Dark Copper `#9a5129`, Turquoise `#38aaba`, Cherry `#970046`, Lime `#3b8e1d`, Peach `#e56e5a`, Blue `#286bbb`, Dark Olive Green `#18470f`, Coral `#d73c50`, Copper `#b16214`, Teal `#00847e`, Fuchsia `#cf0a66`, Olive Green `#578145`, Dark Orange `#c05917`, Dark Mauve `#8c4569`, Tealish Green `#00875e`, Dusty Coral `#c15065`.
- The library's **Line and Slope Charts** group is the same palette with six colors darkened for thin marks and text on white: Camel* `#996d39`, Light Teal* `#2c8465`, Turquoise* `#008291`, Lime* `#338711`, Peach* `#c4523e`, Dark Orange* `#be5915`. Use that group for lines, slopes and scatter dots (`color_audit.py --line`).
- The library's third group, **Categorical Maps**, is a separate muted set for choropleths and is *not* interchangeable with the Default Palette: Sand `#c3a27c`, Light Sand `#d8c0a2`, Taupe `#b9b2a6`, Olive `#5b6d35`, Leaf Green `#6fa54f`, Mustard `#d9bc54`, Tomato `#d94c3f`, Lavendar `#8e97c7`, Soft Purple `#77538f`, Muted Teal `#238a84`, Light Teal `#4fb2ac`, Muted Cherry `#b04e74`, Light Cherry `#cb7fa0`, Muted Denim `#526f9b` (`color_audit.py --maps`). Map colors are agreed with the design team through the `add-provider-regions` workflow, not chosen here.
- **`Gray #6e7581` is a Default Palette color; grapher's own grey is not.** A grapher export renders residual categories in `#585c64`, which is nowhere in the library — so a chart can arrive with an off-palette color through no one's decision. Check the fills you inherit against the library, not just the ones you add. (Swapping the library grey in is not automatically an improvement: on one six-category stack it *lowered* the safety floor from 26.2 to 20.5, because the lighter grey sits closer to the teal. Measure, don't assume.)
- **Don't take the palette from the cheat-sheet swatch grid alone** — it renders 24 colors and omits at least `Gray`. Read the library itself (`search_design_system` for the group name, then `figma.importStyleByKeyAsync(key)` for each hex); the cheat sheet is a picture of the palette, not the palette.
- For **lines, slopes, and scatter dots**, use the line-variant palette and try colors in the cheat-sheet order (better mutual distinction): Denim `#4c6a9c`, Rusty Orange `#b13507`, then the darkened variants Camel* `#996d39`, Light Teal* `#2c8465`, … The starred colors are darkened versions of the standard palette for thin marks on white; in code they are `OwidDistinctLinesPalette` / `DarkerOwidDistinctColors` in owid-grapher's `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`.
- Use color to *mean* something: highlight the entity the story is about, mute the rest with gray. Meaningful associations are fine (forests = green).
- **When a palette has to change, search for the closest one that passes, not the safest one.** Rank candidates by total CIELAB drift from the colors already in use, with the residual category pinned to `Gray` — a designer reads a small shift as a fix and a wholesale repaint as a different chart. Report how many colors actually have to move, and be ready for the answer to be "all of them": on this chart no passing palette kept a single original color, because one structural collision at the end of the stack (`Denim` beside `Gray`, 1.18:1) forces Fish to move, which forces Sheep off Purple, which forces Pork off Coral. When that happens, say so with the chain — otherwise a full repaint looks like overreach rather than the only option. Note that a color can often survive by **relocating** rather than being dropped: Denim was fine once it moved to a category not adjacent to the grey.
- **Put the quietest color on the largest area.** A safe palette can still be unpleasant: on a stacked bar the widest category covers the most ink, so a light saturated fill there (Light Teal on segments running 43–52%) dominates the chart even though it measures fine. Rank candidates by the **chroma** of the color landing on the biggest category and prefer the lowest — a dark, low-chroma fill recedes and reads as depth. Midnight Blue (chroma 35) on the widest segments beat Light Teal (chroma 46) on exactly this chart, at the same ΔE. Passing the audit is the floor; which color sits where is still a design decision.
- **Label-on-fill contrast is its own check, and grapher's choice is not always right.** Bar-versus-bar separation says nothing about whether the value sitting *on* a bar can be read. Measure each in-bar label against its own fill and require **4.5:1** — these labels are ~13.5px regular, so the 3:1 large-text allowance doesn't apply — and compute **both** black and white rather than keeping what the export chose: on one chart grapher put white on Coral at 4.51:1 when black scored 4.65:1. Expect the weakest pair to be whichever category is mid-lightness (Copper measured 4.63:1, the floor of a passing palette); before changing the color, check whether any alternative improves the label contrast *without* breaking the palette's own bars — often none does, and "this is the best available" is the honest answer.
- **Color-vision check on every chart, measured not eyeballed** — run `scripts/color_audit.py` (see SKILL.md → Step 8). Pairs closer than ΔE 20 under deuteranopia or protanopia fail; 20–30 is tight. Two things this catches that inspection does not: a *stacked* chart needs its adjacent segments separable **and** every pair separable, because the reader also matches legend to segment; and more than about four categorical colors is where safe palettes start to run out — a six-color stack measured ΔE 9.2, and no single substitution lifted it. Also check contrast of text and marks against the background, and that the chart still reads in black and white (readers print).
- Map colors should be set in grapher itself (Viridis / ColorBrewer sequential palettes), not repainted in Figma — Brewer palettes distinguish better than the OWID categorical palette when many classes are shown.

## Named styles in the Charts file

Use these rather than typing sizes and greys by hand — they are what the templates are built from, and a new text node cloned from a template node inherits them for free.

**Text styles** — Data Insights: Title (Playfair Display SemiBold 25/29), Subtitle (Lato Regular 16), Source (Lato Regular 14), and an annotation ladder **Annotation XL 16 / L 15 / M 14 / S 13 / XS 12** (all Lato Regular). Instagram adds: Title 25/29, Subtitle 16, Source 14/16, Axis labels 18, Text 30 Bold (Lato SemiBold 30), plus portrait variants Title 28/32 and Subtitle 18.

**Text and furniture colors** — Data Insights: Title `#2D2E2D`, Subtitle `#5B5B5B`, Source `#858585`, Annotations `#5B5B5B`, Axis Lines `#999999`, Axis Grid `#DDDDDD`, Background White `#FFFFFF`. Instagram: same, plus Beige Background `#FBF9F3` and Axis labels `#58595B`.

The annotation ladder is the concrete form of the "12–16px" rule: pick the named size rather than an arbitrary number, and never go below Annotation XS.

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
