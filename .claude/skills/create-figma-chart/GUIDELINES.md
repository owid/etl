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
- **DI images normally carry no note.** The DI template has no note line, and most notes can go: they explain caveats the DI text already covers. When a note is genuinely load-bearing for reading the chart, the cleanest form is to **fold it into the subtitle's own sentence** — "…differences in living costs between countries" plus a note about constant prices becomes "…between countries, **and is expressed in international-$ at 2021 prices**", one flowing sentence with no extra weight. Reach for a **bolded second line** only when the note won't join the sentence (a different subject, or a caveat that has to stand apart). Either way, only if the subtitle isn't already crowded; anything longer belongs in the DI text, not the image.

### General

- Consider replacing "World" with "Global average".
- No abbreviations the audience may not know; write the full word. Plain language throughout.
- Keep text horizontal — title, subtitle, annotations, data labels. If labels end up vertical, change the chart's orientation instead (e.g. horizontal bars).
- Text hierarchy by font size: title > subtitle > source ≈ annotations ≈ labels. All fonts readable, especially the smallest.
- **Check the hierarchy on the numbers, and expect the export to arrive with it broken.** Grapher's square export renders **legend labels at 16.8px against a 16px subtitle** — the least important text in the chart ends up the second-largest thing on it. List every distinct size with what it belongs to and confirm the sequence is non-increasing; anything inside the plot that outranks the subtitle is a defect to fix, not a size to preserve. Bringing that legend to 14px reads as a *correction*, not a compromise: it restores the order and sits with the bar values at 13.7px. This is why "don't change font sizes" carries the exception it does — the rule protects the export's *relative* type scale, and where the export's own scale is wrong, matching the template's hierarchy wins.

## Annotations

- Use them to answer the questions a reader would ask, replicating what the accompanying DI/article text highlights.
- Font size **12–16px**, and **12–14px on maps** — the design guidelines allow 10px there, but the file's ladder has no rung below XS 12 and the Step 8c text-size check rejects anything smaller, so maps take the bottom of the ladder rather than their own scale. Never below 12px for data labels. Pick from the named ladder rather than typing a number — **Annotation XL 16 / L 15 / M 14 / S 13 / XS 12** — taking the largest that fits without forcing an extra line, and staying at or below **L 15** so the subtitle's 16 still leads. Annotations of the same rank take the same size; drop to **XS** for a footnote that qualifies a claim rather than making one (a year caveat, a coverage note), and put it *after* the claim it qualifies. **Applying a text style resets the node's range fills and weights**, so set the size first and re-apply the color-and-bold convention after, never the other way round.
- **The house convention: annotation text in the annotation gray, category words bold and in their category's color.** Set the whole line to `Data Insights/Annotations` `#5B5B5B`, then override just the words naming a series — "Chicken", "beef", "fish and seafood" — to that series' color **and** bold. The colored word is what ties the sentence to the mark, so it does the job an arrow would, without the ink. Use the **line variant** for any category whose fill is too light to read as text (Camel `#BC8E5A` becomes Camel\* `#996D39`); bind both the gray and the category colors to their library styles with `setRangeFillStyleId(start, end, style.id)` and set the weight with `setRangeFontName` — note these range setters are **synchronous**, unlike most of the modern API.
- **Every annotation goes in its own auto-layout frame, filled pure white (or the canvas color), hugging the text on both axes.** That is the house mechanic and it is what the finished pages use: the frame keeps gridlines and context lines out of the letterforms, and because it hugs the text exactly and matches the canvas, it never reads as a box. Set the fill on the frame, `layoutSizingHorizontal`/`Vertical = "HUG"` on both the frame and the text, zero padding, and append the frame **last** so it sits above the chart. Hugging is the part that does the work — a frame left at a fixed size is the background rectangle the guidelines warn about, hiding chart detail on whichever side has slack.
  - Parent the text into the frame **before** setting `HUG`; Figma rejects the value until the node is in an auto-layout context.
  - A **white outside stroke, 2–3px** on the text is the fallback for an annotation that can't take a frame (one sitting inside a filled area, where a white box would punch a hole); place it outside so it doesn't deform the letters.
- If there's no room next to the target, annotate further away and point with an arrow.
- **On a full-width chart, make room by opening a gap rather than overlaying.** A 100%-stacked bar has no free margin, so an annotation has nowhere to go — but dropping a few entities and re-exporting at a flatter aspect ratio frees a band, and a gap of about one row-and-a-half opened *directly beneath the bar being annotated* puts the text where no leader line is needed at all. Shift every row below the gap down by the same amount, then re-center. Beware the export arithmetic: a flatter aspect ratio means a bigger downscale to reach the same width, so the base font has to rise with it — at 2:1 the labels came back 8px until `imFontSize` went to 35. And size the export **backwards from the gap rule**: the plot plus the annotation gaps should come to the band minus 28px, so that a 14px gap falls out at each end. Padding a short plot with 33px gaps instead is the visible symptom of having exported the wrong height — retune `imHeight` and re-export rather than living with it.
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

## Direct labeling (the default improvement to propose)

- **Kill the legend when labels can live in the chart** — legends force the reader's eyes to ping-pong. Line charts: entity label at the end of its line, colored like the line, without the elbow/leader connectors grapher draws; reclaim the freed margin for the chart area.
- **"When labels can live in the chart" is a real test, not a formality.** Each label has to sit on the mark it names — over its segment of the top bar, or inside the widest segment of its series. That holds for about three or four categories; past that they collide, and a color-coded row spread evenly across the plot is a legend with the swatches removed, which is worse than the legend. Keep grapher's legend in that case and tidy it instead.
- Bars/areas: label inside the chart element when contrast allows — white text on dark fills, **≥12px**.
- If a legend must stay: **squares**, not circles or rectangles; consider moving it into empty chart space (stacked bars often have some) rather than under the subtitle.
- **Get the legend onto one row — shortening the labels is the lever, not shrinking the type.** Grapher wraps to two rows because its labels are verbose, and the second row costs ~15px of plot. Trim to the shortest wording that is still unambiguous *in this chart's context*: "Other meats" → **"Other"** and "Beef and buffalo" → **"Beef"** on a chart entirely about meat. That freed 65px here, which bought the legend back up to 14px — squeezing the type to 12px to fit the long labels had been the alternative, and it reads worse. Keep the legend in **stack order** so the eye maps it left-to-right onto the bars. Note the knock-on: collapsing a row makes the plot shorter, so re-export ~15px taller or the gap drifts above the band.
- **A residual category goes last.** "Other meats", "Other", "Rest of the world" and the like belong at the end of a stacked order and at the end of the legend — they are the leftover, so putting them mid-stack breaks the reading order and pushes the named categories apart. Fix this **in the chart** (the dimension's column order), not by moving vectors in Figma, so the image and the interactive chart agree.

## Colors

- Only the OWID palette — the **[Chart Colors] Library**, a shared Figma library, not a local style set (select an object → Fill → the four-circles library icon → Chart colors). It is the source of truth; `scripts/color_audit.py` carries a copy read off the cheat-sheet swatches, verified against `OwidDistinctColors` in owid-grapher. Re-read the library rather than trusting either copy if they ever disagree.
- The 24 fills: Denim `#4c6a9c`, Rusty Orange `#b13507`, Camel `#bc8e5a`, Light Teal `#58ac8c`, Purple `#6d3e91`, Maroon `#883039`, Midnight Blue `#00295b`, Mauve `#a2559c`, Dark Copper `#9a5129`, Turquoise `#38aaba`, Cherry `#970046`, Lime `#3b8e1d`, Peach `#e56e5a`, Blue `#286bbb`, Dark Olive Green `#18470f`, Coral `#d73c50`, Copper `#b16214`, Teal `#00847e`, Fuchsia `#cf0a66`, Olive Green `#578145`, Dark Orange `#c05917`, Dark Mauve `#8c4569`, Tealish Green `#00875e`, Dusty Coral `#c15065`.
- The library's **Line and Slope Charts** group is the same palette with six colors darkened for thin marks and text on white: Camel* `#996d39`, Light Teal* `#2c8465`, Turquoise* `#008291`, Lime* `#338711`, Peach* `#c4523e`, Dark Orange* `#be5915`. Use that group for lines, slopes and scatter dots (`color_audit.py --line`).
- The library's third group, **Categorical Maps**, is a separate muted set for choropleths and is *not* interchangeable with the Default Palette: Sand `#c3a27c`, Light Sand `#d8c0a2`, Taupe `#b9b2a6`, Olive `#5b6d35`, Leaf Green `#6fa54f`, Mustard `#d9bc54`, Tomato `#d94c3f`, Lavendar `#8e97c7`, Soft Purple `#77538f`, Muted Teal `#238a84`, Light Teal `#4fb2ac`, Muted Cherry `#b04e74`, Light Cherry `#cb7fa0`, Muted Denim `#526f9b` (`color_audit.py --maps`). Map colors are agreed with the design team through the `add-provider-regions` workflow, not chosen here.
- **`Gray #6e7581` is a Default Palette color; grapher's own gray is not.** A grapher export renders residual categories in `#585c64`, which is nowhere in the library — so a chart can arrive with an off-palette color through no one's decision. Check the fills you inherit against the library, not just the ones you add. (Swapping the library gray in is not automatically an improvement: on one six-category stack it *lowered* the safety floor from 26.2 to 20.5, because the lighter gray sits closer to the teal. Measure, don't assume.)
- **Don't take the palette from the cheat-sheet swatch grid alone** — it renders 24 colors and omits at least `Gray`. Read the library itself (`search_design_system` for the group name, then `figma.importStyleByKeyAsync(key)` for each hex); the cheat sheet is a picture of the palette, not the palette.
- For **lines, slopes, and scatter dots**, use the line-variant palette and try colors in the cheat-sheet order (better mutual distinction): Denim `#4c6a9c`, Rusty Orange `#b13507`, then the darkened variants Camel* `#996d39`, Light Teal* `#2c8465`, … The starred colors are darkened versions of the standard palette for thin marks on white; in code they are `OwidDistinctLinesPalette` / `DarkerOwidDistinctColors` in owid-grapher's `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`.
- Use color to *mean* something: highlight the entity the story is about, mute the rest with gray. Meaningful associations are fine (forests = green).
- **When a palette has to change, propose the closest one that passes, not the safest one.** A designer reads a small shift as a fix and a wholesale repaint as a different chart, so total CIELAB drift from the colors already in use is what you argue from — with the residual category pinned to `Gray`. Concretely, that means `color_audit.py --suggest`'s ordering, which is **hue variety, then safety, then drift**: ΔE 20 and — on stacked fills — the 1.6:1 seam are pass/fail gates, hue variety comes first because ranking on safety alone returns palettes that are entirely blues and greens, and drift separates whatever ties. Don't re-rank the output by drift alone — the tool's top group is already the passing, varied one, and drift picks within it. Report how many colors actually have to move, and be ready for the answer to be "all of them": on this chart no passing palette kept a single original color, because one structural collision at the end of the stack (`Denim` beside `Gray`, 1.18:1) forces Fish to move, which forces Sheep off Purple, which forces Pork off Coral. When that happens, say so with the chain — otherwise a full repaint looks like overreach rather than the only option. Note that a color can often survive by **relocating** rather than being dropped: Denim was fine once it moved to a category not adjacent to the gray.
- **A light tan and a salmon are the same color to a deuteranope** — Camel `#BC8E5A` against Peach `#E56E5A` measures **ΔE 2.1**, one of the worst pairs the OWID palette can produce, because both desaturate to the same yellowish tone. Any two mid-to-light warm fills are suspect; darkening one of them (Copper `#B16214`, L 50 against Camel's L 62) is what separates them. Check warm pairs first — they look obviously different and measure catastrophically.
- **When a design choice knowingly fails a bar, record the deviation where the next person will read it.** Not everything gets fixed: a warmer color with better label contrast can be worth a color-vision cost, and that is the author's call to make. What is not acceptable is leaving it silent, because the next run measures it, reads it as a bug and "fixes" it. Write the numbers, the reason and the alternatives that were measured — as a page note beside the frame (outside it, so it never lands in the exported PNG) and in the report. State the accepted figure explicitly; "we decided the colors were fine" is not a record.
- **Put the quietest color on the largest area.** A safe palette can still be unpleasant: on a stacked bar the widest category covers the most ink, so a light saturated fill there (Light Teal on segments running 43–52%) dominates the chart even though it measures fine. Rank candidates by the **chroma** of the color landing on the biggest category and prefer the lowest — a dark, low-chroma fill recedes and reads as depth. Midnight Blue (chroma 35) on the widest segments beat Light Teal (chroma 46) on exactly this chart, at the same ΔE. Passing the audit is the floor; which color sits where is still a design decision.
- **Label-on-fill contrast is its own check, and grapher's choice is not always right.** Bar-versus-bar separation says nothing about whether the value sitting *on* a bar can be read. Measure each in-bar label against its own fill and require **4.5:1** — these labels are ~13.5px regular, so the 3:1 large-text allowance doesn't apply — and compute **both** black and white rather than keeping what the export chose: on one chart grapher put white on Coral at 4.51:1 when black scored 4.65:1. Expect the weakest pair to be whichever category is mid-lightness (Copper measured 4.63:1, the floor of a passing palette); before changing the color, check whether any alternative improves the label contrast *without* breaking the palette's own bars — often none does, and "this is the best available" is the honest answer.
- **Color-vision check on every chart, measured not eyeballed** — run `scripts/color_audit.py` (see SKILL.md → Step 8). Pairs closer than ΔE 20 under deuteranopia or protanopia fail; 20–30 is tight. Two things this catches that inspection does not: a *stacked* chart needs its adjacent segments separable **and** every pair separable, because the reader also matches legend to segment; and more than about four categorical colors is where safe palettes start to run out — a six-color stack measured ΔE 9.2, and no single substitution lifted it. Also check contrast of text and marks against the background, and that the chart still reads in black and white (readers print).
- Map colors should be set in grapher itself (Viridis / ColorBrewer sequential palettes), not repainted in Figma — Brewer palettes distinguish better than the OWID categorical palette when many classes are shown.

## Highlighting one series against the rest

When a chart carries many series but the story is about one of them, the static image says so with **one palette color on the protagonist and a flat neutral gray on everything else**. This is the single highest-value edit available on a multi-series line chart, and it is worth checking for on every chart that arrives with more than about four series.

**Grapher will not have done it for you, even when the chart's author asked for it.** A chart with `focusedSeriesNames` set renders focus in the SVG as **`stroke-opacity="0.5"` on the non-focused series** — they keep their own hues at half strength. Half-strength distinct hues still read as "these twelve categories all matter": the reader's eye is pulled twelve ways, and the direct labels inherit twelve colors that mean nothing. What *does* survive the export is the emphasis on the focused series — it comes back at 3px with a white 6px halo (`outline__<Entity>`) and a marker at every year — so grapher hands you a half-finished version of the treatment and you complete it.

The treatment, in the order that avoids rework:

1. **Non-focused lines → one flat gray, `strokeWeight = 1`, opacity reset to 1.** Set the weight explicitly; **don't assume the export already gave you a thin line** — grapher renders context lines at 2px, which is heavy enough that twelve of them still crowd the protagonist however well they are colored. Reset the opacity too, because leaving the inherited 0.5 on top of gray lands you at a near-white the reader loses against the gridlines. The weight contrast is half the treatment: 1px gray against the protagonist's 3px color is what makes the hierarchy read before any color does, and it is the thing to check first when a muted chart still feels busy.
2. **Non-focused labels → a slightly darker gray than the lines**, also at opacity 1, so the words stay findable while the marks recede. One gray for both flattens the two into a single mass.
3. **The protagonist → a palette color, bound as a library style** (`setStrokeStyleIdAsync` for the line, `setFillStyleIdAsync` for the label and any markers).
4. **Widen the white halo to double the line's weight.** A focused line arrives with a sibling `outline__<Entity>` — the same path in white beneath it — but the export ships it barely wider than the line (a 3px line gets a 4px outline, half a pixel of white per side, which does nothing where the line crosses another). Set `outline = 2 × line` (3px → 6px, ~1.5px of white each side). That halo is what lets the line read as continuous through the context lines, and it is the cheapest way to separate any two lines that overlap.
   - **Whenever you change a line's stroke weight, change its outline in the same edit** — they are separate nodes, so thickening the line alone eats the halo, and pushing the halo much past 2× turns it into a white band through the chart.
   - **To give a line a halo it doesn't have** (a second highlight, or two context lines that need untangling at a crossing), clone the line, set its stroke white at double the weight, and drop the clone directly beneath the original in z-order.
5. **The protagonist's label → bold, in the same color.** The color ties label to line; the weight is what raises it above a column of same-size gray labels without spending a size step.
6. **Markers: keep the ones that carry a date, hide the rest** (see Dots). A focused line arrives with a marker per year, which reads as noise.

**The highlight color is a decision, not an inheritance.** Grapher assigns it by series index, so the protagonist arrives in whatever slot it happened to land in — Purple, say — and that is not a choice anyone made. **Rusty Orange `#b13507` is OWID's default single-series highlight**; deviate from it only for a reason (an established topic color, a second highlight already using it).

**The values are settled — use them: lines `#a6a6a6` at 1px, labels `#8c8c8c`.** Two things follow that a later run will otherwise try to "fix":

- **They are furniture, not categorical colors, so the palette rule does not apply to them.** The Default Palette's `Gray #6e7581` is a color meaning "this category is the leftover", which is not what a muted context series is, and it is too dark and too blue for the job. Neither of the two values is a named color in the file yet — worth naming there, but not worth substituting a named color that means something else. Expect the Step 8c off-palette check to list them; that listing is the record, not a finding.
- **`#8c8c8c` measures 3.36:1 against white, under the 4.5:1 that 15px regular text needs, and that is an accepted deviation.** It is deliberate: context labels are meant to recede, and darkening to `#767676` (4.54:1) buys the contrast by spending the thing the treatment exists for — separation from the protagonist drops from 1.85:1 to 1.37:1, and twelve darker labels start competing with the one that matters. **Keep them light.** Report the figure so the decision stays visible, and do not raise it as a defect to be fixed.

The general rule underneath: **on a muted series, light and thin beats compliant.** These marks and words are context, and a context layer that passes every threshold has stopped being context.

**Don't run the color-vision audit on a chart like this.** One categorical color against neutral grays has no pair to check, and reporting "no failures" from a two-color audit reads as coverage you don't have. Say that the palette reduces to one highlight plus grays, and check the two things that *are* live: the highlight's contrast against the background, and whether the highlight still separates from the grays in black and white.

## Named styles in the Charts file

Use these rather than typing sizes and grays by hand — they are what the templates are built from, and a new text node cloned from a template node inherits them for free.

**Text styles** — Data Insights: Title (Playfair Display SemiBold 25/29), Subtitle (Lato Regular 16), Source (Lato Regular 14), and an annotation ladder **Annotation XL 16 / L 15 / M 14 / S 13 / XS 12** (all Lato Regular). Instagram adds: Title 25/29, Subtitle 16, Source 14/16, Axis labels 18, Text 30 Bold (Lato SemiBold 30), plus portrait variants Title 28/32 and Subtitle 18.

**Text and furniture colors** — Data Insights: Title `#2D2E2D`, Subtitle `#5B5B5B`, Source `#858585`, Annotations `#5B5B5B`, Axis Lines `#999999`, Axis Grid `#DDDDDD`, Background White `#FFFFFF`. Instagram: same, plus Beige Background `#FBF9F3` and Axis labels `#58595B`.

The annotation ladder is the concrete form of the "12–16px" rule: pick the named size rather than an arbitrary number, and never go below Annotation XS.

**Every size in the finished frame should be a named style's size, including the text that came in from the export.** A grapher SVG scaled to fit arrives at arbitrary sizes — 13.7px value labels, a 16.8px legend — which no style in the file matches, so the frame drifts out of the type system while looking fine.

**But the ladder is a range to choose from, not a fixed role→size table.** Sizes legitimately vary within one chart, and grapher's own export varies too. What fixes a size is **rank**, not what kind of element it is:

- the text carrying the chart's point can go as large as **Annotation XL (16)** — level with the subtitle, which is the ceiling and should be spent only when the annotation *is* the message;
- supporting claims sit a step or two down, **L (15)** or **M (14)**;
- values and labels read as data, typically **M (14)** or **S (13)**;
- footnotes, year caveats and coverage notes belong at **XS (12)**, and nothing goes below it.

Items of the same rank share a size; items of different rank must differ, or the reader can't tell which text is the claim and which is the caveat. On this chart that gave legend **L (15)**, values **M (14)**, caveat **XS (12)** — a defensible set, not the only one.

Two mechanics to get right. Applying a text style **overwrites the font and clears the fill**, so read the fill first and re-apply it after — the black-on-light versus white-on-dark choice is per-segment and must survive. And **bold text has no matching style** (the ladder is all Lato Regular), so for the bold country names set `fontSize` to the ladder value and leave the weight alone rather than binding the style and losing the bold. Changing sizes changes widths: re-center the labels in their segments afterwards and check none now overflows its bar.

## Flags, animals, no-data pattern

- **Flags** next to country labels or bars help when space allows (small multiples, ranked bars). Copy from the cheat sheet (Charts file node `2654:5`); flag height matches bar height; if a flag edge blends into the background (white stripes), add a **1px `#DBE5F0` outside stroke**. The Flags *plugin* the team uses is manual — and it has a known bug with the US flag's stars; the file provides correct US flags to copy.
- **Animals** (node `5336:5`): chicken, rooster, turkey, fish, cow, egg-laying hen, pig — for livestock/food topics.
- **"No data" hashed pattern**: Figma drops grapher's no-data pattern on import (known bug). The fix is the **Hero Patterns plugin** (instructions at node `4162:5`): select the no-data shapes themselves (not their group), pattern color `#C9C9C9`, the diagonal-stripe pattern, tile 50%. Plugins can't be run by Claude — pre-color the shapes `#C9C9C9` and hand the user the plugin step.

## Per chart type

### Line charts
- Entity labels inside the chart area (end of line, line color, no elbows); use the reclaimed space for the chart.
- Dots + written-out values for first/last/highlighted years.
- **More than about four lines: mute all but the protagonist** — see "Highlighting one series against the rest". On a many-line chart this matters more than any other edit, and it is also what makes the direct labels legible, since a column of same-size labels in twelve different hues reads as noise.
- **Shorten a label only when it is one of the longest**, because the longest label is what caps the plot's width — "United States" → **US** buys nothing if "Switzerland" is still there. Shortening the top one or two is a space edit; shortening the rest is just inconsistency.
- **A sparse time axis is worth a second look after you widen the plot** — grapher thins tick labels for the width it rendered at, so a reclaimed margin can leave room for the missing years. Add them only when the gaps measure out (the first and last labels sit *at* their ticks rather than centered on them, so the two slots beside them need half a label more room than the rest); otherwise keep grapher's axis untouched.
- **When a line's shape has a cause, mark the moment rather than describing it.** A thin dashed vertical guide at the year, a single marker where it meets the line, and one annotation line ending just short of the guide says "this is when it changed" with almost no ink — and it lets you delete the marker on every other year, which a focused line arrives carrying.

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
- Annotations 12–14px (the bottom of the ladder — see Annotations for why maps don't go to 10); straight 1px leader lines or values inside countries — never curvy arrows; give annotated countries a distinct outline stroke so their silhouette stands out; thin lines pointing at small countries work best when the labels sit apart from each other.
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
