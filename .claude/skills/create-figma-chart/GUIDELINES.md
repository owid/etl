# OWID static-chart design guidelines (distilled)

Companion reference for the `create-figma-chart` skill. Distilled from the design team's **DI Charts Guidelines and Cheat Sheets** Figma file (`8gxqkVmZ9x3MK3ky5oigrJ`; pages: line `0:1`, stacked area `130:35045`, bar/stacked bar `130:35046`, slope `130:35047`, scatter `130:35048`, map `130:35049`) and the **Good Data Viz Checklist** (Charts 2026 file, node `20729:1027`). The Figma files are the source of truth — re-read the relevant page when in doubt or when this file looks stale.

A second, different source is the design team's **DI Chart Library** (`pltrHXyVLg2XaNq4AvxPaK`) — an archive of **272** *finished, shipped* charts filed by type. The Cheat Sheets file says what the team intends; the Chart Library shows what they actually ship, and where the two diverge the divergence is noted in place below. **It is read-only: never write to that file key.** Pages, with chart counts as of 2026-08-19:

| Page | Node | Charts |
|---|---|---|
| Line Charts | `1:2` | 123 |
| Bar Charts | `1:3` | 52 |
| Stacked Bar | `1:6` | 24 |
| Maps | `1:1190` | 21 |
| Stacked Areas | `1:4` | 18 |
| Slope Charts | `1:1657` | 17 |
| Scatter Plots | `0:1` | 7 |
| Combination Charts | `201:336` | 4 |
| Marimekkos | `1:5` | 3 |
| Misc | `222:718` | 3 |

Both source files were cross-read on 2026-08-20 — all six Cheat Sheets pages against the archive and against this file — and the divergences that came out of it are marked **→ source says** below. A cheaper way in than opening either file: inside the Cheat Sheets file the archive is published as an **Assets library** (`Assets` → `Chart Library` → chart type), and a chart can be dragged straight into your working file for a closer look. Note the Cheat Sheets pages carry “Delete this section once you finish editing the chart” — they are meant to be removed from a working file, not shipped in it.

Each chart is a component; `get_screenshot` on its node id renders it. Before designing a chart of a given type, open three or four of that page's charts — the exemplars named at the end of each subsection below are a starting set.

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
  - **Move a break with an explicit `\n`, and expect to need one.** Auto-wrap at the template's title width broke *"Global population growth peaked six / decades ago"* — a full first line and two words on the second. `"Global population growth\npeaked six decades ago"` balances it, and the file's own portrait pages carry exactly that newline. Check every title of three lines or more the same way; the wrap that fits is not the wrap that reads.
- **Drop grapher's trailing year.** Grapher appends ", 2023" to single-year titles; that appendix does not belong in the template — for a descriptive title, move it to the subtitle as `Data for <YYYY>.` A year that reads naturally *inside* the sentence can stay ("Four developed countries met the UN's target for foreign aid in 2025"); for the year-specific claims in the next bullet it has to stay there, and comes out of the subtitle instead.
- **If the claim is the kind that changes from year to year, put the year in the title itself — and use the past tense.** Counts, rankings, superlatives and threshold-crossings ("Five countries had more women than men in parliament in 2025", "X became the largest producer") are true of one year, not of the world; next year's data can falsify them outright. A static image has no update path and will be read long after it was made, so the sentence has to carry its own date: `Data for <YYYY>.` in the subtitle reads as provenance and is easy to skip, and the claim on its own then looks like a standing fact. Prefer the year inside the sentence and drop it from the subtitle — don't state it twice. Durable descriptive titles ("Death rate in the United States") need no year and shouldn't carry one.
  - This is also the cue to check **when within the year** the claim became true, because a threshold-crossing often has a date. One of the five countries in that example only crossed 50% at an election partway through the year, so the claim is true of the year's end and false of its start — worth a note in the accompanying text even when the title's year is correct.
- **Highlight the entity or category the chart is about by coloring that word in the title**, using the exact color of the marks it refers to — e.g. "**Four** developed countries met the UN's target…" in the same coral as the four highlighted bars. One highlight per title; it must match a color actually used in the chart.
  - **A single-series chart takes no highlight at all.** The rule works by picking one entity out of a field; with one line and one country there is no field, nothing to disambiguate, and the coloured word is just noise — its own colour already appears on the line and its end label. Caught in review on a one-country DI, where the title had been coloured by rote.
  - **"One highlight" means one *per series*, not one per title.** The Cheat Sheets say a single colored word; the shipped charts routinely color every series the title names — `222:386` colors *China*, *European Union* and *United States*; `201:335` colors *electricity and heating* and *transport*; `519:38`, `186:185`, `545:78` and `575:114` do the same for two. The rule that actually holds is that **every colored word matches a mark's color**. One word is right when the title picks one entity out of a field; one per series is right when the title names the whole cast.
  - **A question title has nothing to color, so it keeps its legend.** `1:5861`, `448:572`, `606:263` and `609:461` all ask a question and all carry a legend — see Maps.

### Subtitles and notes

- Subtitle short, clear, and necessary — cut whatever the visualization already makes obvious.
- The subtitle is where a single-year chart says `Data for <YYYY>.` — unless the title already carries the year, which is the rule for a year-specific claim (see Titles). The year is stated once, in one place or the other, never both.
- **DI images normally carry no note.** The DI template has no note line, and most notes can go: they explain caveats the DI text already covers. When a note is genuinely load-bearing for reading the chart, the cleanest form is to **fold it into the subtitle's own sentence** — "…differences in living costs between countries" plus a note about constant prices becomes "…between countries, **and is expressed in international-$ at 2021 prices**", one flowing sentence with no extra weight. Reach for a **bolded second line** only when the note won't join the sentence (a different subject, or a caveat that has to stand apart). Either way, only if the subtitle isn't already crowded; anything longer belongs in the DI text, not the image.

### General

- Consider replacing "World" with "Global average".
- No abbreviations the audience may not know; write the full word. Plain language throughout. (Units are the exception — see the next bullet.)
- **Units are the exception, and the split runs between labels and prose.** Inside the plot, where a unit repeats on every mark, abbreviate it to its symbol and abbreviate the magnitude with it: `1.3 million tonnes (48%)` becomes **`1.3M t (48%)`**. In prose — title, subtitle, note, source — spell it out, because there it is read once and as language: the note still says "flows smaller than 26,000 tonnes". This is not a contradiction of the bullet above; `t`, `M`, `km`, `%` are read by everyone, and it is *domain* abbreviations that need spelling out. Keep the digit groups as digits (`560,000 t`, not `560K t`) — abbreviating the magnitude only pays where it replaces a whole word.
  - It is worth real space: on a sankey whose labels run down both sides, that one substitution took **59px** off the width of the label columns, on both a 850-wide and a 540-wide frame. Give the saving back to the plot (SKILL.md → the re-render, not a rescale), rather than leaving the chart narrower than its title.
- Keep text horizontal — title, subtitle, annotations, data labels. If labels end up vertical, change the chart's orientation instead (e.g. horizontal bars).
- Text hierarchy by font size: title > subtitle > source ≈ annotations ≈ labels. All fonts readable, especially the smallest.
- **Check the hierarchy on the numbers, and expect the export to arrive with it broken.** Grapher's square export renders **legend labels at 16.8px against a 16px subtitle** — the least important text in the chart ends up the second-largest thing on it. List every distinct size with what it belongs to and confirm the sequence is non-increasing; anything inside the plot that outranks the subtitle is a defect to fix, not a size to preserve. Bringing that legend to 14px reads as a *correction*, not a compromise: it restores the order and sits with the bar values at 13.7px. This is why "don't change font sizes" carries the exception it does — the rule protects the export's *relative* type scale, and where the export's own scale is wrong, matching the template's hierarchy wins.
- **Axis titles are horizontal and sit at the ends of their axes** — the y-axis title stacked above the axis top-left, never rotated; the x-axis title at bottom-right or centered under the axis. Every chart on the Scatter page does this (`1:3615`, `91:1118`, `210:710`, `80:382`), and it is what "keep text horizontal" means in practice for an axis.
- **Emphasize the one axis tick the title's claim rests on.** `99:723` bolds `50%` for "nearly half"; `178:263` bolds `50%` for "more than half". The other ticks stay regular.

## Annotations and arrows

**Moved — [reference/ANNOTATIONS-AND-ARROWS.md](reference/ANNOTATIONS-AND-ARROWS.md).** Read it at Step 8b if the chart is getting either; skip it otherwise. It covers the three knockout tiers, the ~12px clearance, what an annotation may cross, the curvy arrows and their scaling, and straight elbowed arrows.

## Dots

- **10×10 px dots** to highlight specific years on a line/slope, centered on the point — `dot.x = end.x − 5`, `dot.y = end.y − 5`. That is the dot *you add*.
  - **→ source says: grapher's own exported dots are 6×6, not 10×10.** Square-format line exports used to hide dots entirely; now, if the interactive chart shows dots the static export does too, but at 6×6 because of spacing. So an export can arrive carrying 6×6 dots you did not place — don't resize those to match a 10×10 you add, and don't add a 10×10 beside one without deciding which size the chart is using. Pick one and apply it to every dot in the frame.
- **Derive a dot's position from the mark *after* the chart's last move, never before.** Dots created beside the chart group do not travel when the chart is re-centered in its band — dots placed before the final centering sat 3.8px above their own lines here, which a designer spotted immediately. Same trap as annotations and leaders, with one difference: a dot has an exact right answer, so **re-derive it from the line's endpoint at the end** rather than translating it by a remembered delta.
  - **Better: parent the dots *into* the chart group once they're placed** (`chart.appendChild(dot)` — last child, so they stay above the lines). That kills the travel hazard outright, and it fixes a second problem you would otherwise not see: a dot left as a sibling sits **outside** the group's bounding box, so the box understates the chart's real ink by the dot's overhang (5px for a 10px dot on a peak) and every gap you measure off `chart.height` is wrong by that much in one direction only. See SKILL.md → Step 7 on making the box equal the ink.
- **Every value you write out gets a dot, including one that labels a plateau.** The tempting argument against is precision: where a series sits flat for years (0.032 across 1974–79), a dot appears to pin the number to one year it doesn't belong to, so leaving the number to float above the flat stretch looks more honest. It isn't — it is just ambiguous, and a designer's rework put the dot back with the note that *it's unclear where the value is*. The reader's first question about a number is which mark it belongs to, and answering it beats a precision nobody was going to infer. Put the dot at the **first** year of the run — the year the level was reached — and if the run needs saying, say it in words, not by withholding the mark.
- No outline on the dot — except on **stacked areas**, where a **white outline** makes it stand out against the colored fill.
- After any resizing of the chart, verify dots are still round (see "never stretch one axis"). This is the one thing a hand-stretch reliably breaks: dragging a selection wider ovals every ellipse in it, which is why the note that came back with a widened plot was "stretch the chart to fill the remaining width **but don't distort the circles**". The scripted x-map in SKILL.md → Step 8 center-maps small nodes precisely so it can't; assert 10×10 afterwards either way.
- Grapher renders no dots at all on charts with more than ~500 points — they were never in the export.

## Direct labeling (the default improvement to propose)

- **Kill the legend when labels can live in the chart** — legends force the reader's eyes to ping-pong. Line charts: entity label at the end of its line, colored like the line, without the elbow/leader connectors grapher draws; reclaim the freed margin for the chart area.
- **"When labels can live in the chart" is a real test, not a formality.** Each label has to sit on the mark it names — over its segment of the top bar, or inside the widest segment of its series. Judged as single-line labels laid end to end that caps out at three or four categories, **but the cap is not the test** — **six fit** once the labels are allowed to tier, wrap and point at the narrow segments (SKILL.md → Step 8 has the recipe, and the archive's `137:267` and `687:203` are worked examples). What is genuinely disqualifying is a different move: spreading the labels evenly across the plot instead of over their own segments, which yields a color-coded row that is *harder* to read than the legend it replaced. Try the tiered version first; when even that doesn't fit, keep grapher's legend, tidy it, and say why.
- Bars/areas: label inside the chart element when contrast allows — white text on dark fills, **≥12px**.
- If a legend must stay: **squares**, not circles or rectangles; consider moving it into empty chart space (stacked bars often have some) rather than under the subtitle.
- **Get the legend onto one row — shortening the labels is the lever, not shrinking the type.** Grapher wraps to two rows because its labels are verbose, and the second row costs ~15px of plot. Trim to the shortest wording that is still unambiguous *in this chart's context*: "Other meats" → **"Other"** and "Beef and buffalo" → **"Beef"** on a chart entirely about meat. That freed 65px here, which bought the legend back up to 14px — squeezing the type to 12px to fit the long labels had been the alternative, and it reads worse. Keep the legend in **stack order** so the eye maps it left-to-right onto the bars. Note the knock-on: collapsing a row makes the plot shorter, so re-export ~15px taller or the gap drifts above the band.
- **A residual category goes last.** "Other meats", "Other", "Rest of the world" and the like belong at the end of a stacked order and at the end of the legend — they are the leftover, so putting them mid-stack breaks the reading order and pushes the named categories apart. Fix this **in the chart** (the dimension's column order), not by moving vectors in Figma, so the image and the interactive chart agree.
- **Label the identity of every mark; label the value of only the ones worth reading.** A chart that labels all of everything turns its plot into a wall of type, and the labels it can least afford are the ones on the smallest marks — a number nobody can act on, printed beside a sliver, competing with the numbers that carry the story. So keep the **name** on every mark, and drop the **value** below a cut-off. Pick the cut-off on the marks, not on the numbers: read down the sorted list and cut where the marks stop being distinguishable from each other. State the rule you used when you report, so it can be corrected if you inferred it wrong.
  - Two things to get right afterwards. **The cut-off is one number applied to both sides** of a two-column chart, or the two columns disagree about what counts as small. And **a label that loses its partner has to be re-centred** — see SKILL.md, where the mechanics live.
  - On the avocado sankey the cut fell at 110,000 t (≈4% of the total shown), dropping 9 of 20 values and keeping all 20 names.
- **Tied values share one label, joined by a curly brace.**
- **Shorten a label only when it is one of the longest.** The longest label is what caps the plot's
  width, so "United States" → **US** buys nothing while "Switzerland" is still there. Shortening the
  top one or two is a space edit; shortening the rest is just inconsistency.
- **Too many entities for the height is a frame problem, not a labeling problem.** Don't cram them
  in or drop below the font floor — **stretch the frame vertically**. Static charts don't have to be
  square, and the 540×824 mobile template exists for exactly this.
 Repeating the same number down a ranked list reads as noise; one brace and one value reads as a fact. `169:1167` (five countries at 0.12%), `201:164`, `203:108`, `341:82`, `468:287`, `668:54`. The same brace also replaces a run of empty bars — `201:164` writes "Slovakia, Luxembourg, Hungary and Australia did not report aid costs for refugees" where four zeros would have gone.
- **In a set of small multiples, name the series only in the first panel.** The reader carries the mapping across; repeating it in every panel is clutter. `201:335` (slopes, with the title's words colored to match), `162:110` and `293:57` (bars). Axis labels likewise go only on the outermost panels.
- **When endpoint or in-mark labels collide beyond rescue, give up on them and build a bracketed column beside the plot** carrying `Name` over its value(s) — `323:216` and `445:80` (slopes, `63% → 43%` on one line), `603:844` and `633:1417` (ranked bars, numbered `1.`–`5.`).

## Colors

**Bind the library style by name — never type a hex.** Every color in this file is listed with its style *name* first because the name is the handle: `Denim`, `Camel*`, `Annotations`, `Axis Grid`. Bind it from the **[Chart Colors]** library (the range setters are in Annotations above; for a whole node it is the node's fill style, not its `fills`), and let the hex follow from the binding.

The hexes recorded here are for **identification and auditing** — telling you which color a node already carries, letting `color_audit.py` match an imported fill against the palette, and letting you spot an off-palette value. They are not values to assign. A hardcoded hex looks identical on the day you write it and then silently stops tracking the library: when design revises a swatch, every bound node moves and every typed hex stays behind, with nothing in the file or the render to show which is which. That is also why the handover doc asks for **every color as its style name and key** — "a hex alone is unreproducible, nobody can tell whether it came from the palette."

The same rule governs type: apply the **named text style**, don't set `fontSize`/`fontName` to match one. See Named styles below.

**Darken a color in HSL — lightness × 0.6, hue and saturation untouched — not with an RGB multiply.**
The RGB route is what you reach for first and it only works on saturated fills: on a pale color it
slides toward grey. This is general to any darkened variant you need — a mark's own outline, a title
word that has to stay legible, a hover-free emphasis — and the worked numbers are in
`reference/per-chart-type/maps.md`.

**The categorical palette runs out at six categories, however you choose them — and direct labeling
is what makes that survivable.** Grapher's own six-color assignment routinely fails the color-vision
audit (measured: deuteranopia ΔE **9.2** on one pair, 14.6 and 17.7 on two more), and no
recombination fixes it, because six is past what the palette can separate. What rescues the chart is
that **no reader has to match a color to a key**: with every mark named in place, the colors
*distinguish* rather than *encode*, and a failing ΔE stops being a legibility bug. So on any chart
above about four categories, report the audit numbers, note that the marks are directly labeled, and
leave the repaint to the chart's author — rather than repainting toward a palette that cannot hold
them. `color_audit.py` is the tool; `reference/per-chart-type/line.md` has the measured case.


- Only the OWID palette — the **[Chart Colors] Library**, a shared Figma library, not a local style set (select an object → Fill → the four-circles library icon → Chart colors). It is the source of truth; `scripts/color_audit.py` carries a copy read off the cheat-sheet swatches, verified against `OwidDistinctColors` in owid-grapher. Re-read the library rather than trusting either copy if they ever disagree.
- **The fill list below is also the assignment *order*.** Cross-checked against the Cheat Sheets' "Revised Line Chart Color Order" (2026-08-20): that sequence is identical to the list below, so assigning series in list order is what the design team recommends for maximum separation between adjacent colors. Take them in order rather than picking by eye, and see the Line and Slope Charts group below for the darkened variants to use on thin marks.
- The 24 fills: Denim `#4c6a9c`, Rusty Orange `#b13507`, Camel `#bc8e5a`, Light Teal `#58ac8c`, Purple `#6d3e91`, Maroon `#883039`, Midnight Blue `#00295b`, Mauve `#a2559c`, Dark Copper `#9a5129`, Turquoise `#38aaba`, Cherry `#970046`, Lime `#3b8e1d`, Peach `#e56e5a`, Blue `#286bbb`, Dark Olive Green `#18470f`, Coral `#d73c50`, Copper `#b16214`, Teal `#00847e`, Fuchsia `#cf0a66`, Olive Green `#578145`, Dark Orange `#c05917`, Dark Mauve `#8c4569`, Tealish Green `#00875e`, Dusty Coral `#c15065`.
- The library's **Line and Slope Charts** group is the same palette with six colors darkened for thin marks and text on white: Camel* `#996d39`, Light Teal* `#2c8465`, Turquoise* `#008291`, Lime* `#338711`, Peach* `#c4523e`, Dark Orange* `#be5915`. Use that group for lines, slopes and scatter dots (`color_audit.py --line`).
- The library's third group, **Categorical Maps**, is a separate muted set for choropleths and is *not* interchangeable with the Default Palette: Sand `#c3a27c`, Light Sand `#d8c0a2`, Taupe `#b9b2a6`, Olive `#5b6d35`, Leaf Green `#6fa54f`, Mustard `#d9bc54`, Tomato `#d94c3f`, Lavendar `#8e97c7`, Soft Purple `#77538f`, Muted Teal `#238a84`, Light Teal `#4fb2ac`, Muted Cherry `#b04e74`, Light Cherry `#cb7fa0`, Muted Denim `#526f9b` (`color_audit.py --maps`). Map colors are agreed with the design team through the `add-provider-regions` workflow, not chosen here.
- **`Gray #6e7581` is a Default Palette color; grapher's own gray is not.** A grapher export renders residual categories in `#585c64`, which is nowhere in the library — so a chart can arrive with an off-palette color through no one's decision. Check the fills you inherit against the library, not just the ones you add. (Swapping the library gray in is not automatically an improvement: on one six-category stack it *lowered* the safety floor from 26.2 to 20.5, because the lighter gray sits closer to the teal. Measure, don't assume.)
- **Don't take the palette from the cheat-sheet swatch grid alone** — it renders 24 colors and omits at least `Gray`. Read the library itself (`search_design_system` for the group name, then `figma.importStyleByKeyAsync(key)` for each hex); the cheat sheet is a picture of the palette, not the palette.
- For **lines, slopes, and scatter dots**, use the line-variant palette and try colors in the cheat-sheet order (better mutual distinction): Denim `#4c6a9c`, Rusty Orange `#b13507`, then the darkened variants Camel* `#996d39`, Light Teal* `#2c8465`, … The starred colors are darkened versions of the standard palette for thin marks on white; in code they are `OwidDistinctLinesPalette` / `DarkerOwidDistinctColors` in owid-grapher's `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`.
- Use color to *mean* something: highlight the entity the story is about, mute the rest with gray. Meaningful associations are fine (forests = green).
- **When a palette has to change, propose the closest one that passes, not the safest one.** A designer reads a small shift as a fix and a wholesale repaint as a different chart, so total CIELAB drift from the colors already in use is what you argue from — with the residual category pinned to `Gray`. Concretely, that means `color_audit.py --suggest`'s ordering, which is **hue variety, then safety, then drift**: ΔE 20 and — on stacked fills — the 1.6:1 seam are pass/fail gates, hue variety comes first because ranking on safety alone returns palettes that are entirely blues and greens, and drift separates whatever ties. Don't re-rank the output by drift alone — the tool's top group is already the passing, varied one, and drift picks within it. Report how many colors actually have to move, and be ready for the answer to be "all of them": on this chart no passing palette kept a single original color, because one structural collision at the end of the stack (`Denim` beside `Gray`, 1.18:1) forces Fish to move, which forces Sheep off Purple, which forces Pork off Coral. When that happens, say so with the chain — otherwise a full repaint looks like overreach rather than the only option. Note that a color can often survive by **relocating** rather than being dropped: Denim was fine once it moved to a category not adjacent to the gray.
- **A light tan and a salmon are the same color to a deuteranope** — Camel `#BC8E5A` against Peach `#E56E5A` measures **ΔE 2.1**, one of the worst pairs the OWID palette can produce, because both desaturate to the same yellowish tone. Any two mid-to-light warm fills are suspect; darkening one of them (Copper `#B16214`, L 50 against Camel's L 62) is what separates them. Check warm pairs first — they look obviously different and measure catastrophically.
- **When a design choice knowingly fails a bar, record the deviation where the next person will read it.** Not everything gets fixed: a warmer color with better label contrast can be worth a color-vision cost, and that is the author's call to make. What is not acceptable is leaving it silent, because the next run measures it, reads it as a bug and "fixes" it. Write the numbers, the reason and the alternatives that were measured, and state the accepted figure explicitly — "we decided the colors were fine" is not a record.

  **Where it goes: the report, and the handover doc when there is chart-side work to hand back (`ai/<topic>/…md`).** Reusable mechanics go in this skill, so the next run inherits them automatically rather than re-reading them off a canvas. **Add a note to the Figma page only if the user asks for one** — don't volunteer it. The design file holds the chart, not its paperwork, and an unrequested text block beside the frame is clutter the designer has to step around.
- **Put the quietest color on the largest area.** A safe palette can still be unpleasant: on a stacked bar the widest category covers the most ink, so a light saturated fill there (Light Teal on segments running 43–52%) dominates the chart even though it measures fine. Rank candidates by the **chroma** of the color landing on the biggest category and prefer the lowest — a dark, low-chroma fill recedes and reads as depth. Midnight Blue (chroma 35) on the widest segments beat Light Teal (chroma 46) on exactly this chart, at the same ΔE. Passing the audit is the floor; which color sits where is still a design decision.
- **Label-on-fill contrast is its own check, and grapher's choice is not always right.** Bar-versus-bar separation says nothing about whether the value sitting *on* a bar can be read. Measure each in-bar label against its own fill and require **4.5:1** — these labels are ~13.5px regular, so the 3:1 large-text allowance doesn't apply — and compute **both** black and white rather than keeping what the export chose: on one chart grapher put white on Coral at 4.51:1 when black scored 4.65:1. Expect the weakest pair to be whichever category is mid-lightness (Copper measured 4.63:1, the floor of a passing palette); before changing the color, check whether any alternative improves the label contrast *without* breaking the palette's own bars — often none does, and "this is the best available" is the honest answer.
- **Color-vision check on every chart, measured not eyeballed** — run `scripts/color_audit.py` (see SKILL.md → Step 8). Pairs closer than ΔE 20 under deuteranopia or protanopia fail; 20–30 is tight. **Tell it whether the fills touch**: the grayscale seam gate applies to stacked and segmented charts, so pass `--separated` for a plain or grouped bar chart, a line chart or a map — otherwise it judges an adjacency the chart doesn't have. Two things this catches that inspection does not: a *stacked* chart needs its adjacent segments separable **and** every pair separable, because the reader also matches legend to segment; and more than about four categorical colors is where safe palettes start to run out — a six-color stack measured ΔE 9.2, and no single substitution lifted it. Also check contrast of text and marks against the background, and that the chart still reads in black and white (readers print).
- **"Isn't red next to green bad?" — the answer is in the lightness column, not the hue.** Every deficiency collapses some hue axis and none of them touches lightness, so a red/green pair is safe exactly when the two differ in lightness and unsafe when they don't. Measured on one chart: Coral `#d73c50` against Tealish Green `#00875e` shares its lightness (grayscale **1.01:1**) and duly fails protanopia at ΔE 16.4, while against Dark Olive Green `#18470f` (**2.39:1**) both deficiencies pass. So read the audit's grayscale ratio for *every* pair you are worried about, not only for the touching ones it gates on — for non-adjacent fills it is reported but not enforced, and it is the number that predicts whether a hue pairing survives. The corollary is that "avoid red and green together" is the wrong rule; "don't pair two fills of the same lightness" is the right one.
- **A stack that merges in print usually fails *inside* a family, not between them.** Where each category is a base plus tints of itself, the seams that break are base-versus-own-tint: on one chart both three-member families merged (1.46:1 and 1.52:1) while every cross-family seam was ≥1.98:1. No change of hue fixes that — widening the tint weights does (0.4/0.65 → 0.42/0.78 took the floor to 1.60:1 and lifted the CVD floor from 6.4 to 14.2 at the same time). Check the within-family seams first; they are also the cheapest to fix, since the base and the palette stay put.
- **Which category carries a color is a separate decision from which colors you use, and it is free.** Swapping two families' colors leaves every audit number identical — same ten fills, and the adjacency effects cancel — so it costs nothing metrically and changes how heavy the chart reads. Put the darkest fill on the *smallest* category: the winning palette had a near-black green, which on personal care (half of every bar) made the chart a green chart, and on leisure (a fifth) reads as an anchor. Try the swap before rejecting a color for being too strong.
- Map colors should be set in grapher itself (Viridis / ColorBrewer sequential palettes), not repainted in Figma — Brewer palettes distinguish better than the OWID categorical palette when many classes are shown.

## Highlighting one series against the rest

When a chart carries many series but the story is about one of them, the static image says so with **one palette color on the protagonist and a flat neutral gray on everything else**. This is the single highest-value edit available on a multi-series line chart, and it is worth checking for on every chart that arrives with more than about four series.

**Grapher will not have done it for you, even when the chart's author asked for it.** A chart with `focusedSeriesNames` set renders focus in the SVG as **`stroke-opacity="0.5"` on the non-focused series** — they keep their own hues at half strength. Half-strength distinct hues still read as "these twelve categories all matter": the reader's eye is pulled twelve ways, and the direct labels inherit twelve colors that mean nothing. What *does* survive the export is the emphasis on the focused series — it comes back at 3px, with a white halo (`outline__<Entity>`) a single pixel wider than the line, and a marker at every year — so grapher hands you a half-finished version of the treatment and you complete it.

The treatment, in the order that avoids rework:

1. **Non-focused lines → one flat gray, `strokeWeight = 1`, opacity reset to 1.** Set the weight explicitly; **don't assume the export already gave you a thin line** — grapher renders context lines at 2px, which is heavy enough that twelve of them still crowd the protagonist however well they are colored. Reset the opacity too, because leaving the inherited 0.5 on top of gray lands you at a near-white the reader loses against the gridlines. The weight contrast is half the treatment: 1px gray against the protagonist's 3px color is what makes the hierarchy read before any color does, and it is the thing to check first when a muted chart still feels busy.
2. **Non-focused labels → a slightly darker gray than the lines**, also at opacity 1, so the words stay findable while the marks recede. One gray for both flattens the two into a single mass.
3. **The protagonist → a palette color, bound as a library style** (`setStrokeStyleIdAsync` for the line, `setFillStyleIdAsync` for the label and any markers).
4. **Widen the white halo to double the line's weight.** A focused line arrives with a sibling `outline__<Entity>` — the same path in white beneath it — but the export ships it barely wider than the line (a 3px line gets a 4px outline, half a pixel of white per side, which does nothing where the line crosses another). Set `outline = 2 × line` (3px → 6px, ~1.5px of white each side). That halo is what lets the line read as continuous through the context lines, and it is the cheapest way to separate any two lines that overlap.
   - **Whenever you change a line's stroke weight, change its outline in the same edit** — they are separate nodes, so thickening the line alone eats the halo, and pushing the halo much past 2× turns it into a white band through the chart.
   - **To give a line a halo it doesn't have** (a second highlight, or two context lines that need untangling at a crossing), clone the line, set its stroke white at double the weight, and drop the clone directly beneath the original in z-order.
   - **Then raise the pair to the top of `lines`, or the halo does nothing where it matters most.** Grapher emits one `outline__<E>` / `line__<E>` / `datapoints__<E>` triple per series in *selection* order, so the protagonist usually lands **mid-stack** and every series after it paints straight over both the line and its white halo — at exactly the crossings the halo exists for. Widening it changes nothing there. `lines.appendChild(outline)` then `lines.appendChild(line)` (last child is topmost, halo first so the line sits above its own halo) — **and `lines.appendChild(datapoints)` last whenever step 6 keeps any marker**, since a `datapoints__<E>` left at its original depth is both paintable over by a later series and cut through by the halo you just widened. Then assert that the last *three* visible children are that triple; drop to asserting the pair only when every marker on the series is hidden. It is invisible in a node listing and obvious in a render: on a four-series chart the fix changed 174 pixels, every one of them at a crossing.
5. **The protagonist's label → bold, in the same color.** The color ties label to line; the weight is what raises it above a column of same-size gray labels without spending a size step.
6. **Markers: keep the ones that carry a date, hide the rest** (see Dots). A focused line arrives with a marker per year, which reads as noise.

**The highlight color is a decision, not an inheritance.** Grapher assigns it by series index, so the protagonist arrives in whatever slot it happened to land in — Purple, say — and that is not a choice anyone made. **Rusty Orange `#b13507` is OWID's default single-series highlight**; deviate from it only for a reason (an established topic color, a second highlight already using it).

**The values are settled — use them: lines `#a6a6a6` at 1px, labels `#8c8c8c`.** Use them even when the page you are working from does something else. Some finished pages mute with `Data Insights/Axis Grid` `#DDDDDD` at 1.5px and `Data Insights/Axis Lines` `#999999` instead, and those *are* named local styles, which is tempting — but `#DDDDDD` is the gridline color, so it drops the context series to the weight of the grid and a reader can no longer follow any individual one. The designed pair keeps the context followable while still receding. When a reference page and this file disagree, this file wins on the muting values; say in the report that you deviated from the page and why.

Two things follow that a later run will otherwise try to "fix":

- **They are furniture, not categorical colors, so the palette rule does not apply to them.** The Default Palette's `Gray #6e7581` is a color meaning "this category is the leftover", which is not what a muted context series is, and it is too dark and too blue for the job. Neither of the two values is a named color in the file yet — worth naming there, but not worth substituting a named color that means something else. Expect the Step 8c off-palette check to list them; that listing is the record, not a finding.
- **`#8c8c8c` measures 3.36:1 against white, under the 4.5:1 that 15px regular text needs, and that is an accepted deviation.** It is deliberate: context labels are meant to recede, and darkening to `#767676` (4.54:1) buys the contrast by spending the thing the treatment exists for — separation from the protagonist drops from 1.85:1 to 1.37:1, and twelve darker labels start competing with the one that matters. **Keep them light.** Report the figure so the decision stays visible, and do not raise it as a defect to be fixed.

The general rule underneath: **on a muted series, light and thin beats compliant.** These marks and words are context, and a context layer that passes every threshold has stopped being context.

**Don't run the color-vision audit on a chart like this.** One categorical color against neutral grays has no pair to check, and reporting "no failures" from a two-color audit reads as coverage you don't have. Say that the palette reduces to one highlight plus grays, and check the two things that *are* live: the highlight's contrast against the background, and whether the highlight still separates from the grays in black and white.
**Keep a reference row, and color it differently from everything else.** A ranked chart of entities almost always carries a `World` / `Global average` / `Global share` row left in rank position and drawn in its own color, so the reader can see which side of the average each entity falls. `364:129` (World blue among magenta), `450:638` (Global share navy among red), `535:350` (Global average tan, label bolded), `625:44`, `342:141` (gray, between a green top group and an orange bottom group), `643:517`, and on other pages `99:723` and `687:203`. It is what turns a league table into a claim, and it costs one row.

**Muting drops saturation, not identity.** Context series keep their labels, set in the same gray as their line — `213:896` and `80:1158` label every muted slope. Deleting the labels along with the color leaves the reader unable to say what the gray lines are.

**Line weight is a second highlight channel.** `397:50` gives the protagonist a ~3px stroke against ~1px for the context, on top of the color difference.

## Named styles in the Charts file

**Apply the named style; do not reproduce its values.** These are what the templates are built from, and a text node cloned from a template node inherits them for free — so cloning beats creating. Setting `fontSize = 14` gives you something that *looks* like `Annotation M 14` and is not it: it carries no style id, so it never moves when design revises the ladder, and Step 8c's "sizes are named styles" check reads it as an off-ladder number. Bind the style and let the size follow. (One ordering trap, spelled out under Annotations: applying a text style **resets** the node's range fills and weights, so apply the style first and re-apply the color-and-bold convention after.)

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

**Moved — [reference/FLAGS-ANIMALS-NODATA.md](reference/FLAGS-ANIMALS-NODATA.md).** Read it when the chart carries flags, animal icons, or a map with missing data.

## Per chart type

Each chart type's conventions live in its own file. **Read the one for the chart in hand, not the set** —
together they are ~44 KB and a run needs one of them.

| Chart type | Read |
|---|---|
| Line charts | [reference/per-chart-type/line.md](reference/per-chart-type/line.md) |
| Stacked area charts | [reference/per-chart-type/stacked-area.md](reference/per-chart-type/stacked-area.md) |
| Bar / stacked bar charts | [reference/per-chart-type/bar.md](reference/per-chart-type/bar.md) |
| Slope charts | [reference/per-chart-type/slope.md](reference/per-chart-type/slope.md) |
| Scatter plots | [reference/per-chart-type/scatter.md](reference/per-chart-type/scatter.md) |
| Maps | [reference/per-chart-type/maps.md](reference/per-chart-type/maps.md) |
| Marimekkos | [reference/per-chart-type/marimekko.md](reference/per-chart-type/marimekko.md) |
| Combination charts | [reference/per-chart-type/combination.md](reference/per-chart-type/combination.md) |
| Misc — treemaps, arrow charts, dot-and-interval | [reference/per-chart-type/misc.md](reference/per-chart-type/misc.md) |

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
