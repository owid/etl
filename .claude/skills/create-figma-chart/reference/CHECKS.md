# Step 8c — The checks that must pass before you show it

> Read at Step 8c, before you show the user anything.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


Every one of these caught a real defect on this skill's first run, and none of them is visible by looking at the frame. Run them as a pass, and report the numbers rather than "looks fine".

> **⚠️ `verify_page.js` does not fit in a `use_figma` call as it ships.** The `code` argument caps at
> **50,000 characters** and the file is **~117,000**, so the instruction below is not executable
> verbatim — a run that pastes it is rejected. Emit it stripped instead:
>
> **Run it in slices — that is the supported path, and the only one.** Stripping the comments used to
> get it *just* under the cap (the helper is context-aware, so URLs, regex literals and template
> strings survive), and even then it sat at 97%, all of which has to be relayed verbatim — where a
> one-character corruption yields a *wrong verdict* rather than an error, the exact failure this gate
> exists to catch. It has since grown past the cap outright: **60,937 stripped**. So
> `inline_script.py verify_page.js` with no `--rows` **refuses and exits 1**, naming the size and
> pointing here, and `--whole` no longer overrides that — the cap is now a hard floor for this
> script, not a judgement call. The rows are grouped, each slice carries the shared preamble:
>
> ```bash
> .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js --list-rows
> .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js --rows series --frame-id <your frame>
> ```
>
> | group | rows | size |
> |---|---|---|
> | `type` | text-floor, annotation-ladder, ladder-sizes, named-styles, source-line-weight, text-hierarchy | 69% of cap |
> | `series` | series-weight, furniture-weight, furniture-dash | 64% |
> | `geometry` | box-alignment, gap, margins, off-palette | 60% |
> | `annotations` | polylines, annotation-overlap, annotation-knockout, annotation-block-gap, label-contrast | 76% |
>
> Groups combine, so the whole pass is two calls: `--rows type,series` (41,982) then
> `--rows geometry,annotations` (43,597, **87% of cap** — the figure `inline_script.py --check`
> reports: it measures **these two calls**, declared as `DOCUMENTED_CALLS` in the script, rather
> than the smallest split it could find for itself — an optimiser would go on reporting a
> comfortable number by picking a split nobody is told to send. Change the pair here and there
> together; `--check` fails if their groups no longer cover the file exactly once.
> Read its **`floor`** column beside that percentage: `sent` is what today's two calls cost and can
> always be bought down by re-splitting, while `floor` is the preamble plus the single largest row
> group — the smallest any call can be, whatever the split. `verify_page.js` reads **87% sent against
> a 76% floor**, so re-splitting still buys real room; when the floor itself passes the cap, slicing is
> exhausted and the only move left is splitting the script into separate files with their own
> preambles. `--check` fails on that and warns past 85%.
> **Run all four** — each reports its own rows and nothing else, so a group you skip is a group
> nobody checked.
> `diff_against_template.js` (~12,000 stripped) needs none of this.
>
> **Do NOT substitute a hand-rolled subset. It is worse than skipping the pass, and this is
> measured.** A run that could not relay the script wrote its own seven rows instead, reported six
> `PASS` and shipped three defects a reviewer caught by eye within minutes: a series line left at
> **1.296px** after a `rescale()` (the stroke-weight rows were simply absent), a **bold producer
> name** in the footer (the text-weight rows were absent), and — the instructive one — annotations
> with **no knockout at all**, which its own knockout row had passed. That row tested crossings
> against **data**, where this file requires a 3px stroke whenever **furniture** is crossed; both
> annotations sat squarely across gridlines. A skipped row announces a gap. A reimplemented row
> *closes* the gap on paper while leaving it open in the frame, and you cannot tell from the report
> which you have. So: relay the script, or declare the whole pass unrun.
>
> **[`scripts/verify_page.js`](../scripts/verify_page.js) runs the MECHANICAL rows in ONE read-only
> `use_figma` call** — text floor, annotation ladder, named styles, text hierarchy, series and
> furniture weights, dash patterns, box alignment, gap, margins, unbound fills, annotation knockout
> tier, annotation block gap, and the series polylines the annotation-overlap row needs. Done one at
> a time those are a dozen round trips at ~8-10s each.
>
> **Every row it cannot judge comes back `SKIPPED` with the reason and the tool that owns it** —
> colour-vision and grayscale (`color_audit.py`), spelling (`codespell`), the data-truth row
> (`/adversarial-data-review`), entity completeness (needs the *effective* selection from outside
> Figma), the arrow row (it needs rendered pixels), `leader-on-map` (a **vector** ray-cast against
> the country's rings — pixels are only its fallback), and the page count (a page-level fact, where
> the script is handed frames).
>
> **`colour-vision` and `grayscale-seams` hand back a runnable command, not just an owner.** The
> script cannot run `color_audit.py` — a Figma plugin sandbox has no shell — but the palette is
> already on the canvas, so it emits the invocation with the palette filled in, the interpreter
> spelled out and `--names` attached when the node names allow it. Paste and run it; both rows are
> one run of one script.
>
> The palette is taken from **identified data marks and line/slope series strokes**, never from the
> script's `fills` inventory. That inventory holds every solid paint on an area node in the plot and
> a TEXT node has one, while a line chart's series colour is a **stroke** and is not in it at all —
> so sourcing from it audits axis and legend labels as categories while omitting the data. Measured
> on the test fixture, it returned a one-entry palette consisting of a text label's fill. `outline__*`
> strokes are excluded too: that is the white halo under a line, shared by every series.
>
> **A legend is furniture, and its swatches are not categories.** grapher draws the legend *inside*
> the chart group and *outside* the map — a map page reads `chart > numeric-color-legend > {lines,
> swatches, labels, swatch-hit-areas}` as a **sibling** of `map` — so every filled swatch reached the
> palette as an ordinary chart-side mark. An ordinary choropleth then reported **two** palettes: the
> map under `--maps` and its own legend under `--separated`, with a `--suggest` rerun ready to
> recommend restyling the legend rather than the categories. On a line chart the harm landed on
> `--names` instead: a numeric legend's bins are unnamed rects, so one swatch in a colour of its own
> put an import default into the palette and dropped the flag for the whole run. A legend repeats the
> categories' colours and adds none of its own, so the swatches are excluded, **counted** in the row,
> and any colour appearing *only* in the legend — an empty bin — is named rather than quietly lost.
> They keep their boxes for `annotation-overlap`, where an annotation dropped over the legend still
> covers something the reader needs, and are reported there as "a legend swatch".
>
> The mode flag is chosen from what the palette was sourced from, and it is not cosmetic — it also
> selects which palette a `--suggest` rerun searches. A line or slope palette gets **`--line`**
> (`--separated` plus the Line and Slope Chart variants, the darker set for thin marks on white);
> a map gets `--maps`; anything else gets `--separated`. All three mean "nothing shares an edge".
>
> **A palette of one colour gets no command at all.** Both rows compare *pairs*, so a single colour
> has nothing to compare — but `color_audit.py` does not say so: handed one hex it prints an empty pair
> list and `overall: min dE inf`, and exits 0, which reads exactly like a clean audit.
> [GUIDELINES.md](../GUIDELINES.md) already rules on this ("one categorical color against neutral grays
> has no pair to check, and reporting no failures from a two-color audit reads as coverage you don't
> have"), so the row withholds the command, names the colour, and hands over the two checks that *are*
> live: that colour's contrast against the background, and whether it still separates from the grays in
> grayscale. The clash note survives the withholding — two categories painted one colour **are** a
> one-colour palette, and that is the severest collision there is. A declared `highlightTreatment` gets
> the same warning attached to its command rather than the command withheld, because which entries are
> muting grays is a judgement this script cannot make.
>
> Four things it will not decide for you:
> - On a **stacked or segmented** chart drop the mode flag and reorder the colours into stack order
>   first, because the seam check reads adjacency off the order you give it. That is the only chart
>   where seams exist, so it is the only case where the emitted flag is wrong.
> - On a **map** the command covers a *categorical* choropleth only. `--maps` selects the Categorical
>   Maps palette and the ΔE 20 gate is a categorical test, so a **sequential ramp** — ordered by
>   construction, and set in grapher — fails it by design, and `--suggest` would then offer an
>   unordered palette in place of a correct ramp. Nothing in the plugin can tell the two apart from
>   the fills, so judge a ramp by lightness order instead of running this.
> - `--names` is dropped wholesale, with the reason stated, if any mark is unnamed, carries a comma
>   (it would split into two entries and misalign every label after it), or carries an apostrophe
>   (it would end the single-quoted shell argument mid-name).
> - The palette is deduplicated by **colour**, so two categories painted the same colour reach the
>   audit as one entry and it cannot report the clash. The row names them instead, ungraded — a
>   highlight treatment greys every unhighlighted series on purpose, and a choropleth bin is one
>   colour by definition (map shapes are left out of the note entirely). The category comes from the
>   nearest `<kind>__<Entity>` **ancestor**, because grapher puts the name on the group and the paint
>   on its leaves: a `datapoints__<Entity>` marker is a filled leaf called `Ellipse 12`.
>
> **A frame that paints no pixels returns one row, not a sheet of them.** Figma switches a node off
> two independent ways — `visible: false` and opacity — and both are **inherited**, while the walk
> starts at the frame's *children*, whose own `visible: true` says nothing about whether they render.
> So a hidden frame, or one under a hidden section, used to certify thirty rows of verdicts about a
> deliverable nobody can see. Either switch now returns a single `frame-not-rendered` **FAIL** naming
> which switch is off and where, and the verdict reads `NOT CHECKED` — never "no mechanical row
> failed". Unhide or reset the frame *and every group and section above it*, then re-run.
>
> **A translucent knockout is not certified.** An annotation's knockout works by painting the frame's
> colour *over* what it crosses, so one at 0.005 masks nothing while still passing the weight,
> alignment and colour checks — a clean `ok` on a crossing the reader can see straight through. Zero
> is already "no knockout"; anything between is reported **REVIEW** with its effective opacity (paint
> × node) named, because at 0.98 it masks fine and at 0.05 it does not, and which side of that a frame
> is on depends on what sits behind the annotation.
>
> **The knockout's colour is the ground behind *that* annotation, not the frame's fill.** Requiring
> `frameFill` unconditionally failed a correct chart: an annotation placed inside a tinted region takes
> a halo the colour of the *tint*, and a canvas-coloured one there paints a white outline around every
> letter. The row now composites — the ground is almost always a fill at partial opacity, so the halo
> matches what the reader sees (`#dddddd` at 45% over white is `#f0f0f0`) and matching the raw fill
> still **FAILS**. Two outcomes are **REVIEW** rather than `ok`, because the ground is matched by
> *bounding box* and a tint is usually a triangle or a wedge whose ink fills part of it: a halo that
> matches a ground's composite, and a canvas-coloured halo with some filled shape's box around it.
> Both name the shape and the sum so the call can be made by eye.
>
> Seven consequences that are easy to get wrong in the other direction.
>
> **Grounds stack, and so do a single node's fills.** A translucent tint over an opaque plot background
> renders as the *ordered* composite of both, which equals neither shape's own composite over the
> frame; and a node carrying several visible paints renders their composite too, so reading only its
> first paint reports a colour that is not on the canvas. The row folds both — the node's fills into one
> effective colour and alpha, then the candidates over each other in paint order — and where candidates
> overlap and nothing matches it **REVIEWS** instead of failing, because any subset of the stack is a
> possible ground and a bounding box cannot say which.
>
> **Where the answer depends on something this script cannot establish, it declines instead of
> guessing.** Folding a node's fills needs to know which end of `fills` is the top, and nothing we rely
> on states it — the harness can only encode whatever the script assumes, so it cannot referee. Getting
> it backwards fails a correct halo, or blesses a colour that is nowhere on the canvas. So the stack is
> folded **both ways** and asserted only where the two agree: every single-fill ground, and any stack
> the order does not change. Where they disagree — and where a paint has no single colour at all, a
> gradient or an image — the ground is declared **NOT measurable** and named, the same treatment a
> translucent mark and a sequential ramp already get. It was worth doing: before this, a ground that
> was a solid *plus a gradient* dropped the gradient silently and the row certified the solid base as
> "the colour ANNOTATIONS-AND-ARROWS.md asks for", on a ground that page puts on tier 1.
>
> **Only what is painted *under* the annotation is behind it.** A containing shape appended *after* the
> annotation sits on top of it — the re-import z-order bug this page describes further down, where a
> tint appended last washes the text out. Matched by box alone it read as the ground and the row
> recommended colouring the halo to match it, turning the bug into advice. Paint position is carried
> alongside the box.
>
> **A full-bleed node is dropped only when it composites to the frame's own fill.** Such a backdrop
> paints the canvas colour and would turn every correct canvas-coloured halo into a review. A full-bleed
> rect in a *different* colour is the opposite case — it is the ground behind every annotation on the
> frame, which is exactly how the Instagram templates carry their beige — and dropping it by geometry
> alone failed a correct halo twice over: once as a needless knockout, then again against a frame fill
> the reader never sees.
>
> **...and only when that colour is actually known.** "Composites to the frame's fill" is a question
> about a ground whose colour was established; asked of an **unmeasurable** one it is answered by the
> arbitrary forward fold, and a full-bleed layer carrying a white solid *under a gradient* coincides
> with a white frame and disappears — taking the "not measurable" signal with it, so the halo is
> certified `ok` against a frame the reader never sees. A ground whose colour cannot be established is
> never a no-op; it is the reason to decline.
>
> **A group's opacity is applied once, to its children already combined.** Every descendant carries the
> cumulative opacity of its ancestors, which is right for a single ground and double-counts the moment
> two of them overlap: two opaque children of a 50% group are one 50% layer, not two. Modelling that
> needs per-group compositing *and* the paint order the row already declines to assume, so where two
> candidates share a translucent ancestor the stack is declared **NOT measurable** instead. Decided per
> annotation, never written back onto the ground — whether a group counts as shared depends on how many
> of its children contain *this* annotation.
>
> **The tier branch knows about the ground too.** "Crosses nothing yet carries a knockout" is a FAIL on
> *bare canvas* only. An annotation inside a tint keeps its halo while the region under it is still
> empty — that is the point ANNOTATIONS-AND-ARROWS.md makes — so it is REVIEWED. But a *backdrop* is
> canvas whatever colour it paints, so it never excuses a halo over empty space; only a bounded shape
> does, and the FAIL stands where no bounded filled shape contains the annotation.
>
> **Non-rendering means exactly zero, never a floor.** A node or paint at 0.005 does reach the canvas,
> and a cutoff dropped its whole subtree from *every* row — an 8px label at 0.005 left `text-floor`
> reporting that all of its ranges cleared the floor. Anything positive is **translucent**: held out of
> the palette, named there so the shortfall is visible, and still judged by every row that is about
> geometry rather than colour.
>
> A `SKIPPED` row is a declared gap in coverage, never a pass — which
> is the whole reason to read the list rather than the verdict.
>
> **[`scripts/diff_against_template.js`](../scripts/diff_against_template.js) is the other half of the
> gate, and it answers a question `verify_page.js` cannot: *did this frame drift from the template it
> was cloned from?*** The workflow is start from the template, modify it, and **check back against the
> template** — and that last step is the one that gets skipped. Run it in one read-only `use_figma`
> call with the template id and the finished clones; it fingerprints the template **at runtime**, so it
> works for any of the ten rather than hard-coding one. Text CONTENT is excluded by design (that is
> what a run is meant to change); everything else is the template's law. Declare deliberate drift in
> `CONFIG.expected` and it reports as `accepted` instead of `DRIFT`.
>
> On one eight-frame run it found what a screenshot pass had missed entirely: every footer row left
> `layoutSizingHorizontal: FIXED` where the template HUGs — which stops the source line resizing with
> its text — and it separates the one API limitation that is *not* a defect (a bolded `Data source:`
> prefix cannot be both bold and style-bound through the plugin API, so it reports as `halfBound`; see
> [TEXTS.md](TEXTS.md)) from real drift. Its harness is
> [`scripts/test_diff_against_template.js`](../scripts/test_diff_against_template.js) (**75**
> assertions), which found four defects in the script that review had not: a header that lost a row
> reported as matching, five fingerprinted footer properties never actually compared, and a
> `TypeError` that killed the whole diff when a row changed type. A fifth, from review: the text
> fingerprint held each range's font **style** but not its **family**, so a row retyped in Arial
> Regular read as the template's Lato Regular and the clone reported as matching. Both are compared now.
>
> Validated by planting defects and confirming each row **fails**, twice over: 11 planted in Figma and
> 11 caught, then a stubbed-figma harness ([`scripts/test_verify_page.js`](../scripts/test_verify_page.js),
> `node` it after any edit) covering **276** assertions including the rows that are awkward to plant on a
> real page. **A check that cannot fail is worse than no check**, so when you extend this script,
> extend both passes with it.
>
> Between them those passes found six bugs in the script itself, five of which were rows that could
> not fail: annotations are appended to the **frame**, so a walk over only [chart, header, footer] left
> `annotations` empty on every real page; the 12px floor rejected the 302-wide format's legitimate 11px
> text; the ladder row judged only *bound* nodes, so a rescaled export's arbitrary 13.36px labels
> passed as "imported, expected"; the knockout row judged only annotations that already had a stroke,
> certifying a **missing** one; both 4.5:1 contrast rows were neither computed nor declared; and the
> muted-context classification read each node's own weight, so the 2px halo of a legally-crossed 1px
> context line was reported as a protagonist.

> **On a 302-wide small or pull chart, five of these bars are different**, and reporting the 540-wide figures there produces false failures — the text floor is **11px** (the template's own subtitle, source and year labels are 11px by design), the margins are **12 … 290**, the chart's width need not match the header box, and the gap rule doesn't apply as written. The table is in SMALL-CHARTS.md → Checks. Everything else below holds unchanged.

| Check | How | Bar |
|---|---|---|
| Color-vision safety | `color_audit.py` | no pair under **ΔE 20** for deuteranopia or protanopia; tritanopia noted, never acted on alone. **Categorical fills only** — a sequential map ramp is exempt, see below |
| Spelling and prose | `.venv/bin/codespell` over the texts, plus a read against the style guide | American spelling (CLAUDE.md), no typos, no style-guide breaches — see below |
| The text is *true* of the indicator | `/adversarial-data-review` on the dataset behind the chart | **every** string that says something about the data survives checking against the producer's documentation — title, subtitle, note, year, annotations, direct and value labels, legend and category labels, units, entity names, source line. Labels you shortened are in scope |
| Entities all render | the **effective** selection (Step 1's table, not the saved `selectedEntityNames`) vs the labels in the SVG | every selected entity appears — a member missing its latest year is dropped silently (`/check-empty-entities` is the pipeline sweep). **On an unselected chart that plots everything, the producer's own API gives a baseline the SVG cannot fake**: count the entities it reports for the displayed year and compare against the marks drawn. WHO GHO's OData endpoint returned 167 countries reporting both sexes for 2022, against 167 `__datapoint` groups in the export — a real pass, where an SVG-vs-SVG comparison could only ever agree with itself. Also count the *raw snapshot*, not the garden table, if you want the number the chart should show: garden adds OWID's regional aggregates (174 there against the source's 167) |
| Year or period stated, and not stale | the period the export actually shows — the link's `time=` where there is one, otherwise the rendered SVG — plus the source chart's `maxTime` | a **single-time** image says which year it shows, in the title or subtitle; a **time series** states its period on its own time axis and takes no caption (adding one makes a series read as a snapshot — see "A pinned year, and a frozen image" below). Either way the source chart isn't pinned to an old year (`/check-hardcoded-years`) |
| Grayscale survival | `color_audit.py` (grayscale seam section) | **adjacent** pairs above ~**1.6:1**; below that they merge in print. **Stacked or segmented fills only** — for a plain or grouped bar chart, a line chart or a map pass `--separated` (`--line`/`--maps` imply it) and read the closest pairs as information, since legend order says nothing about which marks meet |
| Off-palette fills | compare every fill against the library groups | every fill is a library color, **bound as a style** — grapher emits `#585c64` for residual categories, which is in no group. Two standing exceptions, listed rather than flagged: the muting grays of a highlight treatment, and a grapher-managed sequential map ramp (see below) |
| Legend agreement | pair swatch→label by geometry, compare against the bars | zero mismatches |
| Direct labels name what they sit on | for each category label, compare its **fill** against the fill of the segment it names, and its **x** against that segment's edges in the reference row | the color is identical (same bound style, not merely a close hex) and the label is anchored on its own segment. A direct label carries the swatch's job with none of the swatch's proximity, so a mispaired one is unfalsifiable by eye |
| Direct labels readable as text | `contrast(labelHex, "#ffffff")` for every category label drawn on the background | **4.5:1**. The same color must also clear 4.5:1 against the white value label inside its bar — a palette that only clears one of the two has to move (Step 8) |
| Text size | read `fontSize` off every text node | nothing below **12px**; annotations on the named ladder |
| Mark weight | read `strokeWeight` off **every** line and halo, after the last scale | on a highlight treatment: context **1px** (the settled value — GUIDELINES.md → Highlighting; 1.5px is the reference-page treatment this skill tells you not to copy), protagonist **3px**, halo 2× (or line+1 where nothing crosses). Read it even when you never set it — and especially *because* you never set it: `rescale()` multiplies stroke weight, so fitting a chart to the band took grapher's 2.5px lines down to **0.88px** hairlines on a frame that otherwise measured perfect. Set the weights explicitly *after* the final scale, never before |
| Furniture weight | read `strokeWeight` **and `dashPattern`** off the gridlines, the zero line and the tick marks | all **1px** — but the dash target is **per node type**, not blanket: the dashed gridlines are `[4, 4]`, while the **zero line and the tick marks are solid** and must keep an **empty `dashPattern`**. Applying one `[4, 4]` target to all three restyles the furniture instead of unscaling it. The safe repair is conditional — reset the weight everywhere, and only re-dash a node that already had a dash pattern, scaling its existing values back rather than assigning new ones. `rescale()` thins these too, and they are the easiest properties in the frame to miss because you never touch them and "don't restyle the grid" reads as "don't look at it": a 0.7× height fit left every gridline at **0.7px with a [2.81, 2.81] dash**, i.e. a visibly fainter, finer grid than any OWID chart ships. Restore them in the same pass as the series weights |
| Label-on-fill contrast | `contrast(labelHex, barHex)` for every in-bar label | **4.5:1** at 13.5px regular — the 3:1 large-text allowance does not apply |
| Text hierarchy | list every distinct `fontSize` with what it belongs to, **and its rank** | title > subtitle ≥ annotations > supporting text ≥ labels. Sizes may vary inside the plot by rank; a lead annotation may *equal* the subtitle (Annotation XL 16) but nothing may exceed it, and same-rank items must share a size |
| Sizes are named styles | every size matches a style in the file | no arbitrary sizes left over from scaling the export (13.7, 16.8). Choose from the ladder by rank rather than by element type — see GUIDELINES.md → Subtitles and notes |
| Annotations cover only furniture | for each `annotation__*` node, test its rect against every line's **sampled polyline** (not bboxes — see below), and against the dots and value labels | gridlines, empty space or a muted context line — never a highlighted line, a dot, a value label or a bar segment carrying a number |
| Knockout tier matches what it crosses | the same test decides the tier: compare each annotation's crossings against whether it carries a stroke | an annotation crossing furniture has a **3px** `OUTSIDE` stroke in the template's canvas color; one crossing nothing has **no** stroke and no frame. A sub-pixel weight (0.65) means the stroke was assigned without setting the weight after a `rescale()` — see Step 8 |
| Label alignment | compare each label's center against its mark | bar values centered on bars, legend labels on swatches |
| Box alignment | compare the chart's left/right against the header frame | identical to the subtitle box **exactly** — `verify_page.js` gates at **0.05** (`BOX_EPS`), not the ±1px it used to allow. Every other full-width element sits on those two edges, so a 0.57px shortfall is invisible in the render and plainly wrong in the properties panel. It is a `FAIL`, not a rounding note: re-pin per [FITTING.md](FITTING.md) — rescale to the content width, restore the type ladder **and** the furniture stroke weights that `rescale` multiplied, translate, then re-centre the block and re-check anything parented to the FRAME rather than the chart |
| Gap | `(footerTop - headerBottom - chart.height) / 2`, with `footerTop = footer.y + Math.min(0, source.y)` — a source row raised inside the footer lifts the band's bottom (Step 7) | equal top and bottom, at the band figure of **the template you filled**: **12–16px** on the 540-wide frames, **30px** on the IG portrait (see Step 7). **Exception — a tightly measured group:** on an axis-less chart whose furniture was trimmed and label boxes hugged (Step 8), the band no longer applies as written; the figure to match is the one the **reference page** measures the same way, typically **20–30px**. Measure it there, record yours with a note that the group is tightly measured, and do not shrink a correct chart to force the band |
| Annotation block gap | the **block's** outer edges (topmost annotation, bottommost annotation, plot — whichever is extreme) vs the header and footer frames | the same clearance the plot owes: **27px** each side on the 540×540 pages. An annotation outside the plot is part of the block, so spacing the plot alone is not enough ([ANNOTATIONS-AND-ARROWS.md](ANNOTATIONS-AND-ARROWS.md)) |
| Every pointer lands on its target | for each leader, the **terminal vertex** (transformed, not the bbox) vs the thing it names — the country's own **ink** on a map, the band border at the stated year on a chart | the dot or tip is inside/on its target, and where the text names a year, at that year's x — with the first and last year taken from the plot's edge, not the tick label's centre. **A country's bounding box is not the target.** Countries are concave and multi-part, so a point can sit well inside the box and still be in open ocean — the US box reaches past Hawaii, an antimeridian straddler's spans the whole Pacific (see the map fit in `reference/per-chart-type/maps.md`). **Do it in VECTORS first — it is exact, and it is one call.** Transform the terminal into the country's local space through the inverse of its `absoluteTransform`, parse its `vectorPaths` into rings, and ray-cast. No renders, no masks, and it caught a leader whose terminal sat in the Bay of Bengal while its bbox test passed. Fall back to the **pixel** mask — hide the country vector, diff the renders, require the dot within ~1px of that pixel set — only where the vector test cannot answer: a country a few pixels across whose ring is smaller than the dot, or a shape whose fill rule makes the ray-cast ambiguous |
| Nothing in the margins | every visible mark's `absoluteBoundingBox` vs the content band | no ink outside **16…524** on a 540-wide frame. A speck left in the margin after a map fit renders as a cut sliver at the frame edge |
| How much is on the page | count the plot-bearing objects anywhere on the page — `countries-with-data` groups on a map, the equivalent plot group otherwise — and name what each one is for | one per **intended** item: the deliverable, plus the reference copies you meant to place. A third is clutter. **Do not check this by testing top-level children for overlap** — that answers a different question and answers it "clean": on one page three world maps sat at three distinct positions, so an overlap test passed twice while the reader was looking at a pile of near-identical maps in the left-hand column, one of which displayed the export's own legend/map collision. The reader's question is *how many of this thing am I looking at*, and only a count answers it. Watch the truncation trap too: a per-item node census keyed on a **shortened** name silently merged `<slug>` with `<slug> — original SVG (unstyled)` into one bucket, which is how a 467-node count for a 233-node frame read as normal |

**For arrows, drop vectors entirely and probe the rendered pixels.** Arrow groups are rotated, so every vector-space measurement of theirs is wrong (see Gotchas), and "very close but never on top" is a pixel property anyway. Screenshot the frame at 1:1, take the arrow's **`absoluteBoundingBox`** in frame coordinates, and inside it measure how close the arrow's pixels come to the target line's.

**Identify each shape's pixels by node identity, never by color.** A pixel belongs to the shape whose hiding changed it, which is true whatever either shape is colored. Screenshot the frame at 1:1 **four times** — whole, with the arrow's `visible = false`, with the target line's, and with **both** hidden — and diff each shape against the both-hidden render, from the pass where the *other* shape was already gone:

**`scripts/measure_pixels.py` does the arithmetic — don't route the renders through your own eyes.**
Every number below is computable from the PNGs, so looking at them costs a turn and an image each
and is less accurate than the arithmetic. Get the three pair-specific renders onto disk — one
screenshot per visibility state, and if you fetch URLs, in parallel with one output file each, or
every `curl` overwrites the same PNG (GOTCHAS.md). Then:

```bash
.venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py arrow-gap \
  --no-arrow no_arrow.png --no-target no_target.png --no-both no_both.png \
  --crop <arrow bbox, padded> [--full full.png] \
  [--arrow-bbox <absoluteBoundingBox>] [--target-bbox <absoluteBoundingBox>]
```

It masks each shape from the pass where the *other* was already hidden, reports `min_gap`,
`touching_pairs` and `arrow_px`/`target_px`. Pass `--full` and it also reports what the discredited
from-full masking would have said, and warns when that would have missed a real contact.

**The desktop read path can serve these renders — but not in one call, and the difference bites.**
`figma_desktop_read.py shot` toggles nothing: it screenshots the nodes you name in whatever state
they are already in. So naming one frame three times returns the *same* render three times, and all
three collide on one `<node>.png`, because the output name is derived from the node id.
`measure_pixels.py` then reads identical PNGs, finds an empty mask and answers `UNMEASURABLE` with
`arrow_px: 0, target_px: 0` — the guard holds, but the renders bought nothing. Each state is a
hosted `use_figma` write, so the real sequence is **one write then one screenshot per state**, three
times. Property writes to a page already open in the app do replicate, and this protocol is the
measured case for that (GOTCHAS.md → the desktop MCP server, per-step verdict) — what does not
exist is a single call that captures three states.

**The exit code is the part to read, and it is the same for all three probes:**

| | |
|---|---|
| **0** | the check ran and passed |
| **1** | the check ran and failed — *the chart is wrong*, and nothing else means this |
| **2** | the check did **not** run, so there is no verdict |

Everything wrong with the *input* is 2: an unreadable PNG, a malformed `--background`, a crop
outside the render or one whose coordinates are not finite numbers, a numeric flag outside its
meaningful range, an empty mask, renders of different sizes, a mask straying outside a declared
`--*-bbox`. **Never read a 2 as a pass and never read it as a defect** — it means the probe could
not measure, and the remedy is to fix the input and re-run. A bare number cannot tell "the arrow is
clear" from "I measured nothing", which is the whole reason these return a code at all.
`figma_desktop_read.py` uses the same three codes, with 2 reserved there for the daily read quota.

**What it does not buy is arithmetic speed — measured, on the same three renders.** The all-pairs
loop it replaces is `O(arrow × target)` against a linear distance transform, and that only pays at
scale: at a 1× probe the loop wins (**0.01 s** against **0.32 s**, since the script pays numpy and
scipy import), the two draw around 2×, and only at the **4×** render the hairline check requires
does the loop fall behind (**1.61 s** against **0.31 s**, 8.9M pairs). Both agree on the answer at
every scale. So reach for the script for what it guards, not for what it computes: the loop's
numbers were never wrong, its *failure modes* were. `touching` is also clarified rather than
changed — it counted *pairs* within 1.5px, exactly the 3×3 neighbourhood, and the script reports
that alongside the count of distinct arrow pixels in contact, so a non-zero result is interpretable.

**Probe a render at its natural size. A resampled one cannot be masked reliably — and both render
paths resample.** A pixel mask is a per-pixel difference, so it only means "this shape's ink" while
each pixel still corresponds to one rendered pixel. Downscaling breaks that, and how badly depends
on the filter: an area-average resample merely softens each edge, but a **ringing** filter overshoots
two or three pixels past it, which inflates both masks until two shapes five pixels apart have
*overlapping* masks and every gap reads as `0.0` with hundreds of contacts. That is a property of the
resampling, not of the chart, and it looks exactly like a real defect. So: `get_screenshot`'s
`maxDimension` downscales, and the desktop server caps the longer edge at **1024px** (Gotchas) — a
540-wide frame is safe on either, but the **616×1096 Reel arrives at 576×1024 from the desktop
server**, already resampled. For a pixel probe on a frame taller or wider than 1024, use the hosted
`get_screenshot` at natural size, or the 4× clone trick. Same reason the arrow renders are specified
at 1:1 above.

Verified under fake AA (supersample, area-average down): masks hold their size at every separation
and `min_gap` tracks the true gap exactly. Note `min_gap` is the distance between pixel *centres*,
so it reads one more than the number of blank pixels between the two edges — two blank columns
measure `3.0`. That is the same convention as the `hypot` above, so the 3–7px band applies to it
unchanged.

**And verified against this file's own recorded number, on a real chart.** Run on a clone of the
population-growth page's peak arrow — a real curvy arrow at **rotation −162.4°**, real
anti-aliased renders through the desktop server — the script returns **`min_gap` 3.0 with zero
contacts**, which is exactly the figure recorded above for that arrow after its fix. A second arrow
on the same chart at **+170.7°** returns 5.385px, zero contacts, and `arrow_px_outside_bbox: 0`
against its declared `absoluteBoundingBox`. Both sit inside the 3–7px band. That pair is also the
empirical proof of the rotated-bbox warning: the first arrow's raw `x` is **167.1** where its
absolute box starts at **100.0**, and the second reports raw `x: 536.7, width: 29.8` against an
absolute `495.1, 41.7` — crop from the raw numbers and you probe empty canvas.

The same script covers the other two pixel checks: `contrast` for a hairline (the sub-pixel stroke
trap in Gotchas — measure it on the 4× clone, not the 540px preview) and `ink-box` for "nothing in
the margins", which reads the true extent of everything that paints. Run against a stock 540 frame,
`ink-box` returns `[16, 16, 524, 524]` — the content band this file specifies — with the background
inferred rather than declared.

**Use them as a pair, and here is the pair settling a real case.** Run on a *shipped* page — the
population-growth DI — `ink-box` came back `[16, 16, 527, 524]`: ink reaching **527** against a band
that ends at **524**. On its own that is ambiguous, because antialiasing along a mark's edge also
registers as ink. So ask `contrast` what the ink in that strip actually is:

```bash
.venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py ink-box \
    --png frame.png --region 524,0,540,540 --background '#ffffff'
.venv/bin/python .claude/skills/create-figma-chart/scripts/measure_pixels.py contrast \
    --png frame.png --region 524,0,540,540 --background '#ffffff'
```

34 ink pixels, 3px wide by 76px tall, **peak contrast 6.79:1** — that is the annotation gray at full
strength, not a fringe (the fringe is there too, and shows as the 1.78 median). So it is real ink: the
tail of a curvy arrow whose `absoluteBoundingBox` reaches **536.8**, overrunning the right content
edge. `verify_page.js`'s `margins` row flags the same thing more harshly, since it measures the box
rather than the ink.

The generalisable part is the division of labour: **`ink-box` finds the overrun, `contrast` rules out
the artefact.** A margin verdict from either alone is a guess — one cannot tell ink from fringe, the
other does not know where the band is.

**Don't diff either mask against the whole render — that hides the overlap you are testing for.** Whichever node paints on top covers part of the other, and hiding the *covered* one changes nothing in those pixels, so a mask taken from `full` comes back with a hole exactly where the two shapes meet. An arrowhead sitting on the end of its line then measures its `minGap` to the nearest still-*exposed* line pixel and reports a comfortable 3–7px with `touching == 0` while the two are plainly overlapping — the one verdict this check exists to prevent. Diffing from the other-hidden pass costs one extra screenshot and is symmetric, so it holds whichever node is on top.

Restore `visible = True` on both afterwards, and **guard the masks**: each difference must fall inside that shape's own `absoluteBoundingBox`. If it doesn't, hiding the node reflowed something else (a group's derived box, an auto-layout sibling) and the mask is measuring the reflow, not the shape.

**Classifying pixels by color instead is the version to avoid — it produced two different false verdicts before it was replaced.** That first cut called the arrow "gray" (`abs(r−g) < 14 and 60 < r < 165`) and the line by its own hue, and both halves break on ordinary charts. A **gray target** — an arrow aimed at a muted context line — satisfies the arrow predicate, so every target pixel is filed as arrow ink and the check dies with an empty target set on a chart where nothing is wrong. And **gray furniture** in the padded crop — a gridline, a second context series, gray annotation text — is collected as arrow ink too, so the target line merely *crossing a gridline* reports `touching > 0` while the arrow itself is comfortably clear. Neither is fixable by narrowing the crop, since a crop cannot separate two shapes that answer the same predicate. Three extra screenshots cost less than one wrong verdict, and hardcoding the hue is worse again: the palette runs to 24 fills, so a fixed `TARGET` collects nothing on most charts and `min()` then dies on an empty sequence instead of reporting a clearance.

**Pass is `touching == 0` with `minGap` about 3–7px.** This is the only check that caught the real defects: it found the peak arrow overlapping the line by 11 pixel pairs where the vector math had reported a comfortable clearance, and it confirmed the fix at 3.0px with zero contacts. Report both numbers per arrow.

**On a line chart the bbox overlap test is not conservative, it is useless — sample the polyline.** A diagonal line's bounding box is most of the plot, so a bbox test reports every annotation as covering every line: on this run it returned 5 collisions across 4 frames, all but one of them phantom, and it *buried the one real defect in the noise* (a portrait annotation genuinely clipping the projection line). Extract the path's points — the numbers in `vectorPaths[0].data` alternate x,y, so map each pair through the node's own transform — then walk the segments and sample each at ~1px:

```js
const pts = (v, frame) => {                          // path space -> FRAME space
  const n = ((v.vectorPaths||[]).map(p=>p.data).join(" ").match(/-?\d+\.?\d*/g)||[]).map(Number);
  const [[a,b,tx],[c,d,ty]] = v.absoluteTransform;   // rotation + scale + translation, in one matrix
  const fb = frame.absoluteBoundingBox;
  const out = [];
  for (let i = 0; i + 1 < n.length; i += 2)          // path coords are local to the node
    out.push({ x: a*n[i] + b*n[i+1] + tx - fb.x,
               y: c*n[i] + d*n[i+1] + ty - fb.y });
  return out;
};
```

**Drive it off `absoluteTransform`, not `v.x`/`v.y`, even though the naive version happens to work on a fresh import.** Group ancestors are transparent for coordinates, so a line nested under `lines` → `chart-area` does report frame coordinates and the short form measures correctly — that is why it produced sound numbers here. But the assumption is invisible and it fails three ways: under a nested **FRAME** ancestor, under an ancestor that was **scaled** rather than rescaled, and on any node with non-zero **rotation** (which is exactly how the arrow measurements in this skill came out as fiction). The transform costs one property read and cannot be wrong, so prefer it and keep the audit trustworthy when someone later reparents the chart.

**And take the transform, not the bounding box, or rotation silently defeats the fix.** The tempting short version — normalize the local x,y into `absoluteBoundingBox` by their own min/max — reads like it handles rotation, because for a rotated node the bbox *is* the visual one. It does the opposite: normalizing two axes independently into an axis-aligned box cannot rotate anything, so you get an **unrotated polyline stretched across the visual box**, a shape the reader never sees, and the audit then certifies the wrong geometry with more confidence than before. `absoluteTransform` carries the rotation in the matrix, so applying it to each point is both shorter and the only version that is actually rotation-safe. (The regex takes every number in the path data, which is right for the M/L polylines grapher exports; a path with curve commands would need its control points dropped first.)

That took the same four frames to **one** finding, which was real. And the same routine fixes it without guesswork: take the topmost line point under the annotation's x-range and set `box.y = thatY − 12 − box.height` — the ~12px the knockout rule asks for ([ANNOTATIONS-AND-ARROWS.md](ANNOTATIONS-AND-ARROWS.md)), not the 5px that merely clears the test. **A clear audit is necessary here, not sufficient:** the polyline check only asks whether the box *touches* the line, so it reports 5px as clean, and 5px is the gap a reviewer called visibly too close. If 12px pushes the block somewhere awkward, narrow the block — re-wrap the same sentence into more, shorter lines — rather than moving it further away. Then re-run the test and confirm it still reports clear. (This is the line-chart counterpart of the subpath-bbox rule for maps: boxes decide where things may go, geometry decides how it reads.)

**A sequential map ramp is not a categorical palette, and two of the rows above don't apply to it as written.** GUIDELINES.md → Colors keeps map colors in grapher on a Viridis or ColorBrewer sequential scale and off the OWID categorical palette, because ordered bins separate better once a map shows many classes. That has two consequences here, and both look like defects if you don't know them. **The ΔE 20 bar is an all-pairs *categorical* test, so a ramp fails it by construction** — neighboring stops are supposed to be close, that is what makes the ramp read as ordered — and `color_audit.py` has no sequential mode: `--maps` swaps the search over to the **Categorical Maps** group, so `--maps --suggest` on a ramp cheerfully proposes an unordered set and destroys the encoding. Don't run it there. **And the off-palette sweep can't pass either**, because grapher's ramp belongs to no library group and arrives as raw fills — demanding a bound style would mean repainting the map in Figma, which the guidelines forbid. So for a sequential map, check the scale where it is actually set, in grapher: that the bins are ordered and distinguishable, and that the legend labels and any values written onto the shapes clear their own contrast bar. Then record the ramp as grapher-managed in one line instead of listing every stop as an off-palette fill. `--maps` and the ΔE gate are for a **categorical** choropleth — one color per region or class, no order between them — which is the case those rows were written for.

**Filter the fill sweep to what actually paints, or it invents failures.** Two kinds of phantom show up and both look exactly like a real off-palette color in a listing. **Hidden ancestors:** `visible` is per-node, so the children of a group you hid are still individually `visible: true` — walk up to the frame and skip anything with a hidden ancestor, or a hidden `connectors` group reports a dozen stray colors. **Zero-area vectors:** grapher's exported tick marks are zero-width stroked paths that carry a default black `fill` which can never paint, so an unfiltered sweep reports twelve `#000000` fills on a chart that has none. With both filters the same chart went from 4 apparent off-palette colors to the 2 real ones.

**But apply that second filter to `fills` only — a stroke sweep needs the opposite rule.** A gridline is a zero-*height* node and a tick mark a zero-*width* one, and their strokes are the most visible furniture in the chart. Requiring nonzero area on both properties silently drops every axis line: one sweep came back with three stroke colors on a chart that has five, reporting no gridline stroke at all and — worse — reading as a clean result. So: fills need `width > 0 && height > 0`, strokes need `width > 0 || height > 0`. A stroke inventory that lists no `#dddddd` on a chart with visible gridlines is the tell.

```js
const paints = n => { let m = n; while (m && m !== clone) { if (!m.visible) return false; m = m.parent } return true }
// ...and ignore `fills` on nodes whose width or height rounds to 0
```

**Make label-centering part of the build, not a follow-up.** It regressed three times in one run — each rebuild re-hugs the text, which restores the drift, and a separate "now center the labels" step is forgotten or applied to a chart instance that is later replaced. Put the centering loop at the end of the same function that imports, scales and re-hugs, so it cannot be skipped.

**Re-run this whole pass after the last change, not after each one.** Fixes get lost silently: a label-centering pass applied to a chart instance that is later swapped for a re-export leaves the drift back exactly as it was, and every screenshot in between looks correct. And a structural change spends budget elsewhere — lifting an aggregate row to the top added 8px of height, which came straight out of the 12–16px gap and took it to 8.2 without anything reporting a problem. Treat "I already checked that" as false after any re-export, reorder, rescale or restyle.

**Someone else editing your frame is a change like any other — re-run the pass, and diff the texts.**
The Charts file is shared, so an author or a designer can rewrite your title, restyle your
annotations and move nodes while you are still working, and none of it announces itself. Two
different losses come out of that, and only one is mechanical:

- **The gate catches a hand-edit only where a check already exists.** A rewrite that stripped both
  annotations' halo strokes went unnoticed until a row for them was written. So when you find that a
  hand-edit undid something, add the check *before* you re-apply the fix — otherwise the next
  hand-edit undoes it again just as quietly.
- **The gate cannot catch what it does not know was deliberate.** A subtitle rewrite that was a
  genuine improvement on the wording also removed a reading aid the author had asked for, and nothing
  on the frame distinguishes a sentence that was dropped from one that was never there. Keep the
  approved strings and diff them: the frame does not remember what it used to say.

Keep the improvements — the point is not to revert a colleague's edits, but to notice which of them
were trades nobody actually chose, and put those back to the author.

### Checking the words, not just the geometry

The chart's text is not yours — you transcribed it from the indicator's metadata — so a defect in it is a defect **upstream**, and fixing it only in the image leaves the interactive chart, the data page and every other surface still wrong. Check it here because this is where someone finally reads it slowly; fix it where it lives.

- **Spelling and prose.** You transcribe these strings verbatim, so you are not the one introducing a typo — you are the last reader before it is frozen into an image, which is a worse place for it than a chart that can be corrected in place. Run `.venv/bin/codespell` over the strings (it is a dev dependency; `/check-metadata-typos` covers the same ground on `.meta.yml` and `.dvc`). American spelling always, per CLAUDE.md, including in text copied out of a chart. For the wording itself, `/check-metadata-style` holds the Writing and Style Guide, whose FAUST rules govern exactly the strings this skill moves.
- **Whether the text is true.** Run **`/adversarial-data-review`** on the dataset behind the chart, over the data *and* every string in the frame that says something about it. That skill fetches the producer's own documentation from the snapshot's links and treats each sentence as a claim to be refuted, which is the right posture for text about to be published as an image. Its scope here is **everything, not just the FAUST**:

  | Text | The claim it makes |
  |---|---|
  | Title | the headline assertion — that the data shows this |
  | Subtitle | what is measured, in what units, over what population |
  | Note | the caveats, and that these are the ones the producer actually states |
  | The year or period, wherever it is stated — in the title, as `Data for <YYYY>.`, or on a time axis — and any year caveat | that it is what the export actually shows, for every entity |
  | Annotations | each number, comparison and superlative — transcribed *or* derived |
  | Direct labels and value labels | that this number belongs to this mark |
  | **Legend and category labels** | that the category contains what the label says it does |
  | Axis labels and units | the scale, and whether it is a share, a rate or a count |
  | Entity names | that the entity is the one the producer means (aggregates especially) |
  | `Data source:` line | the producer, and the year of *their* release |

  **Shortening a label is a factual edit, so put it through this check too — not just the strings you inherited.** Words in a category label are the definition of the category: "Other meats" → "Other" loses nothing on a chart entirely about meat, but "Beef and buffalo" → "Beef" drops a species the category counts, and where buffalo is most of it (India, Pakistan) the shorter label understates what the bar contains. Check it against the producer's own indicator title — FAO's is "beef and buffalo meat" — and note that the interactive chart will still carry the long form, so the image and the chart will disagree.

  That does not make the short form forbidden. A team may prefer the plain word and accept the imprecision; on this chart the owner did. What it makes it is **a decision, taken knowingly and recorded** (see the accepted-deviations rule below) rather than a side effect of needing 20px. Say what the short label costs, say what keeping the long one costs — here, one row at 12px or two rows at 15px — and let the owner choose.
- **Rendered spacing.** Metadata is often Jinja-templated, and a template defect shows up only in the rendered string — a double space, or a missing one where a conditional collapsed. You are pasting the rendered form, so you inherit it silently. `/check-metadata-spacing` is the pipeline check for this; here it is enough to read the placed strings once for spacing, and to distrust any sentence whose shape suggests a template (`in {country}`, a units clause that reads oddly).
- **Entities that render empty — the check this skill learned the hard way.** A pinned selection can silently lose a member: grapher drops an entity whose data doesn't reach the displayed year, with no warning anywhere. This run shipped ten of eleven countries for exactly that reason, and only the accompanying text naming the missing country exposed it. `/check-empty-entities` is the pipeline sweep for this class; the local version is Step 1's rule — compare the **effective** selection against the entity labels in the exported SVG, every time. Effective, not saved: a link carrying `country=` overrides `selectedEntityNames` entirely, and diffing against the config there reports every saved default as missing on a chart where nothing is wrong.
- **A pinned year, and a frozen image.** `/check-hardcoded-years` exists because a chart pinned to `maxTime: 2019` quietly stops showing new data. The static image has the sharper version of the problem: it is pinned to whatever year it was exported at, permanently, and nothing will ever refresh it. So check two things — that the *source chart* isn't pinned to a stale year (you would be freezing someone else's oversight), and, **for a single-time export**, that the year is stated **somewhere the reader will see it**: in the title when the claim is year-specific, otherwise in the subtitle as `Data for <YYYY>.` (GUIDELINES.md → Titles). Check for it in both places before calling it missing, and check it appears in only one of them. The year to state is the one the export shows — a `time=` in the link overrides `maxTime`, so read it off the link or the rendered SVG rather than the saved config. An undated single-time image is the one defect that gets worse with time.

  **A time series needs no such caption — its own axis is the date line.** There is no single year a 1990–2025 chart "shows", and appending `Data for 2025.` to one makes a whole series read as a snapshot of its last year. What a time series needs from this check is the *other* half: that the axis actually runs to the latest year the data has, which is the stale-`maxTime` question above. The caption rule is scoped to single-year charts everywhere else it appears (Step 4's subtitle rule, GUIDELINES.md → Subtitles and notes); keep it scoped here too.
- **Where a finding goes.** A wrong or misspelled string belongs upstream in the chart's own text, not in the Figma frame — same rule as sort order and colors. Route the fix through `/edit-faust-metadata`, always, and don't pick the layer yourself: that skill decides which layer the field actually lives in (garden `.meta.yml`, an MDim's yaml, or the chart config on staging) and reports which *other* charts inherit the same string before anything changes. Editing the garden file directly because it looked like the obvious home is how a one-chart correction silently rewrites text on a dozen others. Report the finding, hand it over, and hold the image until it's fixed if the claim is load-bearing; a static image outlives the chart text it was copied from, so shipping a known-wrong sentence is worse here than on the live chart, where it can be corrected in place.
- **Annotations you wrote are your own claims.** Anything you drafted rather than transcribed — a derived percentage, a "more than half" — carries no upstream provenance, so verify it against the data yourself and say in the report which annotations are transcribed and which are derived.

**A failing check is a finding to report, not a veto.** Measure it, say plainly what fails and by how much, offer the alternatives with their own numbers — then do what the author decides. If they accept the deviation, record it in the report; chart-side work goes in the handover doc and reusable mechanics go in this skill. **Add a note to the Figma page only if the user asks for one** — don't volunteer it (GUIDELINES.md → Colors).

**Check the properties you didn't change, not just the ones you did.** A verification pass naturally retraces the edits — it measures the colors because you set colors, the positions because you moved things — and that is exactly how an inherited value survives it. The context lines on this chart stayed at the export's 2px through a full pass that confirmed their color, because nothing in the pass ever asked what weight they were. Derive the check from **what the finished frame is supposed to look like**, property by property, rather than from your own edit history; anything the treatment specifies gets read back, whether or not you believe you touched it.

Two habits make the difference. **Assert, don't eyeball** — a 1.2px label drift, a 1.18:1 grayscale pair and a scrambled legend all looked perfectly fine in a screenshot. And **re-run the affected checks after every change**, because they interact: applying a text style resets range colors, rescaling rewraps text and shifts label centers, adding an annotation changes the group's width, and swapping one color moves the safety floor to a different pair.

**Two pixel-probe mechanics that decide whether any of the render-sampling checks above are telling
you the truth.** Both were found on map leaders and both apply to every probe on this page —
including the arrow probe.

- **A canvas coordinate of `N.0` is the seam between pixels `N-1` and `N`, so sample
  `floor(coord − 0.5)`.** Reading `int(coord)` checks the neighbouring pixel, which reports a
  correctly placed mark as misplaced. That cost two rounds of "fixing" placement that was already
  right — and it is the same off-by-one whichever probe you are running.
- **Match a fill strictly — summed `|ΔRGB| ≤ ~9`, not `~40`.** A loose tolerance admits the
  antialiased edge, where each pixel is a blend of the mark and the background, so the "interior"
  you are testing against silently includes the boundary. A `#e8f1f9` sample is within 18 of
  `#deebf7` and is half background; that tolerance is what let a dot in open water pass a pixel
  check.

## Two rows that a real run re-calibrated

Both were changed after the first run of `verify_page.js` against eight real frames rather than the
mock, and both changes make a row *less* red on purpose. Read them before treating either as slack.

- **`furniture-dash` no longer flags grapher's zero line.** grapher names each gridline after its tick
  value, so the zero line arrives as `0`, `0%` or `0-years` and matches none of the zero/tick/axis words
  the row looks for — it was reported as a "cleared" dash on **5 of 8** frames, and on a slope chart,
  whose two end verticals are named `1980` and `2023` and sit in a group of two, on 2 of 2. Those are
  now reclassified by **identity** (a name denoting zero; a small furniture group of *vertical* lines),
  never by the dash the node carries — that circularity is the defect the row exists to catch.
  And they are **reclassified, not exempted**: they go through the solid-by-design validation, so a zero
  line genuinely restyled to `[4,4]` still fails. The row reports what it reclassified, so you can see
  it happen.

  **The shape matters as much as the count, and so does whose exception `[3,2]` is.** A first pass
  reclassified *any* group of fewer than three members, and that cut both ways on a legitimate two-line
  horizontal grid: correctly dashed it failed as a dashed "axis" node, and with its dash cleared it
  **passed** — the very defect the row exists to catch, hidden by the exemption. A slope's end axes are
  vertical and a y-grid's lines are not, so only a small group of verticals is moved. Likewise the
  `[3,2]` allowance belongs to a slope chart's native **zero line**, not to the whole solid-by-design
  bucket: granted in bulk it also accepted an ordinary tick or axis line dashed `[3,2]`, which this
  document permits nowhere.

## A skip with a false reason is the failure mode, not a wrong number

**First, the common case is not the one this section assumed.** `chartName` defaults to `chart`, and
counting the live file found **three** names in use, not one: `chart` (a grapher import),
`chart__agriculture-share` (a `static_viz` import) and `chart-desktop` / `chart-mobile` (a two-format
page). An exact-only match resolved nothing on two of the three, fell through to the ungrouped branch,
walked the right node anyway, and reported *"the chart group looks ungrouped"* about a frame whose
chart group is present and correctly named. The answer was right and the explanation was wrong, which
is the worse of the two: it sends the next reader to fix a non-problem. The resolver now takes an
exact match first, then a `__` or `-` suffix, and names what it matched.

**And there are two `<kind>__<name>` conventions, which is what made the palette labels wrong.**

| Source | Names | Is the second token a category? |
|---|---|---|
| Grapher's SVG export | `line__<Entity>`, `outline__<Entity>`, `label__<Entity>` | **yes** — one per series, in selection order |
| A `static_viz` matplotlib step | `bars__<slug>`, `diagram__median`, `{slug}__{part}` | **no** — a dataset slug or a part name, repeated across marks |

`CATEGORY_ANY` matches `<word>__<rest>` and cannot tell them apart, so on a `static_viz` bar chart every
bar resolved to the same "category" and `--names` labelled three distinct colours
`agriculture-share,agriculture-share,agriculture-share`. That is why `--names` is now gated on the names
being **distinct across the palette** and not import defaults (`Vector`, `Rectangle 12`): the grapher
case passes that gate and keeps its labels, the `static_viz` case fails it and the flag is dropped with
the reason. Do not "fix" this by widening `CATEGORY_ANY` — the two conventions are genuinely ambiguous
at the name level, and distinctness is the property `--names` actually promises.

**The grapher half is confirmed against a live export**, not inferred. `child-mortality.svg?imType=uncaptioned`
carries 7 `line__<Entity>`, 7 `outline__<Entity>` and 7 `label__<Entity>` ids — Ghana, India, Brazil,
France, Sweden, United-Kingdom, United-States — with 7 distinct stroke colours, so the row emits
`--names 'Ghana,India,…' --line` and the audit runs. You can check this without Figma at all:

```bash
# the naming and the palette, straight from grapher
curl -s "https://ourworldindata.org/grapher/<slug>.svg?imType=uncaptioned" | grep -o 'id="line__[^"]*"'
```

Two things that run did **not** prove, so don't cite it for them. Its labels carry the same colours as
its lines, so a palette wrongly sourced from text fills would produce the *same* hexes and look
correct — only a chart whose labels differ from its marks tests that. And it is grapher's automatic
7-series assignment, which GUIDELINES replaces with the highlight treatment anyway; the audit failing
it (France/UK at **ΔE 0.0** under tritanopia, India/UK at **1.00:1** in grayscale) is the row working,
not a defect in a shipped page.

The rest of this section is the genuinely ungrouped case. It has bitten in three different rows. When
`CONFIG.chartName` finds nothing — a designer has **ungrouped** the chart — the plot has to be
resolved from the frame's children, and doing that from a list of container names was line-chart-shaped:
it knew the axis, grid and lines groups and missed a map's `map`, a bar's `bars`, a scatter's point
container and a slope's `slopes`. Those children were then walked as *not* in the plot, which does not
make a row fail — it **empties** one. `off-palette` then reported "no solid fills found in the plot" on
a map full of them, both annotation rows found no marks to test against, and `isMap` was never set, so
the map was judged by the band rule written for a chart whose aspect we control. The resolution is
structural now: everything that is not the header, the footer, the logo or one of our own annotations is
plot content, and the row records what it resolved. **When you extend either script, ask what a row says
when it finds nothing — not only what it says when it finds a defect.**
- **`ladder-sizes` reports imported text instead of failing it.** Judged strictly it failed on **8 of
  8**, i.e. on every fitted import that can exist, and a row that always fails carries no information.
  The cause is a real three-way conflict: text metrics help set the group's width, so snapping labels to
  rungs moves the box off the content edge, and re-fitting to the edge moves the sizes back off the
  ladder. So the verdict is split by *who set the size* — an **annotation** is authored here and still
  FAILS, while imported text is REVIEW with its distance to the nearest rung, FAILing only past 0.75px.
  Measured drift on the eight frames was 0.11–0.48px; the scatter's bubble legend was 6.01px from its
  rung, which is the case the threshold exists to catch.

**The end-to-end run that exercises all of this** — a fixed request, written down before its first
run so it cannot be tuned to its own result — is [BENCHMARK.md](BENCHMARK.md). Use it to check a
change did not break the flow; use a fixed sub-task, not it, to claim a speedup.
