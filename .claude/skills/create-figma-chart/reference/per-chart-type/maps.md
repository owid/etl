# Maps

> Chart-type conventions from the DI Charts Guidelines. Read **only** the file for the chart in
> hand — see [GUIDELINES.md](../../GUIDELINES.md) → Per chart type for the index.

- **On a two-bin categorical map, delete the legend and let the title's colored words be the key.** "Countries with fertility rates **above** or **below** replacement level" with those two words in the two bin colors is a complete legend, sitting where the reader already is; grapher's legend strip then costs ~36px of height and repeats the title. Keeping it was the single biggest thing wrong with a first attempt here — it squeezed the map to 214px and pushed the annotations into the subtitle. Removing it gave the map the full width and freed the band above and below for annotations over the ocean, which is how the finished pages are built. (Three or more bins usually still need the strip.)
  - **When the map's palette can't be read as text, say so rather than forcing it.** The colored title word must be the bin's actual color, but grapher's binary map palettes are often pale — `#92c5de` on white is about 1.9:1 — so the honest move is a darkened same-hue palette color in the title and a note that the map bins are lighter. Never invert the pairing: if the map's high bin is salmon, the title's word for the high bin cannot be teal, and any value called out in an annotation follows the same rule. Inheriting those colors from an older page whose palette was the other way round produces a frame that contradicts itself in three places at once.
    - **This is a stated exception, not a loophole, so the audit has to know about it.** "Use the bin's actual color" and "darken a pale bin" contradict each other on the face of it, and the resolution is that **legibility wins on text and the pairing wins on hue**: the title word takes the darkened same-hue value, the map keeps the bin's own. So a color check comparing title words against fills will find a mismatch here **by design** — expect the darkened variant on the text, expect the bin value on the map, and record the pair (bin → darkened) in the run's notes so a later reader doesn't "fix" it back to an unreadable 1.9:1. Anything that is *not* the same hue is still a defect.
- **Grapher's map export outlines every country in `#333333`; the house treatment is a white hairline.** Sweep the country vectors and set the stroke to white at **0.22px** — the finished pages' value, and thin enough that internal borders describe shapes without drawing themselves. Dark outlines make a choropleth read as a political map and fight the bins.
  - **Exclude the no-data shapes from that sweep.** Their fill is a hatch on white, so a white hairline has no ground to sit against and the country simply loses its silhouette — Greenland stops being a shape and becomes stripes. Grapher already distinguishes them (`#aaaaaa` against `#333333` for the rest), which is the tell that the two layers want different treatment: keep the export's grey at the same 0.22px, on the map **and on the legend's own no-data swatch**. Scope by parent (`countries-without-data`), not by colour.
- **Give each named country a 0.3–0.35px outline in a darker shade of its own bin colour, and raise it to the front of its layer.** Darken it in HSL, not with an RGB multiply (GUIDELINES.md → Colors has the rule and why) — on this file's pale bins that is the difference between a true darker blue `#478fd2` and a grey-green `#8a9299`. Then `parent.appendChild(country)` — without it the outline comes out **visibly incomplete**, because every neighbour paints its own white hairline over the shared border afterwards. The symptom looks like a broken path and is pure z-order.
  - **This rule assumes the named country sits among *differently* coloured neighbours. Where two same-bin countries TOUCH, it is wrong — they take the white hairline like everything else.** A darker own-bin outline on both sides of a shared border draws a ~0.65px near-black line *through* a single-colour mass, and on a highlight map that is most of the map: nine highlighted countries included China–Russia, China–North Korea and China–India–Pakistan, so `#521d22` at 0.32px cut the maroon body into pieces and fought the white hairlines everywhere else. Two stroke *colours* on one map always reads as a mistake. The archive settles it — on `222:1048`, scanlines across adjacent highlighted countries show separators **lighter** than the fill (`(223,223,223)`, `(201,184,186)`, `(199,181,183)`), never darker. So: one colour for the whole map, white.
  - **But not one `strokeAlign`: the highlighted countries take theirs OUTSIDE, the context keeps CENTER.** A centred stroke puts half its weight *inside* the path, so it eats the country's own fill — invisible on a big shape and most of a small one. Measured at 4× on the same frame, full-strength fill pixels before → after moving the highlighted countries' hairline outside: **Israel 31 → 56 (+81%)**, UK 1084 → 1206 (+11%), France 2730 → 2887 (+6%), the US 45434 → 46429 (+2%). The gain scales inversely with size, which is why the complaint arrives as "the small ones look smaller than they are" — Israel is 2.19px wide and a centred 0.22px hairline was taking a fifth of that, leaving only 28% of its ink at full strength.
  - **`OUTSIDE` also gives the right answer per boundary, for free, which is the real reason to prefer it over dropping the stroke.** Against **water** the white stroke lands on white and is invisible, so a coastline reads as a crisp edge — the US, the UK and Israel all sit mostly on coast, which is why they suffered most. Against a **muted** neighbour it draws on the neighbour's side, so the separation survives without costing the highlighted country anything. And where **two highlighted countries touch**, both draw outward, so that seam comes out slightly *heavier* than the rest of the map — emphasis exactly where a reader has to tell two same-coloured countries apart (China–Russia, China–North Korea, China–India–Pakistan). Dropping the stroke from the highlighted set instead would merge that whole mass into one blob.
  - So the sweep expects **two buckets, not one**: `{"#ffffff 0.22px CENTER": 186, "#ffffff 0.22px OUTSIDE": 9}` on a nine-country highlight map. And since an outside stroke extends a node's bounding box, **re-assert the fit afterwards** rather than assuming it survived — at 0.22px the group's box measured unchanged (`x=16, w=508`), but that is a measurement, not a guarantee.
  - **The measurement that decides it, in either direction:** sample a horizontal scanline across the mass and look at the pixels sitting between two runs of the highlight fill. Lighter than the fill = a white hairline; darker = the dark-outline defect. It is one crop and it is unambiguous, where "the borders look off" is not.
- Annotations 12–14px (the bottom of the ladder — see Annotations for why maps don't go to 10); values inside countries, or hairline leaders — never curvy arrows; thin lines pointing at small countries work best when the labels sit apart from each other.

**Map leaders: hairline elbows that end in a dot.** This is a distinct treatment from the arrows above — no arrowhead anywhere on a map. Read off the finished pages:

- **`#2d2e2d` at 0.3px**, `strokeJoin = "MITER"`. (**→ source says** straight lines *“1px thick”*. The finished pages use 0.3px and the reason follows in the next sentence, so 0.3px is what this file prescribes — with the 1px on record as the stated intent, in case a designer asks why the leaders read lighter than the guideline.) At that weight the line antialiases to a light gray and stays subordinate to the map; a 1px gray line reads as a border.
- **The country end carries a filled dot** — `strokeCap: "CIRCLE_FILLED"` on the terminal vertex, set through `setVectorNetworkAsync` (per-vertex, so the tail stays `NONE`). The dot is what makes a hairline land *on* a country rather than near it; it is also the whole reason to build these with a vector network instead of `vectorPaths`.
  - **But the cap SCALES WITH the stroke, so at 0.3px it renders sub-pixel and there is no dot at all.** Measured on a 540px render: the terminal pixels were indistinguishable from the hairline. A cap is the right mechanism only where the stroke is heavy enough to carry it; at the prescribed hairline weight, place a real **`ELLIPSE`, ~2.2px across, filled `#2d2e2d`**, centred on the terminal point instead — that measured `(45,46,45)` at its centre pixels, i.e. actually there. Keep the vector network anyway: it is still how you build a multi-segment elbow, and the per-vertex cap remains correct for any leader you draw at 1px or above.
  - **Expect the dot to disappear into a dark bin, and let it.** `#2d2e2d` on a dark highlight fill measures **1.64:1** — visible as a slight darkening, no more. That is the same dark-bin limit as the hairline itself (below), and it lands in the last two pixels, after the reader has already been led. Report the figure rather than "fixing" it with a second colour, which would give one map two dot treatments.
  - **Put the dot inside the country's *shape*, which is not its bounding box.** A box centre is in the sea for anything crescent-shaped, forked or split by a gulf: Mexico's landed in the Gulf of California, and "inside the bbox" was the check that let it through. Take the country's pixel mask off the render (match its bin fill inside its padded bbox), run a multi-source BFS in from the mask's boundary to get each pixel's depth, and put the dot on the deepest pixel — then drag the elbow's corner with it so the final run stays axis-aligned. Three traps in doing that:

    - **Match the fill strictly** — a loose tolerance admits the antialiased coast, so the "interior" includes the shoreline. That is what put a dot in the Persian Gulf. Tolerance and worked numbers: CHECKS.md → pixel-probe mechanics.
    - **Averaging the deepest plateau can land back outside the mask** — a concave shape's mean is not in the shape. Snap to an actual mask pixel: deepest first, then the most 8-neighbours also on fill, then nearest the centroid.
    - **Sample `floor(coord − 0.5)`, not `int(coord)`** — see CHECKS.md → pixel-probe mechanics, which this cost two rounds to find.

    For a country only a few pixels across, also **arrive along its widest run**: the UAE is 7 pure pixels whose widest axis is horizontal, so a leader dropping in vertically meets a 1px-tall coast, while one turning in from the side puts the country's own fill on both sides of the dot. And accept the limit — at 2–15px the ~2px dot spans the country and every depth is 1. That is the treatment working, not failing; state the verification as "the dot's pixel carries that country's own fill", never "the dot is surrounded by it".
- **One leader per named country**, never one leader for a list. If the sentence says "In the US, UK or France…", that is three leaders from one text block.
- **Route the corridor over open ground, and check the corner pixel, not just the run.** A leader is a claim about one country, so it should not track across an unrelated one; on a map with ocean available, put the long run in the water. Scan the candidate corridor on the render and count both the pixels that cross a fill and the pixels where the hairline goes faint — then take the corridor that clears both. **Include the elbow's own corner row in that scan.** Checking the vertical from one pixel below the corner is the kind of off-by-one that survives every measurement and then shows up in the render: a corridor whose run was verifiably all water had its corner sitting on India's fill, four pixels from a coastline the scan never looked at.
- **A `#2d2e2d` hairline needs about 2.5:1 against every fill it crosses, and a dark bin defeats it.** Over `#08306b` or a dark red `#b13507` the leader simply disappears — contrast 1.3:1 and 1.8:1 — so a leader that must reach a country hemmed in by a near-black bin will have an invisible stretch whatever you do. Keep that stretch to the last few pixels *beside the dot*, where the reader has already been led, and make the visible part long enough to carry the direction: lengthening one jog from 8.5px to 18.5px turned a 4px visible stub into 14px, with the same 4px lost at the end. Measure it — sample the path on the render and report the faint count and the worst run — rather than assuming a hairline reads because it is dark.
- **The long run is vertical; the horizontal jog is short (≤40px).** Long horizontals drag across the map and read as graticule. Where several leaders serve one annotation, let **one run straight down at its country's x and the others fork off that corridor** with a short jog 10–15px below the exit — that shared-corridor-plus-fork is what the finished pages do, and it keeps a fan of leaders from looking like unrelated lines.
- For an annotation *below* the map, run up the corridor and put the jog at the **country** end.
- **Leave ≥12px between the text block and where the leader starts.** 6px looks like an underline on the text.
- **The one sanctioned exception to "never overlap the annotation box": a leader may cross it to start at the end of a *shorter line*.** When a text block's last line is shorter than the one above it, the knockout still hugs the longest line, so there is empty canvas beside the short line — and starting the leader there attaches it to the sentence instead of hanging it off the block's bottom edge, which is what the finished pages do. Ride the jog **1.5px above that line's baseline** — `box.bottom − 1.5` with a bare `CAP_HEIGHT`-trimmed text node, or `box.bottom − paddingBottom − 1.5` if the annotation is in a tier-3 frame — so it reads as continuing the line of text, and start it **~11px after the last glyph**. Append the leader *after* the annotation, whichever tier it is: a tier-3 fill hides it outright, and a tier-2 halo eats its first few pixels. Assert the z-order rather than assuming it.
  - **The API exposes no per-line width, so measure the short line's ink off the render** — one crop settles it. The mechanics, and why a probe clone cannot answer it, are in GOTCHAS.md → no per-line width.

**Fitting the map, and hiding what can't be read:**

- **Hide island countries that render as barely-visible specks** (under ~2.2px), but only *island-like* ones — a shape with a neighbour within ~1.5px is an enclave or a small mainland country, and hiding it punches a **white hole** in a continent, which is far worse than the speck you removed.
- **A country that straddles the antimeridian reports a bbox spanning the whole map.** Fiji's box is 506px wide and 4px tall; it will pin any bbox-derived measurement and it defeats size filters. Detect it (`w > 150 && h < 12`) and exclude it from the fit — keep it *visible*, it is a real place.
  - **Except once the fringe trim is taken: then the straddler's halves land OUTSIDE the content box and "keep it visible" stops meaning anything.** Excluding it from the fit is what does this — the map is scaled so the *mainland* union spans 16…524, which pushes anything beyond that union past the frame edge. Measured on a 540 frame: after the trim, Fiji spanned **x −50.11 … 543.34**, i.e. both specks off the artboard, clipped or littering the canvas depending on the frame's `clipsContent`. So when the user approves the fringe trim, the straddler goes with it — and say so in the list you ask about, rather than keeping it and shipping two specks nobody can see. Hiding it also makes the chart group's box equal the map's ink, which every gap and margin measurement downstream depends on (`box == ink`, FITTING.md).
- **Trim the Pacific fringe at the subpath level.** The US is a single vector of ~10 subpaths, and Hawaii is five ~1px specks 60px southwest of Alaska: they pin the US bbox's left edge and, through it, cost the whole map ~30px of width. Rewrite `vectorPaths` with only the kept subpaths, then put the node back where it was — Figma re-origins a vector when its geometry changes, so measure the kept union first and correct `x`/`y` after:

  ```js
  node.vectorPaths = [{ windingRule: wind, data: kept.join(" ") }];
  const b = node.absoluteBoundingBox;                    // where it landed
  node.x += want.x0 - (b.x - fb.x); node.y += want.y0 - (b.y - fb.y);
  ```

- **ASK before trimming the fringe — it is a visibility decision, not a fit detail.** Trimming buys
  real map size, and it does it by removing real places. So put the trade-off to the user rather than
  deciding it, **and ask it in plain words.** Name the countries, say how much bigger the map gets, say
  what stops being drawn, and stop there:

  > *"The map is sitting small in its space because a few tiny Pacific islands reach out to the far left
  > and right edges and stop it filling the width. I can leave them out — Hawaii, Fiji, Kiribati, Samoa,
  > Tonga, Tuvalu, Nauru and the Marshall Islands — which makes the map about 14% bigger. Those places
  > would no longer be drawn; most are under 2px across here, so they are barely visible either way.
  > Leave them out, or keep every country and accept the smaller map?"*

  Keep the internal vocabulary out of it. "Antimeridian straddler", "content width", "the fringe",
  "ink vs bbox" and the pixel arithmetic are how *you* decide; none of them help the person answering,
  and a question full of them reads as a status report rather than a choice. One sentence on why, one
  list of what goes, one number for what it buys.

  Default to keeping them if there is no answer. A map that silently loses islands is the kind of edit
  a reader notices and nobody approved.

- **Detect a straddler by width AND height — width alone hides Russia.** The test above is
  `w > 150 && h < 12`, and the height half is the load-bearing half: Chukotka crosses 180°, so Russia's
  box measured **461px wide** on a live frame — indistinguishable from Fiji's 593px by width. A
  width-only rule set `Russia.visible = false` and took the largest country on earth off the map, which
  a screenshot makes obvious and a numeric fit check does not. Russia's box is 67px tall, so the height
  clause rejects it cleanly. The rule was already written here; the ad-hoc script that ran the trim
  reimplemented it from memory with the height clause dropped — reuse the predicate, don't retype it.
- **What trimming actually bought, end to end, on a 540 frame.** Hiding the seven island nodes plus
  Hawaii's five subpaths took the map body from **444 → 508px** wide (×1.143 then ×1.059, in two passes
  because Hawaii is inside the US vector and only surfaces once the separate nodes are gone) and 207 →
  251px tall. The gaps above and below the chart went from **39.4 / 42.1px to 12.96 / 12.96** — from
  well outside the 12–16 target to inside it, which is the actual point of the trim. Afterwards the
  edges are set by real content: Alaska at x=16, New Zealand at x=523.98.
- **Adjusting the map is not finished until you have re-adjusted the LEGEND.** Grapher lays the legend
  out for the map it exported, so the moment you trim or rescale the map the legend is wrong in two ways
  at once, and neither shows up in a width check on the chart group. Measured on the live frame: the
  legend sat **55.57px** in from the left content edge and **0.1px past** the right one — indented and
  flush at the same time, because it had been sized for the untrimmed map — and it floated **66.4px**
  below the map, mid-band, reading as a separate object rather than part of the chart. Fix both:

  ```js
  legend.x += (fb.x + CONTENT_L + (CONTENT_W - lInk.w) / 2) - lInk.x0;   // centre on the content box
  legend.y += (mapInk.y1 + 16) - lInk.y0;                                // tuck under the map
  // then re-centre the whole chart group, because the block just got shorter
  ```

  Centre it on the content box rather than left-aligning it — a legend is a caption for the map above
  it, not a column of body text, and at 452px against a 508px box the 27.7px it gets on each side is
  what makes it read as attached.
- **Expect the outer gaps to GROW when you tuck the legend in, and leave them.** Pulling the legend from
  66px to 16px shortened the block by 50px, so the symmetric gaps above and below went **12.96 → 38.15px**
  — outside the 12–16 target, and correct anyway. A map is the one chart type where the target does not
  bind: it is width-limited, so the band's leftover height has nowhere to go, and ocean is placeable
  space (which is how the reference fits a 247px map in a 540px frame). Do **not** claw the gaps back by
  pushing the legend down again; that trades a real relationship — legend belongs to map — for a number.
- **The fringe is worth ~6% of the map's width, measured.** On a live 540-wide frame the map group's raw
  bbox ran 490.06px while the straddler-excluded union ran 481.76 and the mainland began 29.57px in — so
  Fiji's box costs **8.29px** and the Tonga/Samoa/Kiribati specks another **21.28px**, which is the
  "~30px" below, confirmed. Worth knowing before you decide whether to spend the trim.
- **Fitting the fringe out is not a one-line rescale, because the legend moves too.** Scaling the chart
  group so the straddler-excluded union spans the content width gained 27.67px of map and produced three
  breaches: Fiji's box (see the bbox note above) plus the legend's right end at 542.92 against a 524
  edge, because the legend already sat at the margin and scaled with the group. So the order is: trim
  the fringe first, then fit the map, then re-fit the legend — not scale-and-hope.

- **Fit on the real content, then check the margins.** Measure the union of visible country boxes (minus the straddlers), scale so it spans the content width, and afterwards assert that nothing sits outside the 16…524 band — a speck left in the frame's margin shows up as a cut sliver at the edge. And re-set the hairlines *after* the final scale (GOTCHAS.md → `rescale()` multiplies every stroke width).
- **Place a bottom annotation against the deepest ink in its own column, not the map's global bbox.** Empty ocean is placeable space, which is how the reference gets a 247px map into a 540px frame; but a 3px island left in that column will push the text down as if it were a continent (hide it — see above).
- Legends: **centre a map's legend on the content box, not left-aligned** — see the tuck-and-centre rule above, which is measured; a legend is a caption for the map, and this bullet used to say "align left", which contradicted it. Vertical columns matched to label lengths; one–two categories → shrink the legend; horizontal stretched legends only for sequential palettes, not categorical.
- **A binned legend's labels are claims about ranges, and every boundary is a chance to be wrong.** Three traps, all of which cost a round on the same chart:
  - **Match the bin's own inclusivity.** Grapher's manual bins are `(lower, upper]`, so a label reading `> 75%` is false for the countries sitting at *exactly* the boundary — two of them here. Check the data for exact-boundary values before writing any `>` or `<`, and prefer inclusive phrasing (`75% or more`) or a neutral range (`25% to 50%`).
  - **No range label may reach an extreme that has its own bin.** If `0%` (or `100%`) is called out separately — "No women" — then it is a strict *subset* of any range written for the neighbour, so `75-100% men`, `75% or more men` and `25% or less` all swallow that bin whole. This is not the ordinary shared-edge convention (25 and 50 appearing on both sides of a boundary is fine and readers resolve it by position); it is one label claiming another's contents. Start the neighbouring range above the extreme.
  - **"Between" reads as excluding both endpoints**, so it is only safe on a bin that genuinely excludes both. Prefer `X% to Y%`, which is neutral about the ends and a third of the width.
  Also: **frame the ranges in the same direction as the colour ramp.** Labelling an ordered ramp by the *other* side of the ratio ("75-100% men" on a scale of women's share) makes the numbers count down as the colours go up, which reads as an error even when every label is true.
- **Prefer one row of labels for an ordered ramp; the wording has to earn it.** A sequential legend is a *sequence*, and a single line of labels reads it in one sweep where stacked two-line labels read as a grid of cells to be parsed individually. The constraint is arithmetic: equal bins must be at least as wide as the widest label, so `widest × 5 + no-data key + gap ≤ content width` — miss it and one label wraps to three lines and the row is ruined. If it doesn't fit, shorten the labels *before* stacking them, and re-check the vertical balance afterwards, since a shorter legend leaves the map+legend block off-centre.
- Tidy grapher's default legend: labels 12–14px, swatch square sized to the font, label color dark gray `#2D2E2D` instead of pure black, group items of similar length into columns.
- No-data pattern: see above — scriptable, no plugin needed.

Further map notes from the archive:

- **A question title keeps its legend**, because there are no key-words to color — `1:5861`, `448:572`, `606:263`, `609:461`. The delete-the-legend rule above is really "don't repeat what the title already names".
- **A highlight map needs no legend at all** when one saturated color marks the countries meeting the condition and everything else stays pale — `222:1048`, `206:276`, `273:320`.
- **The cheapest two-bin alternative to both: colored category labels above the map, each with a short down-arrow into it** — `609:461` ("More births than deaths ↓" blue beside "More deaths than births ↓" orange). Keeps the title free for a question and costs less height than a legend strip. `327:235` goes further and labels the background category *in the ocean* in its own pale color.
- **Mark a ramp's open-ended bin with an arrowhead on that end of the gradient bar** — `185:379`, `232:253` (high end), `479:248` (low end, "Before 1900").
- **Two ways to label a binned ramp, both in use**: labels *between* the swatches at the boundaries (`375:541`, `351:435`, `22:708`, `695:251`) or range labels *inside* each swatch (`232:253` "under 2% / 2–4% / …", `375:235`). Inside-the-swatch is what makes a single row fit when boundary labels would collide; stacking labels into two rows (`22:708`, `259:200`) is the fallback.
- **Legend above the map when it is small, below when it is large** — above: `1:5861` (2 items), `383:253`, `695:251` (5), `375:235` and `448:572` (7); below: `26:971` (12), `22:708`, `232:253`, `259:200`.
- **Values written inside countries take whichever color reads against the fill** — white on dark bins (`259:200`), the category's own color on pale ones (`606:263`).
- **Anchor an in-country label on the MAIN TERRITORY's area centroid, never on the node's bounding box.** A country node is one vector of many subpaths, and the outliers own the box: the US node includes Alaska (and Hawaii before the fringe trim), Russia carries 13 subpaths including Arctic islands and a Chukotka fragment that wraps to the far left at x=18.5. Centring on the box therefore puts the label in the sea or off to one side — measured, Russia's label sat **15px up-left** of where it belonged and the US's was pulled west of the mainland. The recipe:
  1. split `vectorPaths[0].data` on `M` and take the subpath with the largest `|signed area|` — that is the main territory (80% of the US node's area, 97% of Russia's, 100% of China's);
  2. take its **area centroid** (`Cx = Σ(xᵢ+xᵢ₊₁)·cross / 6A`), not its bbox centre;
  3. verify the centroid is inside that ring by ray-casting, and for a concave shape that fails, fall back to the interior point furthest from the boundary;
  4. assert the label's whole box, not just its centre — sample a grid over it and require every point on the main territory. A US label that scored 43/45 was overhanging the Gulf coast by one row of samples.
- **A two-line name inside a small shape is a sign to use the short form.** `United` / `States` stacked in the US mainland is a 43×27px block in a 91×46px shape — it dominates the country and it centres badly. `US` is 19×10, sits cleanly on the centroid, and matches the `UK` already on the map; the Writing and Style Guide settles both (no periods). Propose it rather than shrinking the type or accepting the overhang.
- **Exclude the subject of the map from its own scale** — `375:235` grays out China and labels it, because China has no rank among its own import partners.
- **A leader's country end does not always carry a dot.** The filled-dot terminal documented above is the treatment for landing inside a *small* country; the archive also uses a plain hairline with no terminal (`351:435`, `185:379`, `115:258`) and a short perpendicular tick (`259:200`).
- A regional map may carry a graticule and a non-rectangular projection (`259:200`, `383:253`), and a bordered legend box can be overlaid on an empty corner (`383:253`).

Exemplars: `569:975` (two-bin, title as legend — the worked example above), `609:461` (labeled down-arrows instead of a legend), `606:263` (values in-country, eight categories), `232:253` (range labels inside the swatches), `327:235` (no leaders, background category labeled in the ocean).
