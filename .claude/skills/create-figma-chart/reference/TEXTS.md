# Step 6 — Fill the template texts

> Read at Step 6.  Part of [`/create-figma-chart`](../SKILL.md); the spine has the step order.


Replace the lorem-ipsum text nodes in the cloned template. Source everything from the chart config (Step 1) and the user's answers (Step 2):

- **Title** — for a DI or Instagram image, start from the DI title collected in Step 2, not grapher's; otherwise suggest a more colloquial rewrite per GUIDELINES.md ("Death rate in the United States", not "Death rate, US"). Keep the user's final say. The page name uses this final title. Two or three lines is normal; check the line breaks and the year and highlight-color rules in GUIDELINES.md → Titles.
- **Subtitle** — the chart's subtitle, trimmed to what's necessary. When the chart shows a single year (or a narrow period the reader needs), the image has to state that year somewhere. Append **`Data for <YYYY>.`** here — *unless the title already carries the year*, which is the rule for a year-specific claim (GUIDELINES.md → Titles). One or the other, never both.

  **When the entities aren't all on the same year, state the span, not the exception.** A subtitle that names the odd one out — `Data for 2023, except Japan (2022).` — spends a clause on a caveat no reader acts on, and invites the same treatment for the next straggler. Append the range to the sentence with a comma instead: `Breakdown of meat supply in a given country by type, 2022–2023.` It is shorter, it is true of every entity, and it keeps the year where the single-year form puts it. Use an en dash.
- **Data source:** `Data source: ` + `chart.citation` from Step 1 — that field *is* grapher's own footer line, so don't re-derive a `<producer> (<year>)` string by hand, and don't abbreviate it to save space. A long producer name overruns the CC BY text at x=468.

  **The one sanctioned edit: dropping a producer that contributes nothing to the window the image shows — and only after establishing provenance from the pipeline, not from the numbers.** `chart.citation` credits every source behind the *whole indicator*, which for a spliced long-run series can span centuries; an image cropped with `time=` may draw entirely from one of them. Crediting a producer whose data is not in the picture is its own inaccuracy — but proving which producer is in the picture takes the pipeline, because **matching values do not identify a source**: two producers reporting the same figure for the same country-year are common, and a point-by-point match is then consistent with either having won the splice.

  So establish it in this order:

  1. **Read the garden step's splice logic.** It states the priority explicitly. For the long-run child mortality indicator, `combine_datasets` in `etl/steps/data/garden/un/2026-06-09/long_run_child_mortality.py` prioritises UN IGME wherever both sources have a value — which settles the 1970–2024 window outright, before any CSV is fetched.
  2. **Check the indicator's `origins`** for what each producer's presence actually obliges. An origin whose licence or attribution terms require a credit **keeps its credit even when its values are not the ones displayed** — provenance decides whether the data is shown, the licence decides whether the name may come off. If the terms are unclear, leave the citation alone; it is the cheaper error.
  3. **Then corroborate with the data**, as a consistency check on the conclusion rather than as the proof: fetch the single-producer chart's `csvType=full` CSV and diff it against the displayed window point by point. On this run all **220** points of a 1970–2024 window matched UN IGME exactly (0 mismatches, coverage from 1956) while the citation also named Gapminder (1800–2015) — agreeing with the splice logic, which is what made it safe to credit UN IGME alone. Had the diff *disagreed* with the splice logic, that is a signal to stop and re-read, not to trust the numbers.

  4. **Read the SNAPSHOT SCRIPTS' own comments, because a producer whose rows are absent from the window can still be the source of the values in it.** The case that breaks the whole "whose rows are these?" framing is a hand-built *continuation* table: OWID extends a source that stopped publishing by carrying its last state forward. On the nuclear-weapons indicator, Bleek's garden step caps open-ended intervals at `LATEST_YEAR = 2016`, and the NTI snapshot script builds rows "from 2017 onwards" up to 2023 — so a 2023 image contains **no Bleek rows at all**, and the naive read is "drop Bleek". But that script's own comments say the table "should be considered as a continuation of Bleek (2017)" and that "no country has changed its status since 2016": the 2023 values *are* Bleek's 2016 codings, restated and checked against NTI, and the classification the subtitle quotes is Bleek's. Crediting NTI alone would attribute a scheme to a producer that did not create it. Bleek's licence (`Copyright 2017, President and Fellows of Harvard College`, against NTI's CC BY 4.0) says the same thing via step 2. **Both credits stay.**
     - The tell that you are looking at a continuation rather than a real series: identical values across every year of the extension. Verified by rendering the map at 2015, 2016, 2017, 2020 and 2023 — all five report the same bin composition (9 / 1 / 1 / 191). Which is worth knowing for its own sake: it means the year in the title is provenance, not news, and it settles "should we wait for the refresh?" as *no* — the refresh will extend the same values to a later year.

  Do **not** do any of this from the indicator's prose: `descriptionKey` said Gapminder covers 1800–2015 and UN IGME "some countries from 1932", which sounds like both contribute and does not tell you which wins where. `descriptionProcessing` is no better — "double-checked with information from the Nuclear Threat Initiative" reads as verification-only for a table that in fact supplies every row in the window. Record the drop as a deliberate deviation, note that the interactive chart's footer will still list both, and re-measure the footer afterwards — a shorter line can collapse a planned two-row footer back to the template's single row, which is worth ~20px of chart.

  > **First check whether the template already gives the source its own row — the static mobile ones do.** `Frame 15` on both static mobile templates is a two-row block, 38px tall, with `Data source:` and the license each on their own full-width row at `x=0`. There is nothing to rearrange there, and running the manoeuvre below on one of those clones adds a **third** row. The recipe applies to the genuinely one-row footers — `DI_Template`'s `Frame 37` at y=508. The Instagram templates are already two-row for their own reasons (the `OurWorldinData.org/[Topic]` line).
  >
  > Step 7's `footerTop = footer.y + Math.min(0, source.y)` needs no change for the shipped two-row frame: `source.y` is 0 there, so it returns `footer.y`, which is correct. Don't "fix" it.

  **Give the source its own full line and move CC BY to the row beneath it** — the source stays one unbroken line, which reads better than a wrap, and the template's own two-row footers already use exactly this geometry.

  **Do it by changing the footer's `layoutMode`, not by assigning `y`.** DI's `Frame 37` is a **HORIZONTAL** auto-layout (`itemSpacing: 220`) whose two children are both `layoutPositioning: "AUTO"` — flowed, verified 2026-08-20. Auto-layout owns the position of a flowed child, so `source.y = 0; ccby.y = 20` is silently discarded and the rows stay side by side. Turn the axis instead:

  ```js
  const BOTTOM = footer.y + footer.height;       // read off the clone, before anything moves
  source.textAutoResize = "WIDTH_AND_HEIGHT";    // one line, its natural width
  footer.layoutMode = "VERTICAL";                // the actual operation — not a y assignment
  footer.primaryAxisSizingMode = "AUTO";         // REQUIRED — see below; without it the row overflows
  footer.itemSpacing = 4;                        // the row pitch both shipped two-row footers use
  footer.counterAxisAlignItems = "MIN";          // left-align, so CC BY sits under the source
  // Re-pin in a LATER call, or from the rows' own extent — footer.height is stale in this one:
  footer.y = BOTTOM - Math.max(...footer.children.map((c) => c.y + c.height));
  ```

  **`primaryAxisSizingMode = "AUTO"` is not optional, and leaving it out fails silently.** On a
  VERTICAL auto-layout the *primary* axis is the vertical one, so height is governed by
  `primaryAxisSizingMode` — not `counterAxisSizingMode`, and not `layoutSizingHorizontal`. DI's
  `Frame 37` ships **`FIXED`** (measured 2026-08-20, alongside `layoutSizingVertical: "FIXED"`), so
  turning the axis alone leaves the box at its one-row **16px** while the second row renders *below*
  it: CC BY sits at `y: 20, h: 16` inside a frame whose `height` still reads 16. Nothing errors, both
  rows draw, and every geometry read — `footer.height`, the band bottom, the gap — is wrong by 20px
  in the direction that makes the chart look fine and the footer overhang the artboard.

  **And `footer.height` does not settle inside the call that changed `layoutMode`.** Re-pinning with
  `BOTTOM - footer.height` in that same call therefore subtracts the *old* height and moves the footer
  nowhere. Pin from `Math.max(...children.map(c => c.y + c.height))`, which is live, or split the
  re-pin into the next call. Verified on this template: content extent 36, `footer.y` 488, bottom
  landing exactly on the clone's 524.

  **Take `BOTTOM` from the clone rather than typing it.** It is 524 in a 540-wide template, and the footer edge differs in every template (Step 7's table) — hardcoding the square template's value lifts the footer off the bottom everywhere else. Then re-fit the chart into what's left (Step 7). Only if the source is too long even for a full line — beyond the template's content width — wrap it with `textAutoResize = "HEIGHT"` at a width that breaks after the organization's name, and top-align CC BY with its first line. Either way CC BY is **left-aligned** once it has its own row — it only sits at x=468 while it shares the source's line.

  **The `source.y = -20` variant only works on a footer whose children are NOT flowed.** It leaves the footer's own geometry untouched — nothing else in the template shifts — and the band's real bottom becomes `footer.y + source.y`, which is the number to feed Step 7. But it is a `y` assignment, so auto-layout discards it on every current template: check `layoutPositioning` on the child first (`"ABSOLUTE"` means it will hold, `"AUTO"` means it won't). It is recorded here because the file's own older finished pages use it, from when those footers were absolute.

  **Diagnose the overrun by measuring the source's own width, not by reasoning about the layout.**
  DI's `Frame 37` is already `primaryAxisAlignItems: "SPACE_BETWEEN"` and already FIXED at 508, which
  makes its `itemSpacing: 220` inert — so "the fixed spacing is pushing CC BY off" is a plausible
  story that costs a wasted write to disprove. Read `source.width`: on this run it was **535** against
  the 508 content box, i.e. the line genuinely does not fit, and CC BY's `x: 535` was just the source's
  own right edge. The arithmetic that decides the fix is then one division — 508/535 needs a **0.949**
  factor, so 14px must drop to 13 (13.5 leaves it at 516, still over).

  **And know when *not* to spend the second row.** A source that overruns CC BY by only a few pixels is not worth 20px of chart: the full FAO name at the Source style's 14px measured 473px against CC BY starting at x=468 — a 2px overlap — and the finished page's answer was to set that one line to **13px** and keep the footer one row deep. Weigh the two costs explicitly (one off-ladder size on the least important text, versus a fifth of the gap budget and a re-export) and record whichever you pick.
- **Note:** only in templates that carry a Note line, and only if the chart has one worth keeping. **DI images normally carry no note at all** — drop it, or, when it's genuinely load-bearing for understanding the chart, fold it into the subtitle as a bolded second line (only if the subtitle isn't already crowded).
- **`OurWorldinData.org/[Topic]`** → the confirmed topic path (e.g. `OurWorldinData.org/child-mortality`).
- **CC BY** stays on the DI and Instagram templates. The static templates — desktop **and mobile** — instead carry `Licensed under CC-BY by the author <Name>`, the author of the piece from Step 2, not the page-name credit. On static mobile that line is the second row of `Frame 15` and reads `Licensed under CC-BY by the author [Name of author]` in the template.

**Changing a footer row's line count moves the band, and the re-spacing happens for you — but the growth direction does not.** Every footer is auto-layout with its children flowed, so tightening a two-line note to one line lets the rows beneath it close up on their own. Which edge stays put is `constraints.vertical`, and **almost every footer is `MIN`** — it keeps its *top* and grows **downward, out of the frame**, so a note that gains a line or a license that wraps to two simply renders below the artboard. Nothing errors and nothing clips. Measured 2026-08-19: `MIN` on IG square, IG portrait, DI, static mobile 2 and both 850-wide templates; **`MAX` on static mobile 1 alone**. Do not infer it from the family — read it.

So re-pin the bottom by hand after any edit that changes a footer's height:

```js
const FOOTER_BOTTOM = footer.y + footer.height;   // read BEFORE editing the rows
// ... edit the note / license ...
footer.y = FOOTER_BOTTOM - footer.height;         // keep the designed bottom edge
```

Read `FOOTER_BOTTOM` off the clone rather than typing it — it is 1078.81 on Static Vertical and 622 on Static Horizontal, both ~16px above the frame's own bottom edge. It is harmless to apply on a `MAX` footer (there it is a no-op), so make it unconditional rather than branching on the constraint.

**Check that structure rather than assuming it — a footer with loose children does not reflow.** The tell is `footer.layoutMode === "NONE"`, or a child reporting `layoutPositioning === "ABSOLUTE"`, and an old clone of a since-restructured template is the usual way to meet one. There a shrinking note leaves the rows put and opens the note→source gap to **19.41px** while source→tagline stays 5.41. When you find one, re-space by hand and **take the designed gap from a pair of rows you did not touch** — source → tagline — rather than from the template's nominal `y` values, which encode the two-line assumption:

```js
const DESIGNED_GAP = tagline.y - (source.y + source.height);   // 5.41 in both static templates
note.y = source.y - DESIGNED_GAP - note.height;
```

Either way the knock-on is the same: **the band's bottom edge is the footer's top, and it just moved.** Read it back off `footer.y` — not off the note, which stops bounding the band once the footer reflows — then re-fit the chart, and if the band grew by more than a few pixels, re-export to the new height rather than leaving an oversized gap. Moving the note down 14px took one band from 456.6 to 470.6 and left a 402-tall chart floating with 34px gaps.

**An orphan last line is a copy problem, not a layout problem — tighten the words.** When a string
spills a word or two onto a new line, the fix is to rewrite it shorter so it finishes on the line
before, not to shrink the type, widen the slot, or leave it. This applies to **every** text in the
frame — title, subtitle, note, source, license — not just the note where it is most obvious. A note
reading `… are not shown.` with `shown.` alone on line two costs 14px of chart and reads as an
accident; `… are hidden.` fits on one line and says the same thing.

Make it measurable rather than eyeballing it, because at 12px a one-word overflow is easy to miss:
clone the node, set `textAutoResize = "WIDTH_AND_HEIGHT"` to get its **unwrapped** width, and compare
against the slot.

```js
const lines = Math.max(1, Math.ceil(unwrapped / slotW));
const lastFill = lines === 1 ? 1 : (unwrapped - (lines - 1) * slotW) / slotW;
const orphan = lines > 1 && lastFill < 0.2;    // last line under a fifth full
```

A width settles **inside the same call**, as long as the node is in an auto-resizing mode — so probe
and read in one call, then delete the probes. What does *not* move is a node still pinned to a fixed
width (see Gotchas): set `textAutoResize = "WIDTH_AND_HEIGHT"` before reading, not after.
A deliberate two-line title is not an orphan — the wine chart's title runs to a second
line that is 38% full, which is a real line of text; the test is the fill, not the line count.

**The placeholder text tells you the intended line count — match it.** Every template ships "This is a title on **two lines**, lorem ipsum…" against a slot two lines tall, and "This is a subtitle or description on **one line**…" against a one-line slot. That is a shape instruction, and it is easy to invert: a one-line title with a two-line subtitle fills the same box and reads wrong, because the title stops carrying the weight and the subtitle starts. So give the title enough substance to run to two lines, keep the subtitle to one, and push any reading instruction ("each band is the volume traded between a pair of countries") down into the **Note**, which is where an instruction belongs. Check it by reading the heights back: on the Horizontal template a correct pair measures title `h=58`, subtitle `h=19`.

Rules: replace `characters`, and leave the node's **base** styling alone — the fonts, sizes, colors, and positions are the template's, not yours. `await figma.loadFontAsync(node.fontName)` before each text edit. If you need a *new* text block the template doesn't have, **clone the nearest template text node and edit it** — that inherits the correct shared style without hunting style ids.

**Watch for template text that is already mixed-weight, and restore it after writing.** Setting `characters` propagates the *first character's* style over the whole new string, so any node whose label is bolder than its content comes out uniformly bold. **Two slots ship that way, not one** — `Data source:` and, on the templates that carry it, `Note:`. Fixing only the source line leaves a wholly bold note, which reads as deliberate emphasis on a caveat and was spotted on a finished frame rather than in review. Write the string, then push Regular back over the tail:

```js
const PREFIX = "Data source:";
src.characters = PREFIX + " " + citation;
// REQUIRED, and missing from this recipe until it was measured: assigning `characters` DETACHES the
// node's text style. Re-bind before touching weights, with the FULL style id.
await src.setTextStyleIdAsync(SOURCE_STYLE_ID);   // resets range weights — so the bold goes after
await figma.loadFontAsync(src.getStyledTextSegments(["fontName"])[0].fontName);
src.setRangeFontName(0, PREFIX.length, {family:"Lato", style:"Bold"});
// and do NOT re-set the tail to Regular: the style already provides it. The prefix range loses its
// style binding here and cannot get it back — see below; the tail keeps it.
```

**Only three of the nine templates bind their source to a style at all — read the clone, never assume.**
Measured across every in-scope template:

| template | title | subtitle | source |
|---|---|---|---|
| IG square, IG portrait, DI | `Instagram/Title` (portrait: `…(portrait)` 28) | `Instagram/Subtitle` (portrait 18) | **`Instagram/Source` 14** |
| static mobile 1 & 2 | `Instagram/Title` 25 | `Instagram/Subtitle` 16 | **no style**, 14 |
| static horizontal & vertical | **`Data Insights/Title`** 25 | **`Data Insights/Subtitle`** 16 | **no style**, **12** |
| small guided & pull | **no style**, 16 **Bold** | **no style**, 11 | — (no footer) |

Three consequences, all of which a DI-only fix gets wrong:

- **The re-bind below applies to IG square, IG portrait and DI only.** On the static and 302-wide
  templates the source ships with no style id, so an unbound source there is the template's own state
  and not a defect — the template is law, and matching it means leaving it unbound.
- **The 850-wide pair uses the `Data Insights/` family while DI itself uses `Instagram/`.** That is
  counterintuitive enough to check rather than infer from the template's name, and their source runs
  **12px** against mobile's 14.
- **The footer carries weights the annotation ladder does not mention.** static mobile's license line
  is `Regular+Bold+Regular+Bold` (four segments) and the 850-wide pair's is `Medium+Bold+Medium+Bold`,
  with its Note at `Bold+Medium+Regular`. So a mixed-weight restore that assumes Bold-then-Regular
  will flatten a **Medium** run on those templates. Read the segments before writing, and put back
  what was there.

**Assigning `characters` detaches the text style, and the bold prefix cannot be re-bound. Know which
of the two reachable states you are shipping.** Measured step by step on a filled clone:

1. The node starts node-level `Instagram/Source` with `Bold + Regular` segments — **both** segments
   carry the style. That is the template's state.
2. `src.characters = …` alone drops it to **unbound**, segments collapsed to the first character's
   style. This is the bug: a recipe that writes the string and fixes the weights produces a
   correct-*looking* footer whose source carries no style at all. Eight of eight on one run.
   - **It does not always drop the binding, and that is the trap in step 3.** Observed on the DI
     template: after `characters`, the *tail* still reported the style id while the whole line had
     gone Bold — the propagation had landed as a range override *on top of* a surviving binding. Which
     of the two states you are in is not predictable from the recipe, so read it rather than assume it.
3. `setTextStyleIdAsync(fullId)` re-binds the whole node, at the style's uniform Regular — **but it is
   a NO-OP when the node is still bound to that same id, and it fails silently.** Applying the id the
   node already carries changes nothing, including the range font overrides, so the line stays
   uniformly Bold and step 4's `setRangeFontName(0, PREFIX.length, Bold)` is a no-op too. The footer
   then ships with a wholly bold citation, which looks deliberate. Two ways out, and the order matters:
   either clear the weight at node level first (`src.fontName = {family:"Lato", style:"Regular"}` —
   which strips the binding, so the style application afterwards *does* take effect), or set the tail's
   weight explicitly by range. **Assert the outcome either way**, on the segments and not on
   `node.textStyleId`: the target is `"Data source:" Bold unbound` + `" <citation>" Regular BOUND`.
4. `setRangeFontName(0, PREFIX.length, Bold)` then **strips the style from that range**: the prefix
   goes `Bold | unbound`, the tail stays `Regular | BOUND`, and `node.textStyleId` becomes
   `figma.mixed`.
5. `setRangeTextStyleIdAsync` on the prefix re-binds it — and resets it to Regular, losing the bold.
   Re-bolding strips it again. **The loop does not close.**

So the template's Bold-and-bound state is reachable in Figma's UI and **not** through
`setRangeFontName`. Pick deliberately:

| ship | node reads | trade |
|---|---|---|
| prefix Bold, tail bound *(default)* | `figma.mixed` | matches the template visually; the 12-character label stops tracking the style, the citation still does |
| whole line bound, no bold prefix | the style id | tracks a design revision completely; loses the house bold convention |

Take the first unless someone says otherwise, and **do not report the source as unstyled when it
reads `figma.mixed`** — check the segments, not the node. Which is also why a truthiness test is the
wrong check here: `figma.mixed` is a **Symbol**, so `node.textStyleId ? "bound" : "none"` says
"bound" for a node that is only half bound, and `=== styleId` says "none" for the same node. Both
readings are wrong in opposite directions; read `getStyledTextSegments(["textStyleId"])`.

Three details the fix depends on:

- **The full style id, not a prefix.** These are 43 characters and end in a comma
  (`S:2fea779e2ca5d1ff32c496ab104a86a7942f51dc,`). `setTextStyleIdAsync` with a truncated id fails
  *silently* — the call returns and `textStyleId` stays `(NONE)`, which reads exactly like the bug
  you are trying to fix. Read the id off the template node, or off
  `figma.getLocalTextStylesAsync()`.
- **Load fonts AFTER applying the style, not before.** The style can introduce a font the node did
  not carry, and the next `setRangeFontName` then throws `Cannot write to node with unloaded font`.
- **The DI template binds `Instagram/Source`**, not `Data Insights/Source`, even though both exist
  at 14px. Read the binding off the clone rather than assuming the family matches the template's
  name.

Read the segments back (`getStyledTextSegments(['fontName'])`) and compare against the untouched template node — a wholly-bold source line looks deliberate enough that nobody catches it in a screenshot.

Two **range-level** exceptions the guidelines actively require, applied after the characters are in place and scoped to just those characters — never to the whole node:

- the title's highlight word → `setRangeFills`, in the exact color of the marks it names (GUIDELINES.md → Titles);
- a load-bearing note folded into the subtitle as a bolded second line → `setRangeFontName` to the family's bold weight, which needs its own `loadFontAsync` (GUIDELINES.md → Subtitles and notes).

Nothing else gets restyled.
