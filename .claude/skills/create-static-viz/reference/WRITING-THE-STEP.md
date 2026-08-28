# Step 4 — Write the `export://static_viz` step

> Read at Step 4, once you know which chart you are building.  Part of [`/create-static-viz`](../SKILL.md); the spine has the step order.

> **Create the branch and worktree first — before the first edit.** `etl pr` seeds the new branch
> with an *empty* commit and "never stages or commits your local changes"
> (`apps/pr/cli.py:3-5`), and `--worktree` builds a clean sibling checkout from
> `origin/<base_branch>` (`branch_out_worktree`, `apps/pr/cli.py:467-485`). So it carries nothing
> across: run it *after* writing the step and your Step 4–8 work is stranded in whatever checkout
> you were in, while the PR and the babysitter see an empty branch.
>
> ```bash
> .venv/bin/etl pr "<title>" data --worktree   # opens the draft PR and starts the staging server
> ```
>
> Then do all of Step 4–8 **inside that worktree**. `etl pr` switches the *main* checkout's branch,
> not your current directory, so confirm where you are before editing and again before committing:
> `git branch --show-current`.

Path `etl/steps/export/static_viz/<namespace>/<version>/<short_name>.py`, DAG entry in
`dag/static_viz.yml` (a flat `steps:` map, one comment line per step, keyed by the full
`export://` URI with its garden/grapher deps as the value).

### The Figma handoff contract

Non-negotiable — `scripts/verify_static_viz.py` checks all of it, with one gap you have to close by
hand, named under `gid=` below:

```python
import matplotlib
matplotlib.rcParams["svg.fonttype"] = "none"       # real <text>, editable in Figma
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"  # deterministic ids, clean diffs
```

- **Sweep clipping off before saving**, or shapes arrive at the axes boundary cropped:
  ```python
  for artist in fig.findobj():
      artist.set_clip_on(False)
  ```
- **Make the figure and axes backgrounds transparent.** matplotlib fills both white by default, and
  they leave the step as a frame-sized opaque rectangle (`patch_1`) plus a plot-sized one (`patch_2`).
  On import those land *above* the template's cream background, its logo and its text and hide all
  three — and because they are their own groups, dropping the SVG's duplicate text does not uncover
  them. This is the same rule as "the step does not set colors": the background belongs to the
  template, and white is a background even though nobody chose it.
  The PNG wants the opposite, though: it is what a human reviews, often against a dark editor
  background, where a transparent canvas makes gray text unreadable. So keep the canvas opaque on the
  figure and drop it at save time, which means **two `export_fig` calls, not one**:
  ```python
  fig.patch.set_facecolor("white")  # legible when the PNG is reviewed on a dark background
  paths.export_fig(fig, short_name, ["png"], dpi=300)
  paths.export_fig(fig, short_name, ["svg"], transparent=True)
  ```
  Any extra axes you add for annotation needs the same treatment
  (`ax.patch.set_visible(False)`), or it arrives as one more opaque rectangle.
  **The verifier does not check this yet** — read `patch_1` in the saved SVG: `fill: none` is right,
  `fill: #ffffff` is the bug.
- **`gid=` on every artist**, so the layer panel reads `boys__median` rather than `Path 41`.
  Grapher does exactly this with `makeFigmaId` (`packages/@ourworldindata/utils/src/Util.ts`).
  Use `<subject>__<role>` so the names sort into groups. **The verifier cannot enforce this clause on
  its own, so name the layers and pass them.** It fails an SVG carrying no deliberate name at all,
  but it cannot tell which artists *ought* to have been named: matplotlib emits `line2d_<n>` groups
  for every tick mark exactly as it does for an unnamed data line, so "no generated ids left" is not
  a test any correct figure would pass. Give it the data layers explicitly and the contract becomes a
  real check rather than a presence check — otherwise a figure whose title is named and whose plotted
  line is not still reports `OK`. Pass them as
  `--expect-gid boys__median --expect-gid girls__median`.
- **Do not pass `bbox_inches="tight"`** when the frame must match a template — cropping to content
  changes the proportions, which is the whole thing the template fixes. `export_fig` already
  injects `metadata={"Date": None}` on the SVG pass for reproducible diffs.
- Emit both formats; `paths.export_fig` writes into the step's own directory, so **the PNG and SVG
  are committed next to the `.py`** — for every frame the step emits.

### A dashed line has to be resampled, or Figma renders its dash unevenly

The one part of the handoff `verify_static_viz.py` cannot see, because the SVG is correct and it is
Figma that reads it differently. **Figma fits a whole number of dash repetitions into each segment of
a path individually**, so the dash length a reader sees is set by the vertex spacing rather than by
the dash you asked for. Matplotlib dashes continuously along a path and does not care, so the PNG
looks right and only the placed frame is wrong.

Path simplification makes a curve the worst case: it keeps vertices where curvature is high and drops
them where the line is straight, so one end of a curve collapses into dots while the other runs as
long dashes. Measured in Figma: 6.7px spacing renders as dots, 13px as over-long dashes, >=50px as
specified.

So resample any dashed line by arc length, one dash period per segment. `who/2026-08-07/height_for_age.py`
carries the worked version -- `resample_for_even_dashes` plus an `even_dashes(fig, period_pt)` pass that
finds the lines by gid. Three things it has to get right, each of which is a silent wrong answer:

- run it **after** `subplots_adjust`, or the axes are still at their default size and you measure the
  wrong page;
- convert the period to display pixels with **`fig.dpi`**, not with a template-pixel constant -- these
  figures lay out at 100 template px per inch but render at 200 dpi, so the constant puts half a
  period in each segment and changes nothing visible in the numbers you are checking;
- switch path simplification off on the resampled line, or matplotlib drops the vertices you placed.

Also worth knowing when you pick the pattern: matplotlib **multiplies a dash sequence by the line
width** (`rcParams["lines.scale_dashes"]`), so `(5, 3)` on a 1.4pt line draws a 7pt dash -- five times
the stroke width, which reads as stretched at any size. Write the pattern in multiples of the width
and say so, or check the emitted `stroke-dasharray` against what you intended.

### Style, and where it stops

- **seaborn** `set_style("ticks")` + `set_palette("deep")`, and reference colors by **palette
  position** (`palette[0]`, `palette[1]`) rather than pinned hexes, so the chart moves with the
  shared palette. seaborn is a `dev` dependency.
### Borrow grapher's design language, read from its source

A static chart should read like our interactive ones, so take the axis, tick, facet and reference-line
treatment from grapher rather than inventing one. **The table below is that treatment, already read out
of the source — use it, and don't recall these values from memory either.**

Open the `owid-grapher` files themselves only when you need something the table doesn't cover, or to
re-verify a row after a grapher change: `grapher/src/axis/AxisViews.tsx`, `axis/Axis.ts`,
`facet/FacetChart.tsx`, `color/ColorConstants.ts` — about 97 KB together, which is why this is gated
rather than a standing instruction. The `Source` column names the symbol each value came from, so a
single row can be re-checked without opening all four.

| Property | Value | Source |
|---|---|---|
| Gridlines | **dashed** `4,4`, `#ddd` | `GRID_LINE_DASH_PATTERN`, `TICK_COLOR` |
| Tick labels | `#5b5b5b` | `GRAPHER_DARK_TEXT` = `GRAY_80` |
| Axis label | **bold** (`fontWeight: 700`) | `Axis.ts` |
| x-axis tick marks | 5px long, 1px wide, `#999`, hanging below the axis | `HorizontalAxisComponent`; `LineChart` passes `showTickMarks={true}` |
| Outermost tick labels | anchored **inwards** — `text-anchor` `start` on the first, `end` on the last | `HorizontalAxisComponent` |
| y-axis line | **none** — the gridlines carry the reading | no component exists |
| y domain | `[lowest tick, highest tick]` — the extreme gridlines land **on** the plot's edges | verified on a rendered SVG |
| The gridline on the baseline | drawn **solid** in the tick color, not dashed — it *is* the axis line | `t.solid` in `VerticalAxisGridLines` |
| x gridlines on a line chart | hidden | grapher sets `hideGridlines` on the x axis |
| Facet titles | **bold**, `GRAPHER_DARK_TEXT` (*not* the series color), above the panel, left-aligned with its content, half a line of padding beneath | `FACET_LABEL_FONT_WEIGHT = 700`, `labelPadding = 0.5 * facetLabelFontSize` |
| Facet title size | about `1 / 0.9` of the tick size — one rank up, not a display size | `facetBaseFontSize = facetLabelFontSize / GRAPHER_FONT_SCALE_12 * 0.9` |

**There is no x-axis-line component, and that is not the same as "grapher has no axis line".** What a
reader sees along the bottom of a grapher line chart is `VerticalAxisZeroLine` — same `#999`, same
1px, spanning the plot at y=0 — and the end tick marks continue from it, which is what makes it look
like an axis closed off with elbows. On a chart whose y-axis does not reach zero there is nothing at
y=0 to draw, so apply the same treatment to the **baseline** instead, and **pin the x range to the
outermost ticks** (`set_xlim(first_tick, last_tick)`) so those two marks sit at the ends of the line
and close it. Leave the y-axis lineless either way.

**Check the source claim against a rendered SVG before you build on it.** Grepping for a component
that doesn't exist is weak evidence about what the chart *looks like*: it told me grapher draws no
axis line, when in fact every zero-crossing chart appears to have one. One fetch settles it —
`curl` a grapher SVG and read the `horizontal-axis` group, which lists exactly `tick-marks` and
`tick-labels` and nothing else:

```bash
curl -sL "https://ourworldindata.org/grapher/life-expectancy.svg?country=USA~CHN" -o /tmp/g.svg
```

**Snap the value axis out to whole gridline steps, and then suppress the gridline the baseline draws.**
This is grapher's answer to "the lowest gridline runs a few pixels clear of the axis line": it never
has that case, because its domain *is* `[lowest tick, highest tick]`, so there is only ever one line at
each edge. Leave slack below the lowest tick and you get two nearly-coincident lines; snap the limits
and you get one. Then drop the gridline at the baseline, or a dashed `#ddd` stroke is laid over the
solid `#999` one and breaks it up:

```python
ticks = np.arange(np.floor(low / STEP) * STEP, np.ceil(high / STEP) * STEP + 1, STEP)
ax.set_ylim(ticks[0], ticks[-1])
ax.set_yticks(ticks)
ax.yaxis.get_gridlines()[0].set_visible(False)   # the baseline already draws this one, solid
```

**A grapher-style axis costs vertical space in two places, and both reserves have to grow.** Tick marks
with a length push the tick labels and the axis label down — enough to overrun a reserve that fitted
when the ticks were zero-length, which put the axis label's descenders into the note. And facet titles
above the plot need `font size + 0.5 line` of their own, taken off the top of the plot rather than out
of the frame. Re-measure both reserves after adding either.

**When a tick label crowds its neighbor, drop the tick.** That is what grapher does — it hides
overlapping labels rather than shrinking or rotating them — so a tick set is a per-layout choice, and a
tick whose label sits closer to its neighbor than the rest of the axis is a tick to remove, even when
the value is one the note mentions.

**The x range and the label anchoring interact — set the range first.** Anchoring the first tick label
left will collide with its neighbor while the range still carries padding, because padding compresses
the left end; pinned to the ticks, the same anchoring has room (measured: 6.7px clear on desktop,
25px on mobile). So when an anchor "doesn't fit", re-check it after any range change instead of
concluding it can't be done — and measure the gap rather than eyeballing the render.

- **Nested bands get precomputed flat tints, not alpha.** One `BAND_ALPHA` deepening where the bands
  overlap looks like it gives the fan for free, but an alpha fill has to composite onto *something*,
  and the canvas is deliberately transparent so the template supplies the background — so the fan
  ends up depending on whatever sits behind the SVG, and vanishes when the white patch goes. Blend
  each band towards white yourself; it renders identically on any backdrop and hands Figma one flat
  fill per band.
  ```python
  def tint(color, weight: float) -> tuple[float, float, float]:
      """Blend `color` towards white; weight 0 keeps it, 1 is white."""
      return tuple(c + (1.0 - c) * weight for c in to_rgb(color))
  ```
  If a key does show swatches, they must carry the **same** tints in the **same** order, or it reads
  inside out against the chart. (With alpha the equivalent trap was a swatch showing the per-band
  alpha rather than the cumulative `1-(1-a)**(i+1)`.)
- Reference lines labeled *on the plot* get a **bold title above a regular-weight value** via
  `AnnotationBbox` / `TextArea` / `VPacker`. Where an encoding diagram already names the line, label
  it there instead and drop the inline copy — two labels for one line leave the reader looking for
  the difference between them.

### Explain the encoding with a diagram, not a legend

When the encoding is *structural* — nested bands, a threshold inside a band, a line whose position
within a shape carries meaning — draw a miniature of the real thing and name its parts, instead of
listing swatches. A legend asks the reader to carry a color across the frame and find the shape it
belongs to; an exemplar shows the structure directly, in the order it appears.

**Shape the miniature like the chart, not like a swatch.** A flat slab still asks the reader to map a
rectangle onto a rising ribbon; a miniature that curves the way the real marks curve is recognised
without that step. Draw it as a **schematic, not a data slice** — at the real proportions the marks
are a few pixels apart where the labels have to attach (3–5px at the last age, on this chart), so widen
the bands until the parts separate, and keep only the relationships that carry meaning (which band is
inside which, and where the threshold falls between them).

Conventions that make one legible:

- **A span gets a bracket; a line gets a label.** A tick pointing at a band's boundary invites the
  reader to take that boundary as the thing being named. Bracket the band's full height instead.
- **Nested brackets: put the *bigger* one nearer the marks.** Both bands share a midpoint, so the two
  brackets sit at different x, and the labels have to clear whichever bracket is further out. With the
  small bracket nearer, the label for it must start right of the big bracket — landing in the same
  column as the other label, which is exactly what makes a reader think both point at one bracket.
  Big-first, each label sits immediately beside its own bracket at its own x. Label the big bracket at
  its **top arm** and the small one at its **middle**.
- **A leader only where a label cannot sit next to the thing it names.** Adjacency reads faster
  than a line to follow, and it is one less thing to route around. A bracket plus a separate leader
  reads as scaffolding — two lines meeting at a corner where the cap already turns the other way — so
  if a leader is unavoidable, make it one continuous polyline with the bracket rather than a second
  stroke, and take the geometry as the fix rather than the weight: "not clear enough" about a leader is
  usually about its shape, and making it heavier makes it worse.
- **Moving a label buys width.** Beside a shape a label is boxed in by whatever else is beside it;
  below the shape it usually has the full frame. That is often the difference between one row and
  three — and three rows of explanation reads as a paragraph, not a label.
- **Lead with the plain meaning, keep the technical term.** `Stunted: too short for their age`, not
  `Stunted below here (2 SD below median)` — the jargon is what the chart exists to explain, so
  putting it first explains nothing.
- **Draw the key in gray**, even when the chart is colored: gray is what marks it as explanation
  rather than a further data series. Keep the tints and line styles identical to the chart's.
- **Get the schematic's internal proportions right.** If a threshold really falls inside a band, the
  diagram must show it inside — derive its offset from the same z-values the data uses rather than
  eyeballing the position.

### Keep the desktop and mobile versions paired

The versions get published together, so they should differ only where the template forces it —
frame, tick count, which footer rows exist. Two different explanatory devices reads as two
different charts.

When a device does not fit inside a narrow mobile panel, **move it rather than substitute it**: the
header row spans the full content width, which is more than twice a side-by-side panel's width, and
a wide-and-short block fits there comfortably. Give it its own frameless axes
(`fig.add_axes(...)`, `set_axis_off()`, `patch.set_visible(False)` so the transparent SVG stays
transparent) and reuse the same drawing code.

Three traps when the same drawing serves two hosts:

- **Fractional offsets do not survive a change of host.** A `0.03` gap is 10 px in a 347 px panel
  and 2 px in a 76 px strip. Any offset that has to look the same in both must be a parameter.
- **Text does not scale with the box.** Shrinking a container shrinks its shapes but not its
  absolute-point-size labels, so a block that fits at one size overflows at another.
- **A wide, short host flattens the drawing, and height is what fixes it.** The same miniature that
  read as a curve in a near-square panel came out 142px wide against 72px of ink in a 92px header row
  and lost the shape it exists to convey. Compare the host's aspect against the one the drawing was
  tuned for, and buy the height from **inside** the block's own budget — where the block's top and
  bottom are both fixed, a taller key costs plot height and leaves the frame's fit untouched.

### Text slots — take them from the template

Read [`TEMPLATES.md`](../TEMPLATES.md) for the geometry. Fill the template's slots, in its order,
with its labels (`Note:`, `Data source:` — singular — the exact tagline and license strings).

**The mobile templates have no `Note:` slot and no tagline.** That forces a decision per caveat:

- A caveat about a **visual artifact** can go, if the artifact is sub-pixel at mobile size. Check
  the arithmetic rather than assuming — a 0.7 cm step on a 160 cm axis over 450 px is under 2 px,
  so there is genuinely nothing left to explain.
- A caveat about **what the chart claims** cannot go. Move it into the subtitle, which mobile does
  have. Dropping it silently reintroduces an over-claim.

### Derive every string from the data

Crossover ages, discontinuity positions, the source citation built from `col.metadata.origins` —
all computed, none typed. That is what makes the step survive a data update without hand edits.

The corollary: **a wording change reflows the layout**, so re-read the rendered PNG after any text
change, not just after a geometry change.

**Labels that sit side by side belong in one register, and a count has to survive arithmetic.** Naming
one band `8 in 10 children` and its neighbor `Almost all children` mixes a count with a qualitative
phrase, and the reader is left asking whether the two are the same kind of statement — so make both
counts. Then check the count against the percentiles the band is actually drawn from: the 0.1st to the
99.9th holds 99.8%, which is **998** in 1,000, not the 999 that "almost all" rounds to. A round-looking
wrong number is worse than either the accurate awkward one or the honest qualitative phrase, and it
survives review precisely because it looks like a summary rather than a claim.

**A series that traces another for most of the range is redundant, not informative.** Repeating one
panel's median in the other, to show a crossover, put a second line within a few millimetres of the
first from birth to age 9 — re-drawing the same information across two thirds of the chart, which is
the doubling that splitting the panels was meant to remove. Prefer stating the fact in the subtitle and
letting the reader compare panels at a shared gridline. Check any "for context" series this way: plot
the two and measure where they actually diverge before deciding it earns its ink.

### Labelling a chart with many categories

Four decisions, each settled by measuring rather than by taste:

- **A value label belongs to a category, not to a row.** Print one wherever it happens to fit and the
  categories that fit on only a few rows read as facts about *those* rows rather than as "the others
  were too narrow". Draw a category's values only where some share of rows can hold one, and otherwise
  none of them. Measure the share per frame — a narrower frame drops more categories.
- **Put each category's names inside its own bracket, one per line.** Every name then sits over the run
  it belongs to, and a residual bucket can shorten to `Other` because the bracket already supplies the
  category. Derive that short form rather than renaming the groups, so the full name survives for
  layouts where a group stands alone. Stack them all even where one would fit on a single line; a
  mixture reads as several different treatments.
- **Labelling every segment in place, with leaders, is rarely worth what it costs — and whether it is
  even possible is a property of the data.** The row you point at decides it, and no amount of vertical
  room rescues a row whose narrowest segment leaves no corridor, because the obstruction is horizontal.
  If you attempt it, place the hardest label first and expect the dataset's narrowest segment to be the
  binding constraint. (Built in full for the time-use refresh, then deleted: it read no better than
  brackets.)
- **Rank rows by something the reader can verify.** A key whose segment is a few pixels wide cannot be
  checked against the chart, ties a large share of rows once rounded, and imports whatever survey
  artifact that category carries. Rank by a wide category, keep the key in one constant so the
  alternative is a one-line change at review, and make the sort **stable** so tied rows hold their order
  instead of churning the output between runs.

### Anchor and baseline every label the way it has to survive Figma

A step's labels are placed once, in matplotlib, and then re-rendered a second time in Figma in a
different font. Three habits decide whether they survive that, and none of them shows up in the
step's own render:

- **Anchor a label where it must stay, never where it happens to start.** `text-anchor: middle` and
  `end` survive the re-render; a left-anchored run does not — its box keeps its x while the glyphs
  shrink, so the label creeps out of the thing it labels and the gaps after it grow. Centre what is
  centred (a value in its segment, a name over its bracket), right-anchor what lines up on an edge.
  The step draws them in the same place either way, so this costs nothing at the time and is
  invisible until someone re-renders.
- **A run's trailing space belongs to the layout, not to its glyphs.** Advance the cursor by the full
  advance, but centre and draw the *stripped* text: SVG (and Figma) centre the trimmed ink, so a run
  centred with its space lands half a space off — which is how a separator dot ends up hugging the
  name after it. (And a run may never *begin* with a space; see TEMPLATES.md.)
- **Put a row's text on an explicit baseline, not on `va="center"`.** Centring uses the font's whole
  line box, which reserves room for descenders that digits never use, so a value sits about a tenth
  of a bar high — small enough to survive review and plainly visible once someone looks for it. Place
  the baseline half a cap-height below the row's centre (`TextPath((0,0), "0").get_extents().ymax`),
  which also puts labels with and without descenders on one line.
- **Draw a multi-line label as one call per line.** A `"Total\nleisure"` passed to a single `ax.text`
  gets no `text-anchor` at all: matplotlib centres each line by baking a different `translate` into
  it, using its own metrics. So the lines arrive as independent left-anchored boxes that lose their
  centring on each other *and* on whatever they label as soon as the font changes — the one case the
  anchor rule above cannot rescue, because there is no anchor to preserve. Grep an emitted SVG for
  `text-anchor` and any label missing one is this.

### Size a value column from its content, not from a round number

A column of numbers beside the bars is separated from them by exactly the slack in its width, so a
column sized generously reads as the numbers drifting away from the chart. Derive it: widest label
plus one gap. Spell the unit out where that still leaves the column a small share of the frame, and
drop it where it would not — on a narrow frame the bars need the room more than the reader needs the
unit on every row. Deriving it also deletes a per-layout constant that nobody would have thought to
re-check.

### Assertions

Assert the claims the chart makes, not only the schema. If the subtitle states a range as one
span, assert it *is* one contiguous span — otherwise two disjoint windows collapse into a single
wide claim that includes the region where the opposite is true. If a series is spliced, assert the
number and position of the discontinuities.
