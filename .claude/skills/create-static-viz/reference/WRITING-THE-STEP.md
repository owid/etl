# Step 4 — Write the `viz://static` step

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

Path `etl/steps/viz/static/<namespace>/<version>/<short_name>.py`, DAG entry in
`dag/static_viz.yml` (a flat `steps:` map, one comment line per step, keyed by the full
`viz://static/...` URI with its garden/grapher deps as the value).

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
  Use `<subject>__<role>` so the names sort into groups. The names are also what Figma's re-import
  (`restyle_static_import.js` with `inheritStyles`) joins on to carry a designer's style bindings
  onto a fresh render, so an unnamed mark loses its binding. `export_frame` names each axes' own
  furniture (gridlines, ticks, tick labels, spines, axis labels) by tick value wherever the step has
  not, which leaves only what the step draws itself, and those it must name. **Verify with
  `--require-named-marks`**, which fails on any painted mark under no `<subject>__<role>` gid and
  names its matplotlib id (`text_7`), so the artist to fix is the one it lists; it also fails when
  two artists share a `<subject>__<role>` gid, since the re-import cannot tell them apart (a `gid=`
  passed to `ax.bar` lands on every bar, so name each bar from the returned container). Pass the
  data layers as well (`--expect-gid boys__median --expect-gid girls__median`): the naming check proves every mark
  has *a* name, the expected gids prove the data layers have the *right* one.
- **Do not pass `bbox_inches="tight"`** when the frame must match a template — cropping to content
  changes the proportions, which is the whole thing the template fixes. `export_fig` already
  injects `metadata={"Date": None}` on the SVG pass for reproducible diffs.
- **Lay out at the template's size, unless a slightly shorter or taller frame helps the chart** — say,
  when the house gaps would stretch the plot taller than the data needs, trim the frame rather than the
  gaps. Keep the template's width, change the height by tens of pixels rather than hundreds, and say why
  in the layout's comment. Declare the height in both checks: `export_frame(..., template=<name>,
  height_px=<H>)`, which holds the figure to the template's width and scale at that height, and
  `--size <stem>=<W>x<H>` for the verifier (SKILL.md Step 5).
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

### Measure in the font you draw in, and set the style before the first measurement

`FontProperties()` with no family resolves `font.family`/`font.sans-serif` — and seaborn's `set_style`
rewrites that list to an Arial-first one. So a step that calls `set_style` inside its build function,
after the title and subtitle have been wrapped, measures matplotlib's **DejaVu** default and then draws
**Arial**. The two are ~15% apart, which is far more than any slot allowance and points the wrong way:
strings look wider than they will be set, so a slot that fits is wrapped early and the fix looks like
"the allowance should be 1.0".

Name **two** stacks, because two different things read them, and they want different answers:

```python
# Lands in the SVG's `font-family`, verbatim: matplotlib copies the rcParam out rather than writing
# the font it resolved. Naming Lato first is a request to whoever OPENS the file.
EMITTED_FONT_STACK = ["Lato", "Arial", "Helvetica", "sans-serif"]
# What this step measures and draws with. Deliberately does NOT name Lato, which is not installed on
# our machines, so it is not a font this step could measure in — only one it can ask for.
MEASURED_FONT_STACK = ["Arial", "Helvetica", "DejaVu Sans", "Liberation Sans", "sans-serif"]

matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
# Drop the per-face misses for faces you deliberately list as alternatives, and NOTHING else.
_OPTIONAL_FACES = tuple({*EMITTED_FONT_STACK, *MEASURED_FONT_STACK})
logging.getLogger("matplotlib.font_manager").addFilter(
    lambda record: "Falling back" in record.getMessage()
    or not any(f"Font family '{face}' not found" in record.getMessage() for face in _OPTIONAL_FACES)
)
```

**`font_manager` has two warning sites, and only one of them is reachable from a `findfont()` probe.**
This matters because probing the wrong one leads to both available wrong answers:

| Site | Message | Fires when |
|---|---|---|
| `_findfont_cached` | `... not found. Falling back to DejaVu Sans.` | a family list resolves to **nothing**, via `findfont()` |
| `_find_fonts_by_props` | `Font family 'X' not found.` | text is **drawn** with an explicit family list — once per missing face, even when a later face answers |

Measured on macOS (matplotlib 3.10.9, Lato absent, Arial installed): `findfont()` on either stack
emits **zero** warnings, and rendering with the rcParams stack emits zero as well — but rendering text
through `FontProperties(family=MEASURED_FONT_STACK)`, which is what a step's own measurements do,
emits one per missing face. A single step run can produce hundreds of them, for example for
`Liberation Sans` — a face that earns its place, being the metric-compatible Arial substitute on Linux
and absent on a Mac. So the noise is real; a blanket `setLevel(logging.ERROR)` is still wrong, because
it takes `Falling back to DejaVu Sans` with it, and that one says a stack failed and every measurement
just moved ~15% against what gets drawn.

**Then assert the invariant, because no filter can protect it.** Silence a declared face and you have
also silenced the case where *every* face of a stack is declared and missing; and Lato-first has its
own trap — a machine that HAS Lato installed draws Lato while the measured stack still resolves Arial,
and nothing warns at all. What the allowances actually depend on is one line:

```python
_DRAWN_FACE, _MEASURED_FACE = (
    findfont(FontProperties(family=EMITTED_FONT_STACK)),
    findfont(FontProperties(family=MEASURED_FONT_STACK)),
)
assert _DRAWN_FACE == _MEASURED_FACE, f"draws {Path(_DRAWN_FACE).name}, measures {Path(_MEASURED_FACE).name}"
```

It passes on a Mac without Lato (Arial/Arial) and on a Linux box with neither Arial nor Lato
(DejaVu/DejaVu — different face, still self-consistent), and fails loudly on the two drifting machines
above. Prefer it to reading logs: a warning nobody reads is not a check.

Pass the measured stack to every measurement, so measuring and drawing cannot drift whatever a
machine has installed:

```python
prop = FontProperties(family=MEASURED_FONT_STACK, size=fontsize, weight="bold" if bold else "normal")
```

**Set the emitted stack again after seaborn**, inside the build function — `set_style` REPLACES
`font.sans-serif` with its own list, so the module-level assignment above is overwritten and the step
emits a stack it never chose (seaborn's `'Arial', 'DejaVu Sans', 'Liberation Sans', 'Bitstream Vera
Sans'`):

```python
sns.set_style("ticks")
sns.set_palette("deep")
matplotlib.rcParams["font.sans-serif"] = EMITTED_FONT_STACK
```

Naming Lato first costs nothing and buys a lot downstream: **Figma renders the import in the
template's own typeface on arrival**, so the parked reference copy looks like the deliverable, and
`/owid-staff:create-figma-chart`'s font pass — plus the anchor pass that exists only to undo that face change —
becomes a no-op. Without it Figma resolves none of `Arial, Helvetica, DejaVu Sans` and substitutes
**Inter**, which is wider — enough for an 850-wide chart to overrun its canvas by tens of px. Verify it rather
than assuming — the emitted stack is one grep, and the faces the import lands in are one read:

```bash
grep -o "font-family: [^;\"]*" <file>.svg | sort -u   # want 'Lato' first
```

Changing the stack must not move anything: the drawn font is unchanged, so the SVG should differ from
its predecessor **only** in `font-family`, and the PNG not at all.

Then check which font `findfont` actually returned before trusting the per-face allowances in
[TEMPLATES.md](../TEMPLATES.md) — they are per installed font.

### Style, and where it stops

- **seaborn** `set_style("ticks")` + `set_palette("deep")`, and reference colors by **palette
  position** (`palette[0]`, `palette[1]`) rather than pinned hexes, so the chart moves with the
  shared palette. seaborn is a `dev` dependency.
- **Every stroke width in whole template pixels, written as `px * 0.72` points** — 3px for a series
  line, 2px for a secondary or dashed one, 1px for gridlines, ticks and leaders. A width chosen in
  points lands fractional in Figma (`linewidth=1.0` is a 1.39px gridline, 2.6pt a 3.61px line), and the
  design team snaps them by hand. A dash is in multiples of the width, so a whole-pixel width with
  integer dash multiples (a 1px gridline at `(4, 4)`) keeps the dash on whole pixels; a dash fixed in
  points, divided by the width, stays fractional.
- **A band shaded around a line is the line's own colour with `alpha`, not a tint blended toward
  white.** Figma can then bind the band to the line's library style at reduced node opacity (the design
  team uses 0.2), which a hand-mixed hex cannot be.
- **Draw every label at full opacity, even where its marks take `alpha`.** Bars may run at grapher's
  0.8, but a name set in its category's colour has far less surface area than its bar, so the same
  reduction costs it legibility. Give `alpha` to the fills only, and measure a label's contrast against
  its own, unreduced colour.

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
that doesn't exist is weak evidence about what the chart *looks like*: it suggests grapher draws no
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
when the ticks were zero-length, which can put the axis label's descenders into the note. And facet titles
above the plot need `font size + 0.5 line` of their own, taken off the top of the plot rather than out
of the frame. Re-measure both reserves after adding either.

**When a tick label crowds its neighbor, drop the tick.** That is what grapher does — it hides
overlapping labels rather than shrinking or rotating them — so a tick set is a per-layout choice, and a
tick whose label sits closer to its neighbor than the rest of the axis is a tick to remove, even when
the value is one the note mentions.

**The x range and the label anchoring interact — set the range first.** Anchoring the first tick label
left will collide with its neighbor while the range still carries padding, because padding compresses
the left end; pinned to the ticks, the same anchoring has room. So when an anchor "doesn't fit", re-check it after any range change instead of
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
  inside out against the chart. (With alpha the equivalent trap is a swatch showing the per-band
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
are often only a few pixels apart where the labels have to attach (3–5px), so widen
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
- **A wide, short host flattens the drawing, and height is what fixes it.** A miniature that
  reads as a curve in a near-square panel can come out flattened in a wide, short header row and
  lose the shape it exists to convey. Compare the host's aspect against the one the drawing was
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
- A caveat too long for the subtitle can **stay a Note, drawn as the footer's first row**: one call
  per line at the footer rows' pitch, its last line box one footer row gap above `Data source:`, and
  the band ending at its top, with as much air below the last mark as above the first. In Figma it then
  moves into the footer's auto-layout (`/owid-staff:create-figma-chart`'s GOTCHAS.md).

**Write a caveat in the chart's own categories, and check it against the producer's mapping.** A
country note copied from the source's terms ("counts as unpaid work") can turn false once the chart
merges or renames categories. Read where the source actually files the data (its activity or category
mapping sheet, not a summary), then say where it lands among the categories the reader sees. And check
whether it moves anything the reader can see: a quirk that leaves the affected bar within the range of
its peers is better covered by a general comparability sentence than by naming one country.

**Leave off an entity the producer flags as not comparable, rather than footnoting it,** when it would
otherwise rank prominently. A footnote on the second-ranked row still lets the bar make its claim.
Drop it in the viz step, not in garden, since the data stays valid for other uses, and assert it is
present before dropping, so a rename upstream fails loudly.

**Say when an average includes people or days with none of the activity.** A per-person average over a
whole population and all seven days reads, unexplained, as the typical participant's figure ("only 3
hours of paid work"). If the Note has room for one more sentence, this is the one that prevents the
most common misreading.

**The step and the template will not agree on how many lines a string takes, and the step is the one
that reserves the space.** The step measures its own type — 10.5pt, say — while the template renders
the same string at the slot's size, 16px for a subtitle on the 850- and 540-wide templates. Those wrap
differently, so the line counts can differ, and the step is reserving the chart area against the wrong
one. It cannot be fixed from the step: it deliberately sets no fonts, so it cannot measure Lato.
The slot's width matters as much as its size: under a one-line title the subtitle slot is cropped to
clear the logo (731 / 420 px, TEMPLATES.md), so wrap at that width, not the content width. Count the
title's lines in the template, not the step, and wrap at the cropped width until that count is known.

What this costs is a surprise on the *next copy edit*, not on the first build. Adding a single
character — an Oxford comma, say — can tip a mobile subtitle from three lines to four in the
template while the step still reserves three, and the chart's topmost label then sits above the
header's last line. So after **any** change to a title, subtitle or note, re-read the
template slot's height in Figma rather than trusting the step's own wrap, and check the header
clearance on the frame.

The firmer fix, once the frame exists: read where the Figma header ends off the frame and place the
plot against that (`chart_area_top`, and the footer's first row below), with the house gap on each
side, rather than against the step's own header. Then break the step's title and subtitle where the
frame does, so the step's PNG shows the spacing that ships instead of the extra gap its narrower faces
leave.

Two things make that cheap. The step should reserve the *larger* of the two counts where they differ,
so the failure mode is a generous gap rather than an overlap. And an orphan — a last line holding a
word or two — is worth removing at the same time, because it is both a copy defect in its own right
and the state one character away from adding a line: measure it by cloning the slot, setting
`textAutoResize = "WIDTH_AND_HEIGHT"` to get the unwrapped width, and comparing against the slot's.

**Break a wrapped subtitle into lines of about equal length, and give the frame the same breaks.** Wrap
to the fewest lines the slot allows, then narrow the wrap width as far as that line count holds. A
greedy fill leaves a long first line over a short last one; even lines read as one block and leave room
beside them for a key to rise into. A sentence boundary outranks balance: when a subtitle holds two
sentences, start the second on a new line wherever that costs no extra line ("…according to the World
Health Organization. / Children more than 2 standard deviations…", not "…Organization. Children / …").
Set the frame's subtitle to the same text with explicit `\n`, so Figma's own wrap cannot move the breaks.

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
panel's median in the other, to show a crossover, can put a second line within a few millimetres of
the first for most of the range — re-drawing the same information across most of the chart, which is
the doubling that splitting the panels was meant to remove. Prefer stating the fact in the subtitle and
letting the reader compare panels at a shared gridline. Check any "for context" series this way: plot
the two and measure where they actually diverge before deciding it earns its ink.

### Labelling a chart with many categories

Four decisions, each settled by measuring rather than by taste:

- **A value label belongs to a category, not to a row.** Print one wherever it happens to fit and the
  categories that fit on only a few rows read as facts about *those* rows rather than as "the others
  were too narrow". Draw a category's values only where some share of rows can hold one, and otherwise
  none of them. Measure the share per frame — a narrower frame drops more categories.
- **Over a stacked bar, set each category's members as one run-in list above its name, with the name
  nearest the bars.** The name, in its category's colour, is what ties the block to its segment, so it
  sits closest to it, and the list above reads as its gloss. Anchor each block where that segment's
  values are anchored (at the segment's start plus the value inset; the last block right-aligned at the
  bar's end), so the key and the values line up in one column per category. Keep a designer's line
  breaks as data in the step and assert that they still spell out the members, rather than re-wrapping
  them. Keep it to one `Other` per key, though: two, under different categories, read as the same
  bucket. Where a category's own name already says "& other", drop its residual from the list rather
  than renaming the other category's.
- **Size a header band by its tallest block, not by adding the maxima of its parts.** Where each
  category's name sits on its own list, bottom-aligned over the bars, the band is the largest
  list-plus-name of any one category. Adding the deepest list to the tallest name counts two
  different categories as soon as those maxima fall in different blocks, which an edit that shortens
  one list (dropping a member) brings about without anything else changing, and the band then carries
  a line of air above every heading. A block that clears the subtitle sideways may rise to the header's
  bottom edge, beside the subtitle's shorter lines, instead of starting below the band's inset.
- **Set an inline member list in sentence case, and keep a capital per line in a stacked one.** A list
  run together after its category name ("Paid work, commuting, homework") is a phrase, and the style
  guide sets phrases in sentence case. Stacked one per line, each name is a label and keeps its
  capital. So a desktop key and its mobile row legend can differ here, by design. Abbreviations
  ("TV") keep their capitals in either.
- **Separate stacked segments with a 1px gap of canvas.** The gap carries the boundary, so neighbouring
  fills no longer need to differ in lightness, and a grayscale-seam floor applies only where segments
  touch. Shorten every segment but the last by the gap, so each still starts at its true value. Skip
  a segment narrower than the gap plus 1px (a zero, say): shortened, it gets a negative width and
  Matplotlib draws it backwards over its neighbour.
- **Align in-segment values to the segment's start, not its centre.** Left-aligned at a fixed inset
  (8px on the 850-wide templates, scaled with the frame's width on narrower ones), and the last
  segment's right-aligned at the bar's end, the values form a column per category. Centred values
  wander with each segment's width. Test the fit against the segment minus the inset.
- **Leave more air between bars than a tight stack, and buy it with height, not with thinner bars.**
  Bars at about 70% of their pitch read as separate rows. Run the frame taller (`export_frame`'s
  `height_px`, see the Figma handoff contract above) so the bars keep the thickness their in-bar values need.
- **Set a row label that carries a year as the name in bold and the year in regular.** Where a row's
  pitch holds two label lines, put the year under the name: the label column then sizes to the wider of
  the two, which widens the plot. On one line, drop the brackets: the weight already separates the two,
  and right-aligned, the years line up in a column.
- **Labelling every segment in place, with leaders, is rarely worth what it costs — and whether it is
  even possible is a property of the data.** The row you point at decides it, and no amount of vertical
  room rescues a row whose narrowest segment leaves no corridor, because the obstruction is horizontal.
  If you attempt it, place the hardest label first and expect the dataset's narrowest segment to be the
  binding constraint. (Built in full, it typically reads no better than brackets.)
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
  centred, right-anchor what ends on an edge, and left-anchor only what starts on one (a value at its
  segment's start, where keeping its x is the point).
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
- **Reserve a block that opens or closes the band from its ink, not its line boxes.** The band's inset
  is meant to be what a reader sees, but a block reserved as `rows × line height` and centred row by row
  starts half a line box minus half a cap height (or half a swatch) below where its space begins, so
  every frame it sits on gets that much extra air. Start the block where its first row's ink starts, and
  take the same amount off its reserved height. The same holds for a stacked header measured to its
  tallest line box.
- **Draw a multi-line label as one call per line.** A `"Total\nleisure"` passed to a single `ax.text`
  gets no `text-anchor` at all: matplotlib centres each line by baking a different `translate` into
  it, using its own metrics. So the lines arrive as independent left-anchored boxes that lose their
  centring on each other *and* on whatever they label as soon as the font changes — the one case the
  anchor rule above cannot rescue, because there is no anchor to preserve. Grep an emitted SVG for
  `text-anchor` and any label missing one is this. Name the lines `<gid>`, `<gid>-line1`, … so the frame
  can join them back into one text: `/owid-staff:create-figma-chart`'s `group_text_blocks.js` does
  that after the import.
- **Space a wrapped label's lines at Figma's own pitch.** A label typed in Figma in 14px Lato runs at
  17px a line (line height AUTO), while matplotlib's default line spacing lands imported lines about
  15px apart, so a re-import never matches a designer's typed label. With one call per line (above),
  place the lines 17px apart.

### Porting a designer's rework

A designer who reworks the Figma frame has edited a copy the next re-import will overwrite, so each
decision has to move into the step or its docstring:

1. Diff their copy against your frame with `/owid-staff:create-figma-chart`'s `scripts/diff_frames.js`
   (`referenceId` their copy, `frameId` yours) to list the decisions.
2. Port each one: geometry, weights, sizes and wraps into the step; bindings, halos and house arrows
   into the docstring's Figma recipe.
3. Re-render, re-import and diff again, until only the deltas you chose remain. Record those in the
   docstring.

What the step cannot emit goes in the docstring's Figma recipe, not in a note to redo it by hand. Text a
designer grouped into auto-layout frames (a list and its name, a legend row) is one such case: record
it as a `group_text_blocks.js` CONFIG, run after every re-import.

Compare ink, not boxes. Uneven visible gaps over an evenly placed chart group are padding left inside
the group, not a spacing choice; keep the house spacing and ask the designer before porting them.

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
