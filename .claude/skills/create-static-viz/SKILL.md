---
name: create-static-viz
description: Build or refresh an OWID static visualization end to end — resolve what data it needs from an old static viz image, an indicator, or a grapher chart; check both the ETL catalog and the producer's own site for a newer release and route to /create-dataset or /update-dataset when one exists; write the export://static_viz matplotlib step that emits Figma-ready SVG and PNG at the static-chart templates' proportions; then hand off to /create-figma-chart. Trigger when the user asks to "refresh this static viz", "remake this chart as a static image", "create a static viz", pastes an old static viz image or filename and asks for a better version, or picks up a viz from the static-viz refresh queue.
metadata:
  internal: true
---

# Create or refresh a static visualization

Joins the three halves of a static-viz refresh that are otherwise separate: getting the data into
ETL at a current vintage, drawing it in an `export://static_viz` step whose SVG a designer can
actually pick up, and getting that SVG into the Charts file.

> **Paired skill — keep in sync.** [`/create-figma-chart`](../create-figma-chart/SKILL.md) owns
> everything that happens inside Figma, and this skill hands off to it at Step 7. If you change
> what the ETL step emits — node naming, frame proportions, which text slots it fills — check
> whether that skill's Step 1/3/7 notes on local SVGs still hold.

Two companions in this directory:

- **[`TEMPLATES.md`](TEMPLATES.md)** — the static-chart template geometry, measured off the Charts
  file. Read it before laying anything out; don't re-derive it through MCP calls.
- **`scripts/verify_static_viz.py`** — mechanical check that the emitted files honor the Figma
  handoff contract. Run it before showing anyone the chart.

**What this skill does not decide:** colors, fonts, background, the logo, and any visual treatment
the template provides. Those are applied in Figma. The ETL step owns the *data*, the *structure*
(which text slots exist, in what order), the *proportions*, and the *axis conventions*. Setting a
cream background or a serif title in matplotlib is work that will be thrown away.

## The project's rules

Policy, quoted from the parent issue (`owid/owid-issues#2459`, and identically in the earlier
`#2278`). These are not this skill's inventions and are not negotiable here.

**Scope.** A refresh is either a *data update plus light visual tweaks* (the common case) or
*visual polish only* (when the data is already current).

> We should not do deep redesigns. Aim to reproduce the same visualization with the same broad
> design choices.

So the default is to rebuild the same chart, better — not to redesign it. **When the existing
design has a real defect** — an encoding that misleads, overlapping translucent fills that produce
a color meaning nothing, a caption that misstates what the data shows — a departure can be the
right call. But it then stops being a refresh, and needs naming as such: say plainly that it
exceeds the scope rule, say which defect justifies it, and get design sign-off on the departure
rather than letting it pass as polish.

**Quality bar.**

> - All numbers match the source.
> - As much as possible, make visualizations reproducible so future updates are easier (and transparent).
> - Design consistency: Marwa should sign off on every new static viz.
> - Mobile: discuss with Marwa whether a mobile version is feasible; otherwise, make the desktop
>   version as readable as possible on mobile.

The second line is why this skill exists: an `export://static_viz` step *is* the reproducibility
requirement, met. The third is not optional — @mrwbkrm signs off on every one. The fourth means a
mobile version is a question to ask, not a default to assume.

**Pace.** Work in progress stays at roughly one or two open child issues. Don't start a new
refresh to avoid finishing one.

## Author credit

The license line carries the credit, and who appears on it depends on how much changed:

| What changed | Credit |
|---|---|
| Data updated, design broadly preserved | **the original author, and whoever is doing the refresh** |
| Design changed considerably | **whoever is doing the refresh, alone** |

**"Whoever is doing the refresh" is the person running this skill — not a fixed name.** Resolve it
from `git config user.name` and confirm it at the Step 3 checkpoint, following the repo convention
of crediting the human directing the work and asking when it is ambiguous. Never carry a name over
from a previous run of this skill, or from the `AUTHOR` constant of another step you copied from.

The original author is not always recorded anywhere obvious; `static_viz_popular.csv` carries
`viz_authors` / `viz_authors_source`, and the old image's own footer usually states it. When the
call between the two rows is genuinely close, ask — it is someone's credit.

This is also a useful test of whether the scope rule above has been crossed. If the honest credit
drops the original author, the design changed considerably, and that is the case needing the
sign-off conversation rather than a quiet ship.

## Inputs

Any one of:

- **An old static viz** — the image, its filename, or the page it appears on.
- **An indicator** — a catalogPath, or a description of one.
- **A grapher chart** — live URL, staging URL, admin edit URL, bare slug, or chart id.
- **Just a description** of the chart wanted, plus a source.

Optionally: the article or topic page the viz belongs to, and a reference page in the Charts file
to work like (`/create-figma-chart` has a whole mode for that).

## Step 1 — Resolve the input to data

Reuse the existing resolver rather than writing another one:

```bash
.venv/bin/python .claude/skills/edit-faust-metadata/scripts/resolve_target.py <reference> [--json]
```

It takes a live/staging/admin URL, a bare slug, a chart id, or an indicator catalogPath, reports
the chart's variables **with their catalogPaths**, and names the candidate ETL step files —
including a warning when the grapher catalogPath's version differs from what is on disk.

**From an old static viz image**, two routes, neither of which the popularity CSV can do alone:

1. The grapher `static_viz` table carries a **`grapherSlug`** column. That gives a slug, and the
   slug goes through the resolver above. Note this table is **not** mirrored to the public
   Datasette, so query a staging DB or the local dev DB (see `/query-grapher-db`).
2. `ai/static_viz_popularity/static_viz_popular.csv` gives rank, the pages it appears on, views,
   authors and tags — useful context, but it has **no slug or indicator column**, so it cannot
   reach the data by itself.

If neither resolves it, ask. Guessing which dataset an old hand-drawn image was built from is how
you rebuild the wrong chart.

The project tracks which viz is claimed, parked or already done in a shared tracker, and this
skill deliberately does not read or write it — **ask instead**. If the user has not already said
where this viz stands, ask before doing any work: a viz someone else is mid-way through, or one
already finished, is worth an interruption rather than a duplicate.

Then report what you found: which dataset, which version, and how many charts use it.

## Step 2 — Check for newer data. Always, even when it is already in ETL

Two sides, and both are needed. The ETL side tells you whether a newer *version in our catalog*
exists; only the producer side tells you whether newer *data* exists at all.

**ETL side.**

```python
from etl.version_tracker import VersionTracker
df = VersionTracker().steps_df   # update_state, update_period_days, days_to_update, n_charts
```

`update_state` is one of `Unknown` / `No updates known` / `Outdated` / `Minor update possible` /
`Major update possible` / `Not yet used`. **`Outdated` means a newer version of that step already
exists in the DAG** — the viz is pointing at a stale vintage. Also read each garden dep's snapshot
`.dvc` for `date_published` and `date_accessed`, and the garden `.meta.yml` for
`update_period_days`.

Be clear about what this is: `days_to_update` is `step version date + update_period_days`, a
proxy. `VersionTracker`'s own docstring says so. It is DAG version arithmetic, not knowledge of
the producer's release calendar.

**Producer side — this is the part that needs the internet.** Fetch the origin's `url_main` and
search for the current release. Report what the producer publishes *now* against what the snapshot
captured.

> **Version labels prove nothing.** Producers replace the published file — and the codebook —
> without bumping a stated version and without a changelog. Compare the hosting platform's
> file-modification dates or hashes (an OSF API `date_modified`, an HTTP `Last-Modified`, a
> checksum) against the previous snapshot's `date_accessed` and md5. An unchanged version label is
> not evidence of unchanged data.

Two more traps worth carrying from `/update-dataset`:

- **`date_published` and the year inside `citation_full` never update themselves.** `etl update`
  clones the previous `.dvc` verbatim except `date_accessed`. Re-check both against the source.
- **Producer prose can lag the producer's own tables.** Trust the data over the landing page.

Then route, and **never do the ingest or update inline**:

| Finding | Action |
|---|---|
| Not in ETL at all | hand off to [`/create-dataset`](../create-dataset/SKILL.md) |
| In ETL, newer release exists upstream | hand off to [`/update-dataset`](../update-dataset/SKILL.md) |
| In ETL, `update_state` is `Outdated` | a newer ETL version exists — repoint the viz at it |
| In ETL and current | say so explicitly, and proceed |

"In ETL and current" is a real, reportable finding — say it rather than staying silent, so the
user knows the check happened.

## Step 3 — One checkpoint, before writing anything

Put the whole proposal in front of the user at once, and get an explicit go-ahead:

- the dataset and its vintage, plus what Step 2 found upstream
- what the viz shows, and how it differs from the image it replaces
- which template(s), and **desktop and/or mobile**
- **the author credit and the license line** — the templates leave
  `Licensed under CC-BY by the author [Name of author]`. Propose the name(s) per the Author credit
  rules above, resolving the refresher from `git config user.name`, and say which of the two cases
  you think applies — that is also the moment to say out loud if the design change looks big enough
  to have crossed the scope rule. Whether CC-BY is correct cannot be inferred either: a source
  under CC BY-NC-SA is not automatically redistributable as CC-BY, so ask rather than filling the
  slot.

This mirrors `/create-figma-chart`'s single-checkpoint rule, for the same reason: everything after
here is expensive to redo.

## Step 4 — Write the `export://static_viz` step

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

### Style, and where it stops

- **seaborn** `set_style("ticks")` + `set_palette("deep")`, and reference colors by **palette
  position** (`palette[0]`, `palette[1]`) rather than pinned hexes, so the chart moves with the
  shared palette. seaborn is a `dev` dependency.
### Borrow grapher's design language, read from its source

A static chart should read like our interactive ones, so take the axis, tick, facet and reference-line
treatment from grapher rather than inventing one. Read it out of `owid-grapher`, don't recall it:
`grapher/src/axis/AxisViews.tsx`, `axis/Axis.ts`, `facet/FacetChart.tsx`, `color/ColorConstants.ts`.

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

Read [`TEMPLATES.md`](TEMPLATES.md) for the geometry. Fill the template's slots, in its order,
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

## Step 5 — Render, verify, and look at it

```bash
.venv/bin/etlr export://static_viz/<ns>/<version>/<short_name> --private --export
```

**The `--export` flag is mandatory.** Without it the step silently does not match, and the error
says "No steps matched" while listing your step as the closest match.

**From a fresh worktree, give it its own `.venv` before rendering.** A worktree starts without one,
and borrowing the main checkout's is a trap: `etl` is installed there editable via a `.pth` holding
the *main* checkout's path, so `paths.BASE_DIR` resolves to the main checkout whatever your cwd is.
`etlr` then loads the main checkout's copy of the step and writes the PNG/SVG next to *it* — the run
reports `Finished`, your worktree's files never change, and that reads exactly like a no-op render.

```bash
make .venv                             # uv sync --all-extras --group dev; the pre-commit hook also does this
ln -s /path/to/main/checkout/data data # gitignored; the built deps only exist in the main checkout
.venv/bin/etlr export://... --private --export
```

Confirm before trusting a render: `.venv/bin/python -c "from etl import paths; print(paths.BASE_DIR)"`
should print the worktree, and the log's "Saved chart to" path should too. Compare output mtimes
against the step's own — an output older than the source means you are reading a stale file.

Editing the step's `.py` is enough to trigger a rebuild on its own, so you rarely need to force
anything. For the narrow case where nothing in the repo changed but you still need to re-run, use
**`--force --only`** — never `--force` alone, which would also re-run every upstream dependency.
`--only` is safe here because the deps are already on disk from the run you are repeating.

Then, in this order:

1. Run the verifier, and **always pass the data layers** — without `--expect-gid` the naming check
   only proves *some* node was named, which a figure with a named title and an unnamed line
   satisfies:

   ```bash
   .venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py <step-dir> \
       --template <name> --expect-gid <data-layer> [--expect-gid <data-layer> ...]
   ```
2. **Read the PNG.** The verifier cannot see a collision, a widow, or a label sitting on a curve.
   Every layout bug in this skill's Gotchas was found by looking.

## Step 6 — Iterate with the user

Show the render. When a design choice is genuinely open, **measure the options and offer the
numbers**, not adjectives — see the panel-aspect gotcha below for why.

## Step 7 — Hand off to `/create-figma-chart`

Give it the local SVG path. That skill's Step 1/3 cover the local-file route: there is nothing to
export, and none of the `.metadata.json` text sourcing applies because the text is already in the
file. Its `upload_assets` import is already file-based.

The one adaptation: its Steps 7–8 look up grapher's node names (`connectors`,
`horizontal-grid-lines`, `datapoints__<Entity>`). Ours are the `gid`s from Step 4 — hand over the
naming scheme along with the file.

## Step 8 — Record the Figma handoff in the step's docstring

Once the Figma page exists, write the handoff back into the step's module docstring. **The bar is that a
later session can redo the whole thing from this file alone** — a different person, months on, with none
of this conversation and no memory of the run. Not notes on what was done: a recipe.

That bar is the point of the step. Everything about the handoff lives in one of two places — this file,
or a session transcript that vanishes. Whatever is only in the transcript gets re-derived by trial and
error at the next data update, which is how the numbers drift and how a deliberate design decision
quietly becomes an accident. So write it down even when it feels obvious today.

Record, concretely:

- **Where.** File name *and* key, the page name and where it sits in the page order, each frame's name,
  and the template node id and size it was cloned from. Names, not just a link: a `node-id` is stable
  but says nothing about what to reproduce.
- **The import mechanics that are not obvious.** How the SVG gets in (`upload_assets` + POST, never
  `createNodeFromSvg`), that the wrapper frame is binned and why, and **the scale factor with its
  derivation** — plus its self-correcting form, so a reader can check the number rather than trust it.
- **Every text slot**: what fills it and which parts are bold. A table, because setting `characters`
  flattens the mixed weights the templates ship.
- **Any position that is derived** rather than taken from the template's fixed y, with the arithmetic.
- **Every color**, as its library style *name and key*, plus how anything derived from it (band tints)
  is computed. A hex alone is unreproducible — nobody can tell whether it came from the palette.
- **The in-plot restyle**: each type rank with its size and weight, and the anchor rule per label family.
- **The fit**, and whether a rescale is wanted (usually not, and why not).
- **The audit numbers to expect**, so the next run can tell success from a near-miss.

Two things that belong here and are easy to leave out. The **order** operations must run in where one
depends on another settling — widths settle on the next call; a coordinate patch after a fit uses anchors
the fit has already invalidated. And any **deviation accepted on purpose** — an off-ladder size, a
grayscale seam that does not gate — with its reason, so a later audit reads it as a decision rather than
a defect.

Write it in the imperative, as the step's own reference. If you catch yourself writing "we changed X",
rewrite it as "set X to Y, because Z": the reader wants to reproduce the state, not the history.

## Step 9 — PR and the review chain

The branch, worktree and draft PR already exist from Step 4. **Run `make check` before committing** —
the step is ordinary ETL code and has to be formatted, linted and typechecked like any other. Then
commit the step plus its committed PNG/SVG, push, and fill in the PR body — whose **first line is the
attribution blockquote**, `> _Written by Claude <model name> — @<handle> at the wheel._`, because the
body goes out under a human's identity. It is required on every comment you post to the PR afterwards
too, replies to the review included. Then [`/pr-babysitter`](../pr-babysitter/SKILL.md) for the Codex
round. **Brief the babysitter with the deliberate decisions** (see the last gotcha).

The code review is only the first of several. The project defines the rest, and they are people,
not checks — from `#2459`'s workflow, with the parts this skill touches in bold:

1. **Check for new data and identify where the image is used** — Steps 1–2 here, plus a
   [`/find-chart-references`](../find-chart-references/SKILL.md) sweep for every page that renders
   it. Static viz is classified `embed`, so **a URL redirect does not fix it**; each surface is a
   manual swap.
2. **Find dependencies** — other charts on the same page, or other charts on the same data, that
   would now disagree with the refreshed one.
3. **Consult the author** — required when the image sits on an *article*. Topic pages are
   evergreen: update directly, and update the surrounding text. For an article, whether to publish
   an updated version at all is the author's call.
4. Child issue opened (Bertha).
5. **Pull the data, rebuild, import to Figma** — Steps 4–7 here.
6. Review as needed (Bertha) → **design review with @mrwbkrm, mandatory** → final edits →
   final review with @edomt.
7. **Upload the refreshed image under a new filename** and repoint the references. The API rejects
   a duplicate name, and the old file must stay reachable for anything still pointing at it.
8. Accompanying text edited (Bertha and the author).

Report which of these are done and which are outstanding — the surrounding prose in particular
tends to be forgotten, and a corrected chart under an uncorrected caption is worse than neither.

Finally, **remind the user to set the viz's status in the tracker themselves**, and give them the
PR link to record against it. This skill never reads or writes the tracker — it is a shared team
database, the person running the refresh knows the state of their own queue, and a status is a
claim about human intent that an automated flow should not be making.

## Gotchas

**Data**

- **Verify what a column *means*, numerically. Never trust its name.** WHO's `P01` is the 0.1st
  percentile, not the 1st; reading it as "1st" is wrong by a wide margin at the tails. Reproduce
  every published column from the source's own parameters and assert agreement before using any of
  it. This cost nothing to check and would have shipped a wrong chart.
- **Discontinuities in source data get footnoted, never smoothed** — and asserted. "Exactly two
  backward steps, at exactly these positions, within tolerance" catches a botched splice
  immediately; a plausible-looking curve does not.
- **Check that the framing holds for every part of a spliced series.** A prescriptive standard
  spliced onto a descriptive reference is not "the healthy range" throughout. The snapshot
  descriptions may already say this while the combined metadata contradicts them.
- **An over-claim hides in more places than the reviewer points at.** One wrong phrase was in 26
  per-variable `description_short` fields plus the shared `description_key` plus the chart
  subtitle. Grep the whole surface.

**Layout**

- **Measure text width, don't estimate it.** Estimating from font size (a character ≈ half its
  point size) under-fills by about a tenth; a hardcoded character count was 27% short. Wrap
  greedily against `TextPath((0, 0), line, prop=FontProperties(size=fs)).get_extents().width`.
- **No text run may begin with a space.** `TextPath` measures ink, so a leading space adds nothing to a
  run's advance while matplotlib still draws it — lay runs out by summed advances and that run lands a
  space right of where the layout accounted for it. Keep the space on the end of the previous run; a
  *trailing* one is recovered by `text_advance_px`'s sentinel glyph.
- **A measurement harness must reproduce the chart's own font stack, or its numbers are fiction.**
  `sns.set_style(...)` inside the render function resets `font.family`, so a harness that imports the
  step without calling it measures a different typeface — enough of a width difference to make a
  placement solver "prove" a layout that does not fit. Apply the same style setup, and cross-check one
  number against a real build before trusting a sweep.
- **Panel aspect ratio decides whether a trend is legible.** Two panels stacked in a portrait
  frame give each a 2:1 landscape box, and a growth curve in that box looks flat. Turning them
  portrait gave 2.4× the vertical resolution and the shape appeared. Compute the panel box
  (`content_width`, `available_height / n_rows`) before choosing the grid.
- **A template's fixed y positions encode assumptions.** `subtitle_y = 80` is `16 + two lines of
  title`. Pin to it under a one-line title and you get a dead line.
- **A matplotlib legend extends downwards from its `bbox_to_anchor`**, and occupies more than its
  text height. Its anchor must clear the axes by its own full height plus a gap, or the swatches
  land on the plot. A gap calibrated for a legend's internal padding is too big for anything else,
  so re-derive it if the legend is later replaced.
- **A narrow panel cannot hold an inline reference label.** With clipping off it spills into the
  neighboring panel rather than being cropped — which looks like a rendering bug. Move it to a
  full-width row rather than a different device, per the pairing rule above.
- **Empty space in a chart has a shape, and a text block has to match it.** A rising curve leaves a
  triangle that widens downwards, so a block placed there must put its longest line last; a wide
  line high up runs into the steep part of the curve.
- **Test clearance against the filled span, not against one edge.** A band occupies
  `[lower(x), upper(x)]`; a label near a steeply rising band can be *above* it at one end and
  *below* it at the other, so "is the text above the lower edge?" answers the wrong question and
  reports collisions that are not there. Interpolate both edges over the label's own x range.
- **Do the clearance arithmetic in the script, not by eye.** Measure the label with `TextPath`,
  convert to axes fractions with the real panel box, and compare against the actual curve. Two
  rounds of guess-render-look is slower than one round of measuring, and it silently accepts
  near-misses.
- Fewer axis ticks in a narrow panel. Make the tick set per-layout, not global — and drop a tick whose
  label crowds its neighbor, which is what grapher does rather than shrink or rotate it.
- **A "no room for this" conclusion expires the moment the geometry changes.** Two things I ruled out
  on measurement — anchoring the first tick label inwards, and a one-line stunting label — both became
  possible after an unrelated change (pinning the x range to the ticks; moving the label below the
  slab). Record *why* something didn't fit, so the next geometry change is a prompt to re-measure
  rather than a settled answer to inherit.

**Workflow**

- **A dependency addition goes in its own PR.** Adding seaborn separately kept the viz PR
  reviewable and let the dependency land first. A step that imports an undeclared package turns
  the PR red for a reason unrelated to the work.
- **Pushes carrying committed PNG/SVG need a bigger HTTP buffer:**
  `git config http.postBuffer 524288000`. Without it the push dies with `RPC failed; HTTP 400` or
  `unexpected disconnect` — and a subsequent line can read `Everything up-to-date`, which looks
  like success. **Always verify the remote head after pushing** (`gh pr view <n> --json
  headRefOid`).
- **One worktree per task**, and delete the worktree and branch when the PR merges. `etl pr-clean`
  is the sanctioned tool but is an interactive picker with no non-interactive flag, so it hangs
  when scripted; replicate its steps by hand from the main repo.
- **Brief the review babysitter with the deliberate decisions.** Codex flagged a stale docstring
  and proposed reverting a layout the user had explicitly chosen after seeing measurements; an
  unbriefed agent agreed with it. List what must be **rebutted rather than fixed** — and when a
  reviewer catches a genuine inconsistency between a comment and the code, fix the comment.
- **Keep docstrings current when a design decision changes.** The above happened because the
  module docstring still said "stacked" after the layout became side-by-side. A stale comment
  invites a reviewer to "fix" working code.
- **Removing a design element orphans code, and the orphans are the whole point.** Dropping a legend
  left a label helper, a layout flag and three imports with no callers. `ruff` catches the imports;
  it does not catch a module-level function nobody calls or a config key nobody reads. Grep for each
  name you stopped using before calling the change done — and take the simplification, since dead
  branches are what make the next edit misfire.
- **Scripted edits need `assert old in s` and a uniqueness check.** A `str.replace` that matches
  nothing returns the string unchanged and reports success, so the render silently keeps the old
  behavior while you debug the wrong thing. For the same reason, never delete by slicing between two
  anchors without checking what lies between them: an unrelated helper sitting there goes too.
