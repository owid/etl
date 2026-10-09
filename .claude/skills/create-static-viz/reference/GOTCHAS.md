# Gotchas

> **Data is a required read at Step 1** — those checks fail silently, so no error ever sends you
> here. Layout and Workflow: read on an error, or grep by symptom.  Part of
> [`/create-static-viz`](../SKILL.md); the spine has the step order.

**Data**

- **Verify what a column *means*, numerically. Never trust its name.** WHO's `P01` is the 0.1st
  percentile, not the 1st; reading it as "1st" is wrong by a wide margin at the tails. Reproduce
  every published column from the source's own parameters and assert agreement before using any of
  it.
- **Discontinuities in source data get footnoted, never smoothed** — and asserted. "Exactly two
  backward steps, at exactly these positions, within tolerance" catches a botched splice
  immediately; a plausible-looking curve does not.
- **Check that the framing holds for every part of a spliced series.** A prescriptive standard
  spliced onto a descriptive reference is not "the healthy range" throughout. The snapshot
  descriptions may already say this while the combined metadata contradicts them.
- **An over-claim hides in more places than the reviewer points at.** The same wrong phrase can sit in
  every per-variable `description_short` field, the shared `description_key` and the chart
  subtitle. Grep the whole surface.

**Layout**

- **Measure text width, don't estimate it.** Estimating from font size (a character ≈ half its
  point size) under-fills by about a tenth; a hardcoded character count can be a quarter short. Wrap
  greedily against `TextPath((0, 0), line, prop=FontProperties(size=fs)).get_extents().width`.
- **No text run may begin with a space** — see [TEMPLATES.md](../TEMPLATES.md) → Two rules for laying
  out coloured text runs.
- **A measurement harness must reproduce the chart's own font stack, or its numbers are fiction.**
  `sns.set_style(...)` inside the render function resets `font.family`, so a harness that imports the
  step without calling it measures a different typeface — enough of a width difference to make a
  placement solver "prove" a layout that does not fit. Apply the same style setup, and cross-check one
  number against a real build before trusting a sweep.
- **Panel aspect ratio decides whether a trend is legible.** Two panels stacked in a portrait
  frame give each a 2:1 landscape box, and a growth curve in that box looks flat. Turning them
  portrait gives each more than twice the vertical resolution, and the shape appears. Compute the panel box
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
- Fewer axis ticks in a narrow panel. Make the tick set per-layout, not global (see
  [WRITING-THE-STEP.md](WRITING-THE-STEP.md) → dropping a tick that crowds its neighbor).
- **A "no room for this" conclusion expires the moment the geometry changes.** Something ruled out
  on measurement — anchoring the first tick label inwards, a one-line label — can become possible
  after an unrelated change (pinning the x range to the ticks; moving a label below a shape). Record *why* something didn't fit, so the next geometry change is a prompt to re-measure
  rather than a settled answer to inherit.

**Workflow**

- **A dependency addition goes in its own PR.** Adding a package (e.g. seaborn) separately keeps
  the viz PR reviewable and lets the dependency land first. A step that imports an undeclared package turns
  the PR red for a reason unrelated to the work.
- **Pushes carrying committed PNG/SVG need a bigger HTTP buffer:**
  `git config http.postBuffer 524288000`. Without it the push dies with `RPC failed; HTTP 400` or
  `unexpected disconnect` — and a subsequent line can read `Everything up-to-date`, which looks
  like success. **Always verify the remote head after pushing** (`gh pr view <n> --json
  headRefOid`).
- **One worktree per task**, and delete the worktree and branch when the PR merges. `etl pr-clean`
  is the sanctioned tool but is an interactive picker with no non-interactive flag, so it hangs
  when scripted; replicate its steps by hand from the main repo.
- **Brief the review babysitter with the deliberate decisions.** A reviewer reading a stale docstring
  can propose reverting a layout the user explicitly chose after seeing measurements, and an
  unbriefed agent will agree with it. List what must be **rebutted rather than fixed** — and when a
  reviewer catches a genuine inconsistency between a comment and the code, fix the comment.
- **Keep docstrings current when a design decision changes.** A module docstring that still
  says "stacked" after the layout became side-by-side is exactly that trap. A stale comment
  invites a reviewer to "fix" working code.
- **Removing a design element orphans code, and the orphans are the whole point.** Dropping a legend
  can leave a label helper, a layout flag and imports with no callers. `ruff` catches the imports;
  it does not catch a module-level function nobody calls or a config key nobody reads. Grep for each
  name you stopped using before calling the change done — and take the simplification, since dead
  branches are what make the next edit misfire.
- **Scripted edits need `assert old in s` and a uniqueness check.** A `str.replace` that matches
  nothing returns the string unchanged and reports success, so the render silently keeps the old
  behavior while you debug the wrong thing. For the same reason, never delete by slicing between two
  anchors without checking what lies between them: an unrelated helper sitting there goes too.

## `label__` is a prefix, and a suffix reads as unnamed

`/owid-staff:create-figma-chart`'s rows resolve a text node through its naming ancestors with
`/^(annotation|label)__/`, so a group named the other way round — `ghana__label`, `country__label` —
is invisible to every one of them. It fails silently, and worse, it fails *quietly*: on a chart whose
country labels sit on the frame's own background, `label-contrast-on-background` reports "no
`label__*`/`annotation__*` text sits on the frame's own background". That is a SKIPPED row,
indistinguishable at a glance from the same row on a chart that genuinely has no such text — on the
one check that would have measured those labels' contrast.

Renaming after the fact means renaming in three places at once (the step's gids, the restyle script's
parent regexes, and the frame), so get the order right in the step.
