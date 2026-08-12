---
name: create-static-viz
description: Build or refresh an OWID static visualization end to end — resolve what data it needs from an old static viz image, an indicator, or a grapher chart; check both the ETL catalog and the producer's own site for a newer release and route to /create-dataset or /update-dataset when one exists; write the export://static_viz matplotlib step that emits Figma-ready SVG and PNG at the static-chart templates' proportions; then hand off to /create-figma-chart. Trigger when the user asks to "refresh this static viz", "remake this chart as a static image", "create a static viz", pastes an old static viz image or filename and asks for a better version, or works a row of the static-viz refresh tracker.
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

## Step 1 — Resolve the input to data, and check the tracker

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

**Read the refresh tracker once**, and surface what it says before doing any work:

```sql
SELECT url, "Name", "Status", "Comment", "GitHub", "Rank", "Views per day"
FROM "collection://b9f7b7e5-8923-41f4-9190-d1544914d9e0"
```

Statuses are `Propose` / `Tackle` / `On the bench` / `Blocked` / `Done` or empty. If the row is
`Blocked` or `Done`, say so and stop for confirmation rather than redoing it. **Query once, never
poll** — the Notion allowance is limited.

Then report what you found: which dataset, which version, how many charts use it, and what the
tracker says.

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

Non-negotiable — `scripts/verify_static_viz.py` checks all of it:

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
- **`gid=` on every artist**, so the layer panel reads `boys__median` rather than `Path 41`.
  Grapher does exactly this with `makeFigmaId` (`packages/@ourworldindata/utils/src/Util.ts`).
  Use `<subject>__<role>` so the names sort into groups.
- **Do not pass `bbox_inches="tight"`** when the frame must match a template — cropping to content
  changes the proportions, which is the whole thing the template fixes. `export_fig` already
  injects `metadata={"Date": None}` on the SVG pass for reproducible diffs.
- Emit both formats; `paths.export_fig` writes into the step's own directory, so **the PNG and SVG
  are committed next to the `.py`** — for every frame the step emits.

```python
paths.export_fig(fig, short_name, ["png", "svg"], dpi=300)
```

### Style, and where it stops

- **seaborn** `set_style("ticks")` + `set_palette("deep")`, and reference colors by **palette
  position** (`palette[0]`, `palette[1]`) rather than pinned hexes, so the chart moves with the
  shared palette. seaborn is a `dev` dependency.
- **Axis treatment follows grapher**, so a static chart reads like our interactive ones. These are
  read from source, not guessed — `grapher/src/axis/AxisViews.tsx` and `axis/Axis.ts`:

  | Property | Value |
  |---|---|
  | Gridlines | **dashed** `4,4`, color `#ddd` (`GRID_LINE_DASH_PATTERN`, `TICK_COLOR`) |
  | Tick labels | `#5b5b5b` (`GRAPHER_DARK_TEXT` = `GRAY_80`) |
  | Axis label | **bold** (`fontWeight: 700`) |
  | Axis line | **none** — the gridlines carry the reading |
  | x gridlines on a line chart | hidden (grapher sets `hideGridlines` on the x axis) |

- Nested percentile/uncertainty bands: one `BAND_ALPHA`, deepening where they overlap, gives the
  fan for free. **The legend swatch must show the cumulative alpha** — `1-(1-a)**(i+1)` for the
  nth band — or the key reads inside out against the chart.
- Reference lines get a **bold title above a regular-weight value**, labeled on the line rather
  than pushed into the legend, via `AnnotationBbox` / `TextArea` / `VPacker`.

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

Editing the step's `.py` is enough to trigger a rebuild on its own, so you rarely need to force
anything. For the narrow case where nothing in the repo changed but you still need to re-run, use
**`--force --only`** — never `--force` alone, which would also re-run every upstream dependency.
`--only` is safe here because the deps are already on disk from the run you are repeating.

Then, in this order:

1. `.venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py <step-dir> --template <name>`
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

Once the Figma page exists, write it back into the step's module docstring, so whoever re-runs the
step later finds the design decisions next to the code that feeds them:

```python
"""...

Figma handoff
-------------
Charts (2026), page "20260812 Expected height of boys and girls (Pablo A)"
Frame: expected-height-boys-girls  (the frame name is the website PNG filename)
Link:  https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=<id>

Done by hand in Figma, to redo after a data update:
- retyped title/subtitle onto the template's bound text styles
- swapped the band fills for Chart colors library styles
- direct-labeled the two medians, dropped the legend

To refresh: re-run this step, re-upload the SVG to the same page, and reapply the above.
"""
```

## Step 9 — PR, review chain, tracker

The branch, worktree and draft PR already exist from Step 4. Commit the step plus its committed
PNG/SVG, push, and fill in the PR body — then [`/pr-babysitter`](../pr-babysitter/SKILL.md) for the
Codex round. **Brief the babysitter with the deliberate decisions** (see the last gotcha).

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

Finally, print the tracker row and the PR link for the user to paste into Notion. Don't write to
it.

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
- **Panel aspect ratio decides whether a trend is legible.** Two panels stacked in a portrait
  frame give each a 2:1 landscape box, and a growth curve in that box looks flat. Turning them
  portrait gave 2.4× the vertical resolution and the shape appeared. Compute the panel box
  (`content_width`, `available_height / n_rows`) before choosing the grid.
- **A template's fixed y positions encode assumptions.** `subtitle_y = 80` is `16 + two lines of
  title`. Pin to it under a one-line title and you get a dead line.
- **A matplotlib legend extends downwards from its `bbox_to_anchor`**, and occupies more than its
  text height. Its anchor must clear the axes by its own full height plus a gap, or the swatches
  land on the plot.
- **A narrow panel cannot hold an inline reference label.** With clipping off it spills into the
  neighboring panel rather than being cropped — which looks like a rendering bug. Move the label
  to the legend for narrow layouts.
- Fewer axis ticks in a narrow panel. Make the tick set per-layout, not global.

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
