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

**Model check:** the session context names the running model. On **Fable**, recommend re-running on
**Opus** (or **Sonnet** for a mechanical re-render) before starting, and continue only on the user's
say-so — same rule as `/create-figma-chart`.

> **Paired skill — an update here may oblige an update there, and the reverse.**
> [`/create-figma-chart`](../create-figma-chart/SKILL.md) owns everything that happens inside Figma,
> and this skill hands off to it at Step 7. The two share a contract that lives half in each file, so
> **when you change something on this list, check the other skill in the same session and update it
> too — or state explicitly that you checked and no change was needed.** Neither side may drift
> silently; a stale cross-skill fact is how a later run re-derives geometry by trial and error.
>
> | Shared fact | Owner | Consumed by |
> |---|---|---|
> | Template geometry — node ids, sizes, band top, footer starts | [`TEMPLATES.md`](TEMPLATES.md) | both |
> | The content box and the band a chart is fitted into | TEMPLATES.md | both |
> | Node naming (`gid`s) this step emits, and frame proportions | this skill | that skill's Steps 1/3/7–8 |
> | Which text slots this step fills vs. leaves to the template | this skill | that skill's Step 6 |
> | Type and palette — this step sets neither | that skill | this skill defers to it |
> | The design vocabulary (per chart type, labeling, colors) | that skill's [`GUIDELINES.md`](../create-figma-chart/GUIDELINES.md) | both |
>
> The asymmetry worth remembering: **this skill owns the data, the geometry and the proportions; that
> one owns the type and the palette.** A change that crosses that line belongs in both files. In
> particular, if you change what the step emits — node naming, frame proportions, which text slots it
> fills — check whether that skill's Step 1/3/7 notes on local SVGs still hold.
>
> Its **[`GUIDELINES.md`](../create-figma-chart/GUIDELINES.md)** is where the visual vocabulary this
> skill defers to actually lives — the per-chart-type rules, direct labeling in place of legends, the
> OWID palette, the annotation and reference-line conventions. Read the section for your chart type
> before choosing a form or deciding what to label: the point is not to style anything here, it is to
> avoid emitting a structure the Figma pass then has to undo (a legend that should have been direct
> labels, a category count that cannot be labeled in place). That file also indexes the design team's
> **DI Chart Library** (`pltrHXyVLg2XaNq4AvxPaK`, **read-only**) — 272 finished charts filed by chart
> type, which is the closest thing to precedent for whatever you are about to draw.

Two companions in this directory:

- **[`TEMPLATES.md`](TEMPLATES.md)** — the static-chart template geometry, measured off the Charts
  file. Read it before laying anything out; don't re-derive it through MCP calls.
- **`scripts/verify_static_viz.py`** — mechanical check that the emitted files honor the Figma
  handoff contract. Run it before showing anyone the chart.

The step-by-step detail lives in [`reference/`](reference/) and is read *at* that step:

| Read | When | Covers |
|---|---|---|
| [reference/WRITING-THE-STEP.md](reference/WRITING-THE-STEP.md) | Step 4 | The handoff contract, grapher's axis and tick treatment, encoding diagrams, desktop/mobile pairing, text slots, labelling many categories, Figma-surviving anchors, the assertions to write. |
| [reference/GOTCHAS.md](reference/GOTCHAS.md) | Its **Data** section at Step 1, before any column is used; the rest on an error, or grep by symptom | Data, layout and workflow pitfalls. |

**Size budget: keep this spine under 30 KB and TEMPLATES.md under 25 KB.** Both are read on every
run, so a paragraph added here costs every future viz — new detail belongs in the reference file for
its step. After editing any doc in this skill:

```bash
.venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --skill create-static-viz --structure
# and after moving text between files, prove nothing was dropped:
.venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --skill create-static-viz --against <ref>
```

**When you do make a Figma MCP call, batch it.** A call costs twice — the network hop to the hosted
connector **and the model turn around it** — and both terms move with the environment. The
cross-environment figures here are all **`get_screenshot`, the only tool timed in both**: the hop is
7.8–9.9 s from a cloud sandbox against 12.5–20.5 s locally with Figma's desktop app open, while the
turn is ~12 s in the cloud against 2–4 s on a light local turn. Read them as the shape of the cost,
not as per-tool numbers — `use_figma` and `upload_assets` have not been benchmarked across
environments. Either way, a handful
issued one at a time is the difference between seconds and minutes. Batching pays about the same in
both: the connector serves concurrent calls — eight screenshots in one message measured 4.1× faster
than serially, and twelve runs of a fixed six-call probe mean **4.0× in both environments** (ten
cloud reps 4.00×, two local 4.00×, six in flight every time) — and admits about four or five at once,
so **put independent calls in one message, 4–6 at a time.** A batch's wall is
`first call + rate × (n−1)` — measured at **9.2 s + 0.75 s** per extra screenshot in a cloud session
and **11.7 s + 2.1 s** locally — and that marginal cost is the stopping rule.
Reads always; writes only when they target different pages. If the Figma tools arrive deferred (a
cloud session serves them that way), load the ones you need in a single `ToolSearch`, taking the
prefix from your own session's tool list since it differs between cloud and local; skip that where
they are already loaded. `/create-figma-chart` → **Round-trip budget** has the full
rule and the list of what is genuinely serial.

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
.venv/bin/python .claude/skills/edit-faust-metadata/scripts/resolve_target.py <reference> [--json] [--no-db]
```

It takes a live/staging/admin URL, a bare slug, a chart id, or an indicator catalogPath, reports
the chart's variables **with their catalogPaths**, and names the candidate ETL step files —
including a warning when the grapher catalogPath's version differs from what is on disk.

**In a cloud session, run it with `--no-db` first — only the DB half is unavailable.** Parse-only
mode never calls `read_sql`, so it works in a sandbox: it identifies what the reference *is*,
preserves the dimension query params, and names the candidate ETL step files for an indicator or
MDim catalogPath. Do that before reaching for anything else. What `--no-db` cannot give you is the
DB half — a bare slug stays `chart-or-mdim (needs DB)`, and the chart's variables and their
catalogPaths need MySQL.

**For that half, don't wait on staging.** A cloud sandbox has no MySQL and staging is on Tailscale,
so the DB path reports *"Staging server … is not reachable. Run `etl pr` first or wait for the
staging build to finish"* — which invites waiting for something that will never arrive here. Don't
wait, and don't run `etl pr` for it. Fall back to the read-only routes in
[cloud-sandbox.md](../../docs/cloud-sandbox.md) —
`https://ourworldindata.org/grapher/<slug>.config.json` for a published chart, the public Datasette
for `variables` and `chart_dimensions` — or ask the user to run the resolver locally and paste the
output. This is a fallback for one environment, not a replacement: use the resolver with its DB
wherever MySQL is reachable, since it does strictly more than the fallbacks do.

**From an old static viz image**, two routes, neither of which the popularity CSV can do alone:

1. The grapher `static_viz` table carries a **`grapherSlug`** column. That gives a slug, and the
   slug goes through the resolver above. Note this table is **not** mirrored to the public
   Datasette, so query a staging DB or the local dev DB (see `/query-grapher-db`) — which means a
   cloud session cannot reach it either, and the slug has to come from the user.
2. `ai/static_viz_popularity/static_viz_popular.csv` gives rank, the pages it appears on, views,
   authors and tags — useful context, but it has **no slug or indicator column**, so it cannot
   reach the data by itself.

If neither resolves it, ask. Guessing which dataset an old hand-drawn image was built from is how
you rebuild the wrong chart.

The project tracks which viz is claimed, parked or already done in a shared tracker, and this
skill deliberately does not read or write it — **ask instead**. If the user has not already said
where this viz stands, ask before doing any work: a viz someone else is mid-way through, or one
already finished, is worth an interruption rather than a duplicate.

**Read [reference/GOTCHAS.md](reference/GOTCHAS.md) → Data before you use a single column.** Those
checks are pre-flight, not post-mortem: a column whose name means something other than what it
holds, an unasserted splice, a framing that stops holding partway through a series, and an
over-claim repeated across the metadata each render as a plausible chart with nothing to grep for.
They are how a run that raises no exception still publishes wrong numbers.

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

> **Read [reference/WRITING-THE-STEP.md](reference/WRITING-THE-STEP.md) for this step.**

Everything about authoring the step: the Figma handoff contract the emitted files have to satisfy, grapher's axis and tick treatment, encoding diagrams, pairing desktop with mobile, the template's text slots, labelling many categories, anchoring labels so they survive Figma, and the assertions to write.

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
   Every layout bug in [this skill's Gotchas](reference/GOTCHAS.md) was found by looking.

## Step 6 — Iterate with the user

Show the render. When a design choice is genuinely open, **measure the options and offer the
numbers**, not adjectives — see the [panel-aspect gotcha](reference/GOTCHAS.md) under Layout for why.

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
round. **Brief the babysitter with the deliberate decisions** (see the [babysitter gotcha](reference/GOTCHAS.md) under Workflow).

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
