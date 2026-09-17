# Sketch mode — a data file in, no ETL until the visuals settle

> Read when the input is a data file, or the ask says "sketch", "brainstorm" or "prototype" a **new**
> static viz. Part of [`/create-static-viz`](../SKILL.md); the spine has the step order this replaces.
> Lessons from a sketch run are folded in like any other run's — here when they are about sketching,
> into the step's reference file when they generalize.

A sketch is a `viz://static` step in everything but its home: the same constants-on-top, `run()`,
helpers-below shape, the same `LAYOUTS` registry, the same `export_frame` per frame — rendered from a
file next to it instead of a catalog dataset, in a gitignored `ai/static-viz-sketches/<slug>/`
directory, with no branch, PR, DAG entry or newer-data check behind it. `SketchPaths(__file__)`
(`etl/viz/static.py`) stands in for `PathFinder(__file__)`, which refuses any file outside `etl/steps`.
The point is to settle the *visuals* — in matplotlib and then in Figma — before deciding the ETL
structure; promotion (§7) is a two-line edit because the shape never changed.

**A Data Insight image is not a static-viz sketch.** DIs are sketched in `/owid-staff:create-figma-chart`'s
own sketch mode, straight from the grapher chart (occasionally a local SVG), onto `DI_Template`. This
file covers the four static templates only.

## Entering the mode

The input decides: a CSV, Excel or parquet file is a sketch; "sketch / brainstorm / prototype a static
viz from `<indicator or chart>`" is a sketch after a data pull (§0); an old viz, an indicator or a chart
without those words is the full flow. Don't ask which — say which, in one line, with what it skips:

> *"Sketching this from `<file>` in `ai/static-viz-sketches/<slug>/` — no branch, PR, DAG entry or
> newer-data check until the visuals settle; promotion runs all of them."*

If the person wanted the real thing, they will say so, and the answer is the full flow: data not in ETL
goes to `/create-dataset` first, as Step 2 says.

**Skipped, and said aloud every time:** the branch and worktree, the draft PR, the DAG entry, Step 1's
tracker question, Step 2's newer-data check (ETL and producer side), Step 8's docstring write-back
(the sketch keeps its own record instead, §5), and Step 9's review chain. **Not skipped:** the emitted-file
verifier and reading the PNG — both are seconds, and both are what makes the SVG importable and the
render honest.

## 0 — When the input is an indicator or a chart: pull the data first

One-off, into the sketch dir, and record where it came from in the sketch's docstring (the
`Provenance` line), so promotion knows which dataset to point the step at. The scaffold accepts a
`--data` file that already sits in `ai/static-viz-sketches/<slug>/` and leaves it in place — an existing
directory is not an existing sketch; only an existing `sketch.py` asks for `--force`:

```python
from pathlib import Path
from etl.http import session   # a grapher chart's data; the shared session tags the traffic as ETL's
r = session.get("https://ourworldindata.org/grapher/<slug>.csv?v=1&csvType=full&useColumnShortNames=true")
r.raise_for_status()
Path("data.csv").write_bytes(r.content)   # every entity, whatever the URL's country=/time= say; filter in the sketch
```

```python
from owid.catalog import find   # an indicator by catalogPath
find(table="<table>", namespace="<ns>", version="<version>").load().reset_index().to_csv("data.csv", index=False)
```

## 1 — Scaffold

```bash
.venv/bin/python .claude/skills/create-static-viz/scripts/new_sketch.py \
    --slug <slug> --data <file> --template horizontal [--template mobile] \
    [--source "<Producer> (<year>)"] [--title "<title>"] [--author "<name>"]
```

It creates `ai/static-viz-sketches/<slug>/sketch.py` beside a copy of the data file, from a template that
already **runs**: a placeholder figure at the template's proportions, every text slot at the template's
position and size, a plot inside the band, the band outlined, the layers named. The first `--template`
is the unsuffixed frame; each further one gets the verifier's suffix (`<slug>_mobile`), so a
desktop/mobile pair is one directory. The slug follows the step rule (snake_case) because it becomes
the step's `short_name`; a slug ending in a template hint (`_mobile`, `_square`, …) is refused.

What the scaffold carries, and why (each line is a promotion marker `promote_sketch.py` looks for):

| Line | Now | On promotion |
|---|---|---|
| `paths = SketchPaths(__file__)` | writes next to the sketch | `paths = PathFinder(__file__)` — the script swaps it |
| `load_data()` reads `DATA_FILE` as a `Table(..., underscore=True)` | garden-style column names from day one | `paths.load_dataset(...).read(...)` — you swap it |
| `SOURCE = "<Producer> (<year>)"`, read once in `run()` as `source = SOURCE` | a CSV carries no origins | `source = source_citation(tb[...])` in `run()`, after `load_data()` — you swap it, then delete the constant |
| `LAYOUTS`, font stacks, grapher greys, `BAND_INSET` | transcribed from TEMPLATES.md and WRITING-THE-STEP.md | unchanged; re-verify the band against the live template (TEMPLATES.md's own rule) |

Then write `build()`. Everything in [WRITING-THE-STEP.md](WRITING-THE-STEP.md) about the handoff
contract, grapher's axis treatment, `gid` naming and text measurement applies to a sketch exactly as to
a step — the file *is* the step. Read [GOTCHAS.md](GOTCHAS.md) → Data before using a column: a CSV
downloaded by hand has every trap a snapshot has, and no `.dvc` to say so.

## 2 — Render

```bash
.venv/bin/python ai/static-viz-sketches/<slug>/sketch.py
```

Plain Python, not `etlr` — there is no step to run. From a fresh worktree the `.venv` trap in the
spine's Step 5 applies just the same: confirm `from etl import paths; paths.BASE_DIR` is the checkout you
are in, or the sketch imports another checkout's `etl.viz.static`.

## 3 — Verify, and read the PNG

```bash
.venv/bin/python .claude/skills/create-static-viz/scripts/verify_static_viz.py ai/static-viz-sketches/<slug> \
    --template <first template> --expect-gid <data layer> [--expect-gid ...]
```

The scaffold passes with `--expect-gid line__placeholder`; once `build()` is real, pass the real data
layers (`line__<Entity>`, `label__<Entity>`, …) — without them the naming check proves only that *some*
node was named. Then **read the PNG**: the verifier cannot see a collision or a label on a curve.

## 4 — Iterate, and try variants cheaply

A variant is another `LAYOUTS` key — a second template, a different band inset, a different label
placement — so several proportions render from one `build()` in one run. Data variants (a different
entity set, a different window) are a change to `load_data()` or a filter above `build()`. Keep the
spine's Step 6 rule: when a choice is open, measure the options and offer the numbers.

## 5 — Hand off to the Figma skill's sketch mode

Give `/owid-staff:create-figma-chart` the SVG path(s) and say **sketch**: its sketch mode imports the
file on the local-SVG route onto the matching static template, fills the template's slots, fits the
import and delivers a frame link — no checkpoint, no labeling proposals, no verification gate. Hand
over the `gid` scheme with the file, as the spine's Step 7 says. The page and frame carry that
skill's sketch marker (`… [sketch]`, `<slug>--sketch`), so nothing on the page reads as finished.

**Write the handoff into `sketch.py`'s docstring** — the `Figma handoff` section the scaffold left
empty: file key, page name, each frame's name and node id, the template it was cloned from, the deep
link. This is the spine's Step 8, done early: the sketch is the file that becomes the step, so the
record travels with it, and the frame name is the durable join.

## 6 — Iterate across both sides

Two kinds of change, two homes:

- **Data, geometry, proportions** — `sketch.py`. Re-render (§2), re-verify (§3), then replace the
  frame's `chart` group: the Figma skill's sketch mode re-imports the SVG in one call. **Ask first if the
  person has edited inside the chart in Figma** — a re-import replaces their work.
- **Type, palette, labels, annotations, finish** — Figma, by the person directly or by asking. That
  skill owns them; nothing here should try to reproduce them in matplotlib.

Nothing in this loop runs a gate. **Every reply in it ends by offering the next step in plain words**,
because the person may not know either exists — and **the first time, say what the steps are**:

> *"This is still a sketch: nothing has been checked and it is not in ETL. When the visuals are
> settled, say **promote** and I'll: put the data through a proper ETL step, so the chart can be
> re-rendered whenever the data updates; check for a newer release of the data; verify the numbers,
> the source line and every piece of text against the data and our style guide; and run the Figma
> checks on the frame — type, layout, colours, lines and annotations, and that the frame still matches
> its template — before it is renamed for the website."*

After that, one line is enough — *"Still a sketch: say promote when you're happy with it."* — but never
none. When they sound done, propose starting promotion now. The loop ends when they say yes.

## 7 — Promote

Now the ETL work, in a PR branch, per the spine's Step 4 preamble:

```bash
.venv/bin/etl pr "<title>" data --worktree --share-data   # --share-data symlinks ai/ into the worktree
```

If the data is not in ETL, `/create-dataset` first (Step 2's routing table), so there is a
`data://garden/...` step to depend on. Then, from inside that worktree:

```bash
.venv/bin/python .claude/skills/create-static-viz/scripts/promote_sketch.py <sketch dir> \
    --step viz://static/<namespace>/<version>/<short_name> --dep data://garden/<ns>/<version>/<short> [--dep ...]
```

It refuses to write on `master`/`main`, copies `sketch.py` to `etl/steps/viz/static/<ns>/<version>/<short>.py`,
swaps the `paths` line and the import, appends the DAG entry to `dag/static_viz.yml`, and prints the two
edits that need you: the loader (`paths.load_dataset(...).read(...)`, then delete `DATA_FILE`) and the
source (`source = source_citation(tb[...])` in `run()`, after the loader — `tb` exists only there, so the
swap never goes at module level; then delete the `SOURCE` constant). Without `--share-data`, pass the
sketch dir as an **absolute** path — it lives in the main checkout's `ai/`, which the worktree does not
have. `--dry-run` shows the plan from any branch.

## 8 — Then the normal flow

The step exists; everything the sketch skipped now runs, in the spine's order: **Steps 1–2** (resolve
the dependency; check for newer data on both sides — the CSV's provenance finally gets its real
answer), **Step 5** (`etlr`, the verifier with the real `--expect-gid`s, the PNG), **Step 8** (the
docstring already carries the Figma handoff — complete it with what the finalize pass changes), and
**Step 9** (the PR and the review chain). On the Figma side, hand the sketch frame's link to
`/owid-staff:create-figma-chart` and say **finalize**: it reads the person's Figma edits as decisions,
proposes labels and annotations, runs the full verification gate, and renames the frame to the bare
slug. If the promoted step's SVG differs from the sketch's, that mode decides whether to re-import.
