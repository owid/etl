# The end-to-end benchmark

One fixed request, run whole. **Its output is a single number: how long the DI takes to make.**
Written down before the first run so the spec cannot be tuned to its own result.

That number is the point, and everything else this page describes exists to explain it or to keep
it honest. A run also surfaces defects and produces a chart — but those are what *a run* yields,
not what *the benchmark measures*. Three co-equal outputs make a report; a benchmark reports one
figure you can hold against the last one.

> Create a DI for this chart
> `https://ourworldindata.org/grapher/liberal-democracy-index?tab=line&time=1950..latest&country=~CHL`,
> with annotations in the years 1973 and 1990.

## Why this one

- **It is a real deliverable**, not a test page. Every prior attempt to validate this skill end to
  end stalled on "that writes to the shared file and needs a chart somebody wants".
- **Annotations at two named years force the parts that are hardest and least verified**: the
  annotation block, the curvy arrows, and therefore the Step 8c pixel probes — the arrow-gap check,
  the knockout tier, the annotation-block gap. A chart without annotations exercises none of them.
- **A single-country line chart over 75 years** is the common shape, and its two annotation years
  are far apart, so the arrows land in different regions of the plot rather than crowding.
- **The years are load-bearing, not decorative** — 1973 and 1990 are the two inflection points in
  the series, so the annotations have to land on real features and the numbers have to be right.
  A wrong value here is visible to any reader who knows the history, which is the right pressure.

## Fixed conditions

- The yearly **Charts (YYYY)** file, a new page as Step 4 specifies. **The page is disposable —
  delete it once the numbers are recorded**, unless someone actually wants the chart. It is a
  shared design file, and a benchmark that leaves a page behind on every run silts it up. (The
  first run's page was dropped straight after; nothing was lost, which is the proof.)
- **So the output cannot be the page.** The number, and the detail explaining it, live outside
  Figma. If a finding only exists as something you can see on the page, it has not been recorded.
- Run it as a normal build, **checkpoint rule included** — the approval pause is part of the
  experience, and skipping it would measure a flow nobody actually runs. But see the two clocks
  below: the pause is timed *out* of the headline, not designed out of the run.
- Note which render path was available: the desktop reader is quota-limited and cannot see
  just-written frames, so a build normally runs entirely on the hosted connector (GOTCHAS.md).

## The number

**Two clocks, and the distinction is load-bearing:**

| clock | what it is | use |
|---|---|---|
| **total elapsed** | first Figma call → delivered chart | what a person feels, and what to quote to one |
| **agent time** | total, minus every stretch spent waiting on a human | **the headline** |

Agent time is the headline because it is the only part the skill controls, and because it
separates a **stall** from a **checkpoint** — the first run had one of each, and only one of them
was supposed to be there.

**Baseline — first run, 2026-08-24, on `claude/figma-faster-reads-and-probes`**, recovered from the
session transcript rather than estimated:

| | |
|---|---|
| **agent time** | **30.8 min** ← the headline |
| total elapsed (request 13:20:35 → delivery 13:52:58) | 32.4 min |
| human wait | 93 s — **4.8%** |
| ├ approval, asked 13:26:35 → granted 13:26:51 | 16 s *(the checkpoint, by design)* |
| └ stall, 13:29:16 → 13:30:33 | 77 s *(a defect — see below)* |
| Figma calls | 30 (28 in the build proper) |
| messages issuing them | 28 — one call each, correctly (see the batching warning below) |

**Two corrections this measurement forced, both against things assumed rather than checked:**

*Human latency does not swamp the measurement.* The obvious argument for the split — that approval
waits swing by hours and drown any change being measured — is not what happened. The approval took
**16 seconds**, and all human time together was **under 5%** of the run. The split is still worth
keeping, but for the reason below, not that one. Anyone quoting the ~31 min figure should know that
essentially all of it was the agent.

*The 77-second pause was not a checkpoint.* Mid-build the run stopped to offer a cost write-up, and
the user had to reply "run it to completion" to restart it. That is a **stall — an agent-caused
wait wearing a checkpoint's clothes**, and it is 5× the real approval. This is what the two clocks
are actually for: a checkpoint is part of the design and stays, a stall is a defect and shows up
here as human time the skill did not need to spend.

**Runs 2 and 3, both 2026-08-28, both in a cloud sandbox** (run 2 on
`claude/create-figma-benchmark-chart-3r1st7`, run 3 on `claude/figma-chart-benchmark-pwgleo`):

| | run 1 (local) | run 2 (cloud) | run 3 (cloud) |
|---|---|---|---|
| **agent time** | **30.8 min** | **~48 min** | **29.0 min** |
| whole-run wall clock, incl. pre-Figma work | not recorded | not recorded | **34.3 min** (5.3 of it before the first Figma call) |
| human wait | 93 s (16 s checkpoint + 77 s stall) | ~60 s — one checkpoint, no stall | one checkpoint, no stall — and it fell *before* the first Figma call, so the headline excludes it entirely |
| Figma calls | 28 in the build proper | 28 (14 in the build spine) | 24 (16 in the build spine, 4 check slices, 3 renders, 1 fix) |
| messages with >1 Figma call | 0 | 3, peak 3 in flight | 3, peak 3 in flight |
| `verify_page.js` run? | **no — not at all** | **yes, all four row groups** | **yes, all three documented calls** |
| defects the pass caught | — | **2**, neither visible in a render | **1** (`source-line-weight`) — plus run 2's own new furniture rule caught a second before the pass ran |

**The headline's start point is doing more work than it looks, and run 3 is where that shows.** "First
Figma call → delivered" excludes Step 1's chart resolution, the data verification, the texts, the Step 4
proposal and the checkpoint. Run 3 did *all* of that before touching Figma, so its 29.0 min is a clean
Figma-phase figure and its honest whole-run cost is **34.3 min**. A run that interleaves the same work —
resolving an MDim view mid-build, say — books it inside the headline instead. **Quote both numbers, or
the comparison silently rewards deferring Figma rather than being faster.** The two clocks also collapsed
into one here: with the checkpoint before the first Figma call, agent time and total elapsed are the same
number, which is the cleanest reading the split has produced.

**Run 3 is the first run to benefit from a previous run's lessons, and they paid.** Run 2's furniture rule
fired immediately: the fitted import arrived with every gridline and tick at **0.626px** and the gridline
dash at **[2.503, 2.503]**, and the one prescribed sweep put all of it back to 1px / `[4, 4]` before any
check ran. Without that rule this would have been a third defect for the gate to find, at a
diagnose-fix-reverify cycle apiece. That is the accumulation the number exists to detect.

**Where run 3's own 24 calls went, and the 2 that were rework.** One was a second fit: the chart was
solved and fitted to the full 508 content width, and only then did the single-series end dot turn out to
overhang it, forcing a rescale to 503 plus a re-restore of the type ladder and every furniture stroke.
One was the `source-line-weight` fix. **Both were preventable from documentation that already existed or
now does** — which is the useful form of this row: rework that no rule covers is a lesson, rework that a
rule covers is a reading failure, and these were one of each. See the lessons folded in below.

> **One honesty note on the pass.** Run 3 relayed the `type,geometry` and `series` slices byte-verbatim,
> but hand-shortened some *detail strings* in the `annotations` slice to fit the message. No threshold,
> comparison or classification was touched, so the verdicts stand — but this is precisely the edit
> CHECKS.md warns about, where a corruption yields a wrong verdict rather than an error. **Relay all
> three verbatim**; the slices are sized to fit and there is no reason to trim them.

**Read that as two different runs, not a 17-minute regression — and quote the split, never the total.**
Run 1 never executed the Step 8c script; run 2 ran every group, and relaying them verbatim accounts for
substantially all of the difference. On the part the two share, run 2 was *faster*: **14 Figma calls for
the build spine** against run 1's 28, at the low end of the ~14–18 a template should cost, with the Step 6
and Step 4 questions collapsed into a single checkpoint and no stall. The honest comparison is therefore
**14 calls / spine-only vs 28**, plus a first measurement of what the gate costs. Future runs should carry
both figures, because a total that silently includes or excludes the pass is not comparable to anything.

What the pass bought for that time, both invisible on screen and both passed by the build's own
measurements: a **2.28px `box-alignment` failure** (a hidden node's ancestor groups still counting toward
the chart group's box) and **furniture left mid-scale** (tick marks at 0.666px, gridline dash at
`[2.663, 2.663]`). Both are now rules in FITTING.md, so the next run should not re-find them.

**What run 3 sent back into the skill** — three edits, none of them on this page:

| Lesson | Went to |
|---|---|
| `<slug>.csv` ignores `country=` and `time=` and returns EVERY entity; a year-keyed dict then collapses ~200 countries into one plausible, wrong series. No parameter fixes it — `csvType=filtered` still returns 174. | [GOTCHAS.md](GOTCHAS.md) |
| On a single-series line chart taking an under-line end label, solve the export against `contentW − dotRadius`, not `contentW` — otherwise the end dot overhangs and costs a whole second fit. | [per-chart-type/line.md](per-chart-type/line.md) |
| The `Data source:` slot's weight collapse is already documented, 114 lines below the bullet that tells you to write it — so the bullet now points at it. | [TEXTS.md](TEXTS.md) |

The first is the one that mattered. It is not a Figma bug and not a chart-type quirk: it is a data
endpoint that answers a *different question* than the one asked, plausibly, with no error. The wrong
series was monotonic and well-formed, and it said Chile's democracy *improved* under Pinochet — which
only got caught because the rendered SVG's own path disagreed with it. **The general habit that saved it
is worth more than the specific trap: the exported SVG is the ground truth for what the reader will see,
so cross-check any number you are about to print against the geometry you are about to import.**

**A correction to this page's own rationale, which run 2 falsified.** *Why this one* claims the two
annotations force "the curvy arrows, and therefore the Step 8c pixel probes — the arrow-gap check". They
do not. LABELING.md's device table assigns *a moment* to a thin vertical event rule and *a period told
through the line* to a recolour, and explicitly warns against aiming one arrow at a multi-year story — using
this very domain as its example. Built correctly, this chart carries **no arrow at all**, so
`arrow-clearance` and the four-render probe are never exercised and come back SKIPPED with nothing to
measure. The spec is left as written (it predates the runs on purpose); what needs changing is the
expectation, or the addition of a second fixed request whose note really is about one value.

## Every run ends by folding its lessons in — and they are general, not benchmark lessons

**This is a fixed condition of the run, not a follow-up.** The headline is a number, but a run that
surfaces defects and does not write them back has thrown away most of what it cost. So the run is not
finished when the chart is delivered; it is finished when the lessons are in the skill's files and
`verify_docs.py --structure` passes.

**Where they go is the load-bearing part: into the skill, never into this page.** A lesson filed here is
read only by the next benchmark run, which is the rarest run there is. A lesson in
[GOTCHAS.md](GOTCHAS.md), [CHECKS.md](CHECKS.md), [FITTING.md](FITTING.md), [LABELING.md](LABELING.md),
[GUIDELINES.md](../GUIDELINES.md) or the spine is picked up automatically by **every** future run, on every
chart, whether or not anyone is benchmarking. This page records the number and what explains it. It does
not record fixes.

Three consequences worth stating, because each was got wrong once:

- **Generalize past the chart in hand.** A finding met on a single-country line chart is almost never about
  single-country line charts. "A group's box includes its hidden descendants" is a fact about Figma and
  belongs in the fitting rules for every chart type — not in a note about Chile.
- **Put each lesson at the step that will need it**, so it costs a run nothing until that step runs. The
  spine and GUIDELINES.md are read on every run, so bytes there are a standing tax; a reference file is
  read once, at its step. When the spine has to grow, pay for it by deduplicating something already there.
- **A lesson that makes the next run faster outranks one that makes it more thorough**, when you have to
  choose — but prefer the edit that does both, which is usually "here is the cheaper way to run the check"
  rather than "skip the check".

**Ordinary runs fold their lessons in too.** The benchmark does not own this habit, it just guarantees the
habit happens on a schedule: any run of this skill that learns something the files do not say should end
the same way. What the benchmark adds is the number that tells you whether the accumulated edits are
actually making runs cheaper.

## What explains the number

Sweep the session transcript afterwards — `tool_use` → `tool_result` timestamps, **grouped by the
issuing assistant message id**, never by timestamp proximity:

| | why |
|---|---|
| Figma calls by tool, and the total | against the **~14–18 per template** a build should cost |
| turns, and median gap between them | the term that dominates `turns × (turn + call)` |
| messages containing >1 Figma call, and peak in-flight | but read the warning below before scoring it |
| screenshots that were *looked at* vs measured by script | the thing `measure_pixels.py` exists to reduce |
| re-work: calls spent on a mistake, and what it was | the honest denominator — a fast wrong build is not fast |

### Do not score batching on a single-chart build. It has nothing to batch.

The first run came back **28 calls in 28 messages, peak in-flight 1**, and that was first written up
here as the benchmark's largest finding — the manifest ignored, the ~4.0× left on the table. **That
was wrong, and the mistake is worth more than the claim was.** Reading the calls back, 22 of the 28
were `use_figma` writes to the DI page, and SKILL.md's own rule is that **writes batch only across
different pages**, since a script may switch pages once and two writes at one page race. A build is
one page. The three `upload_assets` are the two-pass export solve, serial by the documented
`measure band → export embed → fit` chain; the two screenshots were four minutes apart on different
states. **Nothing in that run was batchable.** Zero was the correct number, not a failure.

The manifest's rows are surveys and checks — the Step 5 page enumeration, the Step 8c check pair,
the Step 9 delivery renders, the palette harvest — and this build had one Step 8c call and two
renders. So the ~4.0× is real and belongs to *read-heavy* passes; it was never available to the
build spine.

The general lesson, which is why this stays written down: **a zero is not a finding until you have
checked that a non-zero was possible.** Measuring the number was right, and reading a verdict into
it without asking whether the thing was even available is the same error as a check reporting PASS
on input it could not measure. Score this row only against calls the manifest actually names.

**What the number does say, once batching is off the table.** 28 calls against the ~14–18 a
template should cost, and 30.8 min / 28 ≈ **66 s per call** where the call itself is ~4.4 s — so
**~60 s of each is model turn**. The lever on a build is therefore *fewer, fatter* calls, not
parallel ones: the plugin allows ~10 logical operations per call, and four of the 28 were pure
diagnosis and rework (`Diagnose why the annotation text did not grow`, two position fixes, a
re-bold after a style collapse). Turns dominate; that is what `turns × (turn + call)` already says.

## What this cannot tell you

It is **not a controlled A/B**. Design judgment varies far more between runs than the differences
being measured, so comparing one run here against one run on another branch measures the weather.
For that, use a fixed sub-task with identical inputs — the arrow-probe comparison in
[CHECKS.md](CHECKS.md) is the pattern: same three renders, only the method changes.

Treat this as **validation plus a coarse cost envelope**: does the flow complete, does the output
survive review, and roughly what did it take. If a change claims a speedup, prove it on a fixed
sub-task and use this to check nothing broke.
