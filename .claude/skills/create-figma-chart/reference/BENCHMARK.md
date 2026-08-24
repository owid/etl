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
| messages issuing them | 28 — **nothing was batched** |

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

## What explains the number

Sweep the session transcript afterwards — `tool_use` → `tool_result` timestamps, **grouped by the
issuing assistant message id**, never by timestamp proximity:

| | why |
|---|---|
| Figma calls by tool, and the total | against the 120–190 this skill's budget assumes |
| messages containing >1 Figma call, and peak in-flight | whether the batch manifest was actually followed |

**On the first run that last row read zero.** 28 calls, 28 messages, peak in-flight **1** — the
batch manifest was in the skill, and the run followed none of it. That is the single largest
finding the benchmark has produced, because batching is measured at **~4.0×** and is this branch's
central claim. The manifest is not self-executing; SKILL.md already records two of five earlier
sessions batching nothing at all, and the official benchmark is now the sixth data point and the
worst. **Report this row first** — a run that never batched has not tested the thing being sold.
| turns, and median gap between them | the term that dominates `turns × (turn + call)` |
| screenshots that were *looked at* vs measured by script | the thing `measure_pixels.py` exists to reduce |
| re-work: calls spent on a mistake, and what it was | the honest denominator — a fast wrong build is not fast |

## What this cannot tell you

It is **not a controlled A/B**. Design judgment varies far more between runs than the differences
being measured, so comparing one run here against one run on another branch measures the weather.
For that, use a fixed sub-task with identical inputs — the arrow-probe comparison in
[CHECKS.md](CHECKS.md) is the pattern: same three renders, only the method changes.

Treat this as **validation plus a coarse cost envelope**: does the flow complete, does the output
survive review, and roughly what did it take. If a change claims a speedup, prove it on a fixed
sub-task and use this to check nothing broke.
