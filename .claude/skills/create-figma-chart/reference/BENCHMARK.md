# The end-to-end benchmark

One fixed request, run whole, to answer the question no unit measurement can: **does a real build
work, and what does it cost?** Written down before the first run so the spec cannot be tuned to its
own result.

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

- The yearly **Charts (YYYY)** file, a new page as Step 4 specifies. **Do not delete the page
  afterwards** — this run produces something real; that is the point of choosing it.
- Run it as a normal build, checkpoint rule included. The approval pause is part of the cost.
- Note which render path was available: the desktop reader is quota-limited and cannot see
  just-written frames, so a build normally runs entirely on the hosted connector (GOTCHAS.md).

## What to record

Sweep the session transcript afterwards — `tool_use` → `tool_result` timestamps, **grouped by the
issuing assistant message id**, never by timestamp proximity:

| | why |
|---|---|
| wall clock, first Figma call to last | the number a person feels |
| Figma calls by tool, and the total | against the 120–190 this skill's budget assumes |
| messages containing >1 Figma call, and peak in-flight | whether the batch manifest was actually followed |
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
