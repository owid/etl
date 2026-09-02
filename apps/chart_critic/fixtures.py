"""Charts with known answers, used as a regression test for the critic.

Why this exists: a prompt regression and a clean catalog produce **identical output**. During
development, moving the review instruction from the user turn into the agent's system
instructions trebled the miss rate and every sweep still came back looking tidy. Nothing but a
set of charts whose answers are already known can tell those two apart.

Every case here is a real error that a person reported, or a real false positive the critic
produced and a prompt change fixed. Sources: ``#we-need-to-correct-it``, ``owid/owid-issues``,
and the development runs.

    etl chart-critic --eval

Two honest caveats:

- **These are live charts.** When one gets fixed the expectation becomes wrong, which is a
  feature — a fixture flipping to FAIL because the chart was corrected is how you learn it was
  corrected. Update the case and note the date.
- **The model is not reproducible**, but far less than expected here. Re-measured across every
  cached pass on 2026-09-02: eight of the nine cases answered identically on 15/15 passes, and
  only the COVID-vaccination case is genuinely flaky (1/15). So a failure in the stable eight is
  worth treating as a regression rather than re-running, and that one case is marked ``flaky``
  and left out of the gate.

Measured on 2026-08-31 with ``google:gemini-3.7-flash``:

===========  ====================  =================================
passes       result                cost
===========  ====================  =================================
2            6/8 (2/4 errors)      $0.05
5            **8/8 (4/4 errors)**  $0.22
===========  ====================  =================================

Both misses at two passes were the subtlest cases — a subtitle typo and a baseline offset —
and no number of passes made a clean chart fire. So passes buy recall without costing
precision, which is the whole argument for ``--repeat``.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Case:
    slug: str
    # What a working critic should say. Matched case-insensitively against the claim and evidence;
    # every keyword must appear somewhere. Empty means "this chart should come back clean".
    expect_keywords: list[str] = field(default_factory=list)
    params: str = ""
    views: int = 1
    why: str = ""
    # Cases that catch a specific false positive a prompt change fixed. Kept separate because a
    # regression here means the prompt got laxer, not that the critic went blind.
    guards_against: str = ""
    # A case the critic finds only occasionally. Kept in the set as a recall probe, but excluded
    # from the pass/fail gate: a scheduled run that fails most days is a gate nobody can act on,
    # and it would mask the regressions the other cases exist to catch. Say the measured rate in
    # ``why`` so the number is auditable rather than a shrug.
    flaky: bool = False


CASES: list[Case] = [
    # ---------- should be found ----------
    Case(
        slug="life-expectancy-vs-gdp-per-capita",
        expect_keywords=["central african", "life expectancy"],
        why="CAR's life expectancy oscillates 52.3 → 31.5 → 50.6 → 40.3 → 18.8 → 57.4 (2018-2023). "
        "Filed as owid/etl#6779. The indicator sits behind 6 charts and ~199k views/yr.",
    ),
    # ---------- fixed since it was filed, kept as a clean case ----------
    Case(
        slug="share-elec-by-source",
        why="Was the UK-coal-above-100% case: 119.1% in 1951, above 100% for eight consecutive years, "
        "filed as owid/etl#6779. Re-measured 2026-09-02 at 95.5% for 1951 and below 100% throughout, so "
        "the data has been corrected and this is now a clean chart. Kept rather than deleted: it is the "
        "one case in the set that has been through the whole cycle, and the fixture flipping to FAIL is "
        "how the fix was noticed at all.",
    ),
    Case(
        slug="suicide-rate-in-1980-vs-2023",
        expect_keywords=["aumber"],
        why="The subtitle reads 'Estimated aumber of suicides per 100,000 people'. Live on the page. "
        "The only text-level case in the set, so it is what proves the critic is not purely numeric.",
    ),
    Case(
        slug="share-people-fully-vaccinated-covid",
        expect_keywords=["10"],
        flaky=True,
        why="The World series opens at 10.14% on 2020-12-02, before any country reports more than "
        "0.003%. Still live, re-checked 2026-09-02. The critic almost never catches it: 1 of 15 "
        "cached passes, and 0 of 10 fresh ones. It is the hardest kind of defect for a reader "
        "critic — a plausible-looking value at the left edge of a 13-series line chart, invisible "
        "in the numeric summary because it is neither the min nor the max. Kept as the set's "
        "recall probe and excluded from the gate; if a change ever makes this reliable, that is "
        "a real improvement and worth measuring.",
    ),
    # ---------- should come back clean ----------
    Case(
        slug="weekly-growth-covid-deaths",
        why="Data ends 2026-07-19. Reviewed later than that, it is current, not future-dated.",
        guards_against="The critic flagged this as showing 'a future date in July 2026' because nothing "
        "told it today's date. Fixed by stating the date in the prompt.",
    ),
    Case(
        slug="share-of-population-in-extreme-poverty",
        params="country=~COM&time=2000..latest&tab=line",
        why="Title and subtitle are correct: the World Bank's International Poverty Line was revised to "
        "$3.00/day at 2021 PPP.",
        guards_against="The critic objected that $3/day is not the International Poverty Line, reasoning "
        "from the superseded $2.15 figure. Fixed by telling it the chart's metadata is more current than "
        "its own knowledge of definitions.",
    ),
    Case(
        slug="wheat-production",
        why="Renders fine. Its config carries yAxis {'max': 0, 'min': 0}, which is grapher's "
        "auto-scale sentinel, not a zero ceiling.",
        guards_against="Adding the chart config to the bundle made the critic report 'the y-axis maximum "
        "is set to 0, breaking the chart' at high confidence, here and on cocoa-bean-production; a third "
        "variant appeared on weekly-growth-covid-deaths. Fixed by dropping axis bounds from the config "
        "summary entirely — three false positives, no true ones.",
    ),
    Case(slug="literacy", why="An ordinary chart. Nothing wrong with it."),
    Case(slug="real-gdp-growth", why="An ordinary chart, and one where year-to-year swings are genuine."),
]


def matches(case: Case, issues: list[dict]) -> bool:
    """Did the critic say what this case expects?"""
    if not case.expect_keywords:
        return not issues
    haystack = " ".join(f"{i.get('claim', '')} {i.get('evidence', '')}" for i in issues).lower()
    return all(k.lower() in haystack for k in case.expect_keywords)
