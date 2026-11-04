"""Build the reader's-eye brief for one country, for red-teaming.

Everything printed here is what the published page shows a reader: the two series, the source line,
the two quality labels, the three prose blocks, the link, and the age-band decomposition where there
is one. Nothing about how the code works, and nothing we did not publish.

    python redteam.py Kenya          # the brief for one country
    python redteam.py --next 10      # the next countries due a review, in population-rank order
"""

import csv
import os
import re
import sys
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from countries import COUNTRIES, DOCS, TIERS  # noqa: E402

CACHE = os.path.join(HERE, "cache")
LOG = os.path.join(HERE, "redteam")
VALIDATION = {True: "Recalculated from births & women",
              False: "Rate copied from source"}
BAND_YEARS = range(1990, 2027)


def entry(country):
    return next((c for c in COUNTRIES if c["name"] == country), None)


def _series(name, kind):
    path = os.path.join(CACHE, f"{kind}_{name.replace(' ', '_')}.csv")
    if not os.path.exists(path):
        return None
    d = pd.read_csv(path)
    return d if not d.empty else None


def _wpp(country):
    """The UN's own line for the country, as the chart draws it."""
    d = _series(country, "wpp")
    if d is None:
        return None
    d = d[d.label == "estimates"] if "label" in d.columns else d
    return d[["year", "value"]] if not d.empty else None


def _bands(country):
    for year in reversed(BAND_YEARS):
        path = os.path.join(CACHE, f"detail_{country.replace(' ', '_')}_{year}.csv")
        if os.path.exists(path):
            return year, pd.read_csv(path)
    return None, None


def _half_up(v):
    """Round to a whole number the way a statistical office does.

    Some offices publish mean populations that land exactly on a half — Cuba's 2024 women aged 15-19
    are 254,152.5 — and print them rounded up. Python's own rounding goes to the nearest even number,
    which prints 254,152 and had an agent report a one-person discrepancy against the source that is
    purely a rounding convention.
    """
    return int(Decimal(str(float(v))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def brief(country):
    c = entry(country)
    if c is None:
        return f"{country} is not in the collection."
    found, method, caveats, url = DOCS.get(country, ("", "", "", ""))
    tier_label = TIERS[c["tier"]][0]
    plotted = c["loader"] is not None
    out = [f"COUNTRY AS PUBLISHED: {country}",
           f"SOURCE LINE: {c['src']}",
           f"LINK SHOWN TO THE READER: {url or '(none)'}",
           f"QUALITY LABEL — what the national figure is built from: {tier_label}"]
    # A country with nothing plotted carries no validation label on the page, because there is no
    # figure of ours to have validated or copied. Printing one had an agent judge a label the reader
    # is never shown.
    if plotted:
        out.append(f"QUALITY LABEL — validation level: {VALIDATION[bool(c['recalculated'])]}")
    out += ["",
            "THE OTHER LABELS AVAILABLE ON THE FIRST SCALE, for judging whether ours is right:",
            "  " + "; ".join(v[0] for v in TIERS.values())]
    if plotted:
        out += ["THE OTHER LABEL AVAILABLE ON THE SECOND SCALE:",
                f"  {VALIDATION[not bool(c['recalculated'])]}"]
    out += ["",
            "PROSE BLOCK 1 — \"What the office publishes\":", found or "(empty)", "",
           "PROSE BLOCK 2 — \"What we did\":", method or "(empty)", "",
           "PROSE BLOCK 3 — \"Watch out for\":", caveats or "(empty)", ""]

    nso = _series(country, "nso")
    if nso is None:
        out += ["NATIONAL SERIES PLOTTED: none — this country is on the not-plotted list.", ""]
    else:
        # Printed at more precision than the chart shows. Four significant figures made agents read
        # 1.25486 as "1.255" and report a rounding-boundary problem that does not exist.
        out.append("NATIONAL SERIES PLOTTED (year, value). These are the underlying values, shown to more "
                   "digits than the chart displays, so do not treat a trailing digit as a precision claim:")
        out.append("  " + ", ".join(f"{int(y)}: {v:.6g}" for y, v in zip(nso.year, nso.value)))
        out.append("")

    wpp = _wpp(country)
    if wpp is not None and nso is not None:
        shared = sorted(set(nso.year.astype(int)) & set(wpp.year.astype(int)))
        if shared:
            n = dict(zip(nso.year.astype(int), nso.value))
            u = dict(zip(wpp.year.astype(int), wpp.value))
            out.append("THE UN FIGURE THE CHART COMPARES IT WITH, in the same years:")
            out.append("  " + ", ".join(f"{y}: {u[y]:.4g}" for y in shared))
            # The map colors one number per country: the difference in the latest year the country
            # itself reports. It is not an average over the series.
            last = int(nso.dropna(subset=["value"]).year.max())
            if last in u:
                out.append(f"  the single difference the map colors, for the latest national year {last} "
                           f"(UN minus ours): {u[last] - n[last]:+.3f}")
            else:
                out.append(f"  the latest national year is {last}; the map colors the difference in that "
                           f"year against the UN's projection for it, which is not listed above")
            out.append("")

    year, bands = _bands(country)
    # That standing sentence is only shown under a chart, so a not-plotted country never shows it.
    # Printing it anyway had an agent challenge it as a claim about the country's office.
    if bands is None and plotted:
        out += ["AGE-BAND BREAKDOWN: none. In its place the page shows the reader this sentence:",
                "  \"This office publishes fertility rates only, not the births and female population",
                "  behind them, so the two sources cannot be compared age band by age band.\"", ""]
    if bands is not None:
        # The page draws these as two dot charts, one for births and one for women, each pairing the
        # national figure against the UN's. It prints no numbers, so they are rounded here rather than
        # shown at full precision — agents were reading the raw floats as a precision claim the page
        # does not make. Some national birth figures are derived rather than counted, which is the
        # country's own prose block to explain.
        out.append(f"AGE-BAND COMPARISON FOR {year}. The reader sees these as two dot charts, births "
                   "and women, each with the national figure against the UN's — no numbers are printed:")
        for _, r in bands.iterrows():
            cells = []
            for k in bands.columns:
                v = r[k]
                cells.append(f"{k}={v}" if isinstance(v, str) else f"{k}={_half_up(v):,}")
            out.append("  " + ", ".join(cells))
        out.append("")
    return "\n".join(out)


def reviewed():
    """Countries already written up in a findings log.

    Only the batch logs count, and only headings that name a country in the registry. Reading every
    markdown file in the directory pulled in the headings of the prompt, the readme and the agent
    ledger, which inflated the tally and made countries look reviewed when they were not.
    """
    known = {c["name"] for c in COUNTRIES}
    done = set()
    if os.path.isdir(LOG):
        for name in sorted(os.listdir(LOG)):
            if not (name.startswith("batch") and name.endswith(".md")):
                continue
            for line in open(os.path.join(LOG, name), encoding="utf-8"):
                if line.startswith("## ") and line[3:].strip() in known:
                    done.add(line[3:].strip())
    return done


def unmatched_headings():
    """Batch-log headings that do not name a country, so nothing counts them.

    A country written up under the wrong spelling is invisible to the tracker and gets reviewed
    twice, which is how DR Congo happened. Section headings that are deliberately not countries are
    listed here so they stop being reported as problems.
    """
    known = {c["name"] for c in COUNTRIES}
    out = []
    if os.path.isdir(LOG):
        for name in sorted(os.listdir(LOG)):
            if not (name.startswith("batch") and name.endswith(".md")):
                continue
            for line in open(os.path.join(LOG, name), encoding="utf-8"):
                head = line[3:].strip() if line.startswith("## ") else None
                # sections about a pattern rather than a country are headed "Cross-cutting"
                if head and head not in known and not head.startswith("Cross-cutting"):
                    out.append(f"{name}: {head}")
    return out


LEDGER = os.path.join(LOG, "AGENTS.md")
SECTIONS = ("In flight", "Reported, awaiting write-up", "Analyzed", "To do")


def _ledger(section):
    """The country bullets under one heading of redteam/AGENTS.md."""
    out, inside = [], False
    for line in open(LEDGER, encoding="utf-8"):
        if line.startswith("## "):
            # the headings carry their own count, as "## To do (58)"
            head = re.sub(r"\s*\(\d+\)\s*$", "", line[3:].strip())
            inside = head.lower() == section.lower()
        elif inside and line.startswith("- "):
            out.append(line[2:].strip())
    return out


def rank_order():
    """The hundred countries in population-rank order, under the names the registry uses."""
    names = {c["name"] for c in COUNTRIES}
    out = []
    with open(os.path.join(HERE, "top100.csv")) as f:
        for row in csv.DictReader(f):
            if row["country"] in names:
                out.append(row["country"])
    missing = [n for n in names if n not in out]
    return out + sorted(missing)


def sync_ledger():
    """Rewrite the ledger so its four lists always partition the hundred countries.

    In flight and awaiting write-up are the only state that has to be asserted by hand, so those two
    are carried over as they stand. Analyzed comes from the findings logs, and to do is whatever is
    left in rank order — which is what keeps the four adding up to a hundred without anyone counting.
    """
    flight = _ledger("In flight")
    waiting = [n for n in _ledger("Reported, awaiting write-up") if n not in flight]
    analyzed = reviewed() - set(flight) - set(waiting)
    spoken_for = set(flight) | set(waiting) | analyzed
    todo = [n for n in rank_order() if n not in spoken_for]
    lists = {"In flight": flight,
             "Reported, awaiting write-up": waiting,
             "Analyzed": [n for n in rank_order() if n in analyzed],
             "To do": todo}

    head = []
    for line in open(LEDGER, encoding="utf-8"):
        if line.startswith("## "):
            break
        head.append(line)
    body = []
    for name in SECTIONS:
        body.append(f"## {name} ({len(lists[name])})\n\n")
        body += [f"- {c}\n" for c in lists[name]]
        body.append("\n")
    with open(LEDGER, "w", encoding="utf-8") as f:
        f.writelines(head + body)
    return {k: len(v) for k, v in lists.items()}


def audit():
    """Check the campaign's live state. Anything this returns is something to fix now.

    The findings logs are the history, so the ledger holds only what they cannot know: which agents
    are out, and which have come back but not yet been written up. That second list is the one that
    matters — a report sitting in it is a report that arrived and was not acted on. It should be
    empty by the end of every cycle. An earlier version of this kept a hand-typed history of every
    country ever reported, which was a partial copy of the logs and passed while being wrong.
    """
    done, problems = reviewed(), []
    known = set(rank_order())
    lists = {name: _ledger(name) for name in SECTIONS}
    flight, waiting, analyzed = lists["In flight"], lists["Reported, awaiting write-up"], lists["Analyzed"]

    # the four lists have to partition the hundred: every country in exactly one of them
    seen, twice = set(), set()
    for names in lists.values():
        for name in names:
            (twice if name in seen else seen).add(name)
    total = sum(len(v) for v in lists.values())
    if total != len(known):
        problems.append(f"the four lists hold {total} countries, not {len(known)}")
    if twice:
        problems.append(f"listed in more than one section: {', '.join(sorted(twice))}")
    for name in sorted(known - seen):
        problems.append(f"in the registry but in no section of the ledger: {name}")
    for name in sorted(seen - known):
        problems.append(f"in the ledger but not a registry name: {name}")

    for name in waiting:
        problems.append(f"reported but not yet written up: {name}")
    for name in sorted(set(analyzed) - done):
        problems.append(f"listed as analyzed but absent from the findings logs: {name}")
    for name in sorted(done - set(analyzed)):
        problems.append(f"written up in the findings logs but not listed as analyzed: {name}")
    for bad in unmatched_headings():
        problems.append(f"log heading names no country in the registry: {bad}")
    for bad in impossible_arithmetic():
        problems.append(f"arithmetic the text cannot support — read it: {bad}")
    # Five in flight is the rule while there is anything left to start. Once To do empties, the
    # campaign is draining and the count only falls, so requiring five would fail every cycle from
    # then to the end.
    if _ledger("To do") and len(flight) != 5:
        problems.append(f"{len(flight)} agents in flight, not 5, with countries still to do: "
                        f"{', '.join(flight) or 'none'}")
    return problems


def impossible_arithmetic():
    """Prose that quotes two counts and a rate the counts cannot produce.

    Three entries described dividing a year's births by a count of women and getting a fertility rate.
    That division gives a general fertility rate, around 0.1, not a total fertility rate around 4 —
    the total needs each age group divided separately, summed, and multiplied by the band width. All
    three read plausibly and none of them could ever have been right.

    Candidates only: a country legitimately quoting two counts and an unrelated rate in one sentence
    will show up here too, so each hit needs reading rather than trusting.
    """
    pat = re.compile(r"([\d]{2,3}(?:,\d{3})+)[^.]{0,60}?([\d]{1,3}(?:,\d{3})+)[^.]{0,80}?(\d\.\d+)")
    out = []
    for name, doc in DOCS.items():
        for block in doc[:3]:
            for m in pat.finditer(block or ""):
                a = float(m.group(1).replace(",", ""))
                b = float(m.group(2).replace(",", ""))
                c = float(m.group(3))
                # two numbers of the same size are one quantity being compared against another, not a
                # numerator and a denominator — births are always a small fraction of women
                if not b or 0.5 < a / b < 2:
                    continue
                if abs(a / b - c) < 0.05 or abs(a / b * 5 - c) < 0.05:
                    continue
                out.append(f"{name}: {m.group(1)} / {m.group(2)} = {a / b:.4f}, but the text says {c}")
    return out


def progress():
    """One line of ground truth, counted rather than remembered."""
    counts = {name: len(_ledger(name)) for name in SECTIONS}
    total = sum(counts.values())
    return (" · ".join(f"{name.lower()} {n}" for name, n in counts.items())
            + f" — {total} in all, of {len(rank_order())}")


def next_up(count):
    """The next countries due a review, in population-rank order."""
    order = []
    with open(os.path.join(HERE, "top100.csv")) as f:
        for row in csv.DictReader(f):
            order.append(row["country"])
    plotted = {c["name"] for c in COUNTRIES}
    done = reviewed()
    out = [c for c in order if c in plotted and c not in done]
    return out[:count]


if __name__ == "__main__":
    if sys.argv[1:2] == ["--next"]:
        print("\n".join(next_up(int(sys.argv[2]) if len(sys.argv) > 2 else 10)))
    elif sys.argv[1:2] == ["--sync"]:
        print(", ".join(f"{k.lower()} {v}" for k, v in sync_ledger().items()))
        found = audit()
        print("\n".join(found) if found else "nothing outstanding")
        sys.exit(1 if found else 0)
    elif sys.argv[1:2] == ["--audit"]:
        found = audit()
        print("\n".join(found) if found else "nothing outstanding")
        print(progress())
        sys.exit(1 if found else 0)
    else:
        print(brief(" ".join(sys.argv[1:])))
