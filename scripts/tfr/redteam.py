"""Build the reader's-eye brief for one country, for red-teaming.

Everything printed here is what the published page shows a reader: the two series, the source line,
the two quality labels, the three prose blocks, the link, and the age-band decomposition where there
is one. Nothing about how the code works, and nothing we did not publish.

    python redteam.py Kenya          # the brief for one country
    python redteam.py --next 10      # the next countries due a review, in population-rank order
"""

import csv
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from countries import COUNTRIES, DOCS, TIERS  # noqa: E402

CACHE = os.path.join(HERE, "cache")
LOG = os.path.join(HERE, "redteam")
VALIDATION = {True: "Fully validated from births & women",
              False: "Rate copied from source, not validated"}
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


def brief(country):
    c = entry(country)
    if c is None:
        return f"{country} is not in the collection."
    found, method, caveats, url = DOCS.get(country, ("", "", "", ""))
    tier_label = TIERS[c["tier"]][0]
    out = [f"COUNTRY AS PUBLISHED: {country}",
           f"SOURCE LINE: {c['src']}",
           f"LINK SHOWN TO THE READER: {url or '(none)'}",
           f"QUALITY LABEL — what the national figure is built from: {tier_label}",
           f"QUALITY LABEL — validation level: {VALIDATION[bool(c['recalculated'])]}",
           "",
           "THE OTHER LABELS AVAILABLE ON THE FIRST SCALE, for judging whether ours is right:",
           "  " + "; ".join(v[0] for v in TIERS.values()),
           "THE OTHER LABEL AVAILABLE ON THE SECOND SCALE:",
           f"  {VALIDATION[not bool(c['recalculated'])]}",
           "",
           "PROSE BLOCK 1 — \"What the office publishes\":", found or "(empty)", "",
           "PROSE BLOCK 2 — \"What we did\":", method or "(empty)", "",
           "PROSE BLOCK 3 — \"Watch out for\":", caveats or "(empty)", ""]

    nso = _series(country, "nso")
    if nso is None:
        out += ["NATIONAL SERIES PLOTTED: none — this country is on the not-plotted list.", ""]
    else:
        out.append("NATIONAL SERIES PLOTTED (year, value), exactly as charted:")
        out.append("  " + ", ".join(f"{int(y)}: {v:.4g}" for y, v in zip(nso.year, nso.value)))
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
    if bands is None:
        out += ["AGE-BAND BREAKDOWN: none. In its place the page shows the reader this sentence:",
                "  \"This office publishes fertility rates only, not the births and female population",
                "  behind them, so the two sources cannot be compared age band by age band.\"", ""]
    if bands is not None:
        out.append(f"AGE-BAND BREAKDOWN SHOWN FOR {year} (the reader sees these numbers):")
        for _, r in bands.iterrows():
            out.append("  " + ", ".join(f"{k}={r[k]}" for k in bands.columns))
        out.append("")
    return "\n".join(out)


def reviewed():
    """Countries already covered by a batch log."""
    done = set()
    if os.path.isdir(LOG):
        for name in os.listdir(LOG):
            if name.endswith(".md"):
                for line in open(os.path.join(LOG, name), encoding="utf-8"):
                    if line.startswith("## "):
                        done.add(line[3:].strip())
    return done


def _ledger(section):
    """The country bullets under one heading of redteam/AGENTS.md."""
    out, inside = [], False
    for line in open(os.path.join(LOG, "AGENTS.md"), encoding="utf-8"):
        if line.startswith("## "):
            inside = line[3:].strip().lower() == section.lower()
        elif inside and line.startswith("- "):
            out.append(line[2:].strip())
    return out


def audit():
    """Cross-check the agent ledger against the findings logs.

    A country an agent reported on but nobody wrote up is a report that was read and dropped, or
    never read at all. That is the failure worth catching, so it is the one this names.
    """
    done, problems = reviewed(), []
    for name in _ledger("reported"):
        if name not in done:
            problems.append(f"reported but never written up: {name}")
    for name in _ledger("in flight"):
        if name in done:
            problems.append(f"listed in flight but already written up: {name}")
    known = {c["name"] for c in COUNTRIES} | {"England and Wales"}
    for section in ("in flight", "reported"):
        for name in _ledger(section):
            if name not in known:
                problems.append(f"not a registry name, so unmatchable: {name}")
    flight = _ledger("in flight")
    if len(flight) != 5:
        problems.append(f"{len(flight)} agents in flight, not 5: {', '.join(flight)}")
    return problems


def next_up(count):
    """The next countries due a review, in population-rank order."""
    order = []
    with open(os.path.join(HERE, "top100.csv")) as f:
        for row in csv.DictReader(f):
            order.append(row["country"])
    plotted = {c["name"] for c in COUNTRIES}
    done = reviewed()
    out = [c for c in order if c in plotted and c not in done]
    # England and Wales stands in for the United Kingdom in the collection
    if "United Kingdom" in [c for c in order] and "England and Wales" not in done:
        out = [("England and Wales" if c == "United Kingdom" else c) for c in out]
    return out[:count]


if __name__ == "__main__":
    if sys.argv[1:2] == ["--next"]:
        print("\n".join(next_up(int(sys.argv[2]) if len(sys.argv) > 2 else 10)))
    elif sys.argv[1:2] == ["--audit"]:
        found = audit()
        print("\n".join(found) if found else "ledger and logs agree")
        sys.exit(1 if found else 0)
    else:
        print(brief(" ".join(sys.argv[1:])))
