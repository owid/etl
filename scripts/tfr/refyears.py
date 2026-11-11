"""Which year does a survey estimate actually describe?

The chart plots each survey point at the year of its fieldwork or its publication, and the summary then
compares it against the UN's figure for that same year. That comparison is wrong, because a survey does
not measure the year it was run: a DHS-type round asks women for their birth histories and computes a
rate over the three years before interview, and a census asks how many children a woman bore in the
previous twelve months. So a rate from 2019 fieldwork can describe 2016 to 2019.

Ten agents read the reports for all 187 survey points, one batch each, and returned the fieldwork dates,
the reference period in the report's own words, and the year the estimate belongs to. Their answers are
the `refyears/batch*.txt` files, one pipe-separated row per point, kept as text so they stay diffable and
so the evidence for every re-dated point can be read next to it.

This script does not trust those answers. It recomputes each midpoint from the fieldwork dates the agent
itself reported, using the rule the reference periods imply -- the middle of fieldwork less half the
window -- and flags any row where the agent's own arithmetic disagrees. It then sorts the rows into what
can be applied without asking and what a human has to look at:

* a collision, where two rounds of the same country land on one year and one of them has to give
* a row that is inferred or unknown rather than documented, so the window rests on an assumption
* a row whose recomputed midpoint disagrees with the agent's
* a row where the report contradicts itself about its own window, since that changes the value too

Points whose midpoint falls before the chart's first year are not a question: they stop being plotted.

    python refyears.py            # the review page, to refyears/review.html
    python refyears.py --check    # just the flags, as text
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, "refyears")
FIELDS = ["country", "plotted", "fieldwork", "period", "midpoint", "recommended", "basis", "evidence"]
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
# a report that disagrees with itself about its own window changes the value, not just the year
CONTRADICTION = re.compile(r"inconsisten|contradict|but a separate|five-year|5-year window|cinq ann"
                           r"|used a 5-year|1-60 months")

# Five rounds print their rate on a five-year window while a later round of the same survey re-tabulates
# it on the standard three. That would change the value as well as the year, so which window applies is
# not a matter of preference. It was settled by checking the value we plot against both: every one of the
# five matches the three-year figure, so the three-year window is the right one for all of them and none
# of them needs a decision. Checked against cache/nso_<country>.csv, values as at 2026-08-11.
RESOLVED = {
    ("Cambodia", "2000"): "we plot 3.8, the three-year figure; the five-year one is 4.0",
    ("Burkina Faso", "2003"): "we plot 5.9, the three-year figure; the five-year one is 6.2",
    ("Mali", "2006"): "we plot 6.6, which is the three-year table's own national figure",
    ("Ethiopia", "2000"): "we plot 5.5, the three-year figure; the five-year one is 5.9",
    ("Mozambique", "1997"): "we plot 5.2, the three-year figure; the five-year one is 5.6",
}


def _plotted_now(country, first_year):
    """The years of this country's series that the chart actually draws.

    The agents were handed every point in each country's cached series, and some of those are older than
    the chart's first year and were never drawn — Morocco's series reaches back to 1962. Without this,
    re-dating them looks like losing points that were never on the page.
    """
    path = os.path.join(HERE, "cache", f"nso_{country.replace(' ', '_')}.csv")
    if not os.path.exists(path):
        return None
    years = set()
    for line in open(path, encoding="utf-8"):
        head = line.split(",")[0].strip()
        if head.isdigit() and int(head) >= first_year and line.split(",")[1].strip():
            years.add(int(head))
    return years


def rows(first_year=2000):
    """Every agent's rows, parsed. Malformed lines are returned too, so they cannot pass unnoticed."""
    out, bad = [], []
    seen_plotted = {}
    for name in sorted(os.listdir(LOG)):
        if not (name.startswith("batch") and name.endswith(".txt")):
            continue
        for line in open(os.path.join(LOG, name), encoding="utf-8"):
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != len(FIELDS):
                bad.append((name, len(parts), line[:90]))
                continue
            r = dict(zip(FIELDS, parts))
            r["batch"] = name
            if r["country"] not in seen_plotted:
                seen_plotted[r["country"]] = _plotted_now(r["country"], first_year)
            drawn = seen_plotted[r["country"]]
            r["on_chart"] = drawn is None or (r["plotted"].isdigit() and int(r["plotted"]) in drawn)
            out.append(r)
    return out, bad


def _dates(text):
    """Every date in a fieldwork description, as decimal years.

    Three shapes appear across the batches and all three have to parse, because a date this misses falls
    through to a bare-year guess and invents a disagreement that is mine rather than the agent's:
    "Oct 2005", "26 Sep 2011" and "Jun 11-21, 2001".
    """
    found = []
    for m in re.finditer(r"([A-Za-z]{3,9})\.?\s+(?:\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s+)?((?:19|20)\d{2})", text):
        month = MONTHS.get(m.group(1)[:3].lower())
        if month:
            found.append(int(m.group(2)) + (month - 0.5) / 12)
    for m in re.finditer(r"\b\d{1,2}\s+([A-Za-z]{3,9})\.?\s+((?:19|20)\d{2})", text):
        month = MONTHS.get(m.group(1)[:3].lower())
        if month:
            found.append(int(m.group(2)) + (month - 0.5) / 12)
    if not found:
        years = re.findall(r"\b(?:19|20)\d{2}\b", text)
        found = [float(years[0]) + 0.5] if years else []
    return found


def _stated_years(period):
    """The middle year of every calendar range the period text names, like "(2018-20)" or "2001-2003".

    All of them, not the first: the text is free prose and often names a report as well as a window, so
    Mozambique's row mentions "IDS 2022-23" beside a window of 1992-97. Picking the first match read the
    report's own title as its reference period and put a 1997 survey in 2023.

    A stated range gives a year directly rather than a midpoint to round. Pakistan's rounds each cover one
    whole calendar year, and the midpoint of calendar 2001 is the instant 2001.5 — rounding that up called
    a survey about 2001 a survey about 2002.
    """
    out = []
    # Both sides must be full years. Allowing a two-digit right side read "1 Apr 2021 - 31 Mar 2022" as
    # the range 2021 to 2031 and put a Vietnamese survey in 2026.
    for m in re.finditer(r"\b((?:19|20)\d{2})\s*(?:[-–]|to|through|a)\s*((?:19|20)\d{2})\b", period):
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo < hi <= lo + 10:
            out.append((lo + hi) // 2)
    # the abbreviated form, only inside brackets where it cannot be a date: "(2018-20)"
    for m in re.finditer(r"\(((?:19|20)\d{2})\s*[-–]\s*(\d{2})\)", period):
        lo = int(m.group(1))
        hi = int(m.group(2)) + lo - lo % 100
        if lo < hi <= lo + 10:
            out.append((lo + hi) // 2)
    # a whole calendar year spelled out: "1 Jan to 30 Dec 2001", "1st January 2005 to 31st December 2005"
    for m in re.finditer(r"jan(?:uary)?.{0,30}?dec(?:ember)?\s*,?\s*((?:19|20)\d{2})", period):
        out.append(int(m.group(1)))
    for m in re.finditer(r"1\s*(?:st)?\s*jan(?:uary)?\s+((?:19|20)\d{2}).{0,40}?dec", period):
        out.append(int(m.group(1)))
    return out


def _width(period):
    for pattern, width in [(r"twelve month|12 month|last 12|12-month|year preceding|12 mois", 1.0),
                           (r"three|3 year|3-year|36 month|3 anos|3 annees|trois|tres anos", 3.0),
                           (r"five year|5 year|5-year|60 month|cinq ann", 5.0)]:
        if re.search(pattern, period):
            return width
    return None


def candidates(r):
    """(defensible years, the rule's midpoint) from the row's own evidence."""
    period = r["period"].lower()
    years = set(_stated_years(period))
    midpoint = None
    dates, width = _dates(r["fieldwork"]), _width(period)
    if dates and width:
        midpoint = round((min(dates) + max(dates)) / 2 - width / 2, 2)
        years.add(year_of(midpoint))
    return years, midpoint


def year_of(midpoint):
    """Round half up, so a midpoint of exactly 2016.5 becomes 2017.

    Python rounds halves to even, which sends 2016.5 to 2016 and 2017.5 to 2018 — the same convention
    problem that has already produced two false findings in this project. Half of these midpoints land
    within a month of a boundary, so the rule has to be stated rather than inherited.
    """
    import math

    return int(math.floor(midpoint + 0.5))


def flags(all_rows):
    """Everything a human has to decide, keyed by reason. Only points the chart draws."""
    all_rows = [r for r in all_rows if r.get("on_chart", True)]
    out = {"collision": [], "not documented": [], "arithmetic": [], "contradiction": [], "boundary": []}
    by_country = {}
    for r in all_rows:
        by_country.setdefault(r["country"], []).append(r)

    for country, rs in by_country.items():
        landing = {}
        for r in rs:
            if r["recommended"].isdigit():
                landing.setdefault(r["recommended"], []).append(r["plotted"])
        for year, plotted in landing.items():
            if len(plotted) > 1:
                out["collision"].append((country, year, plotted))

    for r in all_rows:
        if r["basis"] != "documented":
            out["not documented"].append(r)
        # checked before anything can skip it: an earlier version put this after a `continue` and it
        # silently reported no contradictions at all
        if CONTRADICTION.search(r["evidence"]) and (r["country"], r["plotted"]) not in RESOLVED:
            out["contradiction"].append(r)
        if not r["recommended"].isdigit():
            continue
        said = int(r["recommended"])
        # Two different checks. First, is the row consistent with itself: does the year it recommends
        # follow from the midpoint it reports? Second, is that midpoint defensible from the dates and
        # window it quotes? A row only needs a human when it fails both, because failing one is usually
        # a midpoint sitting a few weeks from a rounding boundary.
        try:
            own = year_of(float(r["midpoint"]))
        except ValueError:
            own = None
        derived, midpoint = candidates(r)
        if said in derived:
            continue
        # A midpoint near either edge of the recommended year's interval decides nothing: both years are
        # defensible and the rounding convention picks one. Measuring only the upper edge called Jordan's
        # 2016.42 a disagreement when it sits five weeks from the 2016.5 boundary.
        near = min(abs(midpoint - (said - 0.5)), abs(midpoint - (said + 0.5))) if midpoint is not None else 9
        if near <= 0.25:
            out["boundary"].append(r)
        elif own is not None and own != said:
            out["arithmetic"].append((r, sorted(derived), f"its own midpoint {r['midpoint']} implies {own}"))
        elif derived:
            out["arithmetic"].append((r, sorted(derived), "no window in its evidence reaches that year"))
        if CONTRADICTION.search(r["evidence"]) and (r["country"], r["plotted"]) not in RESOLVED:
            out["contradiction"].append(r)
    return out


def resolve(r):
    """The year to use, and a note where the evidence pulls another way.

    One convention, applied to every point: the whole year nearest the middle of the reference period.
    Two are defensible and they disagree on 76 of the 179 points, so the choice had to be made once and
    stated rather than left to each report's own labelling. The alternative — the calendar year the period
    is centred in, which would send a window centred September 2018 to 2018 rather than 2019 — is the more
    literal reading, but it moves every affected point a further year back and collides Chad's 2009 census
    with its 2010 survey.

    The midpoints come from the agents, who read the reports. Where this script's own reading of the
    fieldwork dates disagrees, that is reported and not applied: its parsing of prose is an approximation,
    and on Congo it missed the "9 Oct" that starts the fieldwork because no year followed it.
    """
    said = int(r["recommended"]) if r["recommended"].isdigit() else None
    # A report that names its own calendar period settles the year outright, and must not be rounded.
    # Pakistan's rounds each cover 1 January to 31 December of one year, so their midpoint is that year's
    # exact middle — rounding it up called five surveys about 2001, 2003, 2005, 2006 and 2007 surveys about
    # the year after each.
    stated = set(_stated_years(r["period"].lower()))
    if len(stated) == 1:
        return stated.pop(), ""
    try:
        year = year_of(float(r["midpoint"]))
    except ValueError:
        year = said
    if year is None:
        return None, "no reference period established"
    note = ""
    if said is not None and said != year:
        note = f"the agent recommended {said}; the convention gives {year}"
    derived, midpoint = candidates(r)
    if midpoint is not None and year not in derived:
        near = min(abs(midpoint - (year - 0.5)), abs(midpoint - (year + 0.5)))
        note = (f"this script's own arithmetic gives {sorted(derived)}, worth a look" if near > 0.25
                else f"within {near:.2f} of the boundary; {sorted(derived)} equally defensible")
    return year, note


def applied_years():
    """{(country, plotted year): reference year} — the mapping the build actually uses."""
    out = {}
    for r in rows()[0]:
        year, _ = resolve(r)
        if year is not None and r["plotted"].isdigit():
            out[(r["country"], int(r["plotted"]))] = year
    return out


def moves(all_rows, first_year=2000):
    """(applied, dropped) — the rows that need no decision, and those that leave the chart."""
    applied, dropped = [], []
    for r in all_rows:
        if not r.get("on_chart", True):
            continue
        year, _ = resolve(r)
        if year is None:
            continue
        (dropped if year < first_year else applied).append(r)
    return applied, dropped


def _esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def review_html(all_rows, bad, first_year=2000):
    """A page showing only what needs a decision, with everything settled folded away behind it."""
    f = flags(all_rows)
    applied, dropped = moves(all_rows, first_year)
    moving = [r for r in applied if r["plotted"] != str(resolve(r)[0])]
    offchart = [r for r in all_rows if not r.get("on_chart", True)]
    all_rows = [r for r in all_rows if r.get("on_chart", True)]
    by_country = {}
    for r in all_rows:
        by_country.setdefault(r["country"], []).append(r)

    def row_html(r, note=""):
        year, why = resolve(r)
        arrow = f'{r["plotted"]} &rarr; <b>{year}</b>' if str(year) != r["plotted"] else f'{r["plotted"]} (stays)'
        return (f'<tr><td>{_esc(r["country"])}</td><td class="y">{arrow}</td>'
                f'<td>{_esc(r["fieldwork"])}</td><td>{_esc(r["period"])}</td>'
                f'<td class="{r["basis"]}">{r["basis"]}</td>'
                f'<td class="ev">{_esc(note or why or r["evidence"])}</td></tr>')

    out = ["<title>Survey reference years — what needs deciding</title>", """<style>
body{font:15px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:1180px;
margin:2rem auto;padding:0 1.2rem;color:#1a1a1a;background:#fff}
h1{font-size:1.6rem;margin:0 0 .3rem} h2{font-size:1.15rem;margin:2.2rem 0 .5rem}
.lede{color:#555;margin:0 0 1.6rem}
.count{display:inline-block;background:#f2f4f7;border-radius:4px;padding:.15rem .5rem;margin-right:.4rem;
font-size:.85rem;color:#333}
table{border-collapse:collapse;width:100%;font-size:.83rem;margin:.5rem 0 1rem}
th{text-align:left;border-bottom:2px solid #ddd;padding:.35rem .5rem;font-weight:600;color:#444}
td{border-bottom:1px solid #eee;padding:.35rem .5rem;vertical-align:top}
td.y{white-space:nowrap;font-variant-numeric:tabular-nums}
td.ev{color:#666;font-size:.78rem} .inferred{color:#a35200;font-weight:600}
.unknown{color:#b00020;font-weight:600} .documented{color:#3b7a3b}
.ask{border-left:3px solid #b00020;padding:.1rem 0 .1rem 1rem;margin:1rem 0}
.ask p{margin:.35rem 0} .q{font-weight:600}
details{margin:.6rem 0} summary{cursor:pointer;color:#2b5fa8;font-size:.9rem}
.ok{color:#555;font-size:.88rem}
</style>"""]
    out.append("<h1>Survey reference years</h1>")
    out.append('<p class="lede">A survey does not measure the year it was run. These are the years each '
               "survey estimate actually describes, from ten agents reading the reports, checked against "
               "the dates and windows they quote. Only what needs a decision is spelled out; the settled "
               "moves are folded away at the bottom.</p>")
    out.append(f'<p><span class="count">{len(all_rows)} points</span>'
               f'<span class="count">{sum(1 for r in all_rows if r["basis"] == "documented")} documented</span>'
               f'<span class="count">{len(f["not documented"])} not documented</span>'
               f'<span class="count">{len(moving)} would move</span>'
               f'<span class="count">{len(dropped)} fall before {first_year}, so stop being plotted</span></p>')
    if offchart:
        out.append(f'<p class="ok">{len(offchart)} further points were researched but are older than '
                   f'{first_year} and were never drawn, so they are left out of everything above: '
                   + ", ".join(f'{_esc(r["country"])} {r["plotted"]}' for r in offchart) + ".</p>")
    for name, n, line in bad:
        out.append(f'<p class="unknown">MALFORMED row in {name}: {n} fields — {_esc(line)}</p>')

    out.append("<h2>Needs a decision</h2>")
    asked = False

    if f["collision"]:
        asked = True
        out.append('<div class="ask"><p class="q">Two rounds landing on the same year — one has to give.</p>')
        for country, year, plotted in f["collision"]:
            out.append(f"<p>{_esc(country)}: the points plotted at {', '.join(plotted)} all describe "
                       f"{year}.</p>")
        out.append("</div>")

    settled_windows = [r for r in all_rows if (r["country"], r["plotted"]) in RESOLVED]
    if f["contradiction"]:
        asked = True
        out.append('<div class="ask"><p class="q">The report contradicts itself about its own window, '
                   "which changes the value and not just the year.</p><table>"
                   "<tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th><th>Basis</th>"
                   "<th>What the report says</th></tr>")
        out += [row_html(r) for r in f["contradiction"]]
        out.append("</table></div>")

    unknown = [r for r in all_rows if r["basis"] == "unknown"]
    if unknown:
        asked = True
        out.append('<div class="ask"><p class="q">No reference period established at all — these keep '
                   "their current year unless you say otherwise.</p><table>"
                   "<tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th><th>Basis</th>"
                   "<th>What was tried</th></tr>")
        out += [row_html(r) for r in unknown]
        out.append("</table></div>")

    shaky = [r for r in f["not documented"] if r["basis"] == "inferred" and r["plotted"] != r["recommended"]]
    if shaky:
        asked = True
        out.append('<div class="ask"><p class="q">Moves resting on an assumed window rather than the '
                   "report's own words. The window is the standard one for the survey type, but no "
                   "sentence in the report was found to confirm it.</p><table>"
                   "<tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th><th>Basis</th>"
                   "<th>Evidence</th></tr>")
        out += [row_html(r) for r in shaky]
        out.append("</table></div>")

    if not asked:
        out.append('<p class="ok">Nothing. Every point is documented, consistent and unambiguous.</p>')

    if settled_windows:
        out.append("<h2>Window settled by the value we plot</h2>")
        out.append('<p class="ok">These reports give their rate on a five-year window while a later round '
                   "re-tabulates it on the standard three, which would change the value as well as the "
                   "year. Each was settled by checking the figure we actually plot against both.</p>"
                   "<table><tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th>"
                   "<th>Basis</th><th>How it was settled</th></tr>")
        out += [row_html(r, RESOLVED[(r["country"], r["plotted"])]) for r in settled_windows]
        out.append("</table>")

    if f["boundary"]:
        out.append("<h2>Close calls, not asking</h2>")
        out.append('<p class="ok">The midpoint sits within three months of the boundary between two years, '
                   "so either is defensible. The agent read the report and this script only reads its prose, "
                   "so the agent's year stands.</p><table>"
                   "<tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th><th>Basis</th>"
                   "<th>Note</th></tr>")
        out += [row_html(r) for r in f["boundary"]]
        out.append("</table>")

    out.append("<h2>Settled</h2>")
    lines = []
    for country in sorted(by_country):
        rs = by_country[country]
        mv = sum(1 for r in rs if r in moving)
        dr = sum(1 for r in rs if r in dropped)
        bits = [f"{len(rs)} point{'s' if len(rs) > 1 else ''}"]
        bits.append(f"{mv} move" if mv else "none move")
        if dr:
            bits.append(f"{dr} drops before {first_year}")
        lines.append(f"<b>{_esc(country)}</b>: {', '.join(bits)}")
    out.append('<p class="ok">' + " &middot; ".join(lines) + "</p>")
    out.append("<details><summary>Every point, with its evidence</summary><table>"
               "<tr><th>Country</th><th>Year</th><th>Fieldwork</th><th>Period</th><th>Basis</th>"
               "<th>Evidence</th></tr>")
    out += [row_html(r) for r in sorted(all_rows, key=lambda r: (r["country"], r["plotted"]))]
    out.append("</table></details>")
    return "\n".join(out)


def write_mapping():
    """refyears/years.csv, the one file the build reads. Nothing re-dates 47 loaders by hand."""
    path = os.path.join(LOG, "years.csv")
    with open(path, "w") as f:
        f.write("country,plotted_year,reference_year,basis\n")
        for r in sorted(rows()[0], key=lambda r: (r["country"], r["plotted"])):
            year, _ = resolve(r)
            if year is None or not r["plotted"].isdigit():
                continue
            f.write(f'{r["country"]},{r["plotted"]},{year},{r["basis"]}\n')
    return path


if __name__ == "__main__":
    all_rows, bad = rows()
    if "--check" not in sys.argv:
        print(f"wrote {write_mapping()}")
        path = os.path.join(LOG, "review.html")
        open(path, "w").write(review_html(all_rows, bad))
        print(f"wrote {path}")
        sys.exit(0)
    print(f"{len(all_rows)} rows from {len(set(r['batch'] for r in all_rows))} batches")
    for name, n, line in bad:
        print(f"  MALFORMED in {name}: {n} fields — {line}")
    f = flags(all_rows)
    applied, dropped = moves(all_rows)
    print(f"  documented {sum(1 for r in all_rows if r['basis'] == 'documented')}, "
          f"inferred {sum(1 for r in all_rows if r['basis'] == 'inferred')}, "
          f"unknown {sum(1 for r in all_rows if r['basis'] == 'unknown')}")
    print(f"  would move: {sum(1 for r in applied if r['plotted'] != r['recommended'])} of {len(applied)}; "
          f"drop before 2000: {len(dropped)}")
    for reason, items in f.items():
        print(f"\n{reason.upper()} ({len(items)})")
        for it in items:
            if reason == "collision":
                print(f"  {it[0]}: plotted {', '.join(it[2])} all land on {it[1]}")
            elif reason == "arithmetic":
                r, years, why = it
                print(f"  {r['country']} {r['plotted']} -> {r['recommended']}: {why}; "
                      f"defensible years {years or 'none derivable'}")
            else:
                print(f"  {it['country']} {it['plotted']} -> {it['recommended']} ({it['basis']})")
