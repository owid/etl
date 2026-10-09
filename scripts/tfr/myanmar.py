"""Myanmar: Department of Population, the 2024 census and the 2019 Inter-censal Survey.

Both rounds print the counts, not just the rate: for every five-year age group they give the number
of women and the number of live births in the twelve months before enumeration. So both points can
be rebuilt from the counts rather than taken from a printed total.

The two tables lay their columns out differently. The 2019 appendix table D-1 runs
ever-married women, then total women, then births; the 2024 appendix table A-9 runs total women,
then ever-married women, then births. Each round therefore gets its own reader.

The 2024 census also publishes a second, higher figure from an indirect method, which the department
calls the more robust of the two. It is not plotted, because that would put the two points on
different methods — the plotted line is the direct, reported-births figure for both rounds. The
indirect figure is described in countries.py instead.
"""

import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "mm")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
CENSUS_2024 = ("https://www.dop.gov.mm/sites/dop.gov.mm/files/publication_docs/"
               "2024mphc_appendixtables.pdf")


def _text(name, url=None):
    pdf = os.path.join(DATA, f"{name}.pdf")
    txt = os.path.join(DATA, f"{name}.txt")
    if not os.path.exists(txt):
        if url:
            fetch(url, pdf)
        subprocess.run(["pdftotext", "-layout", pdf, txt], check=True)
    return open(txt, errors="ignore").read().splitlines()


def _start_of_table(lines, caption):
    """Index of the table's own caption, not its entry in the list of tables.

    Both captions appear twice or more — once in the contents, once over the table, and again on
    each continuation page. The one wanted is the first that has the Union block right below it.
    """
    for i, ln in enumerate(lines):
        if caption in ln and any(re.match(r"\s*UNION\b", x, re.I) for x in lines[i + 1:i + 16]):
            return i
    raise AssertionError(f"Myanmar: could not find the table captioned {caption!r}")


def _union(lines, caption, women_col, births_col, asfr_col, tol):
    """{(lo, hi): {"women": n, "births": n}} for the whole country.

    Both tables repeat for Union, then Urban and Rural, then every state and region, so reading
    stops at the end of the first block.

    Each table also prints its own age-specific rate per 1,000 women. Dividing the two columns
    picked out here has to reproduce it — that is what confirms the right columns were read, and
    the two rounds number their columns differently.
    """
    head = _start_of_table(lines, caption)
    start = next(i for i, ln in enumerate(lines[head:], head) if re.match(r"\s*UNION\b", ln, re.I))
    out = {}
    for ln in lines[start + 1:]:
        m = re.match(r"\s*(\d{2})\s*-\s*(\d{2})\s+(.*)$", ln)
        if not m:
            if re.match(r"\s*(URBAN|RURAL)\b", ln, re.I):
                break
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band not in BANDS:
            continue
        nums = [t for t in m.group(3).split() if re.fullmatch(r"[\d,]+(\.\d+)?", t)]
        if len(nums) <= max(women_col, births_col, asfr_col):
            continue
        women = float(nums[women_col].replace(",", ""))
        births = float(nums[births_col].replace(",", ""))
        printed = float(nums[asfr_col].replace(",", ""))
        ours = births / women * 1000
        if abs(ours - printed) > tol:
            raise AssertionError(f"Myanmar {band}: recomputed rate {ours:.2f} against the printed "
                                 f"{printed:.2f} — the columns read are not the right ones")
        out[band] = {"women": women, "births": births}
        if len(out) == len(BANDS):
            break
    assert len(out) == len(BANDS), f"Myanmar: read {len(out)} of {len(BANDS)} age groups"
    return out


def _survey_2019():
    return _union(_text("ics_appendix"), "Table D-1:",
                  women_col=1, births_col=3, asfr_col=6, tol=0.01)


def _census_2024():
    # the census prints its rate rounded to a whole birth per 1,000, so the check is coarser
    return _union(_text("census2024_appendix", CENSUS_2024), "Table A-9:",
                  women_col=0, births_col=2, asfr_col=4, tol=0.5)


def _tfr(counts):
    return sum(v["births"] / v["women"] * 5 for v in counts.values())


ROUNDS = {2019: _survey_2019, 2024: _census_2024}


def myanmar_tfr():
    """Two points, each rebuilt from that round's own counts."""
    return pd.DataFrame([{"year": y, "value": _tfr(load())} for y, load in sorted(ROUNDS.items())])


def myanmar_detail(year):
    return ROUNDS[year]() if year in ROUNDS else None


if __name__ == "__main__":
    printed = {2019: "2.0", 2024: "1.40"}
    for year, load in sorted(ROUNDS.items()):
        counts = load()
        print(f"== {year}")
        for band, v in sorted(counts.items()):
            print(" ", band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}",
                  f"asfr {v['births'] / v['women'] * 1000:7.2f}")
        print("  recalculated TFR:", round(_tfr(counts), 4), "— report prints", printed[year])
