"""Sri Lanka: registered births by age of mother over the Registrar General's mid-year population.

The Department of Census and Statistics stopped computing a fertility rate of its own around 2000,
so there is no official annual series to copy. It does keep publishing the two ingredients, and this
divides one by the other.

Births by age of mother run 1993-2020 in one table of the Statistical Abstract, with 2021 in its own
spreadsheet, and the mid-year population by age runs 2014-2024 in a separate release, so the overlap
is 2014-2021. The detailed births table stops there: 2022 onwards was never published in this form,
and those are the years of steepest decline.
"""

import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "lk")
BIRTHS = "https://www.statistics.gov.lk/abstract2025/CHAP3/3.3.pdf"
BIRTHS_2021 = "https://www.statistics.gov.lk/Population/VitalStatistics/Births/2021/Excel/table2.7"
WOMEN = ("https://www.statistics.gov.lk/Resource/en/Population/Vital_Statistics/"
         "Mid-year_population_by_agegroups_and_sex_2014_2024.pdf")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _text(url, name):
    path = fetch(url, os.path.join(DATA, name))
    txt = path[:-4] + ".txt"
    if not os.path.exists(txt):
        os.system(f"pdftotext -layout {path} {txt}")
    return open(txt, encoding="utf-8", errors="replace").read().splitlines()


def _number(token):
    return float(token.replace(",", "")) if re.fullmatch(r"[\d,]+", token) else None


def _births():
    """{year: {band: births}}, by registration year.

    The columns are total, under 15, the seven five-year bands, 45 and over, then age not stated.
    The last column is ".." before 2017 and never more than 0.05% of the total afterwards, so it is
    left where it is rather than redistributed.
    """
    out = {}
    for line in _text(BIRTHS, "births.pdf"):
        m = re.match(r"\s*(19\d{2}|20\d{2})\s+([\d,]+)\s+(.*)", line)
        if not m:
            continue
        fields = [_number(t) for t in m.group(3).split()]
        if len(fields) < 8 or any(v is None for v in fields[:8]):
            continue
        # fields[0] is under 15; fields[1:8] are the seven bands; fields[8] is 45 and over
        bands = dict(zip(BANDS[:-1], fields[1:7]))
        bands[(45, 49)] = fields[7]
        out[int(m.group(1))] = bands
    out[2021] = _births_2021()
    return out


def _births_2021():
    """{band: births} for 2021, from the last annual release to carry the breakdown.

    Districts run down the rows and ages across the columns; the whole-country row is "Sri Lanka".
    """
    path = fetch(BIRTHS_2021, os.path.join(DATA, "births_2021.xlsx"))
    d = pd.read_excel(path, header=None)
    head = next(i for i in range(len(d)) if "All" in str(d.iloc[i, 1]))
    cols = {}
    for j in range(2, d.shape[1]):
        m = re.fullmatch(r"(\d{2})\s*-\s*(\d{2})", str(d.iloc[head, j]).strip())
        if m and (int(m.group(1)), int(m.group(2))) in BANDS:
            cols[j] = (int(m.group(1)), int(m.group(2)))
    row = next(d.iloc[i] for i in range(head + 1, len(d))
               if str(d.iloc[i, 0]).replace(" ", "") == "SriLanka")
    return {band: float(row.iloc[j]) for j, band in cols.items()}


def _women():
    """{year: {band: women}} at mid-year, in people.

    The release prints three years side by side in each block, each split total/male/female, and
    reports thousands. Ages are taken from the 2012 census structure rolled forward, which is what
    the department itself says it does.
    """
    out = {}
    years = []
    for line in _text(WOMEN, "women.pdf"):
        tokens = line.split()
        if tokens and all(re.fullmatch(r"(19|20)\d{2}\*?", t) for t in tokens):
            years = [int(t.rstrip("*")) for t in tokens]
            continue
        m = re.match(r"\s*(\d{2})\s*-\s*(\d{2})\s+(.*)", line)
        if not m or not years:
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band not in BANDS:
            continue
        values = [_number(t) for t in m.group(3).split()]
        if len(values) != 3 * len(years) or any(v is None for v in values):
            continue
        for i, year in enumerate(years):
            out.setdefault(year, {})[band] = values[3 * i + 2] * 1000
    return out


def sri_lanka_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def sri_lanka_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    print(sri_lanka_tfr().to_string(index=False))
    print("the department's own series stops at 1.9 for 2000; the 2024 census reports 1.3")
