"""Canada: Statistics Canada births by age of mother over its own population estimates.

The cleanest source in this project. Both tables come out of the same open service with no key, the
births are dated by the year they occurred rather than the year they were registered, and dividing
one by the other reproduces Statistics Canada's own published rate to the second decimal in every
year checked.
"""

import os
import re
import zipfile

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "ca")
CSV = "https://www150.statcan.gc.ca/n1/tbl/csv/{pid}-eng.zip"
BIRTHS, POPULATION, RATES = "13100416", "17100005", "13100418"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _table(pid, **kwargs):
    """The table's own CSV, unzipped once and cached."""
    csv = os.path.join(DATA, f"{pid}.csv")
    if not os.path.exists(csv):
        zpath = fetch(CSV.format(pid=pid), os.path.join(DATA, f"{pid}.zip"))
        with zipfile.ZipFile(zpath) as z:
            z.extract(f"{pid}.csv", DATA)
    return pd.read_csv(csv, **kwargs)


def _births():
    """{year: {band: births}}, scaled up for mothers whose age was not stated.

    Statistics Canada folds births to mothers of 50 and over into the 45-49 row for confidentiality,
    and reports the geography as "Canada, place of residence of mother".
    """
    d = _table(BIRTHS, usecols=["REF_DATE", "GEO", "Age of mother", "Characteristics", "VALUE"])
    d = d[d.GEO.str.startswith("Canada") & (d.Characteristics == "Number of live births")]
    out = {}
    for _, r in d.iterrows():
        label, v = str(r["Age of mother"]), pd.to_numeric(r.VALUE, errors="coerce")
        if pd.isna(v):
            continue
        year = int(r.REF_DATE)
        e = out.setdefault(year, {"bands": {}, "counted": 0.0, "unstated": 0.0})
        m = re.search(r"(\d{2}) to (\d{2}) years", label)
        if m and (band := (int(m.group(1)), int(m.group(2)))) in BANDS:
            e["bands"][band] = float(v)
            e["counted"] += float(v)
        elif "not stated" in label:
            e["unstated"] += float(v)
    final = {}
    for year, e in out.items():
        if len(e["bands"]) == len(BANDS):
            scale = (e["counted"] + e["unstated"]) / e["counted"]
            final[year] = {b: v * scale for b, v in e["bands"].items()}
    return final


def _women():
    """{year: {band: women}} at 1 July, from the population estimates.

    The table reports "Women+" rather than sex at birth from 2021, which Statistics Canada says
    barely changes the distribution. Its five-year groups are used directly.
    """
    d = _table(POPULATION, usecols=["REF_DATE", "GEO", "Gender", "Age group", "VALUE"])
    d = d[(d.GEO == "Canada") & (d.Gender.isin(["Women+", "Females"]))]
    out = {}
    for _, r in d.iterrows():
        m = re.fullmatch(r"(\d{2}) to (\d{2}) years", str(r["Age group"]).strip())
        v = pd.to_numeric(r.VALUE, errors="coerce")
        if not m or pd.isna(v):
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band in BANDS:
            out.setdefault(int(r.REF_DATE), {})[band] = float(v)
    return out


def canada_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def canada_published():
    """Statistics Canada's own total fertility rate, for checking ours against."""
    d = _table(RATES, usecols=["REF_DATE", "GEO", "Characteristics", "VALUE"])
    d = d[d.GEO.str.startswith("Canada") & d.Characteristics.str.contains("Total fertility", case=False)]
    return {int(r.REF_DATE): float(r.VALUE) for _, r in d.iterrows() if pd.notna(r.VALUE)}


def canada_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    ours, theirs = canada_tfr(), canada_published()
    d = ours[ours.year >= 2018]
    for _, r in d.iterrows():
        print(int(r.year), f"ours {r.value:.3f}  theirs {theirs.get(int(r.year))}")
    print(len(ours), "years", int(ours.year.min()), "-", int(ours.year.max()))
