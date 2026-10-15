"""Australia: ABS births by single year of the mother's age over its own population estimates.

Births come from the statistical data API, which needs no key. The population does not: the API only
serves five-year bands nationally, so the single-year figures come from table 59 of the population
release, whose URL carries the release month and therefore changes each time.
"""

import io
import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "au")
BIRTHS = ("https://data.api.abs.gov.au/rest/data/ABS,BIRTHS_AGE_MOTHER,1.0.0/1.TOT..AUS.A"
          "?startPeriod=2000&format=csv")
# table 59, "Estimated resident population by single year of age, Australia" — the release month in
# this path moves with each edition
POPULATION = ("https://www.abs.gov.au/statistics/people/population/"
              "national-state-and-territory-population/dec-2025/3101059.xlsx")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _births():
    """{year: {age: births}} for single ages 15-49, by year of registration.

    ABS publishes single years 16 to 48, with 0015 for fifteen and under and 4999 for forty-nine and
    over. Those two are folded into ages 15 and 49, which is the convention that reproduces ABS's own
    published rate.
    """
    path = fetch(BIRTHS, os.path.join(DATA, "births.csv"))
    d = pd.read_csv(path)
    age_col = next(c for c in d.columns if c.upper() == "AGE")
    time_col = next(c for c in d.columns if "TIME_PERIOD" in c.upper())
    out = {}
    for _, r in d.iterrows():
        raw = str(r[age_col]).strip()
        if raw == "0015":
            age = 15
        elif raw == "4999":
            age = 49
        else:
            age = int(raw) if raw.isdigit() and len(raw) <= 2 else None
        v = pd.to_numeric(r.OBS_VALUE, errors="coerce")
        if age is None or not 15 <= age <= 49 or pd.isna(v):
            continue
        year = int(r[time_col])
        out.setdefault(year, {})[age] = out.setdefault(year, {}).get(age, 0.0) + float(v)
    return out


def _women():
    """{year: {age: women}} at 30 June, from table 59.

    Each series is a column headed like "Estimated Resident Population ; Female ; 30 ;".
    """
    path = fetch(POPULATION, os.path.join(DATA, "population.xlsx"))
    with open(path, "rb") as f:
        book = io.BytesIO(f.read())
    frames = []
    for sheet in ("Data1", "Data2"):
        try:
            frames.append(pd.read_excel(book, sheet_name=sheet, header=None))
        except ValueError:
            continue
    out = {}
    for d in frames:
        head = next((i for i in range(min(20, len(d)))
                     if any("Estimated Resident Population" in str(v) for v in d.iloc[i])), None)
        if head is None:
            continue
        cols = {}
        for j in range(1, d.shape[1]):
            m = re.search(r"Female\s*;\s*(\d{1,3})\s*;", str(d.iloc[head, j]))
            if m and 15 <= int(m.group(1)) <= 49:
                cols[j] = int(m.group(1))
        for i in range(head + 1, len(d)):
            stamp = pd.to_datetime(d.iloc[i, 0], errors="coerce")
            if pd.isna(stamp) or stamp.month != 6:
                continue
            for j, age in cols.items():
                v = pd.to_numeric(d.iloc[i, j], errors="coerce")
                if pd.notna(v):
                    out.setdefault(int(stamp.year), {})[age] = float(v)
    return out


def australia_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        ages = [a for a in range(15, 50) if b.get(a) is not None and w.get(a)]
        if len(ages) == 35:
            rows.append({"year": year, "value": sum(b[a] / w[a] for a in ages)})
    return pd.DataFrame(rows)


def australia_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        bb = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        ww = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        if bb and ww:
            out[(lo, hi)] = {"births": bb, "women": ww}
    return out or None


if __name__ == "__main__":
    t = australia_tfr()
    print(t.tail(6).to_string(index=False))
    print("ABS publishes 1.481 for 2024, 1.499 for 2023")
