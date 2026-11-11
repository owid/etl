"""Kazakhstan: births by age of mother over the mean annual female population.

Both come from the statistics bureau's own database, which answers plain requests with no key. The
bureau states the method itself — age-specific rates use the average annual number of women, which it
defines as the mean of the start-of-year and end-of-year populations — so the recalculation reproduces
its published rate to within 0.01 in every year both exist.

Births are counted by the date they were registered, not the date they happened; the bureau says
explicitly that a birth registered in the reporting year is counted in that year even if it happened
earlier.

The five-year age classifier only carries the mean annual population from 2018, so that is where the
series starts, even though the births run from 2009.
"""

import json
import os
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "kz")
API = "https://taldau.stat.gov.kz/ru/Api/GetIndexData"
BIRTHS = ("703841", "67,749,1934")        # births by age of mother
POPULATION = ("703834", "67,749,576,3198")  # mean annual population by sex and age group
NATIONAL = "РЕСПУБЛИКА КАЗАХСТАН"
ALL = "Всего"
FEMALE = "Женский"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _fetch(index, dics, name):
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        url = f"{API}/{index}?period=7&dics={dics}"
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "600", "-o", path, url], check=True)
    return json.load(open(path))


def _band(label):
    """The age bands are labelled in Russian with the noun in whichever case the number takes."""
    digits = "".join(c if c.isdigit() else " " for c in label).split()
    if len(digits) == 2:
        band = (int(digits[0]), int(digits[1]))
        return band if band in BANDS else None
    return None


def _series(rows, name):
    """{year: {band: value}} for the whole country."""
    out = {}
    for r in rows:
        band = _band(r["termNames"][-1])
        if band is None:
            continue
        for p in r["periods"]:
            value = pd.to_numeric(p["value"], errors="coerce")
            if pd.notna(value):
                out.setdefault(int(p["date"][-4:]), {})[band] = float(value)
    return out


def _births():
    rows = [r for r in _fetch(*BIRTHS, "births.json")
            if r["termNames"][0] == NATIONAL and r["termNames"][1] == ALL]
    return _series(rows, "births")


def _women():
    rows = [r for r in _fetch(*POPULATION, "women.json")
            if r["termNames"][0] == NATIONAL and r["termNames"][1] == ALL
            and r["termNames"][2] == FEMALE]
    return _series(rows, "women")


def kazakhstan_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def kazakhstan_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    print(kazakhstan_tfr().to_string(index=False))
    print("the bureau publishes 2.96 for 2023, 2.80 for 2024, 2.57 for 2025")
