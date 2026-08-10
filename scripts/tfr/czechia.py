"""Czechia: the statistical office's own rate, and the age-specific rates behind it.

One spreadsheet from the statistical handbook carries both: fertility rates for every single year of
the woman's age from 1950, and the office's own total on the last row. A second gives the mid-year
population by five-year age group, which is the denominator the office says it uses — the population
at midnight between 30 June and 1 July.

Births by single year of the mother's age exist as counts too, but only inside the yearly demographic
yearbook archives, one download per year. The rates published here already reproduce the office's own
total exactly, so they are what is used.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "cz")
DOCS = "https://csu.gov.cz/docs/107508"
RATES = f"{DOCS}/ec000212-1d63-da9d-fb54-7cc4715fb302/130055250611.xlsx?version=1.0"
POPULATION = f"{DOCS}/1e92b367-ca0d-3aad-8e62-21788aafcd2e/130055250110.xlsx?version=1.0"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
FIRST = 2000


def _years(row):
    """{column: year} from a header row of years, some of which come through as floats."""
    out = {}
    for j, v in enumerate(row):
        y = pd.to_numeric(str(v).replace(".0", ""), errors="coerce")
        if pd.notna(y) and 1900 < y < 2100:
            out[j] = int(y)
    return out


def _rates():
    """({year: {age: rate per 1,000}}, {year: published total}).

    The first age row is 15 and under and the last is 45-49, both footnoted; the sheet's final row is
    the office's own total fertility rate.
    """
    d = pd.read_excel(fetch(RATES, os.path.join(DATA, "rates.xlsx")), header=None)
    years = _years(d.iloc[3].tolist())
    by_age, totals = {}, {}
    for i in range(4, len(d)):
        label = str(d.iloc[i, 0]).strip()
        age = pd.to_numeric(label[:2], errors="coerce")
        is_total = label.startswith("Úhrnná")
        if pd.isna(age) and not is_total:
            continue
        for j, year in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.isna(v):
                continue
            if is_total:
                totals[year] = float(v)
            else:
                by_age.setdefault(year, {})[int(age)] = float(v)
    return by_age, totals


def _women():
    """{year: {band: women}} at mid-year, from the women's block of the population sheet."""
    d = pd.read_excel(fetch(POPULATION, os.path.join(DATA, "women.xlsx")), header=None)
    years = _years(d.iloc[3].tolist())
    start = next(i for i in range(len(d)) if str(d.iloc[i, 1]).strip().startswith("Ženy"))
    out = {}
    for i in range(start + 1, len(d)):
        label = str(d.iloc[i, 0]).strip().replace("–", "-")
        band = next((b for b in BANDS if label == f"{b[0]}-{b[1]}"), None)
        if band is None:
            continue
        for j, year in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(year, {})[band] = float(v)
    return out


def czechia_tfr():
    _, totals = _rates()
    rows = [{"year": y, "value": v} for y, v in sorted(totals.items()) if y >= FIRST]
    return pd.DataFrame(rows)


def czechia_detail(year):
    """Births implied by the office's own rates, against the population they were built on.

    The rates are per single year of age and the population only by five-year band, so each band's
    births are set to whatever reproduces that band's share of the published total.
    """
    by_age, _ = _rates()
    rates, women = by_age.get(year), _women().get(year)
    if not rates or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        total = sum(rates.get(a, 0.0) for a in range(lo, hi + 1)) or rates.get(lo, 0.0)
        w = women.get((lo, hi))
        if total and w:
            out[(lo, hi)] = {"women": w, "births": w * total / 5000}
    return out or None


if __name__ == "__main__":
    t = czechia_tfr()
    print(t.tail(7).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    d = czechia_detail(2024)
    print("2024 from the bands:", round(sum(v["births"] / v["women"] for v in d.values()) * 5, 4))
