"""Czechia: the statistical office's own rate, and the age-specific rates behind it.

One spreadsheet from the statistical handbook carries both: fertility rates for every single year of
the woman's age from 1950, and the office's own total on the last row. A second gives the mid-year
population by five-year age group, which is the denominator the office says it uses — the population
at midnight between 30 June and 1 July.

Births by single year of the mother's age exist as counts too, but only inside the yearly demographic
yearbook archives, one download per year. The published rates reproduce the office's own total to
within a ten-thousandth, so they are what is used -- but only once the last row is read for what it is.
It is labelled 45-49 and is the rate for that whole group rather than for the single age 45, so it
counts five times over. Adding every row once instead leaves the total short by up to 0.0033.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "cz")
DOCS = "https://csu.gov.cz/docs/107508"
RATES = f"{DOCS}/ec000212-1d63-da9d-fb54-7cc4715fb302/130055250611.xlsx?version=1.0"
POPULATION = f"{DOCS}/1e92b367-ca0d-3aad-8e62-21788aafcd2e/130055250110.xlsx?version=1.0"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
GROUPED_TOP = 45      # the rate table's last row covers 45-49 together, not the single age 45
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

    The rates are per single year of age and the population only by five-year group, so each group's
    women are spread evenly across its five ages before the rates are applied.

    The exception is the last row, and getting it wrong understated births to the oldest mothers by a
    factor of five. That row is labelled 45-49 and is a rate for the whole group, computed on all the
    women in it, not on one year of age -- which is why it has to be counted five times to reproduce
    the office's own total. Spreading the group's women across five ages first put 74 births at 45 and
    over in 2024, where the yearbook counts 352 at 45-49 and a handful more above 50.
    """
    by_age, _ = _rates()
    rates, women = by_age.get(year), _women().get(year)
    if not rates or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        w = women.get((lo, hi))
        total = sum(rates.get(a, 0.0) for a in range(lo, hi + 1))
        if not w or not total:
            continue
        per_age = w if lo == GROUPED_TOP else w / 5
        out[(lo, hi)] = {"women": w, "births": per_age * total / 1000}
    return out or None


if __name__ == "__main__":
    t = czechia_tfr()
    print(t.tail(7).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    d = czechia_detail(2024)
    print("2024 rebuilt from the groups:",
          round(sum(v["births"] / v["women"] for v in d.values()) * 5, 4), "against a published 1.3679")
    print("births to mothers 45 and over:", round(d[(45, 49)]["births"]),
          "- the yearbook counts 352 at 45-49, plus a few above 50")
