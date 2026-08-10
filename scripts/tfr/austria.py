"""Austria: registered births by age of mother over the mean population.

Statistik Austria publishes births by five-year age band of the mother from 2006 and the mean
population by single year of age from 2004 — the mean over the year, which it says outright is the
denominator its own rate uses. Both are spreadsheets at stable addresses.

Births by single year of the mother's age exist only for the most recent year, so the bands are what
the series is built from. The office's own rate is published to two decimals and the recalculation
lands within a twentieth of a percent of it.

The two open ends are folded in: births to mothers under 15 are counted against the 15-19 women, and
births at 45 and over against the 45-49 women, which is the convention that reproduces the published
figure.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "at")
BIRTHS = ("https://www.statistik.at/fileadmin/pages/424/"
          "neu_Geborene_nach_demographischen_Merkmalen.ods")
POPULATION = ("https://www.statistik.at/fileadmin/pages/404/"
              "JDBev_Alter_Geschlecht_Staatsangeh_Bundesl_ab2004.ods")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
COUNTRY = "Österreich"
WOMEN = "Frauen"


def _births():
    """{year: {band: births}} for the whole country, from the age-band table.

    Provinces follow the country in the same sheet, so reading stops at the next province's heading.
    """
    d = pd.read_excel(fetch(BIRTHS, os.path.join(DATA, "births.ods")), sheet_name="Tabelle_10",
                      engine="odf", header=None)
    header = next(i for i in range(len(d)) if "unter 15" in str(d.iloc[i, 2]))
    cols = {}
    for j in range(2, d.shape[1]):
        label = str(d.iloc[header, j]).replace("\n", " ")
        if "unter 15" in label:
            cols[j] = (15, 19)                       # under 15, folded in with the youngest band
        elif "und älter" in label:
            cols[j] = (45, 49)                       # 45 and over, folded in with the oldest
        else:
            digits = [int(x) for x in label.replace("bis unter", " ").split() if x.isdigit()]
            if len(digits) == 2:
                band = (digits[0], digits[1] - 1)
                if band in BANDS:
                    cols[j] = band
    start = next(i for i in range(header, len(d)) if str(d.iloc[i, 1]).strip() == COUNTRY)
    out = {}
    for i in range(start + 1, len(d)):
        year = pd.to_numeric(str(d.iloc[i, 0]).strip(), errors="coerce")
        if pd.isna(year):
            if isinstance(d.iloc[i, 1], str):
                break                                # the first province
            continue
        for j, band in cols.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                got = out.setdefault(int(year), {})
                got[band] = got.get(band, 0.0) + float(v)
    return out


def _women():
    """{year: {band: women}} as the mean over the year, summed from single years of age.

    The sheet stacks blocks by citizenship and then by sex; the first women's block is the one for
    Austrian and foreign nationals together.
    """
    d = pd.read_excel(fetch(POPULATION, os.path.join(DATA, "women.ods")), sheet_name="Ö",
                      engine="odf", header=None)
    header = next(i for i in range(len(d))
                  if sum(1 for v in d.iloc[i]
                         if pd.notna(pd.to_numeric(v, errors="coerce"))
                         and 1990 < float(pd.to_numeric(v, errors="coerce")) < 2100) >= 5)
    years = {}
    for j in range(1, d.shape[1]):
        v = pd.to_numeric(d.iloc[header, j], errors="coerce")
        if pd.notna(v) and 1990 < v < 2100:
            years[j] = int(v)
    start = next(i for i in range(len(d)) if str(d.iloc[i, 0]).strip() == WOMEN)
    out = {}
    for i in range(start + 1, len(d)):
        label = str(d.iloc[i, 0]).strip()
        age = pd.to_numeric(label.split()[0] if label else "", errors="coerce")
        if pd.isna(age):
            if label and label != "nan":
                break                                # the next citizenship block
            continue
        band = next((b for b in BANDS if b[0] <= age <= b[1]), None)
        if band is None:
            continue
        for j, year in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                got = out.setdefault(year, {})
                got[band] = got.get(band, 0.0) + float(v)
    return out


def austria_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def austria_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = austria_tfr()
    print(t.tail(6).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    print("Statistik Austria publishes 1.296 for 2025, 1.311 for 2024, 1.32 for 2023")
