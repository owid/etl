"""Greece: ELSTAT's own total fertility rate, with a recalculation from births and women as a check.

ELSTAT publishes the rate itself, for every year from 1950, at full precision, in the time series of
its Demographic Indicators release. That is what is plotted. It does not publish the age-specific
rates behind it, so the check is built from what it does publish: births by the mother's age group for
every year since 1980, and population by five-year age group at 1 January. Its methodology defines the
average population as the mid-year figure, taken as the average of two consecutive years, so that is
the denominator the check uses.

The check runs about 2% below ELSTAT's own rate in recent years -- 1.234 against 1.2557 for 2024 --
even though both inputs match its published tables exactly, so something inside its own calculation
differs from the reconstruction. That is a reason to plot its figure rather than ours.

None of the three files has a guessable address: they are served by a document portlet keyed to an
identifier that has to be read off the publication page.
"""

import html
import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "gr")
PAGES = {
    "births": ("https://www.statistics.gr/en/statistics/-/publication/SPO03/2024", "116915"),
    "women": ("https://www.statistics.gr/en/statistics/-/publication/SPO18/2025", "116979"),
    "rate": ("https://www.statistics.gr/en/statistics/-/publication/DKT75/2024", "114814"),
}
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
FIRST = 2001            # the population table starts here


def _document(which):
    """The portlet address of one of ELSTAT's spreadsheets, read off its publication page."""
    page, document = PAGES[which]
    src = open(fetch(page, os.path.join(DATA, f"{which}.html")),
               encoding="utf-8", errors="replace").read()
    m = re.search(rf'href="([^"]*documentID={document}[^"]*)"', src)
    if not m:
        raise RuntimeError(f"ELSTAT no longer links document {document} on {page}")
    return html.unescape(m.group(1))


def _band_of(label):
    digits = re.findall(r"\d+", str(label))
    if len(digits) == 2:
        band = (int(digits[0]), int(digits[1]))
        return band if band in BANDS else None
    return None


def _births():
    """{year: {band: births}} by year of occurrence. Each year has its own sheet."""
    book = pd.ExcelFile(fetch(_document("births"), os.path.join(DATA, "births.xlsx")))
    out = {}
    for sheet in book.sheet_names:
        year = pd.to_numeric(re.sub(r"\D", "", str(sheet))[:4], errors="coerce")
        if pd.isna(year) or not 1980 <= year <= 2100:
            continue
        d = book.parse(sheet_name=sheet, header=None)
        for i in range(len(d)):
            band = _band_of(d.iloc[i, 0])
            if band is None:
                continue
            # the first numeric column of the row is the total across marital status
            for j in range(1, d.shape[1]):
                v = pd.to_numeric(d.iloc[i, j], errors="coerce")
                if pd.notna(v):
                    out.setdefault(int(year), {})[band] = float(v)
                    break
    return out


def _women():
    """{year: {band: women}} at 1 January, from the women's block of the population table."""
    d = pd.read_excel(fetch(_document("women"), os.path.join(DATA, "women.xlsx")), header=None)
    header = next(i for i in range(len(d))
                  if sum(1 for v in d.iloc[i]
                         if pd.notna(pd.to_numeric(v, errors="coerce"))
                         and 1990 < float(pd.to_numeric(v, errors="coerce")) < 2100) >= 5)
    years = {}
    for j in range(d.shape[1]):
        v = pd.to_numeric(str(d.iloc[header, j]).replace("*", ""), errors="coerce")
        if pd.notna(v) and 1990 < v < 2100:
            years[j] = int(v)
    # the table repeats the age ladder three times — everyone, then men, then women. Each block's
    # first row is its total, and the two rows above it carry the Greek word for the sex.
    start = next(i for i in range(len(d))
                 if str(d.iloc[i, 0]).strip().startswith("ΣΥΝΟΛΟ")
                 and any("ΘΗΛΕΙΣ" in str(v) for v in d.iloc[max(0, i - 2):i].to_numpy().ravel()))
    out = {}
    for i in range(start + 1, len(d)):
        band = _band_of(d.iloc[i, 0])
        if band is None:
            continue
        for j, year in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(year, {})[band] = float(v)
    return out


def greece_tfr():
    """ELSTAT's own rate. Two columns of its time series: the year, then the value."""
    d = pd.read_excel(fetch(_document("rate"), os.path.join(DATA, "rate.xlsx")), header=None)
    rows = []
    for i in range(len(d)):
        year = pd.to_numeric(d.iloc[i, 3], errors="coerce")
        value = pd.to_numeric(d.iloc[i, 4], errors="coerce")
        if pd.notna(year) and pd.notna(value) and 1900 < year < 2100:
            rows.append({"year": int(year), "value": float(value)})
    if not rows:
        raise RuntimeError("ELSTAT's fertility-rate time series parsed empty")
    return pd.DataFrame(rows)


def greece_recalculated():
    """The same rate rebuilt from ELSTAT's own births and population, as a check on the above."""
    births, women = _births(), _women()
    rows = []
    for year in sorted(births):
        if year < FIRST or year not in women or year + 1 not in women:
            continue
        b = births[year]
        mid = {x: (women[year].get(x, 0.0) + women[year + 1].get(x, 0.0)) / 2 for x in BANDS}
        if all(b.get(x) and mid.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / mid[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def greece_detail(year):
    births, women = _births().get(year), _women()
    if not births or year not in women or year + 1 not in women:
        return None
    out = {}
    for band in BANDS:
        mid = (women[year].get(band, 0.0) + women[year + 1].get(band, 0.0)) / 2
        if births.get(band) and mid:
            out[band] = {"births": births[band], "women": mid}
    return out or None


if __name__ == "__main__":
    t = greece_tfr()
    print(t.tail(6).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    own = dict(zip(t.year, t.value))
    r = greece_recalculated()
    print("\nagainst the recalculation from births and women:")
    for _, row in r.tail(6).iterrows():
        y = int(row.year)
        print(f"  {y}: ours {row.value:.4f}, ELSTAT {own.get(y, float('nan')):.4f}")
