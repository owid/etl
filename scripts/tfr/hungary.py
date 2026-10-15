"""Hungary: the statistical office's own rate, and the age-specific rates behind it.

The office publishes its fertility rate for every year since 1900, the age-specific rates by
five-year band since 1980, and population by single year of age at 1 January — all as small files at
stable addresses. It does not publish births by age of mother as counts anywhere free, so the rate is
taken as published rather than rebuilt.

The office says its age-specific rates use the mid-year population, so the decomposition here
averages the 1 January stocks either side of each year, which reproduces its own published births to
within a tenth of a percent.
"""

import io
import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "hu")
STADAT = "https://www.ksh.hu/stadat_files/nep/hu"
TOTAL = f"{STADAT}/nep0006.csv"       # live births and the total fertility rate
RATES = f"{STADAT}/nep0008.csv"       # live births per thousand women, by age group
POPULATION = f"{STADAT}/nep0003.csv"  # population by single year of age and sex, at 1 January
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
FIRST = 2000


def _read(url, name):
    """The office serves semicolon-separated Central European text with comma decimals."""
    with open(fetch(url, os.path.join(DATA, name)), "rb") as f:
        text = f.read().decode("iso-8859-2")
    return pd.read_csv(io.StringIO(text), sep=";", header=None, dtype=str)


def _number(cell):
    if not isinstance(cell, str):
        return None
    v = pd.to_numeric(cell.replace("\xa0", "").replace(" ", "").replace(",", "."), errors="coerce")
    return float(v) if pd.notna(v) else None


def hungary_tfr():
    d = _read(TOTAL, "total.csv")
    rows = []
    for _, r in d.iterrows():
        year = pd.to_numeric(str(r[0]).strip(), errors="coerce")
        value = _number(r[3]) if len(r) > 3 else None
        if pd.notna(year) and year >= FIRST and value:
            rows.append({"year": int(year), "value": value})
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def _rates():
    """{year: {band: rate per 1,000}} from the age-specific rate table.

    The dash between the two ages does not survive the file's encoding, so a heading for 15-19 reads
    "1519 eves" and the band is taken from the first four digits.
    """
    d = _read(RATES, "rates.csv")
    header = next(i for i in range(len(d)) if "15" in str(d.iloc[i, 1]))
    cols = {}
    for j in range(1, d.shape[1]):
        digits = "".join(c for c in str(d.iloc[header, j]) if c.isdigit())
        if len(digits) == 4:
            band = (int(digits[:2]), int(digits[2:]))
            if band in BANDS:
                cols[j] = band
    out = {}
    for i in range(header + 1, len(d)):
        year = pd.to_numeric(str(d.iloc[i, 0]).strip(), errors="coerce")
        if pd.isna(year):
            continue
        for j, band in cols.items():
            v = _number(d.iloc[i, j])
            if v is not None:
                out.setdefault(int(year), {})[band] = v
    return out


def _women():
    """{year: {band: women}} at 1 January, summed from single years of age.

    The years run across the columns once, and the table then repeats the whole age ladder in blocks
    by sex, one under the other. Only the block headed with the word for women is read.
    """
    d = _read(POPULATION, "women.csv")
    header = next(i for i in range(len(d))
                  if sum(1 for v in d.iloc[i] if _number(v) and 1900 < _number(v) < 2100) >= 5)
    years = {j: int(_number(d.iloc[header, j]))
             for j in range(1, d.shape[1])
             if _number(d.iloc[header, j]) and 1900 < _number(d.iloc[header, j]) < 2100}
    start = next(i for i in range(header, len(d)) if str(d.iloc[i, 0]).strip() == "Nő")
    out = {}
    for i in range(start + 1, len(d)):
        label = str(d.iloc[i, 0]).strip()
        age = pd.to_numeric(label, errors="coerce")
        if pd.isna(age):
            if label and label not in ("nan", ""):
                break                      # the next block, or the footnotes
            continue
        band = next((b for b in BANDS if b[0] <= age <= b[1]), None)
        if band is None:
            continue
        for j, year in years.items():
            v = _number(d.iloc[i, j])
            if v is not None:
                got = out.setdefault(year, {})
                got[band] = got.get(band, 0.0) + v
    return out


def hungary_detail(year):
    rates, women = _rates().get(year), _women()
    if not rates or year not in women or year + 1 not in women:
        return None
    out = {}
    for band in BANDS:
        mid = (women[year].get(band, 0.0) + women[year + 1].get(band, 0.0)) / 2
        rate = rates.get(band)
        if mid and rate:
            out[band] = {"women": mid, "births": rate / 1000 * mid}
    return out or None


if __name__ == "__main__":
    t = hungary_tfr()
    print(t.tail(8).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    d = hungary_detail(2022)
    if d:
        print("2022 from the bands:", round(sum(v["births"] / v["women"] for v in d.values()) * 5, 3),
              "implied births", f"{sum(v['births'] for v in d.values()):,.0f}",
              "— the office reports 88,491")
