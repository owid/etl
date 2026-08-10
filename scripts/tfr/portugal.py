"""Portugal: registered births by age of mother over INE's resident population.

Everything comes from INE's open interface, which needs no key. The catch is that every indicator is
reissued whenever the statistical regions are redrawn, and each new vintage only carries the last few
years — so a full series means splicing two codes together at the year where they overlap. They agree
exactly there. Older vintages reach back to 1995 and could extend this, at the cost of one request
per indicator per year.

INE publishes its own rate but not the age-specific rates behind it. Its denominator is the mean
population over the year, which is not published by age group; the population that is published is a
point estimate, so the mean is taken between consecutive years. That is what reproduces the published
figure a little more closely, but not fully: our answer still runs about 3% below INE's, so the
published series is the one plotted and ours is kept alongside as a check.
"""

import json
import os
import re
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "pt")
API = "https://www.ine.pt/ine/json_indicador/pindica.jsp"
# each vintage of an indicator covers only part of the span, so they are read in turn
BIRTHS = {"0008092": range(2012, 2024), "0012441": range(2023, 2026)}
POPULATION = {"0008273": range(2011, 2024), "0012918": range(2023, 2026)}
RATE = {"0008274": range(2013, 2026)}
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
FEMALE = "2"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _get(varcd, year):
    """The indicator's rows for one year, for the country as a whole.

    An out-of-range year comes back as an error message inside an HTTP 200, so the payload is checked
    rather than the status.
    """
    path = os.path.join(DATA, f"{varcd}_{year}.json")
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        url = f"{API}?op=2&varcd={varcd}&Dim1=S7A{year}&Dim2=PT&lang=PT"
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path, url], check=True)
    doc = json.load(open(path))
    if not isinstance(doc, list) or "Dados" not in doc[0]:
        return []
    rows = doc[0]["Dados"].get(str(year), [])
    return rows if isinstance(rows, list) else []


def _band_of(label):
    m = re.fullmatch(r"(\d{2})\s*-\s*(\d{2})\s*anos", str(label).strip())
    if not m:
        return None
    band = (int(m.group(1)), int(m.group(2)))
    return band if band in BANDS else None


def _value(row):
    v = pd.to_numeric(row.get("valor"), errors="coerce")
    return float(v) if pd.notna(v) else None


def _births():
    """{year: {band: births}}, both sexes of child and all birth orders."""
    out = {}
    for varcd, years in BIRTHS.items():
        for year in years:
            for r in _get(varcd, year):
                if r.get("dim_3") != "T" or r.get("dim_5") != "T":
                    continue
                band = _band_of(r.get("dim_4_t"))
                # the age dimension repeats each band at two levels of a hierarchy; the five-year
                # bands are the two-digit codes
                if band and len(str(r.get("dim_4"))) == 2:
                    out.setdefault(year, {})[band] = _value(r)
    return out


def _stock():
    """{year: {band: women}} as INE publishes it, a point estimate rather than a mean."""
    out = {}
    for varcd, years in POPULATION.items():
        for year in years:
            for r in _get(varcd, year):
                if r.get("dim_3") != FEMALE:
                    continue
                band = _band_of(r.get("dim_4_t"))
                if band:
                    out.setdefault(year, {})[band] = _value(r)
    return out


def _women():
    """{year: {band: women}} as the mean of the point estimates either side of the year."""
    stock = _stock()
    out = {}
    for year in stock:
        if year + 1 not in stock:
            continue
        out[year] = {b: (stock[year].get(b, 0.0) + stock[year + 1].get(b, 0.0)) / 2 for b in BANDS}
    return out


def portugal_published():
    """INE's own rate, for checking against."""
    out = {}
    for varcd, years in RATE.items():
        for year in years:
            for r in _get(varcd, year):
                v = _value(r)
                if v:
                    out[year] = v
    return out


def portugal_recalculated():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def portugal_tfr():
    """INE's own rate. Ours runs about 3% below it — see the note in countries.py."""
    return pd.DataFrame([{"year": y, "value": v} for y, v in sorted(portugal_published().items())])


def portugal_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = portugal_tfr()
    published = portugal_published()
    print(t.tail(8).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    for _, r in t.tail(5).iterrows():
        print(int(r.year), "ours", round(r.value, 3), "published", published.get(int(r.year)))
