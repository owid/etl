"""Spain: INE births and population, both from the open JSON/CSV service.

INE serves whole tables as CSV with no key and no registration, so both halves of the
calculation come straight from the office: births by single year of age of mother from the birth
statistics, and female population by single year of age from the Estadística Continua de
Población. Dividing one by the other gives the fertility rate without borrowing any of INE's own
rate arithmetic.
"""

import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data")
BIRTHS = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/31936.csv"       # table 31936
POP = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/56934.csv"          # table 56934
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _num(s):
    """INE writes 318.005 for three hundred and eighteen thousand and five."""
    return pd.to_numeric(str(s).replace(".", "").replace(",", "."), errors="coerce")


def _age(label):
    m = re.match(r"(\d+) años?$", str(label).strip())
    return int(m.group(1)) if m else None


def _births():
    """{year: {age: births}} for the whole country, ages 15-49."""
    path = fetch(BIRTHS, os.path.join(DATA, "es", "births.csv"))
    d = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    d.columns = ["region", "age", "delivery", "maturity", "period", "value"]
    d = d[(d.region == "Total Nacional") & (d.delivery == "Total partos") & (d.maturity == "Total")]
    out = {}
    for _, r in d.iterrows():
        age = _age(r.age)
        if age is None or not 15 <= age <= 49:
            continue
        v = _num(r.value)
        if pd.notna(v):
            out.setdefault(int(r.period), {})[age] = float(v)
    return out


def _women():
    """{year: {age: women}} at 1 July, ages 15-49."""
    path = fetch(POP, os.path.join(DATA, "es", "population.csv"))
    d = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)
    d.columns = ["age", "sex", "period", "value"]
    d = d[(d.sex == "Mujeres") & d.period.str.contains("de julio de")]
    out = {}
    for _, r in d.iterrows():
        age = _age(r.age)
        if age is None or not 15 <= age <= 49:
            continue
        v = _num(r.value)
        if pd.notna(v):
            out.setdefault(int(r.period[-4:]), {})[age] = float(v)
    return out


def _bands(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        b = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        w = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


def spain_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        # single years of age, so each rate contributes its own width of one
        tfr = sum(b[a] / w[a] for a in range(15, 50) if b.get(a) and w.get(a))
        rows.append({"year": year, "value": tfr})
    return pd.DataFrame(rows)


def spain_detail(year):
    return _bands(year)


if __name__ == "__main__":
    print(spain_tfr().tail(6).to_string(index=False))
    d = _bands(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}")
