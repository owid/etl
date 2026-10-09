"""Germany: Destatis birth statistics.

Table 12612-0008 gives live births per 1,000 women for each single year of age; 12612-0005 gives
the births themselves by year, age of mother and birth order. The denominator comes from table
12411-10, the average population over the year by single year of age, which Destatis publishes as
part of its annual population report — the same concept its own rates are built on.
"""

import os
import re
import warnings

import pandas as pd

from fetch import fetch

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _rates():
    """{year: {age: births per 1,000 women}} from table 12612-0008.

    Each year appears twice in the header, once for the value and once for a quality flag.
    """
    lines = open(os.path.join(DATA, "de", "12612-0008_en.csv"), encoding="utf-8-sig").read().splitlines()
    out = {}
    for block, hdr in enumerate(i for i, ln in enumerate(lines) if re.match(r"^;\d{4};", ln)):
        years = [int(y) for y in lines[hdr].split(";")[1:] if y.strip().isdigit()][0::2]
        for ln in lines[hdr + 1:]:
            m = re.match(r"^(\d+) years?;", ln)
            if not m:
                continue
            age = int(m.group(1))
            for y, raw in zip(years, ln.split(";")[1:][0::2]):
                v = pd.to_numeric(raw.replace(",", "."), errors="coerce")
                if pd.notna(v):
                    out.setdefault(y, {})[age] = float(v)
    return out


def _births():
    """{year: {age: births}} from table 12612-0005, summed over birth orders."""
    out = {}
    for ln in open(os.path.join(DATA, "de5", "12612-0005_en.csv"), encoding="utf-8-sig"):
        parts = ln.rstrip("\n").split(";")
        if len(parts) < 4 or not re.match(r"^\d{4}$", parts[0].strip()):
            continue
        m = re.match(r"^(\d+) years?$", parts[1].strip())
        if not m:
            continue                                   # skips "under 15 years" and the total row
        year, age = int(parts[0]), int(m.group(1))
        total = 0.0
        for raw in parts[2:][0::2]:                    # value, quality flag, value, flag, ...
            v = pd.to_numeric(raw, errors="coerce")
            if pd.notna(v):
                total += float(v)
        out.setdefault(year, {})[age] = total
    return out


def germany_tfr():
    return pd.DataFrame(
        [{"year": y, "value": sum(r.values()) / 1000} for y, r in sorted(_rates().items())]
    )


POP = ("https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Bevoelkerungsstand/"
       "Publikationen/Downloads-Bevoelkerungsstand/statistischer-bericht-"
       "bevoelkerungsfortschreibung-zensus-2022-jaehrlich-5124108257005.xlsx?__blob=publicationFile&v=2")


def germany_women():
    """({age: women}, year) from Destatis table 12411-10, or None.

    This is the average population over the year — the mean of the stocks at the start and end —
    which is the concept Destatis divides by. The all-residents column is the right one: the
    German-nationals column is 20-30% smaller and would push the rate far too high. Each edition
    of the report carries only its own year.
    """
    path = fetch(POP, os.path.join(DATA, "de", "bevoelkerung.xlsx"))
    d = pd.read_excel(path, sheet_name="12411-10", header=None)
    m = re.search(r"Durchschnittliche Bevölkerung (\d{4})", str(d.iloc[1, 0]))
    if not m:
        return None
    out = {}
    for _, r in d.iterrows():
        a = re.fullmatch(r"\s*(\d{1,3}) – \d{1,3}\s*", str(r.iloc[0]))
        v = pd.to_numeric(r.iloc[3], errors="coerce")   # all residents, female
        if a and pd.notna(v):
            out[int(a.group(1))] = float(v)
    return (out, int(m.group(1))) if out else None


def germany_detail(year):
    rates, births = _rates().get(year), _births().get(year)
    if not rates or not births:
        return None
    counted = germany_women()
    women = counted[0] if counted and counted[1] == year else None
    out = {}
    for lo, hi in BANDS:
        b = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        if women:
            w = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        else:
            # falls back to the population Destatis's own rates imply
            w = sum(births[a] / (rates[a] / 1000) for a in range(lo, hi + 1) if rates.get(a) and births.get(a))
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


if __name__ == "__main__":
    print(germany_tfr().tail(3).to_string(index=False))
    d = germany_detail(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}")
    print("implied TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 4))
