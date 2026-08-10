"""France: INSEE births and fertility rates by age of mother.

Both files use age reached during the year, so dividing births by the rate recovers INSEE's
own female population denominator without needing a third file.
"""

import os
import re
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _rates():
    """{year: {age: births per 10,000 women}} from the Bilan démographique table."""
    d = pd.read_excel(os.path.join(DATA, "fr_asfr_fix.xlsx"), sheet_name="FR - âge détaillé", header=None)
    ages = {j: int(re.match(r"(\d+) ans", str(d.iloc[3, j])).group(1))
            for j in range(1, d.shape[1])
            if re.match(r"\d+ ans", str(d.iloc[3, j]))}
    out = {}
    for i in range(4, len(d)):
        m = re.match(r"^(\d{4})", str(d.iloc[i, 0]).strip())
        if not m:
            continue
        for j, age in ages.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(int(m.group(1)), {})[age] = float(v)
    return out


def _births():
    """{year: {age: births}} from the long-series civil-registry table."""
    d = pd.read_excel(os.path.join(DATA, "fr_t48_fix.xlsx"), sheet_name="FR", header=None)
    ages = {j: int(re.match(r"(\d+) ans", str(d.iloc[3, j])).group(1))
            for j in range(1, d.shape[1])
            if re.match(r"\d+ ans$", str(d.iloc[3, j]).strip())}
    out = {}
    for i in range(4, len(d)):
        m = re.match(r"^(\d{4})", str(d.iloc[i, 0]).strip())
        if not m:
            continue
        for j, age in ages.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(int(m.group(1)), {})[age] = float(v)
    return out


def france_tfr():
    rates = _rates()
    return pd.DataFrame(
        [{"year": y, "value": sum(r.values()) / 10000} for y, r in sorted(rates.items())]
    )


def france_detail(year):
    """Births and the implied female population, by five-year band."""
    rates, births = _rates().get(year), _births().get(year)
    if not rates or not births:
        return None
    out = {}
    for lo, hi in BANDS:
        b = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        w = sum(births[a] / (rates[a] / 10000) for a in range(lo, hi + 1) if rates.get(a) and a in births)
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


if __name__ == "__main__":
    print(france_tfr().tail(4).to_string(index=False))
    d = france_detail(2025)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>9,.0f}  women {v['women']:>11,.0f}")
