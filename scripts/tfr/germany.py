"""Germany: Destatis birth statistics.

Table 12612-0008 gives live births per 1,000 women for each single year of age; 12612-0005
gives the births themselves by year, age of mother and birth order. Dividing one by the other
recovers the female population Destatis used, so no third table is needed.
"""

import os
import re
import warnings

import pandas as pd

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


def germany_detail(year):
    rates, births = _rates().get(year), _births().get(year)
    if not rates or not births:
        return None
    out = {}
    for lo, hi in BANDS:
        b = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
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
