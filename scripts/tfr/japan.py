"""Japan: MHLW Vital Statistics via e-Stat.

Table 0003411608 gives age-specific birth rates and the total fertility rate; 0003411607 gives
births by the same five-year age groups. The denominator comes from the Statistics Bureau's own
annual population estimates, which MHLW names as its source and which download without a key.
"""

import json
import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data")

AGE = {
    "19歳以下": (15, 19), "15～19歳": (15, 19), "20～24歳": (20, 24), "25～29歳": (25, 29),
    "30～34歳": (30, 34), "35～39歳": (35, 39), "40～44歳": (40, 44), "45～49歳": (45, 49),
    "45歳以上": (45, 49),
}


def _table(fname):
    """{(year, band): value} for the all-birth-orders rows, dropping the age total."""
    s = json.load(open(os.path.join(DATA, fname)))["GET_STATS_DATA"]["STATISTICAL_DATA"]
    maps = {
        c["@id"]: {x["@code"]: x["@name"] for x in (c["CLASS"] if isinstance(c["CLASS"], list) else [c["CLASS"]])}
        for c in s["CLASS_INF"]["CLASS_OBJ"]
    }
    out = {}
    for row in s["DATA_INF"]["VALUE"]:
        if maps["cat02"].get(row["@cat02"]) != "総数":
            continue
        band = AGE.get(maps["cat01"].get(row["@cat01"], ""))
        if not band:
            continue
        year = int(maps["time"][row["@time"]][:4])
        v = pd.to_numeric(row["$"], errors="coerce")
        if pd.notna(v):
            out[(year, band)] = float(v)
    return out


POP = "https://www.stat.go.jp/data/jinsui/{y}np/zuhyou/05k{y}-1.xlsx"


def japan_women(year):
    """{age: women} for single years 15-49, from the Statistics Bureau's population estimates.

    MHLW builds its fertility rate on the Japanese female population, not all residents, so the
    Japanese-only column is the one to use — the file gives both. Figures are in thousands.
    """
    try:
        path = fetch(POP.format(y=year), os.path.join(DATA, "jp", f"pop{year}.xlsx"))
    except subprocess.CalledProcessError:
        return None
    d = pd.read_excel(path, header=None)
    out = {}
    for _, r in d.iterrows():
        m = re.fullmatch(r"\s*(\d{1,3})\s*歳?\s*", str(r.iloc[0]))
        if not m:
            continue
        age = int(m.group(1))
        v = pd.to_numeric(r.iloc[7], errors="coerce")       # Japanese population, female
        if 15 <= age <= 49 and pd.notna(v):
            out[age] = float(v) * 1000
    return out or None


def japan_detail(year):
    """Births by band from MHLW, women by band from the Statistics Bureau.

    Where the population file is missing, the denominator falls back to what MHLW's own rates
    imply: e-Stat publishes each band's value as its contribution to the total fertility rate —
    the seven bands sum exactly to the published total — so the per-woman rate is that value / 5.
    """
    births = _table("jp_births.json")
    rates = _table("jp_asfr.json")
    women = japan_women(year)
    out = {}
    for (y, band), b in births.items():
        if y != year:
            continue
        counted = sum(women.get(a, 0.0) for a in range(band[0], band[1] + 1)) if women else 0.0
        if counted:
            out[band] = {"births": b, "women": counted}
            continue
        contribution = rates.get((y, band))
        if contribution:
            out[band] = {"births": b, "women": b / (contribution / 5)}
    return out or None


if __name__ == "__main__":
    d = japan_detail(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}")
    print("implied TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 4),
          "— MHLW publishes 1.15")
