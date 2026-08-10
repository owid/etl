"""Japan: MHLW Vital Statistics via e-Stat.

Table 0003411608 gives age-specific birth rates and the total fertility rate; 0003411607
gives births by the same five-year age groups. Dividing one by the other recovers MHLW's own
female population denominator, so no third source is needed.
"""

import json
import os

import pandas as pd

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


def japan_detail(year):
    """Births by band and the female population implied by MHLW's own rates.

    e-Stat publishes each band's value as its contribution to the total fertility rate — the
    seven bands sum exactly to the published TFR — so the per-woman rate is that value / 5.
    """
    births = _table("jp_births.json")
    rates = _table("jp_asfr.json")
    out = {}
    for (y, band), b in births.items():
        if y != year:
            continue
        contribution = rates.get((y, band))
        if contribution:
            out[band] = {"births": b, "women": b / (contribution / 5)}
    return out or None
