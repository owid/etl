"""Nepal: the 2021 census fertility tables, from the office's own census API.

The published fertility rate does not follow from these counts — see the note in countries.py. The
counts themselves are solid: they reproduce the office's own general fertility rate, crude birth rate
and gross reproduction rate almost exactly.
"""

import json
import os

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "np")
API = "https://censusapi.cbs.gov.np/api/v1"
BANDS = {
    "fifteen_nineteen": (15, 19), "twenty_twentyfour": (20, 24), "twentyfive_twentynine": (25, 29),
    "thirty_thirtyfour": (30, 34), "thrtyfive_thirtynine": (35, 39), "forty_fortyfour": (40, 44),
    "fortyfive_fortynine": (45, 49),
}


def _load(path, name):
    return json.load(open(fetch(f"{API}/{path}", os.path.join(DATA, name)), encoding="utf-8"))


def nepal_detail(year):
    """Births in the twelve months before the census, and women, by five-year age group."""
    if year != 2021:
        return None
    births = {}
    for s in _load("fertility", "fertility.json")["data"]["childrenBornAlive"]["countSeries"]:
        band = BANDS.get(s["category"])
        if band:
            births[band] = births.get(band, 0.0) + float(s["value"])
    women = {}
    for s in _load("population/age-group", "agegroup.json")["data"]["countSeries"]:
        label = str(s["category"])[:5]
        if str(s.get("sex", "")).upper().startswith("F") and "-" in label:
            lo, hi = (int(x) for x in label.split("-"))
            if (lo, hi) in births:
                women[(lo, hi)] = float(s["value"])
    return {b: {"births": births[b], "women": women[b]} for b in births if b in women} or None
