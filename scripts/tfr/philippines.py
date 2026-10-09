"""Philippines: PSA OpenSTAT (PX-Web). Registered live births by age group of mother, over
the PSA's projected female population.

PSA rejects the default Python user agent, so the fetch sets a browser one. Responses are
cached under data/ so the API is hit once.
"""

import json
import os

import pandas as pd
import requests

DATA = os.path.join(os.path.dirname(__file__), "data")
API = "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"

# one births table per year
BIRTHS = {2024: "VS/BI/0081A1ABIC5.px", 2023: "VS/BI/0301A1ABIA8.px"}
POP = "PO/0021A3BPOP1.px"

AGE = {
    "Under 15": (10, 14), "15-19": (15, 19), "20-24": (20, 24), "25-29": (25, 29),
    "30-34": (30, 34), "35-39": (35, 39), "40-44": (40, 44), "45-49": (45, 49),
    "50 and over": (50, 54),
}


def _px(table, query, cache_key):
    path = os.path.join(DATA, f"ph_{cache_key}.json")
    if os.path.exists(path):
        return json.load(open(path))
    r = requests.post(
        f"{API}/{table}",
        json={"query": query, "response": {"format": "json"}},
        headers={"User-Agent": UA, "Content-Type": "application/json"},
        timeout=90,
    )
    r.raise_for_status()
    data = json.loads(r.content.decode("utf-8-sig"))
    os.makedirs(DATA, exist_ok=True)
    json.dump(data, open(path, "w"))
    return data


def _labels(table, cache_key):
    """Code -> text for every dimension of a table."""
    path = os.path.join(DATA, f"ph_meta_{cache_key}.json")
    if os.path.exists(path):
        meta = json.load(open(path))
    else:
        r = requests.get(f"{API}/{table}", headers={"User-Agent": UA}, timeout=90)
        r.raise_for_status()
        meta = json.loads(r.content.decode("utf-8-sig"))
        json.dump(meta, open(path, "w"))
    return [dict(zip(v["values"], v["valueTexts"])) for v in meta["variables"]]


def births_by_age():
    """{year: {(lo, hi): births}}, with births of unstated maternal age redistributed."""
    out = {}
    for year, table in BIRTHS.items():
        maps = _labels(table, f"b{year}")
        data = _px(table, [{"code": "Birth Order", "selection": {"filter": "item", "values": ["0"]}}], f"b{year}")
        bands, unstated = {}, 0.0
        for row in data["data"]:
            label = maps[0].get(row["key"][0], "")
            val = pd.to_numeric(row["values"][0], errors="coerce")
            if pd.isna(val):
                continue
            if label in AGE:
                bands[AGE[label]] = float(val)
            elif label == "Not Stated":
                unstated = float(val)
        counted = sum(bands.values())
        scale = (counted + unstated) / counted if counted else 1.0
        out[year] = {b: v * scale for b, v in bands.items()}
    return out


def female_pop():
    """{year: {(lo, hi): women}} from the 2020-census-based projection.

    PSA publishes this projection in thousands, so values are scaled up to persons.
    """
    maps = _labels(POP, "pop")
    data = _px(POP, [{"code": "Sex", "selection": {"filter": "item", "values": ["2"]}}], "pop")
    out = {}
    for row in data["data"]:
        band_txt = maps[1].get(row["key"][1], "")
        year_txt = maps[2].get(row["key"][2], "")
        if "-" not in band_txt or not year_txt.isdigit():
            continue
        lo, hi = (int(x) for x in band_txt.split("-"))
        val = pd.to_numeric(row["values"][0], errors="coerce")
        if pd.notna(val):
            out.setdefault(int(year_txt), {})[(lo, hi)] = float(val) * 1000
    return out


def philippines():
    births, pop = births_by_age(), female_pop()
    rows = []
    for year in sorted(births):
        if year not in pop:
            continue
        tfr = 0.0
        for band, b in births[year].items():
            w = pop[year].get(band)
            if w:
                tfr += b / w * (band[1] - band[0] + 1)
        rows.append({"year": year, "value": tfr})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    print(philippines().to_string(index=False))
