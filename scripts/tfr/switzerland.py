"""Switzerland: the federal office's own fertility rate, from its open database.

The office publishes the rate back to 1803, and separately for Swiss and foreign mothers since 1971 —
a gap of about 0.3 in recent years. It does not publish births by age of mother anywhere, in any form,
so the rate cannot be rebuilt from counts; this is the published figure.

The database answers a plain request with no key: fetch the table's description, then post the
selection back to the same address.
"""

import json
import os
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "ch")
TABLE = "px-x-0102020204_111"     # live births by month, and fertility measures, since 1803
API = f"https://www.pxweb.bfs.admin.ch/api/v1/de/{TABLE}/{TABLE}.px"
INDICATORS = {"21": "all", "22": "swiss", "23": "foreign"}
YEAR, MEASURE = "Jahr", "Demografisches Merkmal und Indikator"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
FIRST = 2000


def _meta():
    path = os.path.join(DATA, "meta.json")
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path, API], check=True)
    return json.load(open(path))


def _series():
    """{measure: {year: value}} for the three fertility measures the table carries."""
    path = os.path.join(DATA, "fertility.json")
    if not os.path.exists(path):
        years = [y for y in next(v["values"] for v in _meta()["variables"] if v["code"] == YEAR)
                 if int(y) >= FIRST]
        body = {"query": [{"code": YEAR, "selection": {"filter": "item", "values": years}},
                          {"code": MEASURE,
                           "selection": {"filter": "item", "values": list(INDICATORS)}}],
                "response": {"format": "json"}}
        os.makedirs(DATA, exist_ok=True)
        with open(os.path.join(DATA, "body.json"), "w") as f:
            json.dump(body, f)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "300", "-o", path,
                        "-H", "Content-Type: application/json",
                        "--data-binary", "@" + os.path.join(DATA, "body.json"), API], check=True)
    out = {}
    for cell in json.load(open(path))["data"]:
        year, measure = cell["key"]
        v = pd.to_numeric(cell["values"][0], errors="coerce")
        if pd.notna(v) and measure in INDICATORS:
            out.setdefault(INDICATORS[measure], {})[int(year)] = float(v)
    return out


def switzerland_tfr():
    rows = [{"year": y, "value": v} for y, v in sorted(_series().get("all", {}).items())]
    return pd.DataFrame(rows)


if __name__ == "__main__":
    s = _series()
    t = switzerland_tfr()
    print(t.tail(6).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    for year in (2022, 2023, 2024):
        print(year, "all", s["all"].get(year), "Swiss mothers", s["swiss"].get(year),
              "foreign mothers", s["foreign"].get(year))
