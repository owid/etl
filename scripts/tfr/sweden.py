"""Sweden: SCB births by single year of the mother's age over its mean population.

Both come from SCB's open database, which needs no key. SCB states its own method plainly — births
to women of a given age over the mean number of women of that age, the mean being the average of the
population at the start and the end of the year — so the recalculation reproduces its published rate
to within half a percent.

The residual gap is a definition, not an error. The public births table records the age the mother
reached by the end of the year, while SCB's own rate uses her age at the birth itself. Summing SCB's
own five-year age-specific rates instead gives its published figure exactly, which is how we know
that is where the difference comes from.

The population is everyone registered as resident, whatever their citizenship. SCB publishes a
separate rate for men, from the father's age; this uses the women's.
"""

import json
import os
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "se")
API = "https://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0101"
BIRTHS = "BE0101H/FoddaK"            # live births by region, mother's age and child's sex
POPULATION = "BE0101D/MedelfolkHandelse"   # mean population by region, marital status, age and sex
SWEDEN = "00"
WOMAN = "2"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
FIRST = 2006                         # the mean-population table starts here


def _meta(table):
    path = os.path.join(DATA, "meta_" + table.replace("/", "_") + ".json")
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path,
                        f"{API}/{table}"], check=True)
    return json.load(open(path))


def _values(table, code):
    return next(v["values"] for v in _meta(table)["variables"] if v["code"] == code)


def _query(table, selection, name):
    """The table's own JSON, for the selection given as {variable code: [values]}.

    SCB caps how many cells one request may return and answers an oversized one with a bare 403
    rather than a truncated body, so every query here is pinned to Sweden as a whole.
    """
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        body = {"query": [{"code": code, "selection": {"filter": "item", "values": vals}}
                          for code, vals in selection.items()],
                "response": {"format": "json"}}
        os.makedirs(DATA, exist_ok=True)
        with open(os.path.join(DATA, "body.json"), "w") as f:
            json.dump(body, f)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "300", "-o", path,
                        "-H", "Content-Type: application/json",
                        "--data-binary", "@" + os.path.join(DATA, "body.json"),
                        f"{API}/{table}"], check=True)
    return json.load(open(path))


def _rows(doc):
    """[(key tuple, value)] from SCB's response, dropping the cells it leaves empty."""
    out = []
    for cell in doc["data"]:
        v = pd.to_numeric(cell["values"][0], errors="coerce")
        if pd.notna(v):
            out.append((tuple(cell["key"]), float(v)))
    return out


def _ages(table, code):
    """{value as published: age} for single years 15-49.

    Both tables carry open-ended ends and a total; only the single years and the top group are kept,
    with "49+" read as 49.
    """
    out = {}
    for v in _values(table, code):
        if v.isdigit() and 15 <= int(v) <= 49:
            out[v] = int(v)
        elif v == "49+":
            out[v] = 49
    return out


def _births():
    """{year: {age: births}}, both sexes of child combined."""
    ages = _ages(BIRTHS, "AlderModer")
    years = [y for y in _values(BIRTHS, "Tid") if int(y) >= FIRST]
    doc = _query(BIRTHS, {"Region": [SWEDEN], "AlderModer": list(ages), "Kon": ["1", "2"],
                          "Tid": years}, "births.json")
    out = {}
    for key, value in _rows(doc):
        _, age, _, year = key
        if age in ages:
            y = int(year)
            out.setdefault(y, {})[ages[age]] = out.setdefault(y, {}).get(ages[age], 0.0) + value
    return out


def _women():
    """{year: {age: women}} as the mean over the year, summed across marital status."""
    ages = _ages(POPULATION, "Alder")
    doc = _query(POPULATION, {"Region": [SWEDEN], "Civilstand": _values(POPULATION, "Civilstand"),
                              "Alder": list(ages), "Kon": [WOMAN],
                              "Tid": _values(POPULATION, "Tid")}, "women.json")
    out = {}
    for key, value in _rows(doc):
        _, _, age, _, year = key
        if age in ages:
            y = int(year)
            out.setdefault(y, {})[ages[age]] = out.setdefault(y, {}).get(ages[age], 0.0) + value
    return out


def sweden_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        ages = [a for a in range(15, 50) if b.get(a) is not None and w.get(a)]
        if len(ages) == 35:
            rows.append({"year": year, "value": sum(b[a] / w[a] for a in ages)})
    return pd.DataFrame(rows)


def sweden_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    out = {}
    for lo, hi in [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]:
        bb = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        ww = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        if bb and ww:
            out[(lo, hi)] = {"births": bb, "women": ww}
    return out or None


if __name__ == "__main__":
    t = sweden_tfr()
    print(t.tail(6).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    print("SCB publishes 1.43 for 2024, 1.45 for 2023, 1.52 for 2022")
