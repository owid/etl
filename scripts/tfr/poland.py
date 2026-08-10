"""Poland: GUS births and population by single year of age, from the Local Data Bank API.

Everything here comes out of one open API with no key. Births are published by single year of the
mother's age, and population by single year of age at 30 June — which is the population GUS says it
divides by, so the recalculation reproduces its own published rate to three decimals.
"""

import json
import os
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "pl")
API = "https://bdl.stat.gov.pl/api/v1"
POLAND = "000000000000"
BIRTHS_SUBJECT = "P2167"      # live births by single year of the mother's age
POP_SUBJECT = "P3472"         # population by single year of age and sex, twice a year
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _get(url, name):
    """Fetch a JSON document once and keep it."""
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path, url], check=True)
    return json.load(open(path))


def _variables(subject):
    """Every variable under a subject, following the API's paging."""
    out, page = [], 0
    while True:
        d = _get(f"{API}/variables?subject-id={subject}&lang=pl&format=json&page-size=100&page={page}",
                 f"vars_{subject}_{page}.json")
        res = d.get("results", [])
        out.extend(res)
        if len(res) < 100:
            return out
        page += 1


def _series(var_ids, name):
    """{var id: {year: value}} for Poland as a whole."""
    q = "&".join(f"var-id={v}" for v in var_ids)
    d = _get(f"{API}/data/by-unit/{POLAND}?{q}&lang=pl&format=json&page-size=100", name)
    out = {}
    for v in d.get("results", []):
        out[v["id"]] = {int(x["year"]): x["val"] for x in v["values"] if x.get("val") is not None}
    return out


def _by_age(subject, name, pick):
    """{year: {age: value}} for the variables `pick` selects, keyed by the age it returns."""
    ages = {}
    for v in _variables(subject):
        age = pick(v)
        if age is not None:
            ages[v["id"]] = age
    data = _series(sorted(ages), name)
    out = {}
    for vid, years in data.items():
        for year, val in years.items():
            out.setdefault(year, {})[ages[vid]] = float(val)
    return out


def _birth_age(v):
    label = str(v.get("n1", "")).strip()
    return int(label) if label.isdigit() and 15 <= int(label) <= 49 else None


def _women_age(v):
    if str(v.get("n1", "")).strip() != "stan na 30 czerwca":
        return None
    if str(v.get("n2", "")).strip() != "kobiety":
        return None
    label = str(v.get("n3", "")).strip()
    return int(label) if label.isdigit() and 15 <= int(label) <= 49 else None


def poland_tfr():
    births = _by_age(BIRTHS_SUBJECT, "births.json", _birth_age)
    women = _by_age(POP_SUBJECT, "women.json", _women_age)
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        ages = [a for a in range(15, 50) if b.get(a) is not None and w.get(a)]
        if len(ages) == 35:
            rows.append({"year": year, "value": sum(b[a] / w[a] for a in ages)})
    return pd.DataFrame(rows)


def poland_detail(year):
    births = _by_age(BIRTHS_SUBJECT, "births.json", _birth_age).get(year)
    women = _by_age(POP_SUBJECT, "women.json", _women_age).get(year)
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
    t = poland_tfr()
    print(t.to_string(index=False))
    print("GUS publishes 1.158 for 2023, 1.099 for 2024, 1.068 for 2025")
