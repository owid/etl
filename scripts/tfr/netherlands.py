"""Netherlands: CBS births by single year of the mother's age over its own mean population.

Everything comes from one open interface with no key. CBS states plainly what its own rate is built
from — births in an age group over the mean number of women in that group, the mean being half the
population on 1 January and half on 31 December — so the recalculation reproduces its published
figure to three decimals.

Ages are the mother's age at 31 December of the birth year, which is the basis CBS's own rate uses.
The population is the municipal register: everyone registered as a resident, whatever their
nationality, with no separate nationals-only series to pick wrongly.
"""

import json
import os
import subprocess
import urllib.parse

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "nl")
API = "https://opendata.cbs.nl/ODataApi/OData"
BIRTHS = "37744ned"       # live births by mother's age and birth order
POPULATION = "03759ned"   # population on 1 January and mean over the year, by sex and single age
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
UNDER_16, OVER_48 = "41200", "21200"


def _age_code(age):
    """CBS codes single years of age as 10000 plus a hundred times the age."""
    return str(10000 + 100 * age)


def _get(table, where, select, name):
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        url = (f"{API}/{table}/TypedDataSet?$filter={urllib.parse.quote(where)}"
               f"&$select={urllib.parse.quote(select)}")
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "240", "-o", path, url], check=True)
    return json.load(open(path))["value"]


def _births():
    """{year: {age: births}} for ages 15-49, by year of occurrence.

    The table runs single years 16 to 48 with open-ended tails either side, so under-16 is folded
    into 15 and 49-and-over into 49.
    """
    rows = _get(BIRTHS,
                "BurgerlijkeStaatMoeder eq 'T001019' and VolgordeGeboorteUitDeMoeder eq 'T001111'",
                "Perioden,LeeftijdVanDeMoeder,LevendgebLeeftijdMoederOp3112_1", "births.json")
    codes = {_age_code(a): a for a in range(16, 49)}
    codes[UNDER_16] = 15
    codes[OVER_48] = 49
    out = {}
    for r in rows:
        age = codes.get(str(r["LeeftijdVanDeMoeder"]).strip())
        value = r["LevendgebLeeftijdMoederOp3112_1"]
        if age is None or value is None:
            continue
        year = int(str(r["Perioden"])[:4])
        out.setdefault(year, {})[age] = out.setdefault(year, {}).get(age, 0.0) + float(value)
    return out


def _women():
    """{year: {age: women}} for ages 15-49, the mean population over the year.

    CBS only fills the mean-population column from 1995; earlier years carry the 1 January stock
    alone, and are left out rather than approximated.
    """
    rows = _get(POPULATION, "Geslacht eq '4000   ' and BurgerlijkeStaat eq 'T001019' "
                            "and RegioS eq 'NL01  '",
                "Perioden,Leeftijd,GemiddeldeBevolking_2", "women.json")
    codes = {_age_code(a): a for a in range(15, 50)}
    out = {}
    for r in rows:
        age = codes.get(str(r["Leeftijd"]).strip())
        value = r["GemiddeldeBevolking_2"]
        if age is None or value is None:
            continue
        out.setdefault(int(str(r["Perioden"])[:4]), {})[age] = float(value)
    return out


def netherlands_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        ages = [a for a in range(15, 50) if b.get(a) is not None and w.get(a)]
        if len(ages) == 35:
            rows.append({"year": year, "value": sum(b[a] / w[a] for a in ages)})
    return pd.DataFrame(rows)


def netherlands_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        bb = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        ww = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        if bb and ww:
            out[(lo, hi)] = {"births": bb, "women": ww}
    return out or None


if __name__ == "__main__":
    t = netherlands_tfr()
    print(t.tail(8).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    print("CBS publishes 1.430 for 2023, 1.426 for 2024, 1.418 for 2025")
