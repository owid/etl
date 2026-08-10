"""Taiwan: births by age of mother over the household-registered female population.

Both come from the interior ministry's statistics query service, which returns JSON to a plain GET
with no key. The service is only reachable by building the query by hand: the portal pages that link
to it are navigation shells, and the query tool itself runs in JavaScript.

The ministry publishes its own rate too, and the recalculation lands within 0.3% of it. Two details
matter. Births are counted by the date they happened, not the date they were registered, and are
only released annually for that reason. And the population is the year-end household register, not a
mid-year estimate and not the de facto resident population that the budget agency publishes
separately.
"""

import json
import os
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "tw")
API = "https://statis.moi.gov.tw/micst/webMain.aspx"
BIRTHS = "c0120105"                # births by mother's age, by date of occurrence
POPULATION = "c0110203"            # population by single year of age
FIRST, LAST = 2000, 2025
ROC = 1911                         # the calendar the service uses: ROC year = AD year - 1911
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _query(funid, fields, name, sex=None):
    """{column label: [value per year]} for the whole country, 2000 to 2025.

    `fields` is how many columns the table has; the service wants one selector digit per column, one
    per region, and one per sex, so the strings are built rather than copied.
    """
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        params = {
            "sys": "220", "kind": "21", "type": "1", "cycle": "4", "outmode": "8", "outkind": "1",
            "ym": f"{FIRST - ROC}00", "ymt": f"{LAST - ROC}00", "funid": funid,
            "fldlst": "1" * fields,
            "codlst0": "1" + "0" * 30,          # region: the national total is the first entry
        }
        if sex is not None:
            params["codlst1"] = sex
        url = API + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path, url], check=True)
    d = json.load(open(path))
    return dict(zip(d["colh"][0], d["orgdata"]))


def _births():
    """{year: {band: births}}, by year of occurrence.

    The table's first and last age columns are open-ended — under 20 and 45 and over — so they are
    read against the 15-19 and 45-49 populations. This is how the ministry builds its own rate.
    """
    cols = _query(BIRTHS, 8, "births.json")
    labels = ["未滿20歲", "20-24歲", "25-29歲", "30-34歲", "35-39歲", "40-44歲", "45歲以上"]
    out = {}
    for i, year in enumerate(range(FIRST, LAST + 1)):
        out[year] = {band: float(cols[label][i]) for band, label in zip(BANDS, labels)}
    return out


def _women():
    """{year: {band: women}} at year end, summed from single years of age."""
    cols = _query(POPULATION, 103, "women.json", sex="001")
    out = {}
    for i, year in enumerate(range(FIRST, LAST + 1)):
        out[year] = {(lo, hi): sum(float(cols[f"{a}歲"][i]) for a in range(lo, hi + 1))
                     for lo, hi in BANDS}
    return out


def taiwan_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def taiwan_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = taiwan_tfr()
    print(t.tail(8).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    print("the ministry publishes 0.885 for 2024, 0.865 for 2023, 0.695 for 2025")
