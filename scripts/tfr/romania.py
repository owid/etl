"""Romania: births by age of mother over the resident female population.

Everything comes from the statistics institute's own database. It is only reachable by posting the
selection back to the endpoint that describes the table, and the selection has to be small — asking
for every category at once returns an empty result rather than an error. The answer comes back as an
HTML table inside a JSON field, so it is parsed rather than read.

Romania keeps two parallel population concepts and says which is which. The resident population
counts everyone whose usual residence is in the country; the population by domicile counts Romanian
citizens registered as living there, whether or not they do. The institute states that only the
resident figures should be used for international comparison, so those are the ones used here. That
choice matters a lot in a country with large emigration.

Births are dated to the year they happened, but a year is not final until late registrations from the
following three years have been folded in.
"""

import json
import os
import re
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "ro")
API = "http://statistici.insse.ro:8077/tempo-ins/matrix"
BIRTHS = "POP201I"      # live births to mothers usually resident in Romania, by age of both parents
POPULATION = "POP106A"  # resident population at 1 July, by age, sex and residence
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _describe(code):
    """The table's own description: its dimensions, their categories, and the identifiers to send back."""
    path = os.path.join(DATA, f"{code}.json")
    if not os.path.exists(path):
        os.makedirs(DATA, exist_ok=True)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "180", "-o", path,
                        f"{API}/{code}"], check=True)
    return json.load(open(path))


def _query(code, keep, name):
    """The selected cells, as a list of (row labels, {year: value}).

    `keep` gives one predicate per dimension; the categories it accepts are the ones asked for.
    """
    described = _describe(code)
    path = os.path.join(DATA, name)
    if not os.path.exists(path):
        arr = [[o for o in dim["options"] if pred(o["label"].strip())]
               for dim, pred in zip(described["dimensionsMap"], keep)]
        body = {"language": "ro", "arr": arr, "matrixName": described["matrixName"],
                "matrixDetails": described["details"]}
        with open(os.path.join(DATA, "body.json"), "w") as f:
            json.dump(body, f)
        subprocess.run(["curl", "-s", "--fail", "-A", UA, "-m", "300", "-o", path,
                        "-H", "Content-Type: application/json",
                        "-H", "Referer: http://statistici.insse.ro:8077/tempo-online/",
                        "--data-binary", "@" + os.path.join(DATA, "body.json"),
                        f"{API}/{code}"], check=True)
    html = json.load(open(path))["resultTable"]
    return _parse(html)


def _parse(html):
    """(row labels, {year: value}) per data row of the returned cross-tab."""
    years = [int(y) for y in re.findall(r"Anul (\d{4})", html)]
    out = []
    for row in re.findall(r"<tr>(.*?)</tr>", html, re.S):
        labels = [re.sub(r"<[^>]+>", "", h).strip()
                  for h in re.findall(r"<th[^>]*>(.*?)</th>", row, re.S)]
        # the returned markup closes its cells as "</td align='right'>", so match the opening tag only
        cells = [re.sub(r"<[^>]+>", "", c).strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td", row, re.S)]
        if not cells or len(cells) != len(years):
            continue
        values = {}
        for year, cell in zip(years, cells):
            # thousands are spaced, decimals are commas, and ":" marks a missing value
            v = pd.to_numeric(cell.replace(" ", "").replace(",", ".").replace(":", ""),
                              errors="coerce")
            if pd.notna(v):
                values[year] = float(v)
        out.append((labels, values))
    return out


def _band_of(label):
    m = re.search(r"(\d{2})\s*-\s*(\d{2})", label)
    if not m:
        return None
    band = (int(m.group(1)), int(m.group(2)))
    return band if band in BANDS else None


def _births():
    """{year: {band: births}}. The father's age is fixed to its total so only the mother's varies."""
    rows = _query(BIRTHS, [
        lambda s: s == "Total",                                   # age of father
        lambda s: "mamei" in s and _band_of(s) is not None,        # age of mother
        lambda s: s == "TOTAL",                                    # whole country
        lambda s: s.startswith("Anul"),
        lambda s: True,
    ], "births.json")
    out = {}
    for labels, values in rows:
        band = next((_band_of(x) for x in labels if _band_of(x)), None)
        if band:
            for year, v in values.items():
                out.setdefault(year, {})[band] = v
    return out


def _women():
    """{year: {band: women}} at 1 July, resident population.

    The age dimension mixes single years with five-year groups, so only the grouped labels are kept.
    """
    rows = _query(POPULATION, [
        lambda s: _band_of(s) is not None and "ani" in s,
        lambda s: s == "Feminin",
        lambda s: s == "Total",
        lambda s: s == "TOTAL",
        lambda s: s.startswith("Anul"),
        lambda s: True,
    ], "women.json")
    out = {}
    for labels, values in rows:
        band = next((_band_of(x) for x in labels if _band_of(x)), None)
        if band:
            for year, v in values.items():
                out.setdefault(year, {})[band] = v
    return out


def romania_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def romania_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    print(romania_tfr().to_string(index=False))
