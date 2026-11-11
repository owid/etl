"""China: the census yearbooks' own fertility tables.

For every census round the National Bureau of Statistics publishes, as a downloadable file, the
number of women of childbearing age and the number of births they had in the year before the
count, for each single year of age from 15 to 49. So the fertility rate can be rebuilt from those
counts instead of taken from the bureau's press conference.

Only census years have this. Between censuses the bureau relies on an annual sample survey whose
age detail appears in a yearbook it does not publish free of charge.
"""

import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "cn")
ROOT = "https://www.stats.gov.cn/sj/pcsj/rkpc"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]

# census year -> (file on the bureau's site, local name). The reference period is the twelve
# months ending on 31 October of the census year, not the calendar year.
ROUNDS = {
    2000: f"{ROOT}/5rp/html/l0606.htm",
    2010: f"{ROOT}/6rp/html/B0603a.htm",
    2020: f"{ROOT}/7rp/zk/html/B0603.xls",
}


def _table(year):
    """The round's table as a plain DataFrame, whatever format it is served in."""
    url = ROUNDS[year]
    path = fetch(url, os.path.join(DATA, f"{year}{os.path.splitext(url)[1]}"))
    if path.endswith(".xls"):
        return pd.read_excel(path, header=None)
    # the older rounds are HTML in GB18030, the encoding the bureau's site used at the time
    return pd.read_html(path, encoding="gb18030")[0]


def _counts(year):
    """{age: {"women": n, "births": n}} for single years 15-49."""
    d = _table(year)
    out = {}
    for _, r in d.iterrows():
        # 2020 labels single years as "15", the older rounds as "15岁"; band rows read "15-19岁"
        m = re.fullmatch(r"(\d{2})岁?", str(r.iloc[0]).strip())
        if not m:
            continue
        age = int(m.group(1))
        if not 15 <= age <= 49:
            continue
        women = pd.to_numeric(str(r.iloc[1]).replace(",", ""), errors="coerce")
        births = pd.to_numeric(str(r.iloc[2]).replace(",", ""), errors="coerce")
        if pd.notna(women) and pd.notna(births) and women > 0:
            out[age] = {"women": float(women), "births": float(births)}
    return out


def china_tfr():
    rows = []
    for year in sorted(ROUNDS):
        c = _counts(year)
        if len(c) == 35:                               # every single year from 15 to 49
            rows.append({"year": year, "value": sum(v["births"] / v["women"] for v in c.values())})
    return pd.DataFrame(rows)


def china_detail(year):
    if year not in ROUNDS:
        return None
    c = _counts(year)
    out = {}
    for lo, hi in BANDS:
        b = sum(c[a]["births"] for a in range(lo, hi + 1) if a in c)
        w = sum(c[a]["women"] for a in range(lo, hi + 1) if a in c)
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


if __name__ == "__main__":
    print(china_tfr().to_string(index=False))
    for band, v in sorted(china_detail(2020).items()):
        print(band, f"births {v['births']:>9,.0f}  women {v['women']:>10,.0f}",
              f"asfr {v['births'] / v['women'] * 1000:7.2f}")
