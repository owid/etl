"""Turkey: TurkStat birth statistics and the address-based population register.

Two of the three files here cannot be fetched by a script. TurkStat's current data portal is a
JavaScript application whose download links are single-use tokens, so the two tables from the annual
birth statistics bulletin — births by mother's age group, and age-specific fertility rates — were
downloaded by hand and are kept in data/tr/. The female population comes from an older,
server-rendered portal that does answer plain requests.
"""

import os
import re
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data", "tr")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]

# the older portal answers a form-encoded POST but 404s on a JSON body
NIP = "https://nip.tuik.gov.tr/Home/GetInformation"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _year(cell):
    """Rows are labelled "2023(r)" where the figure has been revised."""
    m = re.match(r"\s*(\d{4})", str(cell))
    return int(m.group(1)) if m else None


def _asfr():
    """{year: {band: rate per 1,000 women}} from the bulletin's age-specific rate table."""
    d = pd.read_excel(os.path.join(DATA, "asfr.xls"), sheet_name="t8", header=None)
    head = next(i for i in range(len(d)) if str(d.iloc[i, 1]).strip() == "15-19")
    bands = {}
    for j in range(1, d.shape[1]):
        m = re.fullmatch(r"(\d{2})-(\d{2})", str(d.iloc[head, j]).strip())
        if m:
            bands[j] = (int(m.group(1)), int(m.group(2)))
    out = {}
    for i in range(head + 1, len(d)):
        y = _year(d.iloc[i, 0])
        if y is None:
            continue
        for j, band in bands.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(y, {})[band] = float(v)
    return out


def _births():
    """{year: {band: births}} from the bulletin's births-by-age table.

    TurkStat splits the teens into 15-17 and 18-19; both are folded into 15-19 to match the age
    bands everything else here uses.
    """
    d = pd.read_excel(os.path.join(DATA, "births_by_age.xls"), sheet_name="t7", header=None)
    head = next(i for i in range(len(d)) if str(d.iloc[i, 2]).strip() == "<15")
    cols = {}
    for j in range(2, d.shape[1]):
        m = re.fullmatch(r"(\d{2})-(\d{2})", str(d.iloc[head, j]).strip())
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            cols[j] = (15, 19) if hi <= 19 else (lo, hi)
    out = {}
    for i in range(head + 1, len(d)):
        y = _year(d.iloc[i, 0])
        if y is None:
            continue
        for j, band in cols.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(y, {})[band] = out.setdefault(y, {}).get(band, 0.0) + float(v)
    return out


def turkey_women():
    """{year: {band: women}} from the address-based population register, 1935 onward."""
    path = os.path.join(DATA, "population_by_age.html")
    if not os.path.exists(path):
        subprocess.run(
            ["curl", "-sk", "--fail", "-A", UA, "-m", "180", "-X", "POST", NIP,
             "-H", "Content-Type: application/x-www-form-urlencoded; charset=UTF-8",
             "--data-urlencode", "status=1", "--data-urlencode", "name=YasGrubunaGoreNufus",
             "--data-urlencode", "value=0", "-o", path],
            check=True,
        )
    d = pd.read_html(path)[0]
    d.columns = ["year", "band", "total", "male", "female"]
    out = {}
    for _, r in d.iterrows():
        m = re.fullmatch(r"(\d{2})-(\d{2})", str(r.band).strip())
        y = pd.to_numeric(r.year, errors="coerce")
        if not m or pd.isna(y):
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band in BANDS:
            v = pd.to_numeric(str(r.female).replace(".", ""), errors="coerce")
            if pd.notna(v):
                out.setdefault(int(y), {})[band] = float(v)
    return out


def turkey_tfr():
    """TurkStat's own rates summed, times the band width. 2001 onward."""
    rows = []
    for year, bands in sorted(_asfr().items()):
        rows.append({"year": year, "value": sum(bands.values()) / 1000 * 5})
    return pd.DataFrame(rows)


def turkey_detail(year):
    births, women = _births().get(year), turkey_women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = turkey_tfr()
    print(t.tail(4).to_string(index=False), "— TurkStat's bulletin gives 1.48 for 2024")
    d = turkey_detail(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}",
              f"asfr {v['births'] / v['women'] * 1000:7.2f}")
    print("implied TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 4))
