"""Russia: births by age of mother and female population by age, both from Rosstat.

The Demographic Yearbook's fertility chapter gives live births by age of mother as counts, and a
separate bulletin gives female population by single year of age at 1 January. Together they let
the age bands be compared, though the plotted line stays Rosstat's own published rate — see the
denominator note in countries.py.

Only 2022 is wired up: that is the last year the yearbook covers, and the only year where the
published rates and the population file are both on the 2020 census basis.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "ru")
BIRTHS = "https://rosstat.gov.ru/storage/2024/04-20/VF5GE3HA/Dem_ej_04-2023.xlsx"
POP = "https://rosstat.gov.ru/storage/mediabank/Chisl_polvozr_01-01-2022_VPN-2020.xlsx"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]

# sheet 4.1's columns, in order after the year and the total. Rosstat splits the teens in two.
COLS = [(15, 17), (18, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _births(year):
    path = fetch(BIRTHS, os.path.join(DATA, "dem04.xlsx"), insecure=True)
    d = pd.read_excel(path, sheet_name="4.1", header=None)
    hit = d[d.iloc[:, 0].astype(str).str.strip() == str(year)]
    if hit.empty:
        return None
    row = hit.iloc[0]
    out = {}
    for i, band in enumerate(COLS):
        v = pd.to_numeric(str(row.iloc[2 + i]).replace("\xa0", "").replace(" ", ""), errors="coerce")
        if pd.notna(v):
            out[band] = float(v)
    return out or None


def _women():
    """{age: women} for the whole country at 1 January 2022, single years of age."""
    path = fetch(POP, os.path.join(DATA, "pop2022.xlsx"), insecure=True)
    d = pd.read_excel(path, sheet_name="Ж_Г+С", header=None)
    ages = d.iloc[6]
    row = d[d.iloc[:, 0].astype(str).str.strip() == "Российская Федерация"].iloc[0]
    out = {}
    for j in range(len(ages)):
        a = pd.to_numeric(ages.iloc[j], errors="coerce")
        v = pd.to_numeric(row.iloc[j], errors="coerce")
        if pd.notna(a) and pd.notna(v) and 15 <= a <= 49:
            out[int(a)] = float(v)
    return out


def russia_detail(year):
    if year != 2022:
        return None
    births, women = _births(year), _women()
    if not births or not women:
        return None
    out = {}
    for lo, hi in BANDS:
        # Rosstat's 15-17 and 18-19 columns add up to the 15-19 band the UN uses
        b = sum(v for (a, z), v in births.items() if a >= lo and z <= hi)
        w = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


if __name__ == "__main__":
    d = russia_detail(2022)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>9,.0f}  women {v['women']:>10,.0f}",
              f"asfr {v['births'] / v['women'] * 1000:7.2f}")
    print("implied TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 3),
          "— Rosstat publishes 1.416")
