"""Chile: registered births by age of mother over INE's population estimates.

One spreadsheet carries everything from 1992 on: births by five-year age band of the mother, the
women in each band, and INE's own rate. Births are counted by the year they happened and corrected
for late registration, which is a small adjustment here — 98.5% of the births registered during 2023
had happened in 2023.

The population is the one INE published alongside these births, based on the 2017 census. It rebased
onto the 2024 census in February 2026, which shifts the women down about 1.6% but moves the rate by
only about 0.02.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "cl")
SERIES = ("https://www.ine.gob.cl/docs/default-source/nacimientos-matrimonios-y-defunciones/"
          "cuadros-estadisticos/series-hist%C3%B3ricas/series-vitales-1992-2024(p).xlsx"
          "?sfvrsn=d7f396fb_4")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _table():
    """{year: {band: {births, women}}}. The sheet pairs each band's births with its own women."""
    path = fetch(SERIES, os.path.join(DATA, "series.xlsx"))
    d = pd.read_excel(path, sheet_name="Fecundidad", header=None)
    births, women = {}, {}
    for j in range(d.shape[1]):
        head = str(d.iloc[0, j])
        for lo, hi in BANDS:
            if head == f"Nacidos vivos de mujeres de {lo} a {hi} años":
                births[(lo, hi)] = j
            elif head == f"Mujeres de {lo} a {hi} años":
                women[(lo, hi)] = j
    out = {}
    for i in range(1, len(d)):
        # the most recent years are labelled 2023(p) and 2024(p) — still provisional
        year = pd.to_numeric(str(d.iloc[i, 0])[:4], errors="coerce")
        if pd.isna(year):
            continue
        row = {}
        for band in BANDS:
            b = pd.to_numeric(d.iloc[i, births[band]], errors="coerce")
            w = pd.to_numeric(d.iloc[i, women[band]], errors="coerce")
            if pd.notna(b) and pd.notna(w) and w:
                row[band] = {"births": float(b), "women": float(w)}
        if len(row) == len(BANDS):
            out[int(year)] = row
    return out


def chile_tfr():
    rows = [{"year": y, "value": sum(v["births"] / v["women"] for v in bands.values()) * 5}
            for y, bands in sorted(_table().items())]
    return pd.DataFrame(rows)


def chile_detail(year):
    return _table().get(year)


if __name__ == "__main__":
    t = chile_tfr()
    print(t.tail(8).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    print("INE publishes 1.03 for 2024, 1.16 for 2023 — and its old projection assumed 1.58")
