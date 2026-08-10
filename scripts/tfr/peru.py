"""Peru: registered births by age of mother over INEI's population estimates.

INEI publishes two different fertility figures — one from its continuous household survey and one
as an assumption inside its population projection — and neither is the registry. The registry itself
is published as counts, so this rebuilds the rate from them.

Each year's annex sits at its own unrelated file id, so the four spreadsheets are listed by hand
rather than generated from a pattern.
"""

import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "pe")
BIRTHS = {
    "births_2021_2022.xlsx":
        "https://www.inei.gob.pe/media/MenuRecursivo/publicaciones_digitales/Est/Lib1923/Anexo2022.xlsx",
    "births_2023.xlsx":
        "https://cdn.www.gob.pe/uploads/document/file/7328715/6256673-anexo-estadisticas-vitales-2023.xlsx",
    "births_2024.xlsx":
        "https://cdn.www.gob.pe/uploads/document/file/9189794/7545255-anexos-estadisticos.xlsx",
}
WOMEN = "https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/proy_02_4.xlsx"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _year_of(name):
    """The reference year each annex covers. The 2022 edition also carries 2021, on its own sheet."""
    return {"births_2021_2022.xlsx": 2022, "births_2023.xlsx": 2023, "births_2024.xlsx": 2024}[name]


def _births_sheet(path):
    """{band: births} from sheet C08's whole-country row, which is labelled "Perú".

    A separate row above it, "Total general", also counts mothers living abroad; the country row is
    the one that matches the population we divide by.
    """
    d = pd.read_excel(path, sheet_name="C08", header=None)
    head = next(i for i in range(len(d)) if str(d.iloc[i, 5]).strip() == "Menor de 15")
    cols = {}
    for j in range(5, d.shape[1]):
        m = re.fullmatch(r"(\d{2})\s*-\s*(\d{2})", str(d.iloc[head, j]).strip())
        if m:
            band = (int(m.group(1)), int(m.group(2)))
            if band in BANDS:
                cols[j] = band
    row = next((d.iloc[i] for i in range(head + 1, len(d)) if str(d.iloc[i, 1]).strip() == "Perú"), None)
    if row is None:
        return None
    out = {}
    for j, band in cols.items():
        v = pd.to_numeric(row.iloc[j], errors="coerce")
        if pd.notna(v):
            out[band] = float(v)
    return out or None


def _births():
    out = {}
    for name, url in BIRTHS.items():
        path = fetch(url, os.path.join(DATA, name))
        bands = _births_sheet(path)
        if bands:
            out[_year_of(name)] = bands
    return out


def _women():
    """{year: {band: women}} at 30 June, from INEI's projection table.

    The sheet is wide: age groups down the rows, years across the columns, in three stacked blocks
    for both sexes, men and women. Only the women's block is read.
    """
    path = fetch(WOMEN, os.path.join(DATA, "women.xlsx"))
    d = pd.read_excel(path, sheet_name=0, header=None)
    head = next(i for i in range(len(d))
                if sum(1 for v in d.iloc[i] if pd.notna(pd.to_numeric(v, errors="coerce"))
                       and 1990 < float(pd.to_numeric(v, errors="coerce")) < 2100) >= 5)
    years = {}
    for j in range(1, d.shape[1]):
        v = pd.to_numeric(d.iloc[head, j], errors="coerce")
        if pd.notna(v) and 1990 < v < 2100:
            years[j] = int(v)                          # a duplicated first column is harmless

    start = next(i for i in range(len(d)) if str(d.iloc[i, 0]).strip().startswith("Mujer"))
    out = {}
    for i in range(start + 1, len(d)):
        m = re.fullmatch(r"(\d{2})\s*-\s*(\d{2})", str(d.iloc[i, 0]).strip())
        if not m:
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band not in BANDS:
            continue
        for j, y in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(y, {})[band] = float(v)
    return out


def peru_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        if all(b.get(x) and w.get(x) for x in BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in BANDS) * 5})
    return pd.DataFrame(rows)


def peru_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    print(peru_tfr().to_string(index=False))
    print("INEI's survey gives 1.8 for 2023; its projection assumes 2.2 for 2020-25")
