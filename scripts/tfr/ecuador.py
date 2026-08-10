"""Ecuador: registered births by age of mother over INEC's population estimates.

One sheet of the vital-statistics series carries both sides — births in each five-year age band of
the mother and the projected women in that band — so the rate can be rebuilt directly. INEC publishes
the age-specific rates from exactly these two columns but does not publish their sum.

Births are dated to the year they happened and counted as registered by the following March, so
recent years are incomplete: INEC calls them provisional for one year and semi-definitive for three.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "ec")
SERIES = ("https://www.ecuadorencifras.gob.ec/documentos/web-inec/Poblacion_y_Demografia/"
          "Nacimientos_Defunciones/2024/Tabulados_series_historicas_ENV_EDF_2024_vf.xlsx")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _table():
    """{year: {band: {births, women}}} from sheet 1.3.6.

    Row 2 names the three blocks — births, population, rate — and row 3 the age band within each,
    so a column belongs to a block until the next block's heading appears.
    """
    path = fetch(SERIES, os.path.join(DATA, "series.xlsx"))
    d = pd.read_excel(path, sheet_name="1.3.6", header=None)
    births, women, block = {}, {}, None
    for j in range(d.shape[1]):
        head = str(d.iloc[2, j])
        if head.startswith("Número de nacidos"):
            block = births
        elif head.startswith("Proyecciones de"):
            block = women
        elif head.startswith("Tasa"):
            block = None
        if block is None:
            continue
        band = str(d.iloc[3, j]).replace(" ", "").replace("años", "")
        for lo, hi in BANDS:
            if band == f"{lo}-{hi}":
                block[(lo, hi)] = j
    out = {}
    for i in range(4, len(d)):
        # the newest year is labelled 2024(p**) — provisional
        year = pd.to_numeric(str(d.iloc[i, 2])[:4], errors="coerce")
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


def ecuador_tfr():
    rows = [{"year": y, "value": sum(v["births"] / v["women"] for v in bands.values()) * 5}
            for y, bands in sorted(_table().items())]
    return pd.DataFrame(rows)


def ecuador_detail(year):
    return _table().get(year)


if __name__ == "__main__":
    t = ecuador_tfr()
    print(t.to_string(index=False))
    print("INEC's projection assumes 1.82 for 2023 and 1.79 for 2024")
