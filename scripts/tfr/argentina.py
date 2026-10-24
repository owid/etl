"""Argentina: registered births from the health ministry, population from INDEC.

INDEC publishes no annual fertility rate at all — only four projected years, 2025 onward. So unlike
most countries here there is no official figure to take, and the rate has to be built from counts:
registered live births by age group of mother, over INDEC's female population.

The population comes in two vintages, one based on the 2010 census and one on the 2022 census. They
are used for the years each covers, which leaves a seam at 2022 that we have not tried to smooth.
"""

import os
import re

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "ar")
CKAN = "https://datos.salud.gob.ar/dataset/d1350588-d8bb-4892-b21c-48738311e218/resource"
BIRTHS = {
    "births_2005_2022.csv": f"{CKAN}/5a68ea36-03fe-4b38-b590-d7cf2a13b821/download/"
                            "nacidos-vivos-registrados-en-la-republica-argentina-entre-los-anos-2005-2022.csv",
    "births_2023.csv": f"{CKAN}/40e722b8-72eb-49a0-89dc-5ee174bf63b4/download/nacimientos2023.csv",
    "births_2024.csv": f"{CKAN}/ace82479-4659-4788-9609-5b98bc9081bd/download/nacimientos2024-.csv",
}
POP_2010 = "https://www.indec.gob.ar/ftp/cuadros/poblacion/c2_proyecciones_nac_2010_2040.xls"
POP_2022 = "https://www.indec.gob.ar/ftp/cuadros/poblacion/proyecciones_nacionales_2022_2040_c2.xlsx"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _births():
    """{year: {band: births}}, scaled up for mothers whose age was not stated.

    The published tables put births with an unstated age in their own bucket — under 1% of the total
    in recent years — so each band is scaled by the same factor to absorb them.
    """
    frames = []
    for name, url in BIRTHS.items():
        path = fetch(url, os.path.join(DATA, name))
        sep = ";" if name == "births_2023.csv" else ","
        frames.append(pd.read_csv(path, sep=sep, usecols=["anio", "edad_madre_grupo", "nacimientos_cantidad"]))
    d = pd.concat(frames, ignore_index=True)
    d["n"] = pd.to_numeric(d.nacimientos_cantidad, errors="coerce")
    g = d.groupby(["anio", "edad_madre_grupo"]).n.sum()

    out = {}
    for (year, label), n in g.items():
        text = str(label).strip()
        m = re.match(r"\d\.(\d{2}) a (\d{2})$", text)
        # the top group is open-ended, "De 45 y m\u00e1s"; births above 49 are very few, so it is
        # treated as 45-49, the same convention the published age tables use
        band = (45, 49) if "45 y m" in text else ((int(m.group(1)), int(m.group(2))) if m else None)
        year = int(year)
        entry = out.setdefault(year, {"bands": {}, "counted": 0.0, "unstated": 0.0})
        if band:
            if band in BANDS:
                entry["bands"][band] = entry["bands"].get(band, 0.0) + float(n)
                entry["counted"] += float(n)
        elif "Sin especificar" in text:
            entry["unstated"] += float(n)
    # under-15s are left out: they fall outside every band we compare on
    final = {}
    for year, e in out.items():
        if not e["counted"]:
            continue
        scale = (e["counted"] + e["unstated"]) / e["counted"]
        final[year] = {b: v * scale for b, v in e["bands"].items()}
    return final


def _women_2010():
    """{year: {band: women}} from INDEC's 2010-census-based projection, five-year age groups."""
    path = fetch(POP_2010, os.path.join(DATA, "pop_2010_2040.xls"))
    d = pd.read_excel(path, sheet_name=0, header=None)
    start = next(i for i in range(len(d)) if str(d.iloc[i, 0]).strip() == "Mujeres")
    years = {}
    for i in range(start, start + 8):
        for j in range(1, d.shape[1]):
            y = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(y) and 2000 < y < 2100:
                years[j] = int(y)
        if years:
            break
    out = {}
    for i in range(start, len(d)):
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


def _women_2022():
    """{year: {band: women}} from INDEC's 2022-census-based projection, single years of age."""
    path = fetch(POP_2022, os.path.join(DATA, "pop_2022_2040.xlsx"))
    d = pd.read_excel(path, sheet_name="Cuadro 2.3", header=None)
    head = next((i for i in range(len(d))
                 if sum(1 for j in range(1, d.shape[1])
                        if str(pd.to_numeric(d.iloc[i, j], errors="coerce")).startswith("20")) >= 5), None)
    if head is None:
        return {}
    years = {j: int(v) for j in range(1, d.shape[1])
             if pd.notna(v := pd.to_numeric(d.iloc[head, j], errors="coerce")) and 2000 < v < 2100}
    single = {}
    for i in range(head + 1, len(d)):
        a = re.fullmatch(r"(\d{1,3})", str(d.iloc[i, 0]).strip())
        if not a:
            continue
        age = int(a.group(1))
        if not 15 <= age <= 49:
            continue
        for j, y in years.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                single.setdefault(y, {})[age] = float(v)
    out = {}
    for y, ages in single.items():
        out[y] = {b: sum(ages.get(a, 0.0) for a in range(b[0], b[1] + 1)) for b in BANDS}
    return out


def _women():
    """Later vintage wins where the two overlap, so 2022 onward is on the newer census base."""
    out = dict(_women_2010())
    out.update(_women_2022())
    return out


def argentina_tfr():
    births, women = _births(), _women()
    rows = []
    for year in sorted(set(births) & set(women)):
        b, w = births[year], women[year]
        bands = [x for x in BANDS if b.get(x) and w.get(x)]
        if len(bands) == len(BANDS):
            rows.append({"year": year, "value": sum(b[x] / w[x] for x in bands) * 5})
    return pd.DataFrame(rows)


def argentina_detail(year):
    births, women = _births().get(year), _women().get(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = argentina_tfr()
    print(t.to_string(index=False))
    print("INDEC publishes no annual rate; its projection gives 1.27 for 2025")
