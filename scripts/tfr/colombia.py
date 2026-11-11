"""Colombia: DANE female population by age, and registered births by age of mother."""

import glob
import os
import re
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")

AGE_ROW = re.compile(r"De\s+(\d+)\s*-\s*(\d+)", re.I)


def dane_births_by_age():
    """{year: {(lo, hi): births}} from DANE's Cuadro 1/9, with unknown maternal age
    redistributed proportionally across the known groups."""
    out = {}
    for path in sorted(glob.glob(os.path.join(DATA, "dane_nac/nac_*.xls*"))):
        year = int(re.search(r"nac_(\d{4})", path).group(1))
        try:
            xl = pd.ExcelFile(path)
        except Exception:
            continue  # encrypted / not a workbook
        sheet = next((s for s in xl.sheet_names if "cuadro" in s.lower() and "1" in s), xl.sheet_names[0])
        df = pd.read_excel(path, sheet_name=sheet, header=None)

        groups, unknown = {}, 0.0
        # layout A (2008+): age groups down the first column, national total in column 1
        for _, r in df.iterrows():
            label = str(r.iloc[0]).strip()
            m = AGE_ROW.match(label)
            val = pd.to_numeric(r.iloc[1], errors="coerce")
            if m and pd.notna(val):
                groups[(int(m.group(1)), int(m.group(2)))] = float(val)
            elif label.lower().startswith("sin informaci") and pd.notna(val):
                unknown = float(val)

        # layout B (pre-2008): age groups across columns, departments down rows
        if not groups:
            hdr = next((i for i, r in df.iterrows() if sum(bool(AGE_ROW.match(str(v).strip())) for v in r) >= 5), None)
            if hdr is None:
                continue
            cols = {}
            for j, v in enumerate(df.iloc[hdr]):
                m = AGE_ROW.match(str(v).strip())
                if m:
                    cols[j] = (int(m.group(1)), int(m.group(2)))
                elif str(v).strip().lower().startswith("sin informaci"):
                    cols[j] = "unknown"
            trow = next(
                (i for i in range(hdr, len(df)) if str(df.iloc[i, 0]).strip().lower() in {"total", "total nacional"}),
                None,
            )
            if trow is None:
                continue
            for j, key in cols.items():
                val = pd.to_numeric(df.iloc[trow, j], errors="coerce")
                if pd.isna(val):
                    continue
                if key == "unknown":
                    unknown = float(val)
                else:
                    groups[key] = float(val)
        if not groups:
            continue

        counted = sum(groups.values())
        scale = (counted + unknown) / counted if unknown else 1.0
        out[year] = {band: b * scale for band, b in groups.items()}
    return out


def dane_registered_tfr(pop):
    """TFR from DANE's own Cuadro 1 (births by age group of mother), per year."""
    rows = []
    for year, groups in sorted(dane_births_by_age().items()):
        tfr = 0.0
        for (lo, hi), births in groups.items():
            ages = [a for a in range(lo, hi + 1) if a in pop.columns]
            denom = pop.loc[year, ages].sum()
            if denom > 0:
                tfr += births / denom * len(ages)
        rows.append({"year": year, "value": tfr, "births": sum(groups.values())})
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def dane_female_pop():
    """Female population by single age, national total, from DANE PPED files."""
    frames = []

    # 1950-2017: header row 11 (0-indexed), columns like 'Hombres_0', 'Mujeres_0'
    a = pd.read_excel(os.path.join(DATA, "PPED-AreaSexoEdadNac-1950-2017.xlsx"), sheet_name=0, header=11)
    a.columns = [str(c).strip() for c in a.columns]
    a = a[a["ÁREA GEOGRÁFICA"].astype(str).str.strip() == "Total"]
    mcols = {c: int(re.search(r"(\d+)", c).group(1)) for c in a.columns if c.startswith("Mujeres_")}
    sub = a[["AÑO"] + list(mcols)].copy()
    sub = sub.rename(columns=mcols).rename(columns={"AÑO": "year"})
    frames.append(sub)

    # 2018-2070: two header rows; ages labeled 'Mujeres N años'
    b = pd.read_excel(
        os.path.join(DATA, "PPED-AreaSexoEdadNac-2018-2070.xlsx"), sheet_name="PobNacionalxÁreaSexoEdad", header=8
    )
    b.columns = [str(c).strip() for c in b.columns]
    ycol = [c for c in b.columns if c.startswith("Unnamed: 1")][0]
    acol = [c for c in b.columns if c.startswith("Unnamed: 2")][0]
    b = b[b[acol].astype(str).str.strip() == "Total"]
    mcols = {}
    for c in b.columns:
        m = re.match(r"Mujeres (\d+) (?:años|año)$", c)
        if m:
            mcols[c] = int(m.group(1))
    sub = b[[ycol] + list(mcols)].copy()
    sub = sub.rename(columns=mcols).rename(columns={ycol: "year"})
    frames.append(sub)

    pop = pd.concat(frames, ignore_index=True)
    pop["year"] = pop["year"].astype(int)
    pop = pop.set_index("year").sort_index()
    pop.columns = [int(c) for c in pop.columns]
    return pop.sort_index(axis=1)


# ---------------------------------------------------------------- DANE official TGF
