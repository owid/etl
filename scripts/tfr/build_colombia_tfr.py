"""Assemble Colombia TFR from four sources: DANE (NSO), UN WPP, HFD, UN Demographic Yearbook."""

import glob
import re

import os

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data")

OUT = "colombia_tfr.csv"


# ---------------------------------------------------------------- DANE population
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

    # 2018-2070: two header rows; ages labelled 'Mujeres N años'
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
def dane_tgf():
    a = pd.read_excel(os.path.join(DATA, "DCD-Fec-EstNal-Dep-1985-2017_VP.xlsx"), sheet_name="FECUNDIDAD", header=9)
    a.columns = [str(c).strip() for c in a.columns]
    a = a[
        (a["DPNOM"].astype(str).str.strip() == "Nacional")
        & (a["ÁREA GEOGRÁFICA"].astype(str).str.strip().str.upper() == "TOTAL")
    ]
    s1 = a[["AÑO", "TGF"]].rename(columns={"AÑO": "year", "TGF": "value"})

    b = pd.read_excel(os.path.join(DATA, "DCD-Fec-EstNal-Reg-2018-2070_VP.xlsx"), sheet_name="Fecundidad", header=9)
    b.columns = [str(c).strip() for c in b.columns]
    b = b[b["TERRITORIO"].astype(str).str.strip() == "Total Nacional"]
    s2 = b[["AÑO", "TGF"]].dropna().rename(columns={"AÑO": "year", "TGF": "value"})

    out = pd.concat([s1, s2], ignore_index=True)
    out["year"] = out["year"].astype(int)
    return out.sort_values("year").reset_index(drop=True)


# ---------------------------------------------------------------- UN WPP
def wpp_tfr():
    from owid.catalog import Dataset

    ds = Dataset("/Users/edouard/dev/owid/etl/data/garden/un/2024-07-12/un_wpp")
    tb = ds["fertility_rate"].reset_index()
    sub = tb[(tb.country == "Colombia") & (tb.age == "all") & (tb.sex == "all") & (tb.variant == "estimates")]
    out = pd.DataFrame({"year": sub.year.astype(int), "value": sub.fertility_rate.astype(float)})
    return out.sort_values("year").reset_index(drop=True)


# ---------------------------------------------------------------- UN Demographic Yearbook
FIVE_YEAR = re.compile(r"^(\d+)\s*-\s*(\d+)$")


def dyb_tfr(pop):
    """TFR from DYB-reported registered births by age of mother / DANE female population."""
    df = pd.read_csv(glob.glob(os.path.join(DATA, "co260/*.csv"))[0], low_memory=False)
    df = df[pd.to_numeric(df.Year, errors="coerce").notna()].copy()
    df["Year"] = df.Year.astype(int)
    df = df[
        (df.Area == "Total") & (df.Sex == "Both Sexes") & (df["Record Type"] == "Data tabulated by year of occurrence")
    ]

    rows = []
    for year, g in df.groupby("Year"):
        ages = g.set_index("Age of mother")["Value"]
        total = ages.get("Total")
        # five-year groups only, so single-year duplicates are not double counted
        groups = {}
        for label, val in ages.items():
            m = FIVE_YEAR.match(str(label).strip())
            if m:
                groups[(int(m.group(1)), int(m.group(2)))] = float(val)
        # open-ended top group, reported as e.g. '50 +'
        for label, val in ages.items():
            if str(label).strip() in {"50 +", "50+"}:
                groups[(50, 54)] = float(val)

        assert groups, f"no five-year age groups for {year}"
        counted = sum(groups.values())
        unknown = float(ages.get("Unknown", 0) or 0)

        # redistribute births of unknown age of mother proportionally (HFD's own convention)
        scale = 1.0
        if unknown > 0:
            scale = (counted + unknown) / counted

        # TFR = sum over age groups of (age-specific rate x width of the group)
        asfr_sum = 0.0
        for (lo, hi), births in groups.items():
            ages = [a for a in range(lo, hi + 1) if a in pop.columns]
            denom = pop.loc[year, ages].sum()
            if denom > 0:
                asfr_sum += (births * scale) / denom * len(ages)
        rows.append(
            {
                "year": year,
                "value": asfr_sum,
                "births_counted": counted,
                "births_total_reported": total,
                "unknown_age": unknown,
                "reliability": g["Reliability"].iloc[0],
            }
        )
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


if __name__ == "__main__":
    pop = dane_female_pop()
    print(f"DANE female population: {pop.index.min()}-{pop.index.max()}, ages {pop.columns.min()}-{pop.columns.max()}")
    print("  sanity 2023 women 15-49:", f"{pop.loc[2023, range(15, 50)].sum():,.0f}")

    dane = dane_tgf()
    wpp = wpp_tfr()
    dyb = dyb_tfr(pop)

    print("\nDANE official TGF:", dane.year.min(), "-", dane.year.max())
    print("UN WPP:", wpp.year.min(), "-", wpp.year.max())
    print("\nDYB-derived:")
    print(dyb.to_string(index=False))

    frames = []
    for name, d in [("DANE (official estimate)", dane), ("UN WPP", wpp), ("UN Demographic Yearbook", dyb)]:
        f = d[["year", "value"]].copy()
        f["source"] = name
        frames.append(f)
    out = pd.concat(frames, ignore_index=True)
    out = out[out.year >= 1950]
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}: {len(out)} rows")


def wpp_variant(variant):
    """UN WPP projection variant (high / medium / low) for Colombia."""
    from owid.catalog import Dataset

    tb = Dataset("/Users/edouard/dev/owid/etl/data/garden/un/2024-07-12/un_wpp")["fertility_rate"].reset_index()
    s = tb[(tb.country == "Colombia") & (tb.age == "all") & (tb.sex == "all") & (tb.variant == variant)]
    out = pd.DataFrame({"year": s.year.astype(int), "value": s.fertility_rate.astype(float)})
    return out.sort_values("year").reset_index(drop=True)
