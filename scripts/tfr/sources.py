"""Comparison source: UN World Population Prospects."""

import os
import re

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data")
WPP = "/Users/edouard/dev/owid/etl/data/garden/un/2024-07-12/un_wpp"

# hue per source; scenarios share the hue and are drawn thin + dashed
COLORS = {
    "nso": "#c94a3b",
    "UN WPP": "#3b82c4",
}


def _f(path):
    return pd.read_feather(os.path.join(DATA, path))


def un_wpp(country):
    from owid.catalog import Dataset

    tb = Dataset(WPP)["fertility_rate"].reset_index()
    tb = tb[(tb.country == country) & (tb.age == "all") & (tb.sex == "all")]

    def grab(v):
        s = tb[tb.variant == v]
        return pd.DataFrame({"year": s.year.astype(int), "value": s.fertility_rate.astype(float)}).sort_values("year")

    est = grab("estimates")
    out = [("estimates", est, False)]
    if len(est):
        anchor = est.iloc[-1:]
        for v in ("high", "medium", "low"):
            p = grab(v)
            if len(p):
                out.append((v, pd.concat([anchor, p], ignore_index=True), True))
    return out


def _tfr_from_asfr(df, per=1000.0, width=5):
    """TFR = sum of age-specific rates x group width."""
    g = df.groupby("year").value.sum() / per * width
    return g.rename("value").reset_index()


def england_wales():
    """ONS Table 10: age-specific fertility rates, England & Wales, 1938-2025.

    Columns are positional: 0 Year, 1 Country, 2 Parent, 3 Age group, 4 births,
    5 ASFR (15-44 base), 6 ASFR (15-49 base). Both rate headers collide once
    newlines are stripped, so index rather than rename.
    """
    d = pd.read_excel(os.path.join(DATA, "uk_births.xlsx"), sheet_name="Table_10", header=5)
    d = d[(d.iloc[:, 2] == "Mother") & (d.iloc[:, 1] == "England, Wales and Elsewhere")]
    out = pd.DataFrame(
        {
            "year": pd.to_numeric(d.iloc[:, 0], errors="coerce"),
            "age": d.iloc[:, 3].astype(str),
            # col 5 is the long-running 15-44 based rate; col 6 (15-49 base) exists only for recent years
            "value": pd.to_numeric(d.iloc[:, 5], errors="coerce").fillna(pd.to_numeric(d.iloc[:, 6], errors="coerce")),
        }
    ).dropna()
    # ONS reports overlapping groups in recent years; keep the set present in every year
    out = out[out.age.isin(["Under 20", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 and over"])]
    out["year"] = out.year.astype(int)
    return _tfr_from_asfr(out)


def united_states():
    """CDC/NCHS via data.cdc.gov: age-specific birth rates per 1,000 females, 2016-2024."""
    d = pd.read_json(os.path.join(DATA, "us_births.json"))
    d = d[(d.subtopic == "Birth rate") & (d.group == "Maternal age group")]
    # 15-19 is also split into 15-17 and 18-19; keep only the non-overlapping groups
    d = d[
        d.subgroup.isin(
            [
                "10-14 years",
                "15-19 years",
                "20-24 years",
                "25-29 years",
                "30-34 years",
                "35-39 years",
                "40-44 years",
                "45-54 years",
            ]
        )
    ]
    d = pd.DataFrame({"year": d.time_period.astype(int), "value": pd.to_numeric(d.estimate, errors="coerce")}).dropna()
    return _tfr_from_asfr(d)


def japan():
    """e-Stat / MHLW table 0003411608: age-specific birth rates per 1,000 women, 5-year groups."""
    import json

    s = json.load(open(os.path.join(DATA, "jp_asfr.json")))["GET_STATS_DATA"]["STATISTICAL_DATA"]
    cls = {
        c["@id"]: {x["@code"]: x["@name"] for x in (c["CLASS"] if isinstance(c["CLASS"], list) else [c["CLASS"]])}
        for c in s["CLASS_INF"]["CLASS_OBJ"]
    }
    v = pd.DataFrame(s["DATA_INF"]["VALUE"])
    v["age"] = v["@cat01"].map(cls["cat01"])
    v["order"] = v["@cat02"].map(cls["cat02"])
    v["year"] = v["@time"].map(cls["time"]).str.extract(r"(\d{4})").astype(int)
    # the age "総数" row for all birth orders is MHLW's published TFR itself
    v = v[(v["order"] == "総数") & (v.age == "総数")]
    v["value"] = pd.to_numeric(v["$"], errors="coerce")
    return v.dropna(subset=["value"])[["year", "value"]].sort_values("year").reset_index(drop=True)


def germany():
    """Destatis 12612-0008: live births per 1,000 women, Germany, by single year of age."""
    path = os.path.join(DATA, "de/12612-0008_en.csv")
    lines = open(path, encoding="utf-8-sig").read().splitlines()
    # the export is split into several blocks, each with its own year header
    heads = [i for i, ln in enumerate(lines) if re.match(r"^;\d{4};", ln)]
    rows = []
    for bi, hdr in enumerate(heads):
        stop = heads[bi + 1] if bi + 1 < len(heads) else len(lines)
        # each year appears twice in the header: once for the value, once for the quality flag
        years = [int(y) for y in lines[hdr].split(";")[1:] if y.strip().isdigit()][0::2]
        for ln in lines[hdr + 1 : stop]:
            m = re.match(r"^(\d+) years?;", ln)
            if not m:
                continue
            age = int(m.group(1))
            vals = ln.split(";")[1:][0::2]  # value, quality-flag, value, flag, ...
            for y, raw in zip(years, vals):
                v = pd.to_numeric(raw.replace(",", "."), errors="coerce")
                if pd.notna(v):
                    rows.append({"year": y, "age": age, "value": float(v)})
    return _tfr_from_asfr(pd.DataFrame(rows), width=1)  # single years of age


# ---------------------------------------------------------------- Thailand
_TH_AGE = re.compile(r"^\s*(?:(\d+)\s*-\s*(\d+)|น้อยกว่า 15|50 และมากกว่า)")
_TH_NUM = re.compile(r"\d[\d,]*")


def _th_rows(path):
    """Yield (age_group, [numbers]) from a layout-extracted yearbook table page."""
    for ln in open(os.path.join(DATA, path), encoding="utf-8", errors="ignore"):
        m = _TH_AGE.match(ln)
        if not m:
            continue
        nums = [float(x.replace(",", "")) for x in _TH_NUM.findall(ln.split("....")[-1])]
        if not nums:
            continue
        key = (int(m.group(1)), int(m.group(2))) if m.group(1) else ("u15" if "น้อยกว่า" in ln else "50p")
        yield key, nums


def _th_years(path):
    txt = open(os.path.join(DATA, path), encoding="utf-8", errors="ignore").read()
    m = re.search(r": (\d{4}) - (\d{4})", txt)
    return list(range(int(m.group(1)), int(m.group(2)) + 1))


def thailand():
    """NSO Statistical Yearbook: table 1.10 births by mother's age / table 1.4 registered population."""
    births = {}
    for f in ("th23_b.txt", "th25_b.txt"):
        yrs = _th_years(f)
        for key, nums in _th_rows(f):
            for y, v in zip(yrs, nums[: len(yrs)]):  # counts first, percentages after
                births.setdefault(y, {})[key] = v

    pop = {}
    for f in ("th23_p.txt", "th25_p.txt"):
        yrs = _th_years(f)
        for key, nums in _th_rows(f):
            if not isinstance(key, tuple):
                continue
            for i, y in enumerate(yrs):  # each year is Total, Male, Female
                if len(nums) >= 3 * i + 3:
                    pop.setdefault(y, {})[key] = nums[3 * i + 2]

    rows = []
    for y in sorted(set(births) & set(pop)):
        tfr = 0.0
        for key, b in births[y].items():
            band = (10, 14) if key == "u15" else ((45, 49) if key == "50p" else key)
            w = pop[y].get(band)
            if w:
                tfr += b / w * 5
        rows.append({"year": y, "value": tfr})
    return pd.DataFrame(rows)


def egypt():
    """CAPMAS Annual Bulletin of Births and Deaths, table 13: age-specific fertility rates.

    One bulletin per year. CAPMAS labels one band "40-45" where it means 40-44; taken at
    width 5, which reproduces their own published TFR.
    """
    import glob

    rows = []
    for f in sorted(glob.glob(os.path.join(DATA, "eg_2*.xlsx"))):
        year = int(re.search(r"eg_(\d{4})", f).group(1))
        d = pd.read_excel(f, sheet_name="جدول Table 13", header=None)
        rates = []
        for _, r in d.iterrows():
            if isinstance(r[0], str) and re.match(r"^\d{2}-\d{2}$", r[0].strip()):
                v = pd.to_numeric(r[2], errors="coerce")
                if pd.notna(v):
                    rates.append(float(v))
        if rates:
            rows.append({"year": year, "value": sum(rates) / 1000 * 5})
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


# ---------------------------------------------------------------- Mexico
_MX_AGE = {
    "Menor de 15 años": (10, 14),
    "De 15 a 19 años": (15, 19),
    "De 20 a 24 años": (20, 24),
    "De 25 a 29 años": (25, 29),
    "De 30 a 34 años": (30, 34),
    "De 35 a 39 años": (35, 39),
    "De 40 a 44 años": (40, 44),
    "De 45 a 49 años": (45, 49),
    "De 50  y más años": (50, 54),
}
MX_LAST = 2022  # 2023-24 occurrence years are still filling up with late registrations


def mexico():
    """INEGI OLAP: registered births by year of occurrence and mother's age, summed over all
    registration years; CONAPO mid-year female population by single age as the denominator."""
    d = pd.read_excel(os.path.join(DATA, "mx2.xlsx"), sheet_name=0, header=None)
    ages = d.iloc[4].ffill()
    births = {}
    for i in range(6, len(d)):
        y = str(d.iloc[i, 0]).strip()
        if not re.match(r"^\d{4}$", y):
            continue
        for j in range(1, d.shape[1]):
            band = _MX_AGE.get(ages[j] if isinstance(ages[j], str) else "")
            if not band:
                continue
            v = pd.to_numeric(str(d.iloc[i, j]).replace(",", ""), errors="coerce")
            if pd.notna(v):
                births.setdefault(int(y), {})
                births[int(y)][band] = births[int(y)].get(band, 0.0) + float(v)

    pop = pd.read_excel(os.path.join(DATA, "0_Pob_Mitad_1950_2070.xlsx"))
    pop = pop[(pop.CVE_GEO == 0) & (pop.SEXO == "Mujeres")]
    women = pop.groupby(["AÑO", "EDAD"]).POBLACION.sum()

    rows = []
    for y in sorted(births):
        if y > MX_LAST or y not in women.index.get_level_values(0):
            continue
        tfr = 0.0
        for (lo, hi), b in births[y].items():
            denom = sum(women.get((y, a), 0) for a in range(lo, hi + 1))
            if denom:
                tfr += b / denom * (hi - lo + 1)
        rows.append({"year": y, "value": tfr})
    return pd.DataFrame(rows)


SOURCES = [
    ("UN WPP", un_wpp),
]
