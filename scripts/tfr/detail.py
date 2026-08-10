"""Where the national and UN WPP numbers actually diverge: births and women, by age band.

Each country exposes the age bands its own statistical office publishes; the WPP side is
aggregated into those same bands so the two are comparable. Countries whose NSO only
publishes rates (no counts) can't be decomposed and are absent from DETAIL.
"""

import os
import re

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data")
WPP = "/Users/edouard/dev/owid/etl/data/garden/un/2024-07-12/un_wpp"

# WPP's five-year groups, the atoms every national band is built from
WPP_BANDS = [(10, 14), (15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49), (50, 54)]


def _label(lo, hi):
    if hi >= 50:
        return f"{lo}+"
    if lo <= 10:
        return f"under {hi + 1}"
    return f"{lo}–{hi}"


# ---------------------------------------------------------------- UN WPP
def wpp_detail(country, year):
    """Births and women by WPP five-year band. Births are the age-specific rate times the
    female population, which is how WPP's own total fertility rate is built."""
    from owid.catalog import Dataset

    ds = Dataset(WPP)
    variant = "estimates" if year <= 2023 else "medium"
    f = ds["fertility_rate"].reset_index()
    f = f[(f.country == country) & (f.year == year) & (f.variant == variant) & (f.sex == "all")]
    p = ds["population"].reset_index()
    p = p[(p.country == country) & (p.year == year) & (p.variant == variant) & (p.sex == "female")]

    asfr = dict(zip(f.age.astype(str), f.fertility_rate.astype(float)))
    women = dict(zip(p.age.astype(str), p.population.astype(float)))

    out = {}
    for lo, hi in WPP_BANDS:
        key = f"{lo}-{hi}"
        w = women.get(key)
        r = asfr.get(key)
        if w is None or r is None:
            continue
        out[(lo, hi)] = {"women": w, "births": r / 1000 * w}
    return out


def _fold(wpp, bands):
    """Aggregate WPP five-year bands into the national bands."""
    out = {}
    for lo, hi in bands:
        b = w = 0.0
        for (a, z), v in wpp.items():
            if a >= lo and z <= hi:
                b += v["births"]
                w += v["women"]
        if w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out


# ---------------------------------------------------------------- national
def colombia_detail(year):
    from colombia import dane_births_by_age, dane_female_pop

    births = dane_births_by_age().get(year)
    if not births:
        return None
    pop = dane_female_pop()
    out = {}
    for (lo, hi), b in births.items():
        ages = [a for a in range(lo, hi + 1) if a in pop.columns]
        w = float(pop.loc[year, ages].sum()) if ages else 0.0
        if w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out


def brazil_detail(year):
    import brazil as br

    raw = br.births().get(year)
    if not raw:
        return None
    pop = br.female_pop().get(year)
    if pop is None:
        return None
    counted = sum(v for k, v in raw.items() if k in br.GROUPS)
    unknown = sum(v for k, v in raw.items() if k in br.UNKNOWN)
    scale = (counted + unknown) / counted if counted else 1.0
    out = {}
    for label, (lo, hi) in br.GROUPS.items():
        if label not in raw:
            continue
        w = float(pop[[a for a in range(lo, hi + 1) if a in pop.index]].sum())
        if w:
            out[(lo, hi)] = {"births": raw[label] * scale, "women": w}
    return out


def mexico_detail(year):
    from sources import _MX_AGE

    d = pd.read_excel(os.path.join(DATA, "mx2.xlsx"), sheet_name=0, header=None)
    ages = d.iloc[4].ffill()
    births = {}
    for i in range(6, len(d)):
        y = str(d.iloc[i, 0]).strip()
        if y != str(year):
            continue
        for j in range(1, d.shape[1]):
            band = _MX_AGE.get(ages[j] if isinstance(ages[j], str) else "")
            if not band:
                continue
            v = pd.to_numeric(str(d.iloc[i, j]).replace(",", ""), errors="coerce")
            if pd.notna(v):
                births[band] = births.get(band, 0.0) + float(v)
    if not births:
        return None

    pop = pd.read_excel(os.path.join(DATA, "0_Pob_Mitad_1950_2070.xlsx"))
    pop = pop[(pop.CVE_GEO == 0) & (pop.SEXO == "Mujeres") & (pop.AÑO == year)]
    women = pop.groupby("EDAD").POBLACION.sum()
    out = {}
    for (lo, hi), b in births.items():
        w = float(sum(women.get(a, 0) for a in range(lo, hi + 1)))
        if w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out


def thailand_detail(year):
    from sources import _th_rows, _th_years

    births, women = {}, {}
    for f in ("th23_b.txt", "th25_b.txt"):
        yrs = _th_years(f)
        if year not in yrs:
            continue
        i = yrs.index(year)
        for key, nums in _th_rows(f):
            if len(nums) > i:
                band = (10, 14) if key == "u15" else ((45, 49) if key == "50p" else key)
                births[band] = births.get(band, 0.0) + nums[i]
    for f in ("th23_p.txt", "th25_p.txt"):
        yrs = _th_years(f)
        if year not in yrs:
            continue
        i = yrs.index(year)
        for key, nums in _th_rows(f):
            if isinstance(key, tuple) and len(nums) >= 3 * i + 3:
                women[key] = nums[3 * i + 2]
    if not births or not women:
        return None
    return {b: {"births": v, "women": women[b]} for b, v in births.items() if b in women}


def _egypt_registered_births(path):
    """{(lo, hi): births} from table 12, the whole-country row of registered live births.

    Column headers are open-ended lower bounds — "-15" heads the 15-19 column — and the table is
    one sheet in the 2021-22 editions but split into urban, rural and total in 2023-24.
    """
    x = pd.ExcelFile(path)
    sheet = next((s for s in x.sheet_names if s.strip().endswith("12 (C)")), None)
    sheet = sheet or next((s for s in x.sheet_names if s.strip().endswith("12")), None)
    if sheet is None:
        return None
    d = pd.read_excel(x, sheet_name=sheet, header=None)
    head = next((i for i in range(len(d)) if str(d.iloc[i, 0]).strip() == "سن الأم"), None)
    if head is None:
        return None
    lows = {}
    for j in range(1, d.shape[1]):
        m = re.fullmatch(r"-(\d{2})(?:\.0)?", str(d.iloc[head, j]).strip())
        if m and 15 <= int(m.group(1)) <= 45:
            lows[j] = int(m.group(1))
    # the first data row repeats the country name; later rows are governorates
    row = next((d.iloc[i] for i in range(head + 1, len(d))
                if str(d.iloc[i, 0]).strip() == "إجمالي الجمهورية"), None)
    if row is None:
        return None
    out = {}
    for j, lo in lows.items():
        v = pd.to_numeric(row.iloc[j], errors="coerce")
        if pd.notna(v):
            out[(lo, lo + 4)] = float(v)
    return out or None


def egypt_detail(year):
    """Female population from table 13, registered births from table 12.

    CAPMAS's own age-specific rates are model estimates rather than its registered births divided
    by its population, so using the counts is the only way to see what the registry itself says.
    """
    path = os.path.join(DATA, f"eg_{year}.xlsx")
    if not os.path.exists(path):
        return None
    d = pd.read_excel(path, sheet_name="جدول Table 13", header=None)
    births = _egypt_registered_births(path)
    out = {}
    for _, r in d.iterrows():
        if not (isinstance(r[0], str) and re.match(r"^\d{2}-\d{2}$", r[0].strip())):
            continue
        lo, hi = (int(x) for x in r[0].strip().split("-"))
        hi = lo + 4  # CAPMAS writes "40-45" where it means 40-44
        w = pd.to_numeric(r[1], errors="coerce")
        rate = pd.to_numeric(r[2], errors="coerce")
        if pd.isna(w):
            continue
        b = (births or {}).get((lo, hi))
        if b is None and pd.notna(rate):
            b = float(rate) / 1000 * float(w)          # falls back to CAPMAS's modeled rate
        if b is not None:
            out[(lo, hi)] = {"women": float(w), "births": b}
    return out or None


EW_BANDS = {
    "Under 20": (15, 19),
    "20 to 24": (20, 24),
    "25 to 29": (25, 29),
    "30 to 34": (30, 34),
    "35 to 39": (35, 39),
    "40 and over": (40, 44),
}


MYEB = ("https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/"
        "populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland/"
        "mid2011tomid2024/myebtablesuk20112024.xlsx")


def _ew_women(year):
    """{age: women} for England and Wales at mid-year, from ONS's own population estimates.

    Sheet MYEB4 covers 2011 to 2024 by single year of age and sex. Returns None outside that span,
    which is what happens for the latest provisional year — ONS itself uses projections there.
    """
    from fetch import fetch

    col = f"population_{year}"
    path = fetch(MYEB, os.path.join(DATA, "uk", "myeb.xlsx"))
    d = pd.read_excel(path, sheet_name="MYEB4", header=1)
    if col not in d.columns:
        return None
    d = d[(d.Name == "ENGLAND AND WALES") & (d.sex == "f")]
    out = {}
    for _, r in d.iterrows():
        a = pd.to_numeric(r.age, errors="coerce")
        v = pd.to_numeric(r[col], errors="coerce")
        if pd.notna(a) and pd.notna(v) and 15 <= a <= 49:
            out[int(a)] = float(v)
    return out or None


def england_wales_detail(year):
    """Births from ONS table 10, women from ONS's mid-year population estimates.

    Where the population file does not reach the year, the denominator falls back to what ONS's own
    published rate implies: births / (rate / 1000).
    """
    d = pd.read_excel(os.path.join(DATA, "uk_births.xlsx"), sheet_name="Table_10", header=5)
    d = d[(d.iloc[:, 2] == "Mother") & (d.iloc[:, 1] == "England, Wales and Elsewhere")]
    d = d[pd.to_numeric(d.iloc[:, 0], errors="coerce") == year]
    women = _ew_women(year)
    out = {}
    for _, r in d.iterrows():
        band = EW_BANDS.get(str(r.iloc[3]).strip())
        births = pd.to_numeric(r.iloc[4], errors="coerce")
        rate = pd.to_numeric(r.iloc[5], errors="coerce")
        if not band or pd.isna(births):
            continue
        counted = sum(women.get(a, 0.0) for a in range(band[0], band[1] + 1)) if women else 0.0
        if counted:
            out[band] = {"births": float(births), "women": counted}
        elif pd.notna(rate) and rate > 0:
            out[band] = {"births": float(births), "women": float(births) / (float(rate) / 1000)}
    return out or None


def philippines_detail(year):
    import philippines as ph

    births = ph.births_by_age().get(year)
    pop = ph.female_pop().get(year)
    if not births or not pop:
        return None
    return {b: {"births": v, "women": pop[b]} for b, v in births.items() if b in pop}


def france_band_detail(year):
    from france import france_detail

    return france_detail(year)


def japan_band_detail(year):
    from japan import japan_detail

    return japan_detail(year)


def germany_band_detail(year):
    from germany import germany_detail

    return germany_detail(year)


def south_africa_band_detail(year):
    from south_africa import south_africa_detail

    return south_africa_detail(year)


def turkey_band_detail(year):
    from turkey import turkey_detail

    return turkey_detail(year)


def russia_band_detail(year):
    from russia import russia_detail

    return russia_detail(year)


def china_band_detail(year):
    from china import china_detail

    return china_detail(year)


def spain_band_detail(year):
    from spain import spain_detail

    return spain_detail(year)


def myanmar_band_detail(year):
    from myanmar import myanmar_detail

    return myanmar_detail(year)


DETAIL = {
    "Colombia": colombia_detail,
    "Myanmar": myanmar_band_detail,
    "Spain": spain_band_detail,
    "China": china_band_detail,
    "Russia": russia_band_detail,
    "Turkey": turkey_band_detail,
    "South Africa": south_africa_band_detail,
    "Germany": germany_band_detail,
    "Japan": japan_band_detail,
    "France": france_band_detail,
    "Philippines": philippines_detail,
    "Brazil": brazil_detail,
    "Mexico": mexico_detail,
    "Thailand": thailand_detail,
    "Egypt": egypt_detail,
    "England and Wales": england_wales_detail,
}


def compare(country, model_country, year):
    """[(label, nso_births, wpp_births, nso_women, wpp_women)] for the national age bands."""
    fn = DETAIL.get(country)
    if fn is None:
        return None
    nso = fn(year)
    if not nso:
        return None
    wpp = _fold(wpp_detail(model_country, year), sorted(nso))
    rows = []
    for band in sorted(nso):
        if band not in wpp:
            continue
        rows.append((_label(*band), nso[band]["births"], wpp[band]["births"], nso[band]["women"], wpp[band]["women"]))
    return rows or None
