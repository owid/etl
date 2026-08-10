"""Malaysia: DOSM's fertility rates over its own population estimates.

Everything comes from DOSM's open data store as parquet, with no key. DOSM publishes the
age-specific rates and its own total, but not births by age of mother as counts — it computes the
rates from the registry's own age-of-mother field and does not release that table. So the total is
taken as published, and the age-band comparison multiplies each rate by the female population, which
is how DOSM built it in the first place.
"""

import os

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "my")
FERTILITY = "https://storage.dosm.gov.my/demography/fertility.parquet"
POPULATION = "https://storage.dosm.gov.my/population/population_malaysia.parquet"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _fertility():
    path = fetch(FERTILITY, os.path.join(DATA, "fertility.parquet"))
    d = pd.read_parquet(path)
    d["year"] = pd.to_datetime(d.date).dt.year
    return d


def malaysia_tfr():
    d = _fertility()
    d = d[d.age_group == "tfr"]
    return pd.DataFrame({"year": d.year, "value": d.fertility_rate.astype(float)}).sort_values("year")


def _women(year):
    """{band: women} for all residents. DOSM reports population in thousands."""
    path = fetch(POPULATION, os.path.join(DATA, "population.parquet"))
    d = pd.read_parquet(path)
    d = d[(d.sex == "female") & (d.ethnicity == "overall")]
    d = d[pd.to_datetime(d.date).dt.year == year]
    out = {}
    for _, r in d.iterrows():
        lo_hi = str(r.age).split("-")
        if len(lo_hi) == 2 and lo_hi[0].isdigit() and lo_hi[1].isdigit():
            band = (int(lo_hi[0]), int(lo_hi[1]))
            if band in BANDS:
                out[band] = float(r.population) * 1000
    return out


def malaysia_detail(year):
    """Births implied by DOSM's own rates, and the population those rates were built on."""
    rates = _fertility()
    rates = rates[(rates.year == year) & (rates.age_group != "tfr")]
    women = _women(year)
    out = {}
    for _, r in rates.iterrows():
        lo_hi = str(r.age_group).split("-")
        if len(lo_hi) != 2:
            continue
        band = (int(lo_hi[0]), int(lo_hi[1]))
        w = women.get(band)
        if w:
            out[band] = {"women": w, "births": float(r.fertility_rate) / 1000 * w}
    return out or None


if __name__ == "__main__":
    t = malaysia_tfr()
    print(t.tail(5).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    d = malaysia_detail(2023)
    print("implied births 15-49:", f"{sum(v['births'] for v in d.values()):,.0f}",
          "— DOSM registered 455,761 in 2023")
    print("implied TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 3))
