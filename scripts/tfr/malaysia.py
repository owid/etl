"""Malaysia: DOSM's fertility rates over its own population estimates.

The rates and the population come from DOSM's open data store. The plotted total is DOSM's own
published figure, taken as published.

Births by age of mother are not in the data store, but they are published: table 3.7 of the annual
Vital Statistics report gives them as counts, by state. The age-band comparison reads that table.
It used to multiply each rate by the female population instead, which is how DOSM built the rates
and so reproduces them by construction — but it is not the same as the registered counts, and in the
smallest band it was 24% out: 967 implied births against 1,278 registered.
"""

import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "my")
FERTILITY = "https://storage.dosm.gov.my/demography/fertility.parquet"
POPULATION = "https://storage.dosm.gov.my/population/population_malaysia.parquet"
VITAL = "https://storage.dosm.gov.my/demography/vitalstatistics_{year}.pdf"
VITAL_TABLE = "Live births by age group of mother and state"
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


def _registered_births(year):
    """{band: births} for the whole country from table 3.7 of the Vital Statistics report.

    The table's national row comes first, then one row per state, so reading stops after it. Its
    columns run under 15, then the seven five-year bands, then 50 and over and unknown; only the
    seven are wanted, and the row's own total is used to check they were read in the right places.
    """
    txt = os.path.join(DATA, f"vital{year}.txt")
    if not os.path.exists(txt):
        pdf = fetch(VITAL.format(year=year), os.path.join(DATA, f"vital{year}.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, txt], check=True)
    lines = open(txt, errors="ignore").read().splitlines()
    # the caption appears in the list of tables as well as over the table, so every occurrence is
    # tried and only one that yields a full national row is accepted
    for i, ln in enumerate(lines):
        if VITAL_TABLE not in ln:
            continue
        for row in lines[i:i + 25]:
            if not row.strip().startswith("Malaysia"):
                continue
            nums = [int(t.replace(",", "")) for t in re.findall(r"\d[\d,]*", row)]
            # total, under 15, the seven five-year bands, 50 and over, unknown
            if len(nums) != 11:
                continue
            total, bands = nums[0], nums[2:9]
            if sum(nums[1:]) != total:
                raise AssertionError(f"Malaysia {year}: table 3.7 columns sum to {sum(nums[1:])}, "
                                     f"not the {total} the row's own total says")
            return dict(zip(BANDS, (float(v) for v in bands)))
    raise AssertionError(f"Malaysia {year}: no national row found under table 3.7")


def malaysia_detail(year):
    """The registered births by age of mother, over the population DOSM divides them by."""
    births, women = _registered_births(year), _women(year)
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = malaysia_tfr()
    print(t.tail(5).to_string(index=False), f"({len(t)} years from {int(t.year.min())})")
    d = malaysia_detail(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>9,.0f}  women {v['women']:>11,.0f}")
    print("births 15-49:", f"{sum(v['births'] for v in d.values()):,.0f}")
    print("TFR from the counts:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 3),
          "— DOSM publishes 1.6 for 2024")
