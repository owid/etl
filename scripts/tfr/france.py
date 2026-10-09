"""France: INSEE births and fertility rates by age of mother.

Every table INSEE publishes here comes in two territorial versions: the whole republic including the
overseas departments ("France"), and mainland France plus Corsica only ("France metropolitaine").
The UN's "France" excludes the overseas departments — it lists Mayotte, Reunion, Guadeloupe,
Martinique and French Guiana as separate entities — so the mainland version is the one that matches
what the comparison is against, and it is the one read here. Sheets and files named FM are mainland;
FR is the whole republic.

Both files use age reached during the year — the calendar year minus the year of birth — and so
does INSEE's population pyramid, which supplies the denominator. The conventions match on both
sides, so the rate is not biased by them.
"""

import os
import re
import warnings

import pandas as pd

from fetch import fetch

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _rates():
    """{year: {age: births per 10,000 women}} from the Bilan démographique table."""
    d = pd.read_excel(os.path.join(DATA, "fr_asfr_fix.xlsx"), sheet_name="FM - âge détaillé", header=None)
    ages = {j: int(re.match(r"(\d+) ans", str(d.iloc[3, j])).group(1))
            for j in range(1, d.shape[1])
            if re.match(r"\d+ ans", str(d.iloc[3, j]))}
    out = {}
    for i in range(4, len(d)):
        m = re.match(r"^(\d{4})", str(d.iloc[i, 0]).strip())
        if not m:
            continue
        for j, age in ages.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(int(m.group(1)), {})[age] = float(v)
    return out


def _births():
    """{year: {age: births}} from the long-series civil-registry table."""
    d = pd.read_excel(os.path.join(DATA, "fr_t48_fix.xlsx"), sheet_name="FM", header=None)
    ages = {j: int(re.match(r"(\d+) ans", str(d.iloc[3, j])).group(1))
            for j in range(1, d.shape[1])
            if re.match(r"\d+ ans$", str(d.iloc[3, j]).strip())}
    out = {}
    for i in range(4, len(d)):
        m = re.match(r"^(\d{4})", str(d.iloc[i, 0]).strip())
        if not m:
            continue
        for j, age in ages.items():
            v = pd.to_numeric(d.iloc[i, j], errors="coerce")
            if pd.notna(v):
                out.setdefault(int(m.group(1)), {})[age] = float(v)
    return out


def france_tfr():
    rates = _rates()
    return pd.DataFrame(
        [{"year": y, "value": sum(r.values()) / 10000} for y, r in sorted(rates.items())]
    )


PYRAMID = "https://www.insee.fr/fr/outil-interactif/5014911/data/FRMetro/donnees_pyramide_act.csv"


def france_women(year):
    """{age: women} on INSEE's own definition: the mean population over the year.

    INSEE defines a fertility rate as births to women of an age divided by "la population moyenne
    de l'année des femmes de même âge". It publishes population at 1 January, so the mean is the
    average of that year's and the next year's snapshots for the same age.
    """
    path = fetch(PYRAMID, os.path.join(DATA, "fr", "pyramide_mainland.csv"))
    d = pd.read_csv(path, sep=";")
    d = d[d.SEXE == "F"]
    now = dict(zip(d[d.ANNEE == year].AGE, d[d.ANNEE == year].POP))
    nxt = dict(zip(d[d.ANNEE == year + 1].AGE, d[d.ANNEE == year + 1].POP))
    if not now or not nxt:
        return None
    return {a: (float(now[a]) + float(nxt[a])) / 2 for a in now if a in nxt and 15 <= a <= 49}


def france_detail(year):
    """Births from the civil register, women from INSEE's population estimates.

    Falls back to the population INSEE's own rates imply where the pyramid does not reach the year.
    """
    rates, births = _rates().get(year), _births().get(year)
    if not rates or not births:
        return None
    women = france_women(year)
    out = {}
    for lo, hi in BANDS:
        b = sum(births.get(a, 0.0) for a in range(lo, hi + 1))
        if women:
            w = sum(women.get(a, 0.0) for a in range(lo, hi + 1))
        else:
            w = sum(births[a] / (rates[a] / 10000) for a in range(lo, hi + 1) if rates.get(a) and a in births)
        if b and w:
            out[(lo, hi)] = {"births": b, "women": w}
    return out or None


if __name__ == "__main__":
    print(france_tfr().tail(4).to_string(index=False))
    d = france_detail(2025)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>9,.0f}  women {v['women']:>11,.0f}")
