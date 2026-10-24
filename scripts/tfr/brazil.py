"""Brazil TFR from IBGE registered births by mother's age / IBGE female population by age."""

import json
import os
import re

import pandas as pd

GROUPS = {
    "Menos de 15 anos": (10, 14),
    "15 a 19 anos": (15, 19),
    "20 a 24 anos": (20, 24),
    "25 a 29 anos": (25, 29),
    "30 a 34 anos": (30, 34),
    "35 a 39 anos": (35, 39),
    "40 a 44 anos": (40, 44),
    "45 a 49 anos": (45, 49),
    "50 anos ou mais": (50, 54),
}
UNKNOWN = {"Ignorada", "Idade ignorada", "Ignorado"}
AGE = re.compile(r"^(\d+)\s*ano")


def sidra(path):
    """SIDRA returns row 0 as a header dict; keys are column codes."""
    raw = json.loads(open(os.path.join(os.path.dirname(__file__), "data", path)).read())
    return pd.DataFrame(raw[1:])


def female_pop():
    df = sidra("br_pop.json")
    df = df[df["D4N"] == "Mulheres"]  # sex
    out = {}
    for _, r in df.iterrows():
        m = AGE.match(r["D5N"].strip())  # single age
        if not m:
            continue
        year = int(r["D6N"])  # projection year
        val = pd.to_numeric(r["V"], errors="coerce")
        if pd.notna(val):
            out.setdefault(year, {})[int(m.group(1))] = float(val)
    return {y: pd.Series(v) for y, v in out.items()}


def births():
    """Table 197 covers 1984-2002, table 2612 covers 2003-2024."""
    rows = {}
    for path in ["br_b197.json", "br_b2612.json"]:
        df = sidra(path)
        for _, r in df.iterrows():
            label = str(r["D4N"]).strip()
            if label not in GROUPS and label not in UNKNOWN:
                continue
            year = int(r["D3N"])
            val = pd.to_numeric(r["V"], errors="coerce")
            if pd.isna(val):
                continue
            rows.setdefault(year, {})[label] = float(val)
    return rows


def tfr():
    pop = female_pop()
    out = []
    for year, g in sorted(births().items()):
        if year not in pop:
            continue
        counted = sum(v for k, v in g.items() if k in GROUPS)
        unknown = sum(v for k, v in g.items() if k in UNKNOWN)
        if counted == 0:
            continue
        scale = (counted + unknown) / counted
        total = 0.0
        for label, (lo, hi) in GROUPS.items():
            if label not in g:
                continue
            ages = [a for a in range(lo, hi + 1) if a in pop[year].index]
            denom = pop[year][ages].sum()
            if denom > 0:
                total += g[label] * scale / denom * len(ages)
        out.append({"year": year, "value": total, "births": counted + unknown})
    return pd.DataFrame(out)


if __name__ == "__main__":
    t = tfr()
    print(t[t.year >= 1998].to_string(index=False))
