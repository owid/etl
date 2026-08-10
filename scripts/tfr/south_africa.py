"""South Africa: Stats SA's modelled fertility rate, and its own birth registry alongside it.

The figure Stats SA publishes is not a rate computed from registered births. It is an assumption fed
into a cohort-component projection, arrived at by reviewing published and unpublished estimates and
then adjusted upward for known under-registration. Stats SA's own words are in countries.py.

The registry counts are published separately, so the age-band comparison can show what registration
alone says — which is a long way below the headline figure.
"""

import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "za")
MYPE = "https://www.statssa.gov.za/publications/P0302/P03022024.pdf"
RLB = "https://www.statssa.gov.za/publications/P0305/RLB%202024%20Appendices.xlsx"
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _mype_text():
    path = os.path.join(DATA, "mype2024.txt")
    if not os.path.exists(path):
        pdf = fetch(MYPE, os.path.join(DATA, "mype2024.pdf"), insecure=True)
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    return open(path, errors="ignore").read().splitlines()


def south_africa_tfr():
    """Table 2 of the mid-year population estimates: the modelled series, 2002 onward."""
    lines = _mype_text()
    start = next(i for i, ln in enumerate(lines)
                 if ln.strip().startswith("Table 2: Assumptions of Total Fertility")
                 and "..." not in ln)
    rows = []
    for ln in lines[start:start + 40]:
        m = re.match(r"\s*(\d{4})\s+(\d,\d{2})\s+\d{2},\d\s+\d{2},\d\s*$", ln)
        if m:
            rows.append((int(m.group(1)), float(m.group(2).replace(",", "."))))
    return pd.DataFrame(sorted(rows), columns=["year", "value"])


def _registered_births():
    """{year: {band: births}} from appendix D of the recorded live births report.

    Births are counted by the year they occurred, not the year they were registered, but only
    registrations captured by the report's cutoff are included — so recent years keep growing.
    """
    path = fetch(RLB, os.path.join(DATA, "rlb2024.xlsx"), insecure=True)
    d = pd.read_excel(path, sheet_name="Appendix D", header=None)
    head = next(i for i in range(len(d)) if str(d.iloc[i, 1]).strip().startswith("20"))
    years = {}
    for j in range(1, d.shape[1]):
        y = pd.to_numeric(d.iloc[head, j], errors="coerce")
        if pd.notna(y):
            years[j] = int(y)
    out = {}
    for i in range(head + 1, len(d)):
        # the sheet mixes a plain hyphen and an en dash in its age labels
        m = re.fullmatch(r"(\d{2})[–-](\d{2})", str(d.iloc[i, 0]).strip())
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


def _women():
    """{band: women} for the whole country, 2024, from table 6 of the mid-year estimates.

    The table sets four population groups side by side and ends with the national total, so the
    female figure wanted is the second-to-last of the fifteen numbers on each row.
    """
    out = {}
    for ln in _mype_text():
        m = re.match(r"\s*(\d{2})[–-](\d{2})\s+(.*)$", ln)
        if not m:
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band not in BANDS or band in out:
            continue
        # thousands are separated by single spaces and columns by runs of them, so split on the
        # runs first — a single pass of findall would swallow whole groups of columns at once
        nums = [t for t in re.split(r"\s{2,}", m.group(3).strip()) if re.fullmatch(r"[\d ]+", t)]
        if len(nums) == 15:
            out[band] = float(nums[-2].replace(" ", ""))
    return out


def south_africa_detail(year):
    births, women = _registered_births().get(year), _women()
    if not births or not women:
        return None
    return {b: {"births": births[b], "women": women[b]} for b in BANDS if b in births and b in women}


if __name__ == "__main__":
    t = south_africa_tfr()
    print(t.tail(3).to_string(index=False), f"({len(t)} years from {t.year.min()})")
    d = south_africa_detail(2024)
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}")
    print("registration-only TFR:", round(sum(v["births"] / v["women"] * 5 for v in d.values()), 3),
          "— Stats SA's modelled figure is 2.41")
