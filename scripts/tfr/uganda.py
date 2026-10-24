"""Uganda: the 2024 census fertility table, and the 2014 census before it.

Table 7.2 of the 2024 census report is the richest single artefact in this whole project: for every
five-year age group it prints the number of women, the births they reported in the previous twelve
months, the same births after a Brass P/F correction, and both the reported and corrected rates. So
the published figure can be reproduced from counts and the size of the correction seen directly.
"""

import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "ug")
CENSUS = ("https://www.ubos.org/wp-content/uploads/publications/"
          "National-Population-and-Housing-Census-2024-Final-Report-Volume-1-Main.pdf")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _text():
    path = os.path.join(DATA, "census2024.txt")
    if not os.path.exists(path):
        pdf = fetch(CENSUS, os.path.join(DATA, "census2024.pdf"), insecure=True)
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    return open(path, errors="ignore").read().splitlines()


def _table():
    """{(lo, hi): {...}} from table 7.2 — women, reported and adjusted births, and both rates."""
    lines = _text()
    # the heading appears in the contents list and again above the table itself
    starts = [i for i, ln in enumerate(lines) if ln.strip().startswith("Table 7.2:") and "..." not in ln]
    out = {}
    for start in starts:
        for ln in lines[start:start + 30]:
            m = re.match(r"\s*(\d{2})-(\d{2})\s+(.*)$", ln)
            if not m:
                continue
            band = (int(m.group(1)), int(m.group(2)))
            if band not in BANDS:
                continue
            nums = [t.replace(",", "") for t in m.group(3).split()]
            if len(nums) < 7:
                continue
            out[band] = {
                "women": float(nums[0]),
                "births": float(nums[2]),                # reported in the last 12 months
                "adjusted_births": float(nums[3]),
                "rate": float(nums[5]),
                "adjusted_rate": float(nums[6]),
            }
        if len(out) == len(BANDS):
            break
    return out


def uganda_tfr():
    """The 2024 census figure, and the 2014 census figure it is compared against in the same report.

    2014 is stated in the text as 5.8; the 2024 value is rebuilt from the adjusted rates so it is
    not just the rounded 4.5 the report prints.
    """
    d = _table()
    rows = [(2014, 5.8)]
    if len(d) == len(BANDS):
        rows.append((2024, sum(v["adjusted_rate"] for v in d.values()) * 5))
    return pd.DataFrame(sorted(rows), columns=["year", "value"])


def uganda_detail(year):
    """The births women actually reported, not the corrected ones — that is what was counted."""
    if year != 2024:
        return None
    d = _table()
    return {b: {"births": v["births"], "women": v["women"]} for b, v in d.items()} or None


if __name__ == "__main__":
    print(uganda_tfr().to_string(index=False))
    d = _table()
    print("reported TFR:", round(sum(v["rate"] for v in d.values()) * 5, 3),
          "| adjusted:", round(sum(v["adjusted_rate"] for v in d.values()) * 5, 3),
          "— the report prints 4.5")
