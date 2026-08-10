"""Myanmar: Department of Population, 2019 Inter-censal Survey.

Appendix table D-1 of the Union Report is unusually generous: for every five-year age group it
prints the number of women enumerated and the number of live births they had in the twelve
months before the survey, alongside the department's own age-specific rate. So the total
fertility rate can be rebuilt from the counts rather than taken from the printed total.
"""

import os
import re
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data")
BANDS = [(15, 19), (20, 24), (25, 29), (30, 34), (35, 39), (40, 44), (45, 49)]


def _text():
    txt = os.path.join(DATA, "mm", "ics_appendix.txt")
    if not os.path.exists(txt):
        subprocess.run(["pdftotext", "-layout", os.path.join(DATA, "mm", "ics_appendix.pdf"), txt], check=True)
    return open(txt, errors="ignore").read().splitlines()


def _union():
    """{(lo, hi): {"women": n, "births": n}} for the whole country from table D-1.

    The table repeats for Union, Urban, Rural and then every state and region, so reading stops
    at the end of the first block.
    """
    lines = _text()
    start = next(i for i, ln in enumerate(lines) if ln.strip() == "UNION")
    out = {}
    for ln in lines[start:]:
        m = re.match(r"\s*(\d{2})-(\d{2})\s+(.*)$", ln)
        if not m:
            if ln.strip() in ("URBAN", "RURAL"):
                break
            continue
        band = (int(m.group(1)), int(m.group(2)))
        if band not in BANDS:
            continue
        nums = [t for t in m.group(3).split() if re.fullmatch(r"[\d,]+(\.\d+)?", t)]
        if len(nums) < 4:
            continue
        women = float(nums[1].replace(",", ""))
        births = float(nums[3].replace(",", ""))
        out[band] = {"women": women, "births": births}
        if len(out) == len(BANDS):
            break
    return out


def myanmar_tfr():
    """One point: the 2019 survey, rebuilt from its own counts."""
    d = _union()
    tfr = sum(v["births"] / v["women"] * 5 for v in d.values())
    return pd.DataFrame([{"year": 2019, "value": tfr}])


def myanmar_detail(year):
    return _union() if year == 2019 else None


if __name__ == "__main__":
    d = _union()
    for band, v in sorted(d.items()):
        print(band, f"births {v['births']:>8,.0f}  women {v['women']:>10,.0f}",
              f"asfr {v['births'] / v['women'] * 1000:7.2f}")
    print("recalculated TFR:", round(myanmar_tfr().value.iloc[0], 4), "— report prints 2.0")
