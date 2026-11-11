"""India: Sample Registration System, Office of the Registrar General.

The SRS is an annual national sample-registration system, not full civil registration, so it
belongs in the "sample registration" tier. The Statistical Report publishes the total
fertility rate to one decimal place; there are no published counts we could recompute from,
so the figure is taken as the office states it.
"""

import os
import re
import subprocess

import pandas as pd

DATA = os.path.join(os.path.dirname(__file__), "data")
PDF = os.path.join(DATA, "srs_2024.pdf")
CACHE = os.path.join(DATA, "srs_2024.txt")


def _text():
    if not os.path.exists(CACHE):
        subprocess.run(["pdftotext", "-layout", PDF, CACHE], check=True)
    return open(CACHE, errors="ignore").read()


def india():
    """Annexure table 15: annual TFR by residence, India and bigger states."""
    for page in _text().split("\f"):
        if "Annual Estimates of Total Fertility Rate" not in page:
            continue
        # the header repeats the year run three times, once each for Total, Rural and Urban
        header = next((ln for ln in page.splitlines() if len(re.findall(r"\b20\d{2}\b", ln)) >= 12), None)
        if not header:
            continue
        seen = list(dict.fromkeys(int(y) for y in re.findall(r"\b20\d{2}\b", header)))
        for line in page.splitlines():
            if not line.strip().startswith("India "):
                continue
            vals = [float(v) for v in re.findall(r"\d\.\d", line)]
            if len(vals) >= len(seen):
                return pd.DataFrame({"year": seen, "value": vals[: len(seen)]})
    return pd.DataFrame(columns=["year", "value"])


if __name__ == "__main__":
    print(india().to_string(index=False))
