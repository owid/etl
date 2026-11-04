"""South Korea: Statistics Korea's own English birth-statistics releases.

The KOSIS database would be the obvious source, but its API refuses every request without a key
and an account needs a Korean phone or identity number, which we do not have. The annual birth
statistics release is published in English as a plain PDF instead, and each edition's first table
carries an eleven-year run of the total fertility rate — so chaining a handful of editions covers
2003 to 2024 with no key at all. Editions before 2013 print only the current and previous year, so
the series cannot be pushed back further this way.
"""

import os
import re
import subprocess

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "kr")
BOARD = "https://mods.go.kr/boardDownload.es?bid=11773"

# Final edition -> (list_no, seq) on the statistics office's own publication board. Oldest first:
# later editions win where two overlap, because the office revises.
RELEASES = {
    "b2010": (273489, 1),
    "b2013": (329729, 1),
    "b2015": (356403, 1),
    "b2018": (378026, 1),
    "b2020": (391897, 1),
    "b2024": (439008, 2),
}
# Each February the office publishes a preliminary figure for the year just ended, about eighteen
# months before the final edition covering it. It fills only years no final edition reaches yet.
PRELIMINARY = {"p2025": (444910, 1)}


def _text(key):
    path = os.path.join(DATA, f"{key}.txt")
    if not os.path.exists(path):
        list_no, seq = {**RELEASES, **PRELIMINARY}[key]
        pdf = fetch(f"{BOARD}&list_no={list_no}&seq={seq}", os.path.join(DATA, f"{key}.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    return open(path, errors="ignore").read().splitlines()


def _from_release(key):
    """{year: tfr} from one edition's table 1."""
    lines = _text(key)
    # the marker is written "[Table 1]" in recent editions and "[ Table ]" in older ones
    start = next((i for i, ln in enumerate(lines)
                  if re.search(r"\[\s*Table\s*1?\s*\].*(?:total fertility rate|fertility rate)", ln, re.I)), None)
    if start is None:
        return {}
    years = set()
    for ln in lines[start:start + 40]:
        if re.match(r"\s*Total fertility rate\b", ln):
            # columns run oldest to newest, so sorting the collected years lines them up. The row
            # ends with a change column, which the slice drops.
            ys = sorted(years)
            vals = [float(v) for v in re.findall(r"\b\d\.\d+\b", ln)]
            return dict(zip(ys, vals[:len(ys)])) if len(vals) >= len(ys) >= 8 else {}
        # the header wraps across two or three lines in the older editions, and a provisional year
        # is written with a trailing p
        years.update(int(y) for y in re.findall(r"\b((?:19|20)\d{2})p?\b", ln))
    return {}


def korea_tfr():
    """Later editions win where editions overlap, because the office revises."""
    rows = {}
    for key in RELEASES:
        rows.update(_from_release(key))
    for key in PRELIMINARY:
        rows.update({y: v for y, v in _from_release(key).items() if y not in rows})
    return pd.DataFrame(sorted(rows.items()), columns=["year", "value"])


if __name__ == "__main__":
    for key in list(RELEASES) + list(PRELIMINARY):
        d = _from_release(key)
        print(key, min(d) if d else "-", max(d) if d else "-", len(d), "years")
    print(korea_tfr().to_string(index=False))
