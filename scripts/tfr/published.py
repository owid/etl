"""Countries where we take the office's own published total fertility rate.

No recalculation happens here — each series is the number the statistical office states.
Where a figure is a single point rather than a series, that is because the office publishes
it only for census or survey rounds. Every value below was checked against the source
document before being written down; the source is named in countries.py.
"""

import json
import os
import re
import subprocess
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")


def _series(pairs):
    return pd.DataFrame(pairs, columns=["year", "value"])


# ---------------------------------------------------------------- Russia
def russia():
    """Rosstat Demographic Yearbook 2023, appendix sheet 2.2 — national row only.

    The appendix carries just the two most recent years; the yearbook PDF adds 2020.
    """
    d = pd.read_excel(os.path.join(DATA, "ru_demog.xls"), sheet_name="2.2 ", header=None)
    rows = []
    for i in range(len(d)):
        if str(d.iloc[i, 0]).strip() == "Российская Федерация":
            for j in range(i + 1, len(d)):
                y = str(d.iloc[j, 0]).strip()
                if not re.match(r"^\d{4}$", y):
                    break
                v = pd.to_numeric(d.iloc[j, 1], errors="coerce")
                if pd.notna(v):
                    rows.append((int(y), float(v)))
            break
    return _series(rows)


# ---------------------------------------------------------------- Vietnam
def vietnam():
    """National Statistics Office PxWeb table V02.15 — total column, 2001 onward."""
    d = json.load(open(os.path.join(DATA, "vn_tfr.json"), encoding="utf-8-sig"))
    meta = json.load(open(os.path.join(DATA, "vn_tfr_meta.json"), encoding="utf-8-sig"))
    # the latest year is labelled "Sơ bộ 2024" (preliminary), so pull the digits out
    years = [re.search(r"\d{4}", t).group() for t in meta["variables"][0]["valueTexts"]]
    rows = []
    for row in d["data"]:
        if row["key"][1] != "0":                      # 0 = whole country, 1 = urban, 2 = rural
            continue
        v = pd.to_numeric(row["values"][0], errors="coerce")
        if pd.notna(v):
            rows.append((int(years[int(row["key"][0])]), float(v)))
    return _series(sorted(rows))


# ---------------------------------------------------------------- Bangladesh
def bangladesh():
    """SVRS 2023 report. Only the two years its own text states are used here.

    The report's narrative says "the total fertility rate (TFR) decreased to 2.17 in 2023
    from 2.20 in 2022", and table 1.6 repeats both with confidence intervals. Earlier
    editions would extend the series.
    """
    path = os.path.join(DATA, "svrs_2023.txt")
    if not os.path.exists(path):
        subprocess.run(["pdftotext", "-layout", os.path.join(DATA, "svrs_2023.pdf"), path], check=True)
    text = open(path, errors="ignore").read()
    m = re.search(r"total fertility rate \(TFR\) decreased to (\d\.\d+) in (\d{4}) from (\d\.\d+) in (\d{4})", text)
    if not m:
        return _series([])
    return _series(sorted([(int(m.group(4)), float(m.group(3))), (int(m.group(2)), float(m.group(1)))]))


# ---------------------------------------------------------------- single-round figures
def indonesia():
    """BPS Long Form of the 2020 census, fieldwork 2022: "hasil Long Form SP2020 sebesar 2,42"."""
    return _series([(2022, 2.42)])


def pakistan():
    """PBS Pakistan Demographic Survey 2020, summary of findings: TFR 3.7.

    The reference period is 2018-2020; it is placed at 2020, the survey year.
    """
    return _series([(2020, 3.7)])


def china():
    """NBS at the Seventh National Population Census press conference: TFR 1.3 for 2020.

    NBS publishes a total fertility rate only around census years. The annual communiqué
    gives births and a crude birth rate but no fertility rate.
    """
    return _series([(2020, 1.3)])


def nigeria():
    """NBS Demographic Statistics Bulletin 2022, table 7, row "Calculated TFR", 2013-2022.

    This is not a measured rate: the National Population Commission interpolates linearly
    between the 2008, 2013 and 2018 DHS rounds, so it is a projection.
    """
    path = os.path.join(DATA, "ng_bulletin_2022.txt")
    if not os.path.exists(path):
        subprocess.run(
            ["pdftotext", "-layout", os.path.join(DATA, "ng_bulletin_2022.pdf"), path], check=True
        )
    for line in open(path, errors="ignore"):
        if "Calculated TFR" in line:
            vals = [float(v) for v in re.findall(r"\b\d\.\d{2}\b", line)]
            if vals:
                return _series([(2013 + i, v) for i, v in enumerate(vals)])
    return _series([])
