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

from fetch import fetch

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
    """SVRS 2023 report, table 3.13 — the whole 1982-2023 series in one table.

    The table runs across several pages with the header repeated, and each row gives five
    fertility measures; the third number after the year is the total fertility rate.
    """
    path = os.path.join(DATA, "svrs_2023.txt")
    if not os.path.exists(path):
        subprocess.run(["pdftotext", "-layout", os.path.join(DATA, "svrs_2023.pdf"), path], check=True)
    lines = open(path, errors="ignore").read().splitlines()
    start = next(i for i, ln in enumerate(lines) if "Trends in fertility as observed in the SVRS" in ln)
    rows = {}
    for ln in lines[start:]:
        m = re.match(r"\s*(19|20)(\d{2})\s+(\d+\.\d+)\s+(\d+(?:\.\d+)?)\s+(\d+\.\d+)"
                     r"\s+(\d+\.\d+)\s+(\d+\.\d+)\s*$", ln)
        if m:
            rows[int(m.group(1) + m.group(2))] = float(m.group(5))
        elif rows and len(rows) >= 42:                 # 1982-2023 inclusive
            break
    return _series(sorted(rows.items()))


# ---------------------------------------------------------------- single-round figures
def indonesia():
    """BPS Long Form of the 2020 census, fieldwork 2022: "hasil Long Form SP2020 sebesar 2,42"."""
    return _series([(2022, 2.42)])


PDS = {"pds2003": "pdswriteup-1.pdf", "pds2005": "pds2005report-1.pdf",
       "pds2006": "complete_report-2-1.pdf", "pds2007": "complete_report-2.pdf",
       "pds2020": "Pakistan_Demographic_Survey-2020-4.pdf"}


def pakistan():
    """PBS Pakistan Demographic Survey, every edition still online.

    Each of the older reports has a table headed "TOTAL FERTILITY RATE (PER WOMAN)" listing its own
    round and the one before it, so reading all of them together gives 2001 through 2007. The 2020
    report puts its figure in a table alongside two other surveys instead, with its own round last.
    """
    rows = {}
    for key in ("pds2003", "pds2005", "pds2006", "pds2007"):
        lines = _pds_text(key).splitlines()
        # the same "PDS-2005  3.8  3.3  4.1" shape appears in other tables too, so only the few
        # lines under this heading are read
        start = next(i for i, ln in enumerate(lines) if "TOTAL FERTILITY RATE (PER WOMAN)" in ln)
        for ln in lines[start:start + 8]:
            m = re.match(r"\s*PDS-(\d{4})\s+(\d\.\d)\s+\d\.\d\s+\d\.\d\s*$", ln)
            if m:
                rows[int(m.group(1))] = float(m.group(2))
    m = re.search(r"Pakistan\s+(\d\.\d)\s+(\d\.\d)\s+(\d\.\d)", _pds_text("pds2020"))
    if m:
        rows[2020] = float(m.group(3))
    return _series(sorted(rows.items()))


def _pds_text(key):
    path = os.path.join(DATA, "pk", f"{key}.txt")
    if not os.path.exists(path):
        pdf = fetch(f"https://www.pbs.gov.pk/wp-content/uploads/2020/07/{PDS[key]}",
                    os.path.join(DATA, "pk", f"{key}.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    return open(path, errors="ignore").read()


def china():
    """NBS at the Seventh National Population Census press conference: TFR 1.3 for 2020.

    NBS publishes a total fertility rate only around census years. The annual communiqué
    gives births and a crude birth rate but no fertility rate.
    """
    return _series([(2020, 1.3)])


NDHS = ("https://cdn.sanity.io/files/5otlgtiz/production/"
        "85827e6e5105f14e496d9cd0bcdd92f201a54ce1.pdf")


def nigeria():
    """National Population Commission, Nigeria DHS 2024, table 5.3.2.

    The table sets every survey round since 2003 side by side, so one document gives the whole
    measured series. Each round's rate covers the three years before its fieldwork; the value is
    placed at the survey year, which is how the commission labels it.
    """
    path = os.path.join(DATA, "ng", "ndhs.txt")
    if not os.path.exists(path):
        pdf = fetch(NDHS, os.path.join(DATA, "ng", "ndhs2024.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    lines = open(path, errors="ignore").read().splitlines()
    for i, ln in enumerate(lines):
        if not ln.strip().startswith("TFR (15–49)"):
            continue
        vals = [float(v) for v in re.findall(r"\b\d\.\d\b", ln)]
        # the header wraps, so the round labels do not appear in column order — but the columns
        # themselves run oldest to newest, so sorting the years lines them up with the values
        years = sorted({int(y) for y in re.findall(r"(\d{4}) NDHS", "\n".join(lines[i - 14:i]))})
        if len(vals) == len(years) == 5:
            return _series(list(zip(years, vals)))
    return _series([])


def tanzania():
    """NBS/OCGS, "Fertility and Nuptiality Levels and Patterns in Tanzania", May 2025, table 3.2.

    The 2022 census asked women about births in the previous twelve months. Table 3.2 prints the
    resulting age-specific rates twice: as recorded, summing to 3.2, and after Arriaga adjustment
    for the births women forget or misdate, summing to 4.6. The adjusted figure is the one NBS
    presents as the country's total fertility rate, so that is what is used here.
    """
    path = os.path.join(DATA, "tz", "fert_nupt.txt")
    if not os.path.exists(path):
        subprocess.run(["pdftotext", "-layout", os.path.join(DATA, "tz", "fert_nupt.pdf"), path], check=True)
    for line in open(path, errors="ignore"):
        if line.strip().startswith("Total Fertility Rate (TFR)"):
            vals = re.findall(r"\b\d\.\d\b", line)
            if len(vals) == 2:                         # recorded, then adjusted
                return _series([(2022, float(vals[1]))])
    return _series([])


def ethiopia():
    """Ethiopian Statistical Service, EDHS rounds. Figure 2 of the 2024-25 key indicators report.

    The report plots the national total fertility rate for every round back to 2000, printing the
    value above each point, so all five survey rounds can be read from the one document.
    """
    return _series([(2000, 5.5), (2005, 5.4), (2011, 4.8), (2016, 4.6), (2024, 4.0)])


def kenya():
    """KNBS, 2019 census analytical report on fertility and nuptiality, volume VI, table 4.5.

    The trends table gives the national total fertility rate for the last two census rounds. The
    whole report is scanned page images rather than text, so the numbers were read by running
    optical character recognition over it and then checked by eye against the page.
    """
    return _series([(2009, 4.8), (2019, 3.4)])


def drc():
    """INS, EDS-RDC III (2023-24), table 5.1 and the trend sentence in section 5.1.

    Table 5.1 gives 5.5 for the three years before the survey; the same section states the
    previous round measured 6.6.
    """
    return _series([(2014, 6.6), (2024, 5.5)])


def turkey():
    """TurkStat Population Statistics Portal, national row of the total-fertility-rate export.

    The modern TurkStat data portal is a JavaScript app whose download links are per-page
    tokens, but this older server-rendered portal answers a plain request. Calling it with an
    empty `value` returns the national series rather than the province breakdown.
    """
    d = pd.read_excel(os.path.join(DATA, "tr_tfr_nat.xlsx"), header=None)
    rows = []
    for _, r in d.iterrows():
        if str(r[1]).strip() != "Türkiye":
            continue
        y = pd.to_numeric(r[0], errors="coerce")
        v = pd.to_numeric(r[2], errors="coerce")
        if pd.notna(y) and pd.notna(v):
            rows.append((int(y), float(v)))
    return _series(sorted(rows))
