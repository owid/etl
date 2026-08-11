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
import zipfile

import pandas as pd

from fetch import fetch

warnings.filterwarnings("ignore")

DATA = os.path.join(os.path.dirname(__file__), "data")


def _series(pairs):
    return pd.DataFrame(pairs, columns=["year", "value"])


# ---------------------------------------------------------------- Russia
def russia():
    """Rosstat Demographic Yearbook 2023, chapter 2, sheet 2.6.

    The yearbook is split into eight chapter spreadsheets behind an HTML index. Chapter 2 carries
    the total fertility rate for the whole country back to the early 1960s; the first column of
    sheet 2.6 is the year and the second is the figure for the whole population. Early rows label
    two-year averages, which are skipped.
    """
    path = fetch("https://rosstat.gov.ru/storage/2024/04-20/VF5GE3HA/Dem_ej_02-2023.xlsx",
                 os.path.join(DATA, "ru", "dem02.xlsx"), insecure=True)
    d = pd.read_excel(path, sheet_name="2.6", header=None)
    rows = []
    for _, r in d.iterrows():
        if not re.fullmatch(r"\d{4}", str(r.iloc[0]).strip()):
            continue
        v = pd.to_numeric(r.iloc[1], errors="coerce")
        if pd.notna(v):
            rows.append((int(str(r.iloc[0]).strip()), float(v)))
    return _series(sorted(rows))


# ---------------------------------------------------------------- Vietnam
PCFPS = ("https://www.nso.gov.vn/wp-content/uploads/2026/08/"
         "Sach-ket-qua-chu-yeu-dieu-tra-bien-dong-dan-so-2025.-pdf.pdf")


def vietnam():
    """National Statistics Office, population change survey report for 1 April 2025, table 5.2.

    The office's PxWeb database stops at 2023, but this report prints the whole 2001-2025 series in
    one table: year, whole country, urban, rural. Rows are read from the whole-country column.
    """
    path = os.path.join(DATA, "vn", "pcfps2025.txt")
    if not os.path.exists(path):
        pdf = fetch(PCFPS, os.path.join(DATA, "vn", "pcfps2025.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    lines = open(path, errors="ignore").read().splitlines()
    start = next(i for i, ln in enumerate(lines)
                 if "Tổng tỷ suất sinh chia theo thành thị, nông thôn giai đoạn" in ln)
    rows = {}
    for ln in lines[start:start + 60]:
        # the table spills onto a second page with the header repeated; a stray row uses dots
        # for the decimal separator instead of commas, so accept both
        m = re.match(r"\s*(20\d{2})\s+(\d[,.]\d{2})\s+\d[,.]\d{2}\s+\d[,.]\d{2}\s*$", ln)
        if m:
            rows[int(m.group(1))] = float(m.group(2).replace(",", "."))
    return _series(sorted(rows.items()))


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
    """BPS statistics release for SUPAS 2025, figure 6 — all four rounds in one chart.

    The chart plots the census and inter-censal rounds in order and prints each value above its
    point, so the whole series can be read from the one document. Each round is placed at the year
    BPS names it for.
    """
    path = os.path.join(DATA, "id", "supas2025.txt")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} — see the BPS notes in countries.py for how it was obtained")
    lines = open(path, errors="ignore").read().splitlines()
    rounds = ["SP2010", "SUPAS 2015", "LF SP2020", "SUPAS 2025"]
    years = [2010, 2015, 2020, 2025]
    for i, ln in enumerate(lines):
        if all(r in ln for r in rounds):
            # each data label sits alone on its own line; numbers inside prose above the chart
            # (the replacement level of 2,10, for one) must not be picked up
            vals = [m.group(1) for ln2 in lines[max(0, i - 20):i]
                    if (m := re.fullmatch(r"\s*(\d,\d{2})\s*", ln2))]
            if len(vals) == 4:
                return _series(list(zip(years, [float(v.replace(",", ".")) for v in vals])))
    return _series([])


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
    """The 2022 Demographic and Health Survey's 4.8, which NBS names as the official rate.

    NBS and Zanzibar's OCGS publish two figures for 2022: 4.6 from the census, and 4.8 from the
    health survey they also run. Their own census report picks between them — "it is recommended to
    use TFR from TDHS as the official rate" — so the survey figure is the one plotted. The census
    numbers are the more interesting story and are described in countries.py, but they are not what
    the office asks to be treated as the national figure.

    The census total is still read from its own report rather than hardcoded, because it is quoted
    in the notes and should move if the report is ever revised.
    """
    return _series([(2022, 4.8)])


def tanzania_census():
    """(recorded, adjusted) totals from table 3.2 of the 2022 census fertility report."""
    path = os.path.join(DATA, "tz", "fert_nupt.txt")
    if not os.path.exists(path):
        subprocess.run(["pdftotext", "-layout", os.path.join(DATA, "tz", "fert_nupt.pdf"), path], check=True)
    for line in open(path, errors="ignore"):
        if line.strip().startswith("Total Fertility Rate (TFR)"):
            vals = re.findall(r"\b\d\.\d\b", line)
            if len(vals) == 2:
                return float(vals[0]), float(vals[1])
    return None, None


def ethiopia():
    """Ethiopian Statistical Service, EDHS rounds. Figure 2 of the 2024-25 key indicators report.

    The report plots the national total fertility rate for every round back to 2000, printing the
    value above each point, so all five survey rounds can be read from the one document.
    """
    return _series([(2000, 5.5), (2005, 5.4), (2011, 4.8), (2016, 4.6), (2024, 4.0)])


SD_BRIEF = ("http://web.archive.org/web/20181113165403/http://cbs.gov.sd//resources/uploads/"
            "files/Fertility%20%20and%20%20married%20%20Status(1).pdf")


def sudan():
    """Central Bureau of Statistics, "Fertility and Married Status", the MICS 2014 figure.

    The Bureau's own website no longer resolves — its nameservers stopped answering — so this brief
    was recovered from a web archive. It is the only national fertility figure we could find inside
    our window; the rest of its table stops at a 1990 survey.
    """
    path = os.path.join(DATA, "sd", "fertility.txt")
    if not os.path.exists(path):
        pdf = fetch(SD_BRIEF, os.path.join(DATA, "sd", "fertility.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    lines = open(path, errors="ignore").read().splitlines()
    for i, ln in enumerate(lines):
        m = re.search(r"Total Fertility Rate for Women age 15-49\s*year\s+(\d\.\d)", ln)
        if m and any("MICS 2014" in x for x in lines[i:i + 8]):
            return _series([(2014, float(m.group(1)))])
    return _series([])


DZ_BULLETIN = "https://www.ons.dz/IMG/pdf/Demographie2019bis.pdf"


def algeria():
    """ONS, Démographie Algérienne 2019 (BIS edition), main indicators table.

    One row carries the fertility index for 2001 through 2019, with an ellipsis where the year was
    never published. The row above it, the number of births, anchors the year alignment: it runs
    from 619 thousand in 2001 to 1,034 thousand in 2019.
    """
    path = os.path.join(DATA, "dz", "demo2019bis.txt")
    if not os.path.exists(path):
        pdf = fetch(DZ_BULLETIN, os.path.join(DATA, "dz", "demo2019bis.pdf"), insecure=True)
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    for ln in open(path, errors="ignore"):
        if "Indice Conjoncturel de F" not in ln:
            continue
        tail = ln.split("femme)", 1)[-1]
        cells = [t for t in tail.split() if t == "\u2026" or re.fullmatch(r"\d,\d", t)]
        if len(cells) != 19:                           # 2001 to 2019 inclusive
            continue
        rows = [(2001 + i, float(c.replace(",", "."))) for i, c in enumerate(cells) if c != "\u2026"]
        return _series(rows)
    return _series([])


IQ_TABLE = "https://cosit.gov.iq/AAS13/Human%20Develop%20Statistics%2019/humdev18.htm"
IQ_CENSUS = "https://cosit.gov.iq/documents/AAS2024/02.pdf"
IQ_CENSUS_TABLE = "TOTAL FERTILITY RATE BY GOVERNORATE"


def _iraq_census_2024():
    """COSIT, Annual Statistical Abstract 2024, table 8/2 — the 2024 census.

    The table runs down the governorates and ends with national and Kurdistan Region rows. The
    growth-rate table just above it also ends with a row labelled Iraq, so reading starts at the
    fertility table's own caption.
    """
    path = os.path.join(DATA, "iq", "census2024.txt")
    if not os.path.exists(path):
        pdf = fetch(IQ_CENSUS, os.path.join(DATA, "iq", "census2024.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    lines = open(path, errors="ignore").read().splitlines()
    head = next(i for i, ln in enumerate(lines) if IQ_CENSUS_TABLE in ln)
    for ln in lines[head:]:
        m = re.match(r"\s*Iraq\s+(\d\.\d)\s", ln)
        if m:
            return [(2024, float(m.group(1)))]
    raise AssertionError("Iraq: no national row under the 2024 census fertility table")


def iraq():
    """COSIT's measured rounds: four household surveys, the 1997 census, and the 2024 census.

    The survey rounds and 1997 come from the Annual Statistical Abstract 2013, table 19/18, each
    named in the table's own source line. The 2024 census figure is in the 2024 abstract instead.
    """
    path = fetch(IQ_TABLE, os.path.join(DATA, "iq", "tfr.htm"))
    d = pd.read_html(path, encoding="cp1256")[0]
    rows = []
    for _, r in d.iterrows():
        y = pd.to_numeric(str(r.iloc[0]).strip(), errors="coerce")
        v = pd.to_numeric(str(r.iloc[1]).strip(), errors="coerce")
        if pd.notna(y) and pd.notna(v) and 1950 < y < 2100 and 0 < v < 10:
            rows.append((int(y), float(v)))
    rows += _iraq_census_2024()
    return _series(sorted(rows))


AFDHS = ("http://web.archive.org/web/20170511114715/http://cso.gov.af/Content/files/"
         "Afghanistan%20DHS%202015%20KIR/AFDHS_Final%20Report.pdf")


def afghanistan():
    """Central Statistics Organization and Ministry of Public Health, AfDHS 2015, table 5.1.

    Its own domain lapsed years ago, so the report comes from a web archive. The table gives urban,
    rural and total columns; the last is the national figure.
    """
    path = os.path.join(DATA, "af", "afdhs2015.txt")
    if not os.path.exists(path):
        pdf = fetch(AFDHS, os.path.join(DATA, "af", "afdhs2015.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    for ln in open(path, errors="ignore"):
        m = re.match(r"\s*TFR \(15-49\)\s+(\d\.\d)\s+(\d\.\d)\s+(\d\.\d)\s*$", ln)
        if m:
            return _series([(2015, float(m.group(3)))])
    return _series([])


YE_NHDS = ("https://web.archive.org/web/20220321032936/http://www.cso-yemen.com/publiction/"
           "suhee_2013/rebort_sehee_2013.pdf")


def yemen():
    """CSO and the health ministry, National Health and Demographic Survey 2013, table 2.

    The organisation's own site is behind a firewall that rejects anything but a browser, so this
    report comes from a web archive. The table gives urban, rural and total columns.
    """
    path = os.path.join(DATA, "ye", "nhds2013.txt")
    if not os.path.exists(path):
        pdf = fetch(YE_NHDS, os.path.join(DATA, "ye", "nhds2013.pdf"))
        subprocess.run(["pdftotext", "-layout", pdf, path], check=True)
    for ln in open(path, errors="ignore"):
        # the table is right to left, so the three figures print before their Arabic label and the
        # national one comes first: "4.4  5.1  3.2  \u0645\u0639\u062f\u0644 ... TFR (15-49)"
        if "TFR (15-49" not in ln:
            continue
        vals = re.findall(r"\d\.\d", ln)
        if len(vals) == 3:
            return _series([(2013, float(vals[0]))])
    return _series([])


def angola():
    """INE and the health ministry, the two IIMS survey rounds.

    Quadro 5.1 of each report gives the national figure and the age-specific rates behind it. The
    2015-16 round gives 6.2 and the 2023-24 round 4.8; each is placed at the year its fieldwork
    ended. Summing the printed rates reproduces both totals — 6.215 and 4.78.
    """
    return _series([(2016, 6.2), (2024, 4.8)])


UA_ZIP = ("https://stat.gov.ua/sites/default/files/2023-10/"
          "%D0%9D%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%BD%D1%8F%20(1990-2021)_2.zip")


def ukraine():
    """Держстат, "Population 1990-2021" workbook — the fertility block, 1990 onward.

    The workbook stacks several tables on one sheet; the fertility one has age-specific rates in
    columns 1 to 8 and the total in column 9. The archive stores its Cyrillic filename in a legacy
    encoding, so the spreadsheet is pulled out by suffix rather than by name.
    """
    xlsx = os.path.join(DATA, "ua", "population.xlsx")
    if not os.path.exists(xlsx):
        z = fetch(UA_ZIP, os.path.join(DATA, "ua", "naselennia.zip"))
        with zipfile.ZipFile(z) as arch:
            name = next(n for n in arch.namelist() if n.lower().endswith(".xlsx"))
            os.makedirs(os.path.dirname(xlsx), exist_ok=True)
            open(xlsx, "wb").write(arch.read(name))
    d = pd.read_excel(xlsx, sheet_name=0, header=None)
    start = next(i for i in range(len(d)) if "Сумарний" in " ".join(str(v) for v in d.iloc[i]))
    rows = []
    for i in range(start, len(d)):
        # from 2014 the year carries a footnote digit glued to it, e.g. "20214" for 2021
        m = re.match(r"\s*(19|20)(\d{2})", str(d.iloc[i, 0]))
        v = pd.to_numeric(d.iloc[i, 9], errors="coerce")
        if m and pd.notna(v):
            rows.append((int(m.group(1) + m.group(2)), float(v)))
        elif rows:
            break                                  # the footnotes, then unrelated tables, follow
    return _series(sorted(rows))


MA_SERIES = ("https://docs.google.com/spreadsheets/d/"
             "1b_FXhLlqC_Yy_CVKZpT8b7GMhLentivryoyfY8SXgvY/export?gid=0")


def morocco():
    """HCP's own long-run fertility series, published as a spreadsheet behind its indicator page.

    The series comes from censuses in 1982, 1994, 2004, 2014 and 2024, and household surveys for the
    years in between, including 2010. The two earliest points, 1962 and 1975, fall in no census year
    and we have not identified which surveys they come from.

    The spreadsheet rounds 2024 to 2.00. HCP's own 2024 census volumes give 1.97 — urban 1.77, rural
    2.37 — so that is what is used: the same producer, one publication later, and no reason to plot a
    figure less precise than the one we can cite.
    """
    path = fetch(MA_SERIES, os.path.join(DATA, "ma", "isf.xlsx"))
    d = pd.read_excel(path, header=None)
    head = next(i for i in range(len(d)) if "Année" in [str(v).strip() for v in d.iloc[i]])
    yc = [j for j in range(d.shape[1]) if str(d.iloc[head, j]).strip() == "Année"][0]
    vc = [j for j in range(d.shape[1]) if str(d.iloc[head, j]).strip() == "Ensemble"][0]
    rows = {}
    for i in range(head + 1, len(d)):
        y = pd.to_numeric(str(d.iloc[i, yc]).strip(), errors="coerce")
        v = pd.to_numeric(d.iloc[i, vc], errors="coerce")
        if pd.notna(y) and pd.notna(v) and 1950 < y < 2100:
            rows[int(y)] = float(v)
    if rows.get(2024) == 2.0:
        rows[2024] = 1.97
    return _series(sorted(rows.items()))


UZ_TFR = "https://api.siat.stat.uz/media/uploads/sdmx/sdmx_data_665.json"


def uzbekistan():
    """Statistics Agency indicator 2.01.01.0028, from its own open JSON endpoint.

    Each indicator is published as one document holding every region; the row whose code is 1700 is
    the republic as a whole, and the years are its keys.
    """
    path = fetch(UZ_TFR, os.path.join(DATA, "uz", "tfr.json"))
    # the document is a one-element list wrapping the metadata and the data rows
    d = json.load(open(path, encoding="utf-8"))[0]
    row = next((r for r in d["data"] if str(r.get("Code")).strip() == "1700"), None)
    if row is None:
        return _series([])
    rows = []
    for k, v in row.items():
        if re.fullmatch(r"(19|20)\d{2}", str(k)) and isinstance(v, (int, float)):
            rows.append((int(k), float(v)))
    return _series(sorted(rows))


def mozambique():
    """INE's own health and demographic survey rounds, from the 2022-23 report's trend table.

    Quadro 5.3.2 of that report sets all four rounds side by side, each computed the same way from
    women's birth histories for the three years before the survey.

    Each round is plotted at the year most of its fieldwork fell in. The last round ran from July 2022
    to February 2023, so it sits at 2022 — it used to sit at 2023, which was inconsistent with the
    2003 round, whose fieldwork also straddled a new year and which has always been plotted at 2003.
    """
    return _series([(1997, 5.2), (2003, 5.5), (2011, 5.9), (2022, 4.9)])


def saudi_arabia():
    """GASTAT, Population Estimates 2024, figure 6 — the whole-population column, 2011 onward.

    GASTAT publishes three parallel series: Saudis, non-Saudis and everyone resident. The last is
    the one comparable with the UN's figures, and the three differ enormously — 2.7, 0.8 and 2.0 in
    2024 — so the choice matters more here than almost anywhere.
    """
    return _series([(2011, 2.8), (2012, 2.8), (2013, 2.7), (2014, 2.7), (2015, 2.6), (2016, 2.6),
                    (2017, 2.7), (2018, 2.7), (2019, 2.5), (2020, 2.3), (2021, 2.2), (2022, 2.1),
                    (2023, 2.0), (2024, 2.0)])


def ghana():
    """GSS's own health and demographic survey rounds, direct estimates from birth histories.

    The 2022 report states the fall plainly: from 6.4 in 1988 to 3.9 in 2022. The earlier rounds here
    are the ones GSS reproduces in its own census monograph's trend chart, plus the 2014 round, whose
    report gives 4.2 — without it the line jumped fourteen years, from 2008 straight to 2022.
    """
    return _series([(2003, 4.4), (2008, 4.0), (2014, 4.2), (2022, 3.9)])


def madagascar():
    """INSTAT's own health and demographic survey rounds, direct estimates from birth histories.

    The 2021 round's trend chart carries the earlier ones. The 2018 census gives 4.3 as well, by a
    different method — see the note in countries.py about the correction INSTAT computed and refused.
    """
    return _series([(2004, 5.2), (2009, 4.8), (2021, 4.3)])


def niger():
    """INS survey rounds. The 2021 report's trend table carries the earlier ones.

    2006 and 2012 are the health surveys; 2021 is a stand-alone national fertility survey.
    """
    return _series([(2006, 7.1), (2012, 7.6), (2021, 6.2)])


def cameroon():
    """INS survey rounds. The 2018 round is the newest fieldwork-based figure Cameroon has.

    The 2005 census gives 5.2 after correction, from a raw 4.1 — see the note in countries.py.
    """
    return _series([(2011, 5.1), (2018, 4.8)])


def nepal():
    """National Statistics Office, census rounds. The 2021 report charts the whole series."""
    return _series([(2001, 3.25), (2011, 2.52), (2021, 1.94)])


def venezuela():
    """INE, "Resumen de Estadísticas 1999-2023" — the fertility chart's observed points.

    The chart runs to 2025 but draws 2020 and 2025 as projections, so only the points up to 2015
    are used here. All of them come out of the projection exercise built on the 2011 census.
    """
    return _series([(2000, 2.9), (2005, 2.6), (2010, 2.4), (2015, 2.3)])


def cote_divoire():
    """Survey rounds — the only fertility figures Côte d'Ivoire publishes.

    Summing each round's own age-specific rates reproduces its total: 4.95 against the published 5.0
    for 2011-12, and 4.595 against 4.6 for 2016.

    The 2021 round's own trend chart gives 5.3 for 1994, 5.2 for 1998-99, 5.0 for 2011-12 and 4.3 for
    itself. 2016 comes from a different survey program and is not in that chart.
    """
    return _series([(2012, 5.0), (2016, 4.6), (2021, 4.3)])


def mali():
    """INSTAT survey rounds. The 2023-24 report charts all seven since 1987.

    The 2022 census gives 6.1 as well, after a correction that nearly doubled its raw birth count —
    see the note in countries.py.
    """
    return _series([(2001, 6.8), (2006, 6.6), (2013, 6.1), (2018, 6.3), (2024, 6.0)])


def north_korea():
    """Central Bureau of Statistics: the 2008 census and the 2014 survey it ran.

    Both are the bureau's own work — the census foreword is signed by its director-general — but
    neither is hosted by any North Korean server; see the note in countries.py.
    """
    return _series([(2008, 2.01), (2014, 1.89)])


def burkina_faso():
    """INSD survey rounds. The 2021 report's trend table carries the earlier ones."""
    return _series([(2003, 5.9), (2010, 6.0), (2021, 4.4)])


def syria():
    """Central Bureau of Statistics, Statistical Abstract chapter 2, table 9/2.

    The table gives age-specific rates for the two family health survey rounds, 2001 and 2009, and
    those rates sum to the printed totals. Every edition we could recover reprints it unchanged.
    """
    return _series([(2001, 3.8), (2009, 3.5)])


def zambia():
    """ZamStats survey rounds. The 2024 report's trend chart carries the earlier ones."""
    return _series([(2002, 5.9), (2007, 6.2), (2014, 5.3), (2018, 4.7), (2024, 4.0)])


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


def malawi():
    """NSO census and health-survey rounds.

    The 2018 census's fertility report charts the whole series back to 1998, and the 2024 survey
    charts the survey rounds. Both are census or survey measurements; the MICS rounds sit slightly
    above them and are left out so the series stays on one pair of instruments.
    """
    return _series([(2000, 6.3), (2004, 6.0), (2008, 6.0), (2010, 5.7),
                    (2015, 4.4), (2018, 4.2), (2024, 3.7)])


def chad():
    """INSEED survey and census rounds.

    Chad has no annual figure at all. 2009 is the census, quoted in the 2024 statistical yearbook;
    2010 is the multiple-indicator survey; 2015 is the health survey, whose fieldwork ran from October
    2014 to April 2015; 2019 is the next multiple-indicator survey.

    The 2015 and 2019 rounds both report 6.4, independently — the same number from two different
    surveys, not one figure double-counted. Each round's own rates by age group confirm it: the 2015
    schedule sums to 6.45 and the 2019 schedule to 6.43, both printed as 6.4. A 2019 attribution of the
    2015 figure circulates, but a health ministry document was already quoting 6.4 in March 2016, so
    the earlier round is where it comes from.
    """
    return _series([(2009, 7.1), (2010, 6.9), (2015, 6.4), (2019, 6.4)])


def somalia():
    """SNBS, Somali Health and Demographic Survey 2020, table 4.4."""
    return _series([(2020, 6.9)])


def senegal():
    """ANSD's continuous health and demographic survey.

    The survey runs every year, and the 2023 census report prints the whole series in one trend
    table. The census's own figures are not in this series — see the note in countries.py.
    """
    return _series([(2005, 5.3), (2010, 5.0), (2012, 5.3), (2014, 5.0), (2015, 4.9), (2016, 4.7),
                    (2017, 4.6), (2018, 4.4), (2019, 4.7), (2023, 4.0)])


def zimbabwe():
    """ZIMSTAT census and health-survey rounds.

    The 2022 census's fertility report charts the census series back to 1969, and the 2015 health
    survey carries its own rounds. Both instruments ask women directly; neither uses registration.
    """
    return _series([(2002, 3.6), (2005, 3.8), (2010, 4.1), (2012, 3.8), (2015, 4.0), (2022, 3.7)])


# the 2022 census's own table 7.7(a): women enumerated and births in the previous twelve months
ZIMBABWE_2022 = {
    (15, 19): (791914, 68753),
    (20, 24): (676121, 121125),
    (25, 29): (559313, 95989),
    (30, 34): (510887, 75054),
    (35, 39): (533369, 56772),
    (40, 44): (410155, 18597),
    (45, 49): (332942, 2486),
}


def zimbabwe_detail(year):
    if year != 2022:
        return None
    return {b: {"women": float(w), "births": float(x)} for b, (w, x) in ZIMBABWE_2022.items()}


def benin():
    """INStaD's health-survey rounds.

    The 2017-18 report's trend table carries the earlier ones. The census figures are not in this
    series — see the note in countries.py.
    """
    return _series([(2001, 5.6), (2006, 5.7), (2012, 4.9), (2018, 5.7)])


def cambodia():
    """NIS health-survey rounds. The 2021-22 report charts every round since 2000.

    These are the direct birth-history estimates. The census and inter-censal-survey figures are
    all indirect estimates and are not in this series — see the note in countries.py.
    """
    return _series([(2000, 3.8), (2005, 3.4), (2010, 3.0), (2014, 2.7), (2022, 2.7)])


def guinea():
    """INS health-survey rounds. The 2018 report's own table carries the earlier ones.

    The 2014 census gives 5.3, after a correction — see the note in countries.py.
    """
    return _series([(2005, 5.7), (2012, 5.1), (2018, 4.8)])


# the 2014 census's annex tables A.0.2 and A.0.8: women aged 12-54 and births in the previous
# twelve months, both as counts
GUINEA_2014 = {
    (15, 19): (589555, 62085),
    (20, 24): (496263, 96498),
    (25, 29): (444157, 101210),
    (30, 34): (350747, 72323),
    (35, 39): (280112, 44261),
    (40, 44): (235595, 21347),
    (45, 49): (167746, 9266),
}


def guinea_detail(year):
    if year != 2014:
        return None
    return {b: {"women": float(w), "births": float(x)} for b, (w, x) in GUINEA_2014.items()}


def rwanda():
    """NISR health-survey rounds. The 2025 report's own table carries every round since 1992."""
    return _series([(2000, 5.8), (2005, 6.1), (2010, 4.6), (2015, 4.2), (2020, 4.1), (2025, 3.7)])


# the 2022 census's main indicators table 5 and the fertility monograph's table 4.3: women by age
# group, and the age-specific rates the census published
RWANDA_2022 = {
    (15, 19): (759178, 0.025),
    (20, 24): (602006, 0.137),
    (25, 29): (512713, 0.176),
    (30, 34): (446776, 0.165),
    (35, 39): (392140, 0.134),
    (40, 44): (321396, 0.075),
    (45, 49): (263941, 0.015),
}


def rwanda_detail(year):
    if year != 2022:
        return None
    return {b: {"women": float(w), "births": r * w} for b, (w, r) in RWANDA_2022.items()}


def burundi():
    """ISTEEBU health-survey rounds. The 2016-17 report's own table carries 1987 and 2010."""
    return _series([(2010, 6.4), (2017, 5.5)])


# the 2008 census's chapter 5, table 5.1: women aged 12-49 and births in the previous twelve months
BURUNDI_2008 = {
    (15, 19): (504648, 16271),
    (20, 24): (405830, 78373),
    (25, 29): (301310, 78868),
    (30, 34): (203614, 53649),
    (35, 39): (185379, 42124),
    (40, 44): (149678, 21364),
    (45, 49): (133780, 9398),
}


def burundi_detail(year):
    if year != 2008:
        return None
    return {b: {"women": float(w), "births": float(x)} for b, (w, x) in BURUNDI_2008.items()}


def south_sudan():
    """The bureau's own census estimate and the two national surveys.

    2008 is the bureau's adjusted estimate from the census, 6.92, published in "Levels and Patterns of
    Fertility and Mortality in South Sudan: Analysis of Census 2008" (NBS, 2013), table 4. That report
    estimates fertility with the indirect techniques of the UN's Manual X, from children ever born and
    births in the last twelve months, because the raw twelve-month answers alone give 3.9. Its national
    age-specific rates sum to 6.92 and its state figures run from 4.71 in Western Equatoria to 8.62 in
    Eastern Equatoria. The bureau's site no longer serves the PDF; it is readable in the Internet
    Archive's 2017 capture of ssnbss.org.

    2010 is the household health survey, run by the health ministry with the statistics bureau
    before independence; 2025 is the bureau's own multiple-indicator survey, published in 2026 as a
    preliminary report.
    """
    return _series([(2008, 6.92), (2010, 7.5), (2025, 6.4)])


def haiti():
    """The EMMUS survey rounds, run for the health ministry.

    The statistics institute is a collaborator on these rather than their author; its own last
    fertility figure is the 2003 census. See the note in countries.py.

    2006 is 3.9, not the 4.0 on the front page of the 2005-06 report (FR192). That report measured
    fertility over the five years before the survey; the 2012 and 2016-17 rounds measured it over
    three, which is the standard window. Both later rounds print 3.9 for 2005-06 in their own trend
    tables and in their own prose -- FR326 p.124, "l'ISF est passe de 3,9 enfants en 2006 a 3,5
    enfants en 2012, pour se situer a 3,0 enfants en 2016-2017", and the trend row of its table 5.3.2,
    "ISF 15-49  4,7  3,9  3,5  3,0". Plotting 4.0 would put a five-year window in a series of
    three-year ones, and would disagree with the way the source itself states its own trend.
    """
    return _series([(2006, 3.9), (2012, 3.5), (2017, 3.0)])


# the 2003 census's published tables 201 and 503: women by age group, and births in the previous
# twelve months
HAITI_2003 = {
    (15, 19): (511520, 19558),
    (20, 24): (438189, 59436),
    (25, 29): (363609, 59305),
    (30, 34): (279138, 43466),
    (35, 39): (256158, 31613),
    (40, 44): (216206, 13676),
    (45, 49): (178252, 4747),
}


def haiti_detail(year):
    if year != 2003:
        return None
    return {b: {"women": float(w), "births": float(x)} for b, (w, x) in HAITI_2003.items()}


def tunisia():
    """INS's registration-based fertility rate.

    The statistical yearbook gives two decimals from 2019; the earlier years come from the office's
    own indicator page and are printed to one.
    """
    return _series([(2015, 2.4), (2016, 2.4), (2017, 2.3), (2018, 2.2), (2019, 2.17),
                    (2020, 1.96), (2021, 1.82), (2022, 1.70), (2023, 1.58)])


# the 2023 yearbook's tables 1.3 and 1.7: mid-year women in thousands, and registered births by the
# mother's age group. 7,709 of the 135,148 births have no age recorded; INS spreads them across the
# bands in proportion, which is what reproduces its own published rates, so we do the same.
TUNISIA_2023 = {
    (15, 19): (406.4, 1391),
    (20, 24): (376.6, 12541),
    (25, 29): (403.4, 35521),
    (30, 34): (438.4, 42195),
    (35, 39): (471.2, 27483),
    (40, 44): (453.6, 7720),
    (45, 49): (413.2, 588),
}
TUNISIA_AGE_NOT_STATED = 7709


def tunisia_detail(year):
    if year != 2023:
        return None
    stated = sum(x for _, x in TUNISIA_2023.values())
    scale = (stated + TUNISIA_AGE_NOT_STATED) / stated
    return {b: {"women": w * 1000, "births": x * scale} for b, (w, x) in TUNISIA_2023.items()}


def bolivia():
    """INE's health-survey rounds. The 2023 report's own trend table carries all five."""
    return _series([(1998, 4.2), (2003, 3.8), (2008, 3.5), (2016, 2.9), (2023, 2.1)])


def tajikistan():
    """The statistics agency's own fertility rate, from the demographic yearbook.

    The yearbook prints the whole series from 1989 in one table. The agency flags 2002-2017 as
    preliminary or estimated; 2007's value is a sharp one-year dip that it does not explain.
    """
    values = [3.493, 3.487, 3.471, 3.420, 3.354, 3.274, 3.266, 2.349, 2.677, 2.655, 2.905, 2.766,
              2.611, 2.616, 2.980, 3.064, 2.930, 2.830, 2.884, 2.885, 2.986, 2.642, 2.799, 3.016]
    return _series(list(zip(range(2000, 2000 + len(values)), values)))


# the 2024 yearbook: age-specific rates per 1,000 women for 2023, and the women in each age group at
# the start of that year. The first band is printed as "under 20" and read against the 15-19 women.
TAJIKISTAN_2023 = {
    (15, 19): (423609, 23.82),
    (20, 24): (452594, 242.54),
    (25, 29): (402525, 166.63),
    (30, 34): (400263, 100.51),
    (35, 39): (338433, 57.77),
    (40, 44): (259325, 14.34),
    (45, 49): (217247, 0.83),
}


def tajikistan_detail(year):
    if year != 2023:
        return None
    return {b: {"women": float(w), "births": r / 1000 * w} for b, (w, r) in TAJIKISTAN_2023.items()}


def jordan():
    """DOS's Population and Family Health Survey rounds.

    Registration is complete enough that DOS publishes registered births every year, but never by
    age of mother, so the rate comes only from the survey.
    """
    return _series([(2018, 2.7), (2023, 2.6)])


def honduras():
    """INE's survey rounds. The 2011-12 report's own table carries the earlier ones."""
    return _series([(2001, 4.4), (2006, 3.3), (2012, 2.9), (2019, 2.6)])


def azerbaijan():
    """The State Statistical Committee's own rate, from its demography tables.

    Table 2.3 carries every year since 1959 as a plain spreadsheet at a stable address.
    """
    values = [2.0, 1.8, 1.84, 1.9, 2.1, 2.3, 2.3, 2.3, 2.3, 2.3, 2.3, 2.4, 2.3, 2.2, 2.2, 2.1,
              2.0, 1.9, 1.8, 1.8, 1.7, 1.5, 1.7, 1.6, 1.4]
    return _series(list(zip(range(2000, 2000 + len(values)), values)))


def iran():
    """Statistical Center of Iran, "Trend of fertility in Iran, 1396 to 1400", table 3.

    The last column, computed for the whole population — Iranian and non-Iranian residents together —
    which is the population the UN's figure also covers. The office publishes a separate series for
    Iranian nationals only: 2.09, 1.95, 1.74, 1.65, 1.65 for the same years.

    Iranian years are placed at the Gregorian year that holds most of them, so 1396 becomes 2017. The
    report is only reachable through a web archive; the office's own host refuses foreign connections.
    """
    return _series([(2017, 2.07), (2018, 1.97), (2019, 1.77), (2020, 1.71), (2021, 1.74)])


def cuba():
    """ONEI's own rate, from table 2.4 of the demographic yearbook."""
    values = [1.72, 1.63, 1.61, 1.65, 1.57, 1.52, 1.47, 1.52, 1.54, 1.29]
    return _series(list(zip(range(2015, 2015 + len(values)), values)))


# the 2024 yearbook's tables 2.7 and 1.9: registered births by the mother's age group and the mean
# female population. ONEI's own footnotes fold births under 15 into the 15-19 band and births at 50
# and over into 45-49, which is what reproduces its published rate.
CUBA_2024 = {
    (15, 19): (254152.5, 11597 + 365),
    (20, 24): (276702.0, 21672),
    (25, 29): (265828.0, 17586),
    (30, 34): (303864.0, 12361),
    (35, 39): (311973.5, 6399),
    (40, 44): (270460.5, 1318),
    (45, 49): (323388.0, 58 + 2),
}


def cuba_detail(year):
    if year != 2024:
        return None
    return {b: {"women": w, "births": float(x)} for b, (w, x) in CUBA_2024.items()}


def papua_new_guinea():
    """NSO's survey rounds.

    2006 and 2016-18 are the health surveys; 2022 is the socio-demographic survey, whose figure is
    adjusted — see the note in countries.py. Neither census ever produced a fertility rate.
    """
    return _series([(2006, 4.4), (2018, 4.2), (2022, 3.72)])


def dominican_republic():
    """ONE's household survey. The only fertility rate the office publishes from measurement."""
    return _series([(2019, 2.4)])


def sierra_leone():
    """Stats SL's health-survey rounds. The 2019 report's own table carries the earlier ones.

    The 2015 census figure is not in this series: its raw and adjusted values differ by a factor of
    three and a half — see the note in countries.py.
    """
    return _series([(2008, 5.1), (2013, 4.9), (2019, 4.2)])


def israel():
    """CBS, table 2.41 of the statistical abstract, "Fertility rates by age and religion".

    The table gives annual values only for the most recent years and five-year averages before
    that, so the series is short. Read from an archived copy: the office's own host refuses
    connections from outside the country.
    """
    return _series([(2020, 2.90), (2022, 2.89), (2023, 2.85)])


# the same table, 2023: the age-specific rates per 1,000 women for the whole population
ISRAEL_2023 = {(15, 19): 5.6, (20, 24): 89.2, (25, 29): 161.2, (30, 34): 172.8,
               (35, 39): 106.6, (40, 44): 30.8, (45, 49): 3.2}
