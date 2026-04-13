"""Extract statistical annex tables (pages 54–58) and chart data (page 9) from the
ILO-UNICEF 2024 Global Estimates of Child Labour report and upload as three snapshots:

  - child_labor_by_region.csv   (pages 54–55): 2024 child labour by region × sex × age
  - hazardous_work_by_region.csv (pages 56–57): 2024 hazardous work by region × sex × age
  - child_labor_trends.csv      (page 58 + page 9): trends 2000–2024 for both indicators

The first two tables are split horizontally across two PDF pages each:
  - Left page: region_type, region, and Total columns (4 age groups × % + No.)
  - Right page: Boys and Girls columns (8 each, identical row order → positional join)

The trends table from the annex (page 58) only covers 2016–2024. Page 9 has chart data
extending back to 2000 (global) and 2008 (regional, by age). These are merged in.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pdfplumber
import requests

from etl.snapshot import Snapshot

VERSION = Path(__file__).parent.name
PDF_URL = "https://www.ilo.org/sites/default/files/2025-06/2024%20Global%20Estimates%20of%20Child%20Labour%20Report.pdf"

# ── Column schemas ─────────────────────────────────────────────────────────────

# Left page (Total): 11 cols including a trailing empty col
_LEFT_COLS = [
    "region_type",
    "region",
    "total_5_11_pct",
    "total_5_11_no",
    "total_12_14_pct",
    "total_12_14_no",
    "total_15_17_pct",
    "total_15_17_no",
    "total_5_17_pct",
    "total_5_17_no",
    "_drop",
]

# Right page (Boys + Girls): 17 cols including a leading empty col
_RIGHT_COLS = [
    "_key",
    "boys_5_11_pct",
    "boys_5_11_no",
    "boys_12_14_pct",
    "boys_12_14_no",
    "boys_15_17_pct",
    "boys_15_17_no",
    "boys_5_17_pct",
    "boys_5_17_no",
    "girls_5_11_pct",
    "girls_5_11_no",
    "girls_12_14_pct",
    "girls_12_14_no",
    "girls_15_17_pct",
    "girls_15_17_no",
    "girls_5_17_pct",
    "girls_5_17_no",
]


# ── Historical chart data (page 9) ─────────────────────────────────────────────
# Values read from chart labels on page 9. Numbers are in millions; shares in %.
# Confirmed via spatial analysis of PDF word positions (pdfplumber extract_words).
_CHART_DATA = {
    # (disaggregation_type, disaggregation_value): {year: (cl_pct, cl_no_millions, hw_pct, hw_no_millions)}
    ("World total", None): {
        2000: (16.0, 245.5, 11.1, 170.5),
        2004: (14.2, 222.3,  8.2, 128.4),
        2008: (13.6, 215.2,  7.3, 115.3),
        2012: (10.6, 168.0,  5.4,  85.3),
    },
    # ILO regions — child labour only (no hazardous work in chart)
    ("ILO regions", "Sub-Saharan Africa"):              {2008: (25.3, 65.1, None, None), 2012: (21.4, 59.0, None, None)},
    ("ILO regions", "Asia and the Pacific"):            {2008: (13.3, 113.6, None, None), 2012: (9.3, 77.7, None, None)},
    ("ILO regions", "Latin America and the Caribbean"): {2008: (10.0, 14.1, None, None), 2012: (8.8, 12.5, None, None)},
    # By age — child labour only
    ("Age", "5-11 years"):  {2008: (10.7, 91.0, None, None), 2012: (8.5, 73.0, None, None)},
    ("Age", "12-14 years"): {2008: (17.0, 61.8, None, None), 2012: (13.1, 47.5, None, None)},
    ("Age", "15-17 years"): {2008: (16.9, 62.4, None, None), 2012: (13.0, 47.5, None, None)},
}

# ── Page 8 chart data ─────────────────────────────────────────────────────────
# Not-in-school shares (page 8, World 2024, sex=total).
# Can't derive the 5-14 share from 5-11/12-14 sub-age shares without total population data.
_NOT_IN_SCHOOL_CHART = {
    ("Not in school", "5-14 years"): {2024: (31.0, None, None, None)},
    ("Not in school", "15-17 years"): {2024: (59.0, None, None, None)},
}

# Child labour including household chores (page 8, World 2024, ages 5-14 by sex).
# Household chores are defined as ≥21 hours per week of unpaid household services.
_HOUSEHOLD_CHORES_CHART = {
    ("Including household chores", "Girls 5-11"): {2024: (11.9, None, None, None)},
    ("Including household chores", "Boys 5-11"): {2024: (11.0, None, None, None)},
    ("Including household chores", "Girls 12-14"): {2024: (11.6, None, None, None)},
    ("Including household chores", "Boys 12-14"): {2024: (11.2, None, None, None)},
    ("Including household chores", "Girls 5-14"): {2024: (11.8, None, None, None)},
    ("Including household chores", "Boys 5-14"): {2024: (11.1, None, None, None)},
}


# ── Extraction helpers ─────────────────────────────────────────────────────────


def _extract_two_page_table(pdf: pdfplumber.PDF, left_page_idx: int) -> pd.DataFrame:
    """Join two horizontally split pages into a single DataFrame."""
    left_rows = pdf.pages[left_page_idx].extract_table()
    right_rows = pdf.pages[left_page_idx + 1].extract_table()

    # Strip 3 header rows from both halves
    left_df = pd.DataFrame(left_rows[3:], columns=_LEFT_COLS).drop(columns=["_drop"])
    right_df = pd.DataFrame(right_rows[3:], columns=_RIGHT_COLS).drop(columns=["_key"])

    df = pd.concat([left_df, right_df], axis=1)

    # Forward-fill region_type across continuation rows
    df["region_type"] = df["region_type"].replace("", None).ffill()

    return _clean(df)


def _extract_trends_table(pdf: pdfplumber.PDF) -> pd.DataFrame:
    """Extract the trends table from page 58 (0-indexed: 57)."""
    rows = pdf.pages[57].extract_table()

    # First 3 rows are headers; row 0 cols 0–1 are empty, rest are year group labels
    cols = [
        "disaggregation_type",
        "disaggregation_value",
        "child_labor_2016_pct",
        "child_labor_2016_no",
        "child_labor_2020_pct",
        "child_labor_2020_no",
        "child_labor_2024_pct",
        "child_labor_2024_no",
        "hazardous_work_2016_pct",
        "hazardous_work_2016_no",
        "hazardous_work_2020_pct",
        "hazardous_work_2020_no",
        "hazardous_work_2024_pct",
        "hazardous_work_2024_no",
    ]
    df = pd.DataFrame(rows[3:], columns=cols)

    # Forward-fill disaggregation_type
    df["disaggregation_type"] = df["disaggregation_type"].replace("", None).ffill()

    df = _clean(df)

    # Merge in chart data from pages 8 and 9.
    all_chart_data = {**_CHART_DATA, **_NOT_IN_SCHOOL_CHART, **_HOUSEHOLD_CHORES_CHART}
    df = _merge_chart_data(df, all_chart_data)

    return df


def _merge_chart_data(df: pd.DataFrame, chart_data: dict) -> pd.DataFrame:
    """Merge chart data into the trends table, updating existing rows or adding new ones."""
    for (dtype, dvalue), year_data in chart_data.items():
        # Find the matching row in the annex table.
        mask = df["disaggregation_type"] == dtype
        if dvalue is not None:
            mask = mask & (df["disaggregation_value"] == dvalue)
        else:
            mask = mask & (df["disaggregation_value"].isna())

        # If no matching row exists, create one.
        if mask.sum() == 0:
            new_row = {"disaggregation_type": dtype, "disaggregation_value": dvalue}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            mask = df.index == len(df) - 1

        for year, (cl_pct, cl_no, hw_pct, hw_no) in year_data.items():
            # Numbers in chart are millions; annex uses thousands.
            cl_no_thousands = cl_no * 1000 if cl_no is not None else None
            hw_no_thousands = hw_no * 1000 if hw_no is not None else None

            df.loc[mask, f"child_labor_{year}_pct"] = cl_pct
            df.loc[mask, f"child_labor_{year}_no"] = cl_no_thousands
            df.loc[mask, f"hazardous_work_{year}_pct"] = hw_pct
            df.loc[mask, f"hazardous_work_{year}_no"] = hw_no_thousands

    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty strings with None and normalize newlines in string columns."""
    df = df.replace("", None)
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"\n", " ", regex=True) if col.dtype == "object" else col
    )
    return df


# ── Main entry point ───────────────────────────────────────────────────────────


def run(upload: bool = True) -> None:
    # Download PDF once
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(requests.get(PDF_URL, timeout=60).content)
        pdf_path = f.name

    try:
        with pdfplumber.open(pdf_path) as pdf:
            child_labor_df = _extract_two_page_table(pdf, left_page_idx=53)
            hazardous_work_df = _extract_two_page_table(pdf, left_page_idx=55)
            trends_df = _extract_trends_table(pdf)
    finally:
        Path(pdf_path).unlink(missing_ok=True)

    _upload_snapshot(f"un/{VERSION}/child_labor_by_region.csv", child_labor_df, upload)
    _upload_snapshot(f"un/{VERSION}/hazardous_work_by_region.csv", hazardous_work_df, upload)
    _upload_snapshot(f"un/{VERSION}/child_labor_trends.csv", trends_df, upload)


def _upload_snapshot(snap_path: str, df: pd.DataFrame, upload: bool) -> None:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        df.to_csv(f, index=False)
        csv_path = f.name
    try:
        snap = Snapshot(snap_path)
        snap.create_snapshot(filename=csv_path, upload=upload)
    finally:
        Path(csv_path).unlink(missing_ok=True)
