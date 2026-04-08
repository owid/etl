"""Extract statistical annex tables (pages 54–58) from the ILO-UNICEF 2024 Global
Estimates of Child Labour report and upload them as three separate snapshots:

  - child_labor_by_region.csv   (pages 54–55): 2024 child labour by region × sex × age
  - hazardous_work_by_region.csv (pages 56–57): 2024 hazardous work by region × sex × age
  - child_labor_trends.csv      (page 58):     trends 2016/2020/2024 for both indicators

The first two tables are split horizontally across two PDF pages each:
  - Left page: region_type, region, and Total columns (4 age groups × % + No.)
  - Right page: Boys and Girls columns (8 each, identical row order → positional join)
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

    return _clean(df)


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
