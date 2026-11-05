"""Script to create a snapshot of dataset.

The data for this snapshot was taken from the IUCN Red List Summary Statistics Table 8b, available at: https://www.iucnredlist.org/resources/summary-statistics
"""

from __future__ import annotations

import io
from collections.abc import Iterable
from pathlib import Path

import click
import pandas as pd
import pdfplumber
import requests

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = Snapshot(f"iucn/{SNAPSHOT_VERSION}/endemic_fish.csv")
    # Extract Table 8b from PDF
    df = extract_table8b(snap.metadata.origin.url_main)
    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


def _make_headers(group_row: Iterable, sub_row: Iterable) -> list[str]:
    """
    Build flattened headers such as:
      'FW Fishes - Total endemics', 'FW Fishes - Threatened endemics', ...
      'Groupers - Total endemics',  ...  'Sharks & Rays - EX & EW endemics'
    """
    headers: list[str] = []
    current_group = None
    for i, (g, s) in enumerate(zip(group_row, sub_row)):
        if i == 0:
            headers.append("region_or_country")
            continue
        g = (g or "").strip()
        s = (s or "").strip().replace("\n", " ")
        if g:
            current_group = g
        headers.append(f"{current_group} - {s}".strip())
    return headers


def _row_has_number(row: pd.Series) -> bool:
    """Filter out section titles like 'AFRICA', 'Europe', etc."""
    return any(any(ch.isdigit() for ch in str(x)) for x in row)


def _find_header_row(df: pd.DataFrame) -> int | None:
    """
    For 8b, the header row usually contains 'FW Fishes' and at least one of the other groups.
    """
    for i in range(min(8, len(df))):
        row = [str(x) for x in df.iloc[i].tolist()]
        if ("FW Fishes" in row) and any(
            lbl in row for lbl in ["Groupers", "Herrings, Anchovies, etc.", "Sharks & Rays"]
        ):
            return i
    return None


def _clean_table(df_raw: pd.DataFrame, prev_headers: list[str] | None) -> tuple[pd.DataFrame, list[str]]:
    """Turn one raw pdfplumber table into a typed, wide DataFrame for 8b.

    Some pages start straight with data rows, without repeating the column header. Those pages reuse the header of
    the previous page, provided they have the same number of columns.

    Returns the cleaned table and the headers used, so the next page can reuse them.
    """
    df = df_raw.copy().fillna("")

    header_row_idx = _find_header_row(df)
    if header_row_idx is not None and header_row_idx + 1 < len(df):
        group_row = df.iloc[header_row_idx].tolist()
        sub_row = df.iloc[header_row_idx + 1].tolist()
        headers = _make_headers(group_row, sub_row)
        headers[0] = "region_or_country"
        first_data_row = header_row_idx + 2
    elif prev_headers is not None and len(prev_headers) == df.shape[1]:
        headers = prev_headers
        first_data_row = 0
    else:
        raise ValueError(
            f"Table with {df.shape[1]} columns has no header row and no previous header to reuse. First row: {df.iloc[0].tolist()}"
        )

    df.columns = headers
    df = df.iloc[first_data_row:].reset_index(drop=True)

    # Drop empties and non-data section headers
    df = df[df["region_or_country"].str.strip() != ""]
    num_cols = [c for c in df.columns if c != "region_or_country"]
    df = df[df[num_cols].apply(_row_has_number, axis=1)]

    # Convert numeric columns to nullable integers. Large counts carry a thousands separator (e.g. Brazil's "1,991"),
    # and any other text in a numeric cell should fail loudly rather than turn into NaN.
    for c in num_cols:
        df[c] = pd.to_numeric(df[c].str.replace(",", "").replace({"": None})).astype("Int64")

    return df, headers


def _extract_raw_tables(page) -> list[pd.DataFrame]:
    """
    Extract raw tables from one page using two strategies:
      (1) line-based single table
      (2) multi-table fallback
    """
    out: list[pd.DataFrame] = []
    # Strategy 1: line-based
    t = page.extract_table(
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
        }
    )
    if t and len(t[0]) >= 10:
        out.append(pd.DataFrame(t))

    # Strategy 2: multi-table
    for tt in page.extract_tables():
        if tt and len(tt[0]) >= 10:
            out.append(pd.DataFrame(tt))
    return out


# ----------------- public API -----------------


def extract_table8b(source: str | Path) -> pd.DataFrame:
    """
    Extract Table 8b (fishes) to a single wide DataFrame.

    Parameters
    ----------
    source : str | Path
        Local path to the PDF or a URL (http/https).

    Returns
    -------
    pandas.DataFrame
        Columns:
          - region_or_country
          - For each group in Table 8b:
              'FW Fishes - {Total|Threatened|EX & EW} endemics'
              'Groupers - ...'
              'Herrings, Anchovies, etc. - ...'
              'Seahorses & Pipefishes - ...'
              'Sturgeons - ...'
              'Wrasses & Parrotfishes - ...'
              'Sharks & Rays - ...'
    """
    if str(source).startswith(("http://", "https://")):
        resp = requests.get(source)
        resp.raise_for_status()
        filelike = io.BytesIO(resp.content)
    else:
        filelike = Path(source)

    cleaned = []
    headers = None
    with pdfplumber.open(filelike) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            page_tables = []
            for raw in _extract_raw_tables(page):
                tb, headers = _clean_table(raw, prev_headers=headers)
                page_tables.append(tb)
            # Every page of Table 8b holds country rows, so a page without any means the parser missed it.
            assert any(not tb.empty for tb in page_tables), f"Page {page_number} of Table 8b yielded no data rows."
            cleaned.extend(page_tables)

    # Concatenate pages/segments
    wide = pd.concat(cleaned, ignore_index=True)

    # The two extraction strategies often return the same table, so drop exact duplicate rows.
    wide = wide.drop_duplicates().reset_index(drop=True)
    assert wide["region_or_country"].is_unique, "Some countries have conflicting rows in Table 8b."
    return wide


if __name__ == "__main__":
    main()
