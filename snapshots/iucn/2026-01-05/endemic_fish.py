"""Script to create a snapshot of dataset.

The data for this snapshot was taken from the IUCN Red List Summary Statistics Table 8b, available at: https://www.iucnredlist.org/resources/summary-statistics
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Iterable, List

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


def _make_headers(group_row: Iterable, sub_row: Iterable) -> List[str]:
    """
    Build flattened headers such as:
      'FW Fishes - Total endemics', 'FW Fishes - Threatened endemics', ...
      'Groupers - Total endemics',  ...  'Sharks & Rays - EX & EW endemics'
    """
    headers: List[str] = []
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


def _clean_table(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Turn one raw pdfplumber table into a typed, wide DataFrame for 8b."""
    df = df_raw.copy().fillna("")

    header_row_idx = _find_header_row(df)
    if header_row_idx is None or header_row_idx + 1 >= len(df):
        return pd.DataFrame()

    group_row = df.iloc[header_row_idx].tolist()
    sub_row = df.iloc[header_row_idx + 1].tolist()
    headers = _make_headers(group_row, sub_row)
    headers[0] = "region_or_country"

    df.columns = headers
    df = df.iloc[header_row_idx + 2 :].reset_index(drop=True)

    # Drop empties and non-data section headers
    df = df[df["region_or_country"].str.strip() != ""]
    num_cols = [c for c in df.columns if c != "region_or_country"]
    df = df[df[num_cols].apply(_row_has_number, axis=1)]

    # Coerce numeric columns to nullable integers
    for c in num_cols:
        df[c] = pd.to_numeric(df[c].replace({"": None}), errors="coerce").astype("Int64")

    return df


def _extract_raw_tables(filelike) -> list[pd.DataFrame]:
    """
    Extract raw tables from every page using two strategies:
      (1) line-based single table
      (2) multi-table fallback
    """
    out: list[pd.DataFrame] = []
    with pdfplumber.open(filelike) as pdf:
        for page in pdf.pages:
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

    raw_tables = _extract_raw_tables(filelike)

    cleaned = [c for c in (_clean_table(t) for t in raw_tables) if not c.empty]
    if not cleaned:
        raise ValueError("No tables matched the expected Table 8b structure.")

    # Concatenate pages/segments
    wide = pd.concat(cleaned, ignore_index=True)

    # Optional: drop exact duplicate rows that sometimes occur at page joins
    wide = wide.drop_duplicates().reset_index(drop=True)
    return wide


if __name__ == "__main__":
    main()
