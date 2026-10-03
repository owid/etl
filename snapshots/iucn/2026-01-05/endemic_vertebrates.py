"""Script to create a snapshot of dataset.

The data for this snapshot was taken from the IUCN Red List Summary Statistics Table 8a, available at: https://www.iucnredlist.org/resources/summary-statistics
"""

import io
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
    snap = Snapshot(f"iucn/{SNAPSHOT_VERSION}/endemic_vertebrates.csv")
    # Extract Table 8a from PDF
    df = extract_table8a(snap.metadata.origin.url_main)
    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


def _make_headers(group_row, sub_row):
    headers = []
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


def _row_has_number(row):
    return any(any(ch.isdigit() for ch in str(x)) for x in row)


def _clean_table(df):
    df = df.copy().fillna("")
    header_row_idx = None
    for i in range(min(6, len(df))):
        row = df.iloc[i].astype(str).tolist()
        if any("Mammals" in x for x in row) and any("Birds" in x for x in row):
            header_row_idx = i
            break
    if header_row_idx is None:
        return pd.DataFrame()

    group_row = df.iloc[header_row_idx].tolist()
    sub_row = df.iloc[header_row_idx + 1].tolist()
    headers = _make_headers(group_row, sub_row)
    headers[0] = "region_or_country"

    df.columns = headers
    df = df.iloc[header_row_idx + 2 :].reset_index(drop=True)

    # Drop empty rows and section titles
    df = df[df["region_or_country"].str.strip() != ""]
    num_cols = [c for c in df.columns if c != "region_or_country"]
    df = df[df[num_cols].apply(_row_has_number, axis=1)]

    for c in num_cols:
        df[c] = pd.to_numeric(df[c].replace({"": None}), errors="coerce").astype("Int64")

    return df


def extract_table8a(source: str | Path) -> pd.DataFrame:
    """
    Extract Table 8a (wide format) from either:
      - a local PDF path
      - a URL to the PDF
    """
    if str(source).startswith("http"):
        # Fetch the PDF from the URL
        resp = requests.get(source)
        resp.raise_for_status()
        filelike = io.BytesIO(resp.content)
    else:
        filelike = Path(source)

    tables = []
    with pdfplumber.open(filelike) as pdf:
        for page in pdf.pages:
            # Try line-based extraction
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
                tables.append(pd.DataFrame(t))
            # Also try multi-table extraction
            for tt in page.extract_tables():
                if tt and len(tt[0]) >= 10:
                    tables.append(pd.DataFrame(tt))

    cleaned = []
    for t in tables:
        c = _clean_table(t)
        if not c.empty:
            cleaned.append(c)

    if not cleaned:
        raise ValueError("No tables matched the expected Table 8a structure.")

    return pd.concat(cleaned, ignore_index=True)


if __name__ == "__main__":
    main()
