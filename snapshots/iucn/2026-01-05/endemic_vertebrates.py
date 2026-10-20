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


def _clean_table(df, prev_headers):
    """Turn one raw pdfplumber table into a typed, wide DataFrame for 8a.

    A page that starts straight with data rows, without repeating the column header, reuses the header of the
    previous page, provided it has the same number of columns. Returns the cleaned table and the headers used.
    """
    df = df.copy().fillna("")
    header_row_idx = None
    for i in range(min(6, len(df))):
        row = df.iloc[i].astype(str).tolist()
        if any("Mammals" in x for x in row) and any("Birds" in x for x in row):
            header_row_idx = i
            break
    if header_row_idx is not None:
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

    # Drop empty rows and section titles
    df = df[df["region_or_country"].str.strip() != ""]
    num_cols = [c for c in df.columns if c != "region_or_country"]
    df = df[df[num_cols].apply(_row_has_number, axis=1)]

    for c in num_cols:
        df[c] = pd.to_numeric(df[c].replace({"": None}), errors="coerce").astype("Int64")

    return df, headers


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

    cleaned = []
    headers = None
    with pdfplumber.open(filelike) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            tables = []
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

            page_tables = []
            for raw in tables:
                c, headers = _clean_table(raw, prev_headers=headers)
                page_tables.append(c)
            # Every page of Table 8a holds country rows, so a page without any means the parser missed it.
            assert any(not c.empty for c in page_tables), f"Page {page_number} of Table 8a yielded no data rows."
            cleaned.extend(page_tables)

    return pd.concat(cleaned, ignore_index=True)


if __name__ == "__main__":
    main()
