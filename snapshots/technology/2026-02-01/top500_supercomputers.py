"""Script to create a snapshot of all TOP500 Supercomputer lists from 1993 to 2025."""

from io import BytesIO
from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize logger
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def download_and_read_top500(year: int, month: str) -> pd.DataFrame:
    """Download and read a TOP500 list for a given year and month."""
    # Determine file extension (xls for older files, xlsx for newer)
    ext = "xls" if year < 2020 else "xlsx"

    # Construct URL
    url = f"https://www.top500.org/lists/top500/{year}/{month}/download/TOP500_{year}{month}.{ext}"

    try:
        # Download file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Read Excel file - older files (pre-2008) have many metadata rows
        # Read with header=None first to find the actual header row
        df_raw = pd.read_excel(BytesIO(response.content), header=None)

        # Find the row that contains 'Rank' as the first non-null value
        header_row = 0
        for idx in range(min(30, len(df_raw))):  # Check first 30 rows
            row_values = df_raw.iloc[idx].astype(str).str.strip()
            # Look for 'Rank' in first few columns
            if any("Rank" in str(val) for val in row_values.iloc[:5].values):
                header_row = idx
                break

        # Re-read with correct header
        df = pd.read_excel(BytesIO(response.content), header=header_row)

        # Remove completely empty rows
        df = df.dropna(how="all")

        # Filter out rows where 'Rank' column has non-numeric values (metadata rows after header)
        if "Rank" in df.columns:
            # Keep only rows where Rank can be converted to numeric
            df = df[pd.to_numeric(df["Rank"], errors="coerce").notna()]

        # Handle duplicate columns by renaming them
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            dup_indices = [i for i, x in enumerate(cols) if x == dup]
            for i, idx in enumerate(dup_indices[1:], 1):
                cols.iloc[idx] = f"{dup}_{i}"
        df.columns = cols

        # Convert GFlops to TFlops for older files
        # Check for Rmax and Rpeak columns in GFlops (older files used GFlops instead of TFlops)
        for col in df.columns:
            col_str = str(col).strip()
            # Check if column contains performance metric in GFlops
            if ("Rmax" in col_str or "RMax" in col_str) and "TFlop" in col_str:
                # Convert from GFlops to TFlops (divide by 1000)
                df[col] = pd.to_numeric(df[col], errors="coerce") * 1000
                # Rename column to indicate TFlops
                df = df.rename(columns={col: "RMax"})
                log.info("converted_gflops_to_tflops", column=col, new_name="RMax")

        # Add year and month columns
        df["list_year"] = year
        df["list_month"] = month
        if "Rmax" in df.columns:
            df = df.rename(columns={"Rmax": "RMax"})

        return df

    except Exception as e:
        log.error("download_failed", year=year, month=month, error=str(e))
        return None


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Years and months to download
    start_year = 1993
    end_year = 2025
    months = ["06", "11"]  # June and November

    all_data = []

    for year in range(start_year, end_year + 1):
        for month in months:
            log.info("downloading_top500", year=year, month=month)
            df = download_and_read_top500(year, month)
            if df is not None:
                all_data.append(df)
                log.info("download_success", year=year, month=month, systems=len(df))

    if not all_data:
        raise ValueError("No data was downloaded successfully")

    # Concatenate all data
    combined_df = pd.concat(all_data, ignore_index=True)
    log.info("concatenation_complete", total_systems=len(combined_df))

    # Create a new snapshot.
    snap = Snapshot(f"technology/{SNAPSHOT_VERSION}/top500_supercomputers.csv")

    # Save to a temporary CSV file
    temp_path = snap.path
    combined_df.to_csv(temp_path, index=False)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)

    log.info("snapshot_created")


if __name__ == "__main__":
    main()
