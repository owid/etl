"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create meadow dataset."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("datacenter_construction.xlsx")

    # Load data from snapshot - read the "Private SA" sheet with header on row 4 (0-indexed as 3)
    tb = snap.read(sheet_name="Private SA", header=3)

    #
    # Process data.
    #
    # Clean column names - remove line breaks and extra whitespace
    tb.columns = tb.columns.str.replace("\n_x000D_", " ").str.strip()

    # Keep only Date and Data center columns
    tb = tb[["Date", "Data center"]].copy()

    # Rename columns
    tb = tb.rename(columns={"Data center": "datacenter_construction_spending"})

    # Remove rows where datacenter_construction_spending is NaN first
    tb = tb.dropna(subset=["datacenter_construction_spending"])

    # Parse the date column
    # Format is like "Jul-25p" (July 2025 preliminary) or "Jun-25r" (June 2025 revised)
    # Remove suffixes like 'p' (preliminary) and 'r' (revised)
    tb["date_str"] = tb["Date"].astype(str).str.replace(r"[pr]$", "", regex=True)

    # Filter to only keep rows with valid date format (MMM-YY)
    # This will remove footer rows automatically
    date_pattern = r"^[A-Z][a-z]{2}-\d{2}$"
    tb = tb[tb["date_str"].str.match(date_pattern, na=False)]

    # Convert to datetime - format is "MMM-YY"
    tb["date"] = pd.to_datetime(tb["date_str"], format="%b-%y")

    # Drop temporary columns
    tb = tb.drop(columns=["Date", "date_str"])
    tb = tb.format(["date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    # Save changes in the new meadow dataset.
    ds_meadow.save()
