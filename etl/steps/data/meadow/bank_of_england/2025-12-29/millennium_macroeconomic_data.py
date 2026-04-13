"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns for wages table and new names
COLUMNS_WAGES = {
    "Unnamed: 0": "year",
    "£": "average_weekly_earnings",
    "Spliced index 2015=100": "cpi",
}

# Define columns for earnings table and new names
COLUMNS_EARNINGS = {
    "Unnamed: 0": "year",
    "Unnamed: 1": "real_consumption_earnings",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("millennium_macroeconomic_data.xlsx")

    # Load data from snapshot.
    tb_wages = snap.read(
        sheet_name="A47. Wages and prices",
        skiprows=5,
        usecols="A:B,D",
    )

    tb_earnings = snap.read(
        sheet_name="A48. Real Earnings ",
        skiprows=4,
        usecols="A:B",
    )

    #
    # Process data.
    #
    # Rename columns
    tb_wages = tb_wages.rename(columns=COLUMNS_WAGES, errors="raise")
    tb_earnings = tb_earnings.rename(columns=COLUMNS_EARNINGS, errors="raise")

    # Merge tables
    tb = pr.merge(
        tb_wages,
        tb_earnings,
        on="year",
        how="outer",
    )

    # Add country column
    tb["country"] = "United Kingdom"

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
