"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns for wages table and new names
COLUMNS_WAGES = {
    "Unnamed: 0": "year",
    "£": "average_weekly_earnings",
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
        usecols="A:B",
    )

    # Rename columns
    tb_wages = tb_wages.rename(columns=COLUMNS_WAGES, errors="raise")

    # Add country column
    tb_wages["country"] = "United Kingdom"

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb_wages.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
