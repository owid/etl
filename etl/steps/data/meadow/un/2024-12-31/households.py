"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("households.xlsx")

    # Load data from snapshot, starting from the row that contains the header "Country or Area"
    tb = snap.read(sheet_name="HH size and composition 2022", skiprows=4)
    #
    # Process data.
    #
    # Create a year column from the "Reference date (dd/mm/yyyy)" column.
    tb["year"] = pd.to_datetime(tb["Reference date (dd/mm/yyyy)"], format="mixed", dayfirst=True).dt.year
    tb = tb.rename(columns={"Country or area": "country"})

    # Select the most recent data for each year and country.
    tb = tb.loc[tb.groupby(["country", "year"])["Reference date (dd/mm/yyyy)"].idxmax()]
    # Drop columns that are not needed.
    tb = tb.drop(
        columns=["Reference date (dd/mm/yyyy)", "ISO Code", "Unnamed: 43", "Unnamed: 44", "Data source category"]
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
