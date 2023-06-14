"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def combine_data_sheets(data: pd.ExcelFile) -> pd.DataFrame:
    # Initialize empty dataframe that will gather data for all sheets.
    combined = pd.DataFrame()
    for sheet_name in sorted(data.sheet_names):
        # Parse sheet for the current year.
        df = data.parse(sheet_name, skiprows=2, header=[0, 1, 2, 3])  # type: ignore

        # Combine multiline header.
        df.columns = ["country"] + [column[0] for column in df.columns[1:]]

        # Drop empty rows, and footer rows.
        df = df.dropna(subset=[column for column in df.columns if column != "country"], how="all")

        # Add a year column
        df = df.assign(**{"year": int(sheet_name)})

        # Set an appropriate index and sort conveniently.
        df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

        # Add current data to the combined dataframe.
        combined = pd.concat([combined, df], ignore_index=False)

    return combined


def run(dest_dir: str) -> None:
    log.info("food_expenditure_in_us.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = cast(Snapshot, paths.load_dependency("food_expenditure.xlsx"))

    # Read excel file with multiple sheets.
    data = pd.ExcelFile(snap.path)

    #
    # Process data.
    #
    df = combine_data_sheets(data=data)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("food_expenditure_in_us.end")
