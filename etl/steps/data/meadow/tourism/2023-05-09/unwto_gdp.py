"""Load a snapshot and create a meadow dataset."""

import pandas as pd
import shared as shrd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unwto_gdp.start")

    # Specify the sheet name to load from Excel file
    sheet_name_to_load = "Data"

    # Load the input data (Excel file) as a Snapshot object
    snap: Snapshot = paths.load_dependency("unwto_gdp.xlsx")

    # Load the data from the Excel object into memory
    excel_object = shrd.load_data(snap, sheet_name_to_load)

    # Load the desired sheet from the Excel file into a pandas DataFrame
    df = pd.read_excel(excel_object, sheet_name=sheet_name_to_load)

    # Select only the desired columns from the DataFrame
    df = df[["GeoAreaName", "TimePeriod", "Total", "Time_Detail", "Source"]]

    # If 'Time_Detail' is equivalent to 'TimePeriod', drop 'TimePeriod'
    if df["Time_Detail"].equals(df["TimePeriod"]):
        df.drop("TimePeriod", axis=1, inplace=True)

    # Rename the columns to the desired names
    df.columns = ["country", "gdp", "year", "source"]

    # Set a multi-index on the DataFrame using 'country' and 'year' columns
    df.set_index(["country", "year"], inplace=True)

    # Check for the uniqueness of the index
    assert df.index.is_unique, "Index is not unique'."

    # Reset the index back to a RangeIndex
    df.reset_index(inplace=True)

    # Create a new table with the processed DataFrame, ensuring column names are in snake-case
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Create a new dataset with the table, using the same metadata as the original snapshot
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save the changes in the new dataset
    ds_meadow.save()

    # Log the end of the process
    log.info("unwto_gdp.end")
