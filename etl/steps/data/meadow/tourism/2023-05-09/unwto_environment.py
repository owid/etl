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
    log.info("unwto_environment.start")

    # Sheet to load from Excel file
    sheet_name_to_load = "Data"

    # Load the input data from Excel file
    snap: Snapshot = paths.load_dependency("unwto_environment.xlsx")

    excel_object = shrd.load_data(snap, sheet_name_to_load)

    # Read the required sheet from the Excel file
    df = pd.read_excel(excel_object, sheet_name=sheet_name_to_load)

    # Subset dataframe to include only necessary columns
    df = df[["SeriesDescription", "GeoAreaName", "TimePeriod", "Value", "Time_Detail", "Source"]]

    # If 'Time_Detail' is equal to 'TimePeriod', drop 'TimePeriod'
    if df["Time_Detail"].equals(df["TimePeriod"]):
        df.drop("TimePeriod", axis=1, inplace=True)

    # Rename columns
    df.columns = ["implementation_type", "country", "value", "year", "source"]

    # Set new index and ensure uniqueness
    df.set_index(["country", "year", "implementation_type"], inplace=True)
    assert df.index.is_unique, "Index is not unique'."

    # Reset index for further processing
    df.reset_index(inplace=True)

    # Pivot the dataframe to reshape data
    df = pd.pivot_table(df, values="value", index=["country", "year"], columns=["implementation_type"])

    # Reset index again
    df.reset_index(inplace=True)

    # Create a new table with the processed dataframe
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Create a new dataset for saving the outputs, with the same metadata as the original snapshot
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new dataset
    ds_meadow.save()

    # Log the end of the process
    log.info("unwto_environment.end")
