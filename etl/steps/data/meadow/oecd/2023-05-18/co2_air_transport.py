"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("co2_air_transport.start")

    # Load inputs.
    # Load the snapshot
    snap: Snapshot = paths.load_dependency("co2_air_transport.csv")

    # Load data from the snapshot into a pandas DataFrame
    df = pd.read_csv(snap.path, low_memory=False)

    rename_cols = {
        "Country": "country",
        "FLIGHT": "flight_type",
        "Frequency": "frequency",
        "SOURCE": "emission_source",
        "TIME": "year",
        "Value": "value",
    }
    df = df.rename(columns=rename_cols)[rename_cols.values()]

    # Convert the 'year' column to datetime
    df["year"] = pd.to_datetime(df["year"], format="mixed")

    # Extract the month and year from 'year' column and create new columns
    df["Month"] = df["year"].dt.month
    df["Year"] = df["year"].dt.year

    # Drop the original 'year' column
    df.drop(["year"], axis=1, inplace=True)

    # Process data.
    # Create a new table and ensure all column names are in snake-case format
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Save outputs.
    # Create a new dataset with the same metadata as the snapshot
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new dataset
    ds_meadow.save()

    # End logging
    log.info("co2_air_transport.end")
