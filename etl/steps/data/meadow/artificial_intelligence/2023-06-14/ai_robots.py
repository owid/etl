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


def run(dest_dir: str) -> None:
    log.info("ai_robots.start")

    #
    # Load inputs.
    #
    # Retrieve the snapshot file.
    snap = cast(Snapshot, paths.load_dependency("ai_robots.csv"))

    # Read the CSV file into a DataFrame
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Merge columns that have country values into one 'country' column
    df["country"] = df.apply(
        lambda row: row["Installed Countries - Country"]
        if pd.notnull(row["Installed Countries - Country"])
        else row["Installed Countries - Label"]
        if pd.notnull(row["Installed Countries - Label"])
        else row["New Robots Installed - Label"]
        if pd.notnull(row["New Robots Installed - Label"])
        else None,
        axis=1,
    )

    # Drop the individual columns that used to have countries
    df = df.drop(
        columns=["Installed Countries - Country", "Installed Countries - Label", "New Robots Installed - Label"]
    )

    # Merge columns that are supposed to be year into 'year' column
    df["Year"] = (
        df["Installed Sectors - Label"].combine_first(df["Installed Application - Label"]).combine_first(df["Year"])
    )

    # Drop the individual columns that used to have year values
    df = df.drop(columns=["Installed Sectors - Label", "Installed Application - Label"])

    # Fill missing values in 'Year' column with 2021 (corresponds to the year in the dataset)
    df["Year"].fillna(2021, inplace=True)

    df["country"].fillna("World", inplace=True)

    # Convert 'Year' column to integer
    df["Year"] = df["Year"].astype(int)
    df["country"] = df["country"].astype(str)

    # Create a new table with snake-case column names
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as the snapshot
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new dataset
    ds_meadow.save()

    log.info("ai_robots.end")
