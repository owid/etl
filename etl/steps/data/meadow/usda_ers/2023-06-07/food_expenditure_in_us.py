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
    log.info("food_expenditure_in_us.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = cast(Snapshot, paths.load_dependency("food_expenditure_in_us.xlsx"))

    # Load data from snapshots.
    df = pd.read_excel(snap.path, skiprows=1, header=[0, 1, 2])

    #
    # Process data.
    #
    # Combine multiline header.
    df.columns = ["year"] + [" - ".join(cols) for cols in df.columns[1:]]

    # Remove rows that contain footer notes.
    df = df[df["year"].astype(str).str.isdigit()].reset_index(drop=True)

    # Create an appropriate index and sort conveniently.
    df.set_index(["year"]).sort_index().sort_index(axis=1)

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
