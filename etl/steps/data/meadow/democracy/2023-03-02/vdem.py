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
    log.info("vdem.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("vdem.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    df = df.rename(
        columns={
            "country_name": "country",
        }
    )

    # Drop rows with missing country names.
    df = df.dropna(subset=["country"])

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df.reset_index(drop=True), short_name=paths.short_name, underscore=True)

    # Default units
    for col in tb.columns:
        tb[col].metadata.unit = ""

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("vdem.end")
