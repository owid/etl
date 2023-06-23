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
    log.info("epoch.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("epoch.csv"))
    # Load data from snapshot.
    df = pd.read_csv(snap.path)
    # Select columns of interest.
    cols = [
        "System",
        "Domain",
        "Organization Categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (datapoints)",
        "Training time (hours)",
        "Equivalent training time (hours)",
        "Training hardware",
        "Compute Sponsor Categorization",
    ]

    df = df[cols]

    tb = Table(df, short_name=paths.short_name, underscore=True)
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Process data.
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("epoch.end")
