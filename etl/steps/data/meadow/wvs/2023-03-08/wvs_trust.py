"""Load a snapshot and create the World Values Survey - Trust meadow dataset."""

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
    log.info("wvs_trust.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("wvs_trust.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wvs_trust.end")
