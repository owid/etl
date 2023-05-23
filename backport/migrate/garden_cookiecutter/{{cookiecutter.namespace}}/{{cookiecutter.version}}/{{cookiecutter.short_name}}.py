"""Load snapshot and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.backport_helpers import long_to_wide
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data from snapshot.
    #
    snap = paths.load_snapshot_dependency()
    df = pd.read_feather(snap.path)

    # Convert long to wide format.
    df = long_to_wide(df)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
