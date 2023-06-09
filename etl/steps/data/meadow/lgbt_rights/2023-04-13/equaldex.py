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
    log.info("equaldex.start")

    #
    # Load inputs.

    # CURRENT DATASET
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("equaldex_current.csv")

    # Load data from snapshot.
    df_current = pd.read_csv(snap.path)

    #
    # Process data.

    # Rename year_extraction to year
    df_current = df_current.rename(columns={"year_extraction": "year"})

    # Set index as country, year and issue and verify that there are no duplicates
    df_current = df_current.set_index(["country", "year", "issue"], verify_integrity=False).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb_current = Table(df_current, short_name="equaldex_current", underscore=True)

    # COMPLETE DATASET
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("equaldex.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    # Set index as country, year and issue and verify that there are no duplicates
    df = df.set_index(["country", "year", "issue"], verify_integrity=False).sort_index()

    # Drop duplicates in the index
    # Equaldex collaborators are cleaning this duplicates, that sometimes provide different values for the same year
    # We are keeping the first value for each year
    df = df[~df.index.duplicated(keep="first")]

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name="equaldex", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb, tb_current], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("equaldex.end")
