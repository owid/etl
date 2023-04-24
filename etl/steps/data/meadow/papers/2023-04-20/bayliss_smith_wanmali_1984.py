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
    log.info("bayliss_smith_wanmali_1984.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("bayliss_smith_wanmali_1984.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Transpose the data for convenience.
    df = df.melt(id_vars=["country"], var_name="year", value_name="wheat_yield")

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name="long_term_wheat_yields", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("bayliss_smith_wanmali_1984.end")
