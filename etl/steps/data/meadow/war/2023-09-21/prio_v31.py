"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("prio_v31.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("prio_v31.xls")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = assign_dtypes(tb)
    # Create a new table and ensure all columns are snake-case.
    tb = tb.set_index(["id", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("prio_v31.end")


def assign_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "gwnoa",
        "gwnoa2nd",
        "gwnob",
        "gwnob2nd",
        "gwnoloc",
    ]
    for column in columns:
        df[column] = df[column].astype(str)
    return df
